// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Statistics, internal_err, plan_err};
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use iceberg::arrow::ArrowReader;
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream, TableScan};
use iceberg::table::Table;

use super::file_planning::PlannedFileScan;
use super::predicate::convert_filters_to_predicate;
use super::read_strategy::{ReadStrategy, read_files, read_ordered};
use crate::{IcebergPlanningConfig, to_datafusion_error};

// A reader is created once per physical scan. Its clones share the delete loader's
// cache while each read() call still owns its own scan metrics.
#[derive(Clone)]
struct ScanReader(ArrowReader);

/// Reusable planned tasks, partition assignment, and their validated read strategy.
/// Keep this value when rebuilding a source whose parent plan relies on its properties.
#[derive(Debug, Clone)]
pub struct PlannedScan {
    files: Arc<PlannedFileScan>,
    partitions: usize,
    strategy: ReadStrategy,
}

impl PlannedScan {
    fn task_indices(&self, partition: usize) -> impl Iterator<Item = usize> + use<> {
        (partition..self.files.tasks().len()).step_by(self.partitions)
    }
}

impl std::fmt::Debug for ScanReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScanReader")
    }
}

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug, Clone)]
pub struct IcebergDataSource {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Schema returned by this data source after projection.
    schema: ArrowSchemaRef,
    /// Projection column names in the output schema.
    projection: Vec<String>,
    /// Filter to apply to the table scan
    predicate: Option<Predicate>,
    /// Optional limit on the number of rows to return
    fetch: Option<usize>,
    planned: Option<PlannedScan>,
    reader: ScanReader,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergDataSource {
    /// Starts building a source that scans `table` and returns the columns of
    /// `schema`, all of them unless [`IcebergDataSourceBuilder::with_projection`]
    /// selects some. Columns are read from Iceberg by name.
    pub fn builder(table: Table, schema: ArrowSchemaRef) -> IcebergDataSourceBuilder {
        IcebergDataSourceBuilder {
            table,
            schema,
            projection: None,
            snapshot_id: None,
            predicate: None,
            fetch: None,
        }
    }

    /// Planned, ungrouped tasks, if file discovery was enabled for this scan.
    /// A distributed consumer may assign these tasks to its own workers instead
    /// of using the local output partitions of this source.
    pub fn planned_files(&self) -> Option<&PlannedFileScan> {
        self.planned.as_ref().map(|planned| planned.files.as_ref())
    }

    /// Retained tasks assigned to a local output partition, in planning order.
    /// Returns `None` for lazy scans, whose tasks are discovered during execution.
    ///
    /// This performs no I/O and does not clone tasks. Readers that outlive this
    /// source must retain a source clone to keep the planning memory reservation.
    ///
    /// # Errors
    ///
    /// Returns an error if `partition` is outside this source's output partitions.
    pub fn planned_partition_tasks(
        &self,
        partition: usize,
    ) -> Result<Option<impl Iterator<Item = &FileScanTask>>> {
        self.validate_partition(partition)?;
        Ok(self.planned.as_ref().map(|planned| {
            planned
                .task_indices(partition)
                .map(|index| &planned.files.tasks()[index])
        }))
    }

    fn validate_partition(&self, partition: usize) -> Result<()> {
        let partitions = self
            .planned
            .as_ref()
            .map_or(1, |planned| planned.partitions);
        if partition >= partitions {
            return internal_err!("Iceberg scan partition {partition} does not exist");
        }
        Ok(())
    }

    /// Retained execution state, including the read strategy and partition assignment.
    /// Pass a clone to [`IcebergDataSourceBuilder::with_planned_scan`] when rebuilding.
    pub fn planned_scan(&self) -> Option<&PlannedScan> {
        self.planned.as_ref()
    }

    /// The read strategy selected during planning; lazy scans read unordered.
    pub fn read_strategy(&self) -> &ReadStrategy {
        self.planned
            .as_ref()
            .map_or(&ReadStrategy::Unordered, |p| &p.strategy)
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    pub fn projection(&self) -> &[String] {
        &self.projection
    }

    pub fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

/// Builds an [`IcebergDataSource`]. Created by [`IcebergDataSource::builder`].
///
/// A table provider passes the table's schema and selects columns with
/// [`Self::with_projection`]. Finish with [`Self::with_planned_files`] to honor
/// the session's eager-planning option before building.
///
/// To rebuild a source, such as in another process,
/// pass its [`IcebergDataSource::schema`], which is already projected, and no
/// projection. This rebuilds a lazy single-partition source; planned file tasks
/// must be transferred separately when reconstructing a distributed scan. For a
/// local rebuild, pass [`IcebergDataSource::planned_scan`] to
/// [`Self::with_planned_scan`] to preserve partitioning and ordering.
#[derive(Debug, Clone)]
pub struct IcebergDataSourceBuilder {
    table: Table,
    schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    snapshot_id: Option<i64>,
    predicate: Option<Predicate>,
    fetch: Option<usize>,
}

impl IcebergDataSourceBuilder {
    /// Returns only the columns of the schema at these indices, or all of them
    /// if `None` (the default). Indices are into the schema passed to
    /// [`IcebergDataSource::builder`].
    pub fn with_projection(mut self, projection: Option<&Vec<usize>>) -> Self {
        self.projection = projection.cloned();
        self
    }

    /// Scans this snapshot, or the table's current one if `None` (the default).
    pub fn with_snapshot_id(mut self, snapshot_id: Option<i64>) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }

    /// Filters the scan with `predicate`, or not at all if `None` (the default).
    pub fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Filters the scan with the parts of `filters` that convert to an Iceberg
    /// predicate. The rest are dropped, so the scan may return rows that
    /// `filters` reject.
    pub fn with_filters(self, filters: &[Expr]) -> Self {
        self.with_predicate(convert_filters_to_predicate(filters))
    }

    /// Stops after `fetch` rows, or reads every row if `None` (the default).
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Finish configuring the scan and optionally select its files before execution.
    ///
    /// Honors eager file planning and ordering preservation options. The returned builder
    /// only exposes `build`, so filters, projection, and snapshot cannot change
    /// after files have been selected.
    ///
    /// # Errors
    ///
    /// Returns an error if the projection is invalid, file planning fails, or
    /// the retained tasks exceed the query memory pool.
    pub async fn with_planned_files(
        self,
        context: Arc<TaskContext>,
    ) -> Result<PlannedIcebergDataSourceBuilder> {
        let config = context
            .session_config()
            .options()
            .extensions
            .get::<IcebergPlanningConfig>()
            .cloned()
            .unwrap_or_default();
        if !config.eager_file_planning && !config.preserve_data_ordering {
            return Ok(PlannedIcebergDataSourceBuilder {
                builder: self,
                planned: None,
            });
        }
        let target_partitions = context.session_config().target_partitions();
        let schema = self.projected_schema()?;
        let projection: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let scan = build_table_scan(
            &self.table,
            self.snapshot_id,
            Some(&projection),
            self.predicate.as_ref(),
        )?;
        // Dropping physical planning drops collection and its memory reservation.
        let planned = PlannedFileScan::collect(scan, &context).await?;
        let partitions = planned.tasks().len().max(1).min(target_partitions.max(1));

        let strategy = if config.preserve_data_ordering
            && planned.tasks().len().div_ceil(partitions)
                <= config.max_merge_files_per_partition
        {
            ReadStrategy::for_files(planned.tasks(), &schema)
        } else {
            ReadStrategy::Unordered
        };

        Ok(PlannedIcebergDataSourceBuilder {
            builder: self,
            planned: Some(PlannedScan {
                files: Arc::new(planned),
                partitions,
                strategy,
            }),
        })
    }

    /// Restore the tasks, partition assignment, and strategy of an existing source.
    /// The snapshot, predicate, and output schema must describe the same scan.
    /// An optimized parent plan may rely on these partitions and their ordering.
    pub fn with_planned_scan(
        self,
        planned: Option<PlannedScan>,
    ) -> PlannedIcebergDataSourceBuilder {
        PlannedIcebergDataSourceBuilder {
            builder: self,
            planned,
        }
    }

    /// Builds the source, which reads the columns of its projected schema by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the projection is out of bounds for the schema.
    pub fn build(self) -> Result<IcebergDataSource> {
        self.build_with_planned_files(None)
    }

    fn projected_schema(&self) -> Result<ArrowSchemaRef> {
        match &self.projection {
            None => Ok(self.schema.clone()),
            Some(projection) => Ok(Arc::new(self.schema.project(projection)?)),
        }
    }

    fn build_with_planned_files(
        self,
        planned: Option<PlannedScan>,
    ) -> Result<IcebergDataSource> {
        let schema = self.projected_schema()?;
        let projection = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        if let Some(planned) = &planned
            && planned.strategy.ordering().is_some()
            && ReadStrategy::for_files(planned.files.tasks(), &schema).ordering()
                != planned.strategy.ordering()
        {
            return plan_err!(
                "Rebuilt Iceberg scan does not preserve its planned ordering"
            );
        }
        let mut reader = self.table.reader_builder();
        if let Some(planned) = &planned
            && planned.partitions > 1
            && planned.strategy.ordering().is_none()
        {
            reader = reader.with_data_file_concurrency_limit(1);
        }
        let reader = ScanReader(reader.build());
        Ok(IcebergDataSource {
            table: self.table,
            snapshot_id: self.snapshot_id,
            schema,
            projection,
            predicate: self.predicate,
            fetch: self.fetch,
            planned,
            reader,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// A configured scan ready to build after optional eager file planning.
///
/// Created by [`IcebergDataSourceBuilder::with_planned_files`]. Configuration
/// is fixed so the retained tasks stay consistent with the resulting source.
#[derive(Debug, Clone)]
pub struct PlannedIcebergDataSourceBuilder {
    builder: IcebergDataSourceBuilder,
    planned: Option<PlannedScan>,
}

impl PlannedIcebergDataSourceBuilder {
    /// Construct the source and its shared reader from the configured scan.
    ///
    /// # Errors
    ///
    /// Returns an error if the projection is out of bounds or restored tasks
    /// cannot preserve their ordering with the requested schema.
    pub fn build(self) -> Result<IcebergDataSource> {
        self.builder.build_with_planned_files(self.planned)
    }
}

impl DataSource for IcebergDataSource {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.validate_partition(partition)?;
        let stream = match self.read_strategy() {
            ReadStrategy::OrderedMerge { ordering } => {
                let Some(tasks) = self.planned_partition_tasks(partition)? else {
                    return internal_err!("Ordered Iceberg scan requires planned tasks");
                };
                read_ordered(
                    self.reader.0.clone(),
                    tasks,
                    self.schema.clone(),
                    ordering,
                    partition,
                    &context,
                )?
            }
            ReadStrategy::Unordered => {
                let tasks: FileScanTaskStream = match &self.planned {
                    Some(planned) => {
                        // The reader owns each task; cloning keeps the physical plan reusable.
                        let files = Arc::clone(&planned.files);
                        let tasks = planned
                            .task_indices(partition)
                            .map(move |index| Ok(files.tasks()[index].clone()));
                        Box::pin(futures::stream::iter(tasks))
                    }
                    None => {
                        let scan = build_table_scan(
                            &self.table,
                            self.snapshot_id,
                            Some(&self.projection),
                            self.predicate.as_ref(),
                        )?;
                        Box::pin(
                            futures::stream::once(async move { scan.plan_files().await })
                                .try_flatten(),
                        )
                    }
                };
                read_files(self.reader.0.clone(), tasks, self.schema.clone())?
            }
        };
        Ok(Box::pin(LimitStream::new(
            stream,
            0,
            self.fetch,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "format=iceberg",)?;
        write!(f, ", projection=[{}]", self.projection.join(", "))?;
        if let Some(predicate) = &self.predicate {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        if let Some(planned) = self.planned_files() {
            write!(f, ", planned_files={}", planned.tasks().len())?;
        }
        if let Some(ordering) = self.read_strategy().ordering() {
            write!(f, ", output_ordering=[{ordering}]")?;
        }
        Ok(())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(
            self.planned
                .as_ref()
                .map_or(1, |planned| planned.partitions),
        )
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(
            self.schema.clone(),
            self.read_strategy().ordering().cloned(),
        )
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut source = self.clone();
        source.fetch = fetch;
        Some(Arc::new(source))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
}

fn build_table_scan(
    table: &Table,
    snapshot_id: Option<i64>,
    column_names: Option<&[String]>,
    predicate: Option<&Predicate>,
) -> Result<TableScan> {
    let builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };
    let mut builder = match column_names {
        Some(columns) => builder.select(columns),
        None => builder.select_all(),
    };
    if let Some(predicate) = predicate {
        builder = builder.with_filter(predicate.clone());
    }
    builder.build().map_err(to_datafusion_error)
}
