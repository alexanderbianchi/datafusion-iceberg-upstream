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

//! Read execution selected from the retained files; independent of file discovery.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use datafusion::common::Result;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::common::spawn_buffered;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReader, schema_to_arrow_schema};
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{
    NullOrder, PrimitiveType, Schema, SortDirection, SortOrder, Transform, Type,
};

use crate::to_datafusion_error;

/// How a scan reads the files assigned to each output partition.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReadStrategy {
    /// Read files without promising an output order.
    Unordered,
    /// Merge individually sorted files into the validated per-partition order.
    OrderedMerge {
        /// The order of the projected output columns.
        ordering: LexOrdering,
    },
}

impl ReadStrategy {
    pub(crate) fn for_files(tasks: &[FileScanTask], schema: &ArrowSchema) -> Self {
        match shared_ordering(tasks, schema) {
            Some(ordering) => Self::OrderedMerge { ordering },
            None => Self::Unordered,
        }
    }

    /// Ordering guaranteed within each output partition, if any.
    pub fn ordering(&self) -> Option<&LexOrdering> {
        match self {
            Self::Unordered => None,
            Self::OrderedMerge { ordering } => Some(ordering),
        }
    }

    pub(crate) fn read(
        &self,
        reader: ArrowReader,
        tasks: FileScanTaskStream,
        schema: SchemaRef,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match self {
            Self::Unordered => read_files(reader, tasks, schema),
            Self::OrderedMerge { ordering } => {
                let ordering = ordering.clone();
                // The outer limit stream records source output, including early stops.
                let metrics = ExecutionPlanMetricsSet::new();
                let output_schema = schema.clone();
                let stream = futures::stream::once(async move {
                    let streams = tasks
                        .map(|task| {
                            read_file(
                                reader.clone(),
                                task.map_err(to_datafusion_error)?,
                                schema.clone(),
                            )
                        })
                        .try_collect::<Vec<_>>()
                        .await?;
                    let reservation = MemoryConsumer::new(format!(
                        "Iceberg ordered merge[{partition}]"
                    ))
                    .register(context.memory_pool());
                    StreamingMergeBuilder::new()
                        .with_streams(
                            streams.into_iter().map(|s| spawn_buffered(s, 1)).collect(),
                        )
                        .with_schema(schema)
                        .with_expressions(&ordering)
                        .with_metrics(BaselineMetrics::new(&metrics, partition))
                        .with_batch_size(context.session_config().batch_size())
                        .with_reservation(reservation)
                        .build()
                })
                .try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    output_schema,
                    stream,
                )))
            }
        }
    }
}

fn read_file(
    reader: ArrowReader,
    task: FileScanTask,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    read_files(reader, futures::stream::iter([Ok(task)]).boxed(), schema)
}

fn read_files(
    reader: ArrowReader,
    tasks: FileScanTaskStream,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let stream = reader
        .read(tasks)
        .map_err(to_datafusion_error)?
        .stream()
        .map_err(to_datafusion_error);
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

/// The order of the columns of `output` that every one of `tasks` is sorted
/// in, or `None` if they record different sort orders, one records none, or
/// the schema they are read with does not match `output`.
fn shared_ordering(tasks: &[FileScanTask], output: &ArrowSchema) -> Option<LexOrdering> {
    let first = tasks.first()?;
    let order = first.sort_order()?;
    if !tasks.iter().all(|task| task.sort_order() == Some(order)) {
        return None;
    }
    if !tasks
        .iter()
        .all(|task| reads_as_declared(task.schema(), output))
    {
        return None;
    }
    lex_ordering(order, first.schema(), output)
}

/// Whether reading `schema`, the snapshot schema the tasks are read with,
/// yields every column of `output` with the type it declares, and no nulls
/// where it declares none. A provider's schema can be older than the
/// snapshot's, and DataFusion ignores the null order of a non-nullable
/// column, while the merge requires its input to match `output`.
fn reads_as_declared(schema: &Schema, output: &ArrowSchema) -> bool {
    let Ok(read) = schema_to_arrow_schema(schema) else {
        return false;
    };
    output.fields().iter().all(|field| {
        read.field_with_name(field.name()).is_ok_and(|read| {
            read.data_type() == field.data_type()
                && (field.is_nullable() || !read.is_nullable())
        })
    })
}

/// The longest leading part of `order` that DataFusion can merge and report as
/// an order of the columns of `output`, or `None` if that is empty. `schema`
/// is the Iceberg schema the scan reads.
fn lex_ordering(
    order: &SortOrder,
    schema: &Schema,
    output: &ArrowSchema,
) -> Option<LexOrdering> {
    let sort_exprs = order.fields.iter().map_while(|field| {
        if field.transform != Transform::Identity {
            return None;
        }
        let source = schema.as_struct().field_by_id(field.source_id)?;
        if !orders_like_writers(&source.field_type) {
            return None;
        }
        let index = output.index_of(&source.name).ok()?;
        Some(PhysicalSortExpr::new(
            Arc::new(Column::new(&source.name, index)),
            SortOptions {
                descending: field.direction == SortDirection::Descending,
                nulls_first: field.null_order == NullOrder::First,
            },
        ))
    });
    LexOrdering::new(sort_exprs)
}

/// Whether DataFusion orders values of `field_type` as the engines that write
/// sorted Iceberg files do. For floating point, DataFusion uses the order the
/// spec defines (<https://iceberg.apache.org/spec/#sort-orders>), with -0.0
/// before 0.0 and negative NaN first. Spark treats the zeros as equal and
/// every NaN as the largest value, so files it writes may not be sorted in the
/// spec's order. The spec leaves the order of UUIDs undefined
/// (apache/iceberg#14216).
fn orders_like_writers(field_type: &Type) -> bool {
    matches!(
        field_type,
        Type::Primitive(
            PrimitiveType::Boolean
                | PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::Decimal { .. }
                | PrimitiveType::Date
                | PrimitiveType::Time
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
                | PrimitiveType::String
                | PrimitiveType::Fixed(_)
                | PrimitiveType::Binary
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, SortField};

    #[test]
    fn reports_only_a_safe_leading_order() {
        for (kind, transform, supported) in [
            (PrimitiveType::Int, Transform::Identity, true),
            (PrimitiveType::String, Transform::Identity, true),
            (PrimitiveType::Int, Transform::Bucket(4), false),
            (PrimitiveType::Float, Transform::Identity, false),
            (PrimitiveType::Double, Transform::Identity, false),
            (PrimitiveType::Uuid, Transform::Identity, false),
        ] {
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "first",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::optional(2, "second", Type::Primitive(kind)).into(),
                ])
                .build()
                .unwrap();
            let field = |id, transform| {
                SortField::builder()
                    .source_id(id)
                    .transform(transform)
                    .direction(SortDirection::Descending)
                    .null_order(NullOrder::Last)
                    .build()
            };
            let order = SortOrder::builder()
                .with_order_id(1)
                .with_sort_field(field(1, Transform::Identity))
                .with_sort_field(field(2, transform))
                .build(&schema)
                .unwrap();
            let arrow = schema_to_arrow_schema(&schema).unwrap();
            let ordering = lex_ordering(&order, &schema, &arrow).unwrap();
            assert_eq!(ordering.len(), if supported { 2 } else { 1 });
            assert!(ordering[0].options.descending);
            assert!(!ordering[0].options.nulls_first);
            // Projecting away the leading key cannot expose the second key as sorted.
            assert!(
                lex_ordering(&order, &schema, &arrow.project(&[1]).unwrap()).is_none()
            );
        }
    }
}
