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

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Result, stats::Precision};
use datafusion::datasource::TableProvider;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::common::collect;
use datafusion::prelude::{SessionConfig, col, lit};
use datafusion::scalar::ScalarValue;
use iceberg::Runtime;
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
    ManifestWriterBuilder, TableMetadata,
};
use iceberg::table::Table;
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_datafusion::test_utils::IcebergTestHarness;
use iceberg_datafusion::{
    IcebergCatalogTableProvider, IcebergPlanningConfig, IcebergStaticTableProvider,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};
use tempfile::TempDir;

fn eager_config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(4)
        .with_option_extension(IcebergPlanningConfig::default())
        .set_bool("iceberg_local.eager_file_planning", true)
}

fn iceberg_source(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
) -> &IcebergDataSource {
    plan.downcast_ref::<DataSourceExec>()
        .expect("the Iceberg provider creates a DataSourceExec")
        .data_source()
        .downcast_ref::<IcebergDataSource>()
        .expect("the plan contains the Iceberg data source")
}

fn copy_fixture(from: &Path, to: &Path) -> io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let destination = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_fixture(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn plans_file_tasks_before_execution_without_changing_statistics() -> Result<()> {
    let eager = IcebergTestHarness::with_config(eager_config()).await?;
    let lazy = IcebergTestHarness::new().await?;
    let eager_provider = eager.context().table_provider("taxi").await?;
    let lazy_provider = lazy.context().table_provider("taxi").await?;
    let eager_plan = eager_provider
        .scan(&eager.context().state(), None, &[], None)
        .await?;
    let lazy_plan = lazy_provider
        .scan(&lazy.context().state(), None, &[], None)
        .await?;
    let eager_source = iceberg_source(eager_plan.as_ref());
    let lazy_source = iceberg_source(lazy_plan.as_ref());

    assert!(lazy_source.planned_files().is_none());
    assert_eq!(lazy_plan.output_partitioning().partition_count(), 1);
    assert_eq!(eager_source.planned_files().unwrap().tasks().len(), 7);
    assert_eq!(eager_plan.output_partitioning().partition_count(), 4);
    assert!(eager_source.open(4, eager.context().task_ctx()).is_err());
    assert!(lazy_source.open(1, lazy.context().task_ctx()).is_err());
    assert_eq!(
        eager_source.partition_statistics(None)?.num_rows,
        Precision::Absent
    );
    assert_eq!(
        eager_source.partition_statistics(None)?.total_byte_size,
        lazy_source.partition_statistics(None)?.total_byte_size
    );

    let empty = eager_provider
        .scan(
            &eager.context().state(),
            None,
            &[col("pickup_date").eq(lit(ScalarValue::Date32(Some(0))))],
            None,
        )
        .await?;
    assert!(
        iceberg_source(empty.as_ref())
            .planned_files()
            .unwrap()
            .tasks()
            .is_empty()
    );
    assert_eq!(empty.output_partitioning().partition_count(), 1);

    Ok(())
}

#[tokio::test]
async fn eager_partition_count_is_bounded_by_selected_files() -> Result<()> {
    let eager =
        IcebergTestHarness::with_config(eager_config().with_target_partitions(16))
            .await?;
    let provider = eager.context().table_provider("taxi").await?;
    let plan = provider
        .scan(&eager.context().state(), None, &[], None)
        .await?;
    assert_eq!(plan.output_partitioning().partition_count(), 7);
    Ok(())
}

#[tokio::test]
async fn filtered_and_empty_eager_scans_match_lazy_results() -> Result<()> {
    let eager = IcebergTestHarness::with_config(eager_config()).await?;
    let lazy = IcebergTestHarness::new().await?;
    for (query, planned_files) in [
        (
            "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
             WHERE pickup_date = DATE '2024-01-10' \
             GROUP BY pickup_date ORDER BY pickup_date",
            1,
        ),
        (
            "SELECT COUNT(*) AS trips FROM taxi \
             WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 50",
            1,
        ),
        (
            "SELECT COUNT(*) AS trips FROM taxi \
             WHERE pickup_date = DATE '2025-01-01'",
            0,
        ),
    ] {
        let (plan, eager_batches) = eager.query(query).await?;
        let (_, lazy_batches) = lazy.query(query).await?;
        assert!(
            plan.contains(&format!("planned_files={planned_files}")),
            "eager scan not visible in plan: {plan}"
        );
        assert_eq!(eager_batches, lazy_batches);
    }
    Ok(())
}

#[tokio::test]
async fn eager_multi_partition_limit_is_global() -> Result<()> {
    let eager = IcebergTestHarness::with_config(eager_config()).await?;
    let (plan, batches) = eager.query("SELECT pickup_date FROM taxi LIMIT 3").await?;
    assert!(plan.contains("planned_files=7"), "unexpected plan: {plan}");
    assert_eq!(batches.matches("2024-01-").count(), 3, "{batches}");
    Ok(())
}

#[tokio::test]
async fn eager_ordered_limit_matches_lazy_results() -> Result<()> {
    let eager = IcebergTestHarness::with_config(eager_config()).await?;
    let lazy = IcebergTestHarness::new().await?;
    let sql = "SELECT vendor_id, pickup_at FROM taxi \
               ORDER BY pickup_at, vendor_id, pickup_location_id LIMIT 3";
    let (plan, eager_batches) = eager.query(sql).await?;
    let (_, lazy_batches) = lazy.query(sql).await?;
    assert!(plan.contains("planned_files=7"), "unexpected plan: {plan}");
    assert_eq!(eager_batches, lazy_batches);
    Ok(())
}

#[tokio::test]
async fn eager_and_lazy_scans_apply_position_deletes() -> StdResult<(), Box<dyn Error>> {
    // Isolate the checked-in fixture: the test writes a new delete manifest and
    // snapshot, but must not modify the files used by other tests.
    let temporary = TempDir::new()?;
    let root = temporary.path().join("iceberg");
    copy_fixture(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/iceberg"),
        &root,
    )?;
    let eager = IcebergTestHarness::with_fixture_root(eager_config(), root).await?;
    let provider = eager.context().table_provider("taxi").await?;
    let catalog_provider = provider
        .downcast_ref::<IcebergCatalogTableProvider>()
        .expect("the fixture creates a catalog Iceberg provider");
    let table = catalog_provider
        .catalog()
        .load_table(catalog_provider.table_ident())
        .await?;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("fixture has a snapshot");
    let manifest_list = table.manifest_list_reader(snapshot).load().await?;
    let data_manifest = table
        .manifest_reader()
        .read(&manifest_list.entries()[0])
        .await?;
    let data_file = &data_manifest.entries()[0].data_file;

    let delete_path = "s3://iceberg-test/warehouse/taxi/data/pos-delete.parquet";
    let schema = Arc::new(Schema::new(vec![
        Field::new(RESERVED_COL_NAME_DELETE_FILE_PATH, DataType::Utf8, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_DELETE_FILE_PATH.to_string(),
            )])),
        Field::new(RESERVED_COL_NAME_DELETE_FILE_POS, DataType::Int64, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_DELETE_FILE_POS.to_string(),
            )])),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![data_file.file_path()])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ],
    )?;
    let delete_file_on_disk = temporary
        .path()
        .join("iceberg/taxi/data/pos-delete.parquet");
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&delete_file_on_disk)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let delete_size = fs::metadata(&delete_file_on_disk)?.len();

    let next_snapshot_id = snapshot.snapshot_id() + 1;
    let mut delete_manifest = ManifestWriterBuilder::new(
        table
            .file_io()
            .new_output("s3://iceberg-test/warehouse/taxi/metadata/pos-delete.avro")?,
        Some(next_snapshot_id),
        snapshot.schema(table.metadata())?,
        table.metadata().default_partition_spec().as_ref().clone(),
    )
    .build_v2_deletes();
    delete_manifest.add_file(
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(delete_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(delete_size)
            .record_count(1)
            .partition(data_file.partition().clone())
            .build()?,
        -1,
    )?;
    let delete_manifest = delete_manifest.write_manifest_file().await?;
    let manifest_list_path =
        "s3://iceberg-test/warehouse/taxi/metadata/snap-with-delete.avro";
    let mut writer = ManifestListWriter::v2(
        table
            .file_io()
            .new_output(manifest_list_path)?
            .writer()
            .await?,
        next_snapshot_id,
        Some(snapshot.snapshot_id()),
        snapshot.sequence_number() + 1,
    );
    writer.add_manifests(
        manifest_list
            .entries()
            .iter()
            .cloned()
            .chain([delete_manifest]),
    )?;
    writer.close().await?;

    let mut metadata: Value = serde_json::from_slice(&fs::read(
        temporary
            .path()
            .join("iceberg/taxi/metadata/v1.metadata.json"),
    )?)?;
    let mut new_snapshot = metadata["snapshots"][0].clone();
    new_snapshot["snapshot-id"] = json!(next_snapshot_id);
    new_snapshot["parent-snapshot-id"] = json!(snapshot.snapshot_id());
    new_snapshot["sequence-number"] = json!(snapshot.sequence_number() + 1);
    new_snapshot["manifest-list"] = json!(manifest_list_path);
    new_snapshot["timestamp-ms"] = json!(snapshot.timestamp_ms() + 1);
    new_snapshot["summary"]["total-delete-files"] = json!("1");
    new_snapshot["summary"]["total-position-deletes"] = json!("1");
    metadata["current-snapshot-id"] = json!(next_snapshot_id);
    metadata["last-sequence-number"] = json!(snapshot.sequence_number() + 1);
    metadata["last-updated-ms"] = json!(snapshot.timestamp_ms() + 1);
    metadata["refs"]["main"]["snapshot-id"] = json!(next_snapshot_id);
    metadata["snapshot-log"]
        .as_array_mut()
        .expect("fixture has a snapshot log")
        .push(json!({
            "snapshot-id": next_snapshot_id,
            "timestamp-ms": snapshot.timestamp_ms() + 1,
        }));
    metadata["snapshots"]
        .as_array_mut()
        .expect("fixture has snapshots")
        .push(new_snapshot);
    let table = Table::builder()
        .file_io(table.file_io().clone())
        .identifier(table.identifier().clone())
        .metadata(serde_json::from_value::<TableMetadata>(metadata)?)
        .runtime(Runtime::try_current()?)
        .build()?;
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    let lazy_state = IcebergTestHarness::with_config(SessionConfig::new()).await?;

    let state = eager.context().state();
    let eager_plan = provider.scan(&state, None, &[], None).await?;
    assert_eq!(
        iceberg_source(eager_plan.as_ref())
            .planned_files()
            .expect("eager plan has files")
            .tasks()
            .iter()
            .filter(|task| !task.deletes().is_empty())
            .count(),
        1
    );
    assert_eq!(
        iceberg_source(eager_plan.as_ref())
            .planned_files()
            .expect("eager plan has files")
            .tasks()
            .len(),
        7
    );
    let lazy_plan = provider
        .scan(&lazy_state.context().state(), None, &[], None)
        .await?;
    let mut eager_rows = 0;
    for partition in 0..eager_plan.output_partitioning().partition_count() {
        for batch in collect(eager_plan.execute(partition, state.task_ctx())?).await? {
            eager_rows += batch.num_rows();
        }
    }
    let mut lazy_rows = 0;
    for batch in collect(lazy_plan.execute(0, lazy_state.context().task_ctx())?).await? {
        lazy_rows += batch.num_rows();
    }
    assert_eq!(lazy_rows, 174_999);
    assert_eq!(eager_rows, lazy_rows);
    Ok(())
}

#[test]
fn eager_scan_uses_the_tables_custom_runtime() -> Result<()> {
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (harness, provider) = io_runtime.block_on(async {
        let harness = IcebergTestHarness::with_config(eager_config()).await?;
        let provider = harness.context().table_provider("taxi").await?;
        let catalog_provider = provider
            .downcast_ref::<IcebergCatalogTableProvider>()
            .expect("the fixture creates a catalog Iceberg provider");
        let table = catalog_provider
            .catalog()
            .load_table(catalog_provider.table_ident())
            .await
            .map_err(iceberg_datafusion::to_datafusion_error)?;
        let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
        Ok::<_, datafusion::error::DataFusionError>((harness, provider))
    })?;
    // The table retains its own runtime while planning is called from another runtime.
    let planning_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    planning_runtime.block_on(async {
        let state = harness.context().state();
        let plan = provider.scan(&state, None, &[], None).await?;
        assert_eq!(
            iceberg_source(plan.as_ref())
                .planned_files()
                .unwrap()
                .tasks()
                .len(),
            7
        );
        let mut rows = 0;
        for partition in 0..plan.output_partitioning().partition_count() {
            let batches = collect(plan.execute(partition, state.task_ctx())?).await?;
            rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, 175_000);
        Ok(())
    })
}

#[tokio::test]
async fn eager_tasks_respect_the_query_memory_pool() -> Result<()> {
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1)))
            .build()?,
    );
    let eager =
        IcebergTestHarness::with_config_and_runtime(eager_config(), runtime.clone())
            .await?;
    let provider = eager.context().table_provider("taxi").await?;
    let error = provider
        .scan(&eager.context().state(), None, &[], None)
        .await
        .expect_err("file metadata cannot fit in one byte");
    assert!(
        error.to_string().contains("Iceberg file planning"),
        "{error}"
    );
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn retained_tasks_follow_plan_and_stream_lifetimes() -> Result<()> {
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024 * 1024)))
            .build()?,
    );
    let harness =
        IcebergTestHarness::with_config_and_runtime(eager_config(), runtime.clone())
            .await?;
    let provider = harness.context().table_provider("taxi").await?;
    let plan = provider
        .scan(&harness.context().state(), None, &[], None)
        .await?;
    let reserved = runtime.memory_pool.reserved();
    assert!(reserved > 0);
    let source = iceberg_source(plan.as_ref()).clone();
    drop(plan);
    assert_eq!(runtime.memory_pool.reserved(), reserved);
    let stream = source.open(0, harness.context().task_ctx())?;
    drop(source);
    assert_eq!(runtime.memory_pool.reserved(), reserved);
    drop(stream);
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}
