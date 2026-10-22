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

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::TableProvider;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect as collect_plan,
};
use datafusion::prelude::{SessionConfig, col, lit};
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_datafusion::test_utils::IcebergTestHarness;
use iceberg_datafusion::{IcebergPlanningConfig, IcebergStaticTableProvider};
use tempfile::TempDir;

#[tokio::test]
async fn merges_only_assigned_files_and_survives_reconstruction() -> Result<()> {
    let dir = TempDir::new()?;
    let harness = IcebergTestHarness::with_config(config(2)).await?;
    // Four overlapping files, two output partitions, and multiple batches per file.
    let files: Vec<Vec<Option<i32>>> = (0..4)
        .map(|offset| (0..3000).map(|i| Some(i * 4 + offset)).collect())
        .collect();
    let inputs: Vec<_> = files.iter().map(|f| (Some(1), f.as_slice())).collect();
    harness.register_sorted_files(dir.path(), &inputs).await?;
    let provider = harness
        .context()
        .table_provider("sorted.public.events")
        .await?;
    let plan = provider
        .scan(&harness.context().state(), None, &[], None)
        .await?;
    let source = iceberg_source(plan.as_ref());
    assert_eq!(plan.output_partitioning().partition_count(), 2);
    assert!(source.read_strategy().ordering().is_some());
    let rebuilt = IcebergDataSource::builder(source.table().clone(), source.schema())
        .with_snapshot_id(source.snapshot_id())
        .with_predicate(source.predicate().cloned())
        .with_fetch(source.fetch())
        .with_planned_scan(source.planned_scan().cloned())
        .build()?;
    let mut all = Vec::new();
    for partition in 0..2 {
        let batches =
            collect(source.open(partition, harness.context().task_ctx())?).await?;
        assert!(batches.len() > 1);
        let actual = ids(&batches);
        assert!(actual.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(actual.len(), 6000);
        assert_eq!(
            actual,
            ids(&collect(rebuilt.open(partition, harness.context().task_ctx())?).await?)
        );
        all.extend(actual);
    }
    all.sort();
    assert_eq!(all, (0..12000).map(Some).collect::<Vec<_>>());
    // Restoring an ordering against a different declared schema is rejected.
    let stale = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    assert!(
        IcebergDataSource::builder(source.table().clone(), stale)
            .with_planned_scan(source.planned_scan().cloned())
            .build()
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn optimizer_uses_only_compatible_ordering_and_keeps_global_limit() -> Result<()> {
    let dir = TempDir::new()?;
    let harness = IcebergTestHarness::with_config(config(2)).await?;
    harness
        .register_sorted_files(
            dir.path(),
            &[
                (Some(1), &[None, Some(1), Some(5)]),
                (Some(1), &[Some(2), Some(6)]),
                (Some(1), &[Some(3), Some(7)]),
                (Some(1), &[Some(4), Some(8)]),
            ],
        )
        .await?;
    for (order, needs_sort) in [
        ("ASC NULLS FIRST", false),
        ("ASC NULLS LAST", true),
        ("DESC", true),
    ] {
        let sql =
            format!("SELECT id FROM sorted.public.events ORDER BY id {order} LIMIT 3");
        let (plan, actual) = harness.query(&sql).await?;
        assert_eq!(plan.contains("SortExec:"), needs_sort, "{plan}");
        harness
            .context()
            .sql("SET iceberg_local.preserve_data_ordering = false")
            .await?
            .collect()
            .await?;
        let (_, expected) = harness.query(&sql).await?;
        assert_eq!(actual, expected);
        harness
            .context()
            .sql("SET iceberg_local.preserve_data_ordering = true")
            .await?
            .collect()
            .await?;
    }
    // Preserve the already-optimized parents: they may have removed their SortExec.
    let plan = harness
        .context()
        .sql("SELECT id FROM sorted.public.events ORDER BY id ASC NULLS FIRST LIMIT 5")
        .await?
        .create_physical_plan()
        .await?;
    let expected = collect_plan(plan.clone(), harness.context().task_ctx()).await?;
    let rebuilt = plan
        .transform_up(|plan| {
            let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(source) = exec.data_source().downcast_ref::<IcebergDataSource>()
            else {
                return Ok(Transformed::no(plan));
            };
            let source =
                IcebergDataSource::builder(source.table().clone(), source.schema())
                    .with_snapshot_id(source.snapshot_id())
                    .with_predicate(source.predicate().cloned())
                    .with_fetch(source.fetch())
                    .with_planned_scan(source.planned_scan().cloned())
                    .build()?;
            Ok(Transformed::yes(
                Arc::new(DataSourceExec::new(Arc::new(source))) as Arc<dyn ExecutionPlan>,
            ))
        })?
        .data;
    assert_eq!(
        ids(&collect_plan(rebuilt, harness.context().task_ctx()).await?),
        ids(&expected)
    );
    Ok(())
}

#[tokio::test]
async fn unsuitable_files_and_merge_cap_reuse_the_eager_plan() -> Result<()> {
    let dir = TempDir::new()?;
    let harness = IcebergTestHarness::with_config(config(1)).await?;
    harness
        .register_sorted_files(
            dir.path(),
            &[(Some(1), &[Some(1), Some(3)]), (None, &[Some(2), Some(4)])],
        )
        .await?;
    let provider = harness
        .context()
        .table_provider("sorted.public.events")
        .await?;
    let plan = provider
        .scan(&harness.context().state(), None, &[], None)
        .await?;
    assert!(
        iceberg_source(plan.as_ref())
            .read_strategy()
            .ordering()
            .is_none()
    );
    assert_eq!(
        iceberg_source(plan.as_ref())
            .planned_files()
            .unwrap()
            .tasks()
            .len(),
        2
    );

    let dir = TempDir::new()?;
    harness
        .register_sorted_files(
            dir.path(),
            &[(Some(1), &[Some(1)]), (Some(1), &[Some(2)])],
        )
        .await?;
    harness
        .context()
        .sql("SET iceberg_local.max_merge_files_per_partition = 1")
        .await?
        .collect()
        .await?;
    let provider = harness
        .context()
        .table_provider("sorted.public.events")
        .await?;
    let plan = provider
        .scan(&harness.context().state(), None, &[], None)
        .await?;
    assert!(
        iceberg_source(plan.as_ref())
            .read_strategy()
            .ordering()
            .is_none()
    );
    assert_eq!(
        iceberg_source(plan.as_ref())
            .planned_files()
            .unwrap()
            .tasks()
            .len(),
        2
    );
    assert_eq!(
        ids(&collect(plan.execute(0, harness.context().task_ctx())?).await?).len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn single_file_empty_and_stale_schema_are_safe() -> Result<()> {
    let dir = TempDir::new()?;
    let harness = IcebergTestHarness::with_config(config(4)).await?;
    let table = harness
        .register_sorted_files(dir.path(), &[(Some(1), &[None, Some(1), Some(2)])])
        .await?;
    let provider = harness
        .context()
        .table_provider("sorted.public.events")
        .await?;
    let state = harness.context().state();
    let plan = provider.scan(&state, None, &[], None).await?;
    assert!(
        iceberg_source(plan.as_ref())
            .read_strategy()
            .ordering()
            .is_some()
    );
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    assert_eq!(
        ids(&collect(plan.execute(0, state.task_ctx())?).await?),
        [None, Some(1), Some(2)]
    );
    let empty = provider
        .scan(&state, None, &[col("id").gt(lit(99))], None)
        .await?;
    assert!(
        collect(empty.execute(0, state.task_ctx())?)
            .await?
            .iter()
            .all(|b| b.num_rows() == 0)
    );

    // A non-null declaration for nullable snapshot data must not claim NULL ordering.
    let stale = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let source = IcebergDataSource::builder(table, stale)
        .build_with_context(state.task_ctx())
        .await?;
    assert!(source.read_strategy().ordering().is_none());

    let dir = TempDir::new()?;
    let table = harness.register_sorted_files(dir.path(), &[]).await?;
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    let empty = provider.scan(&state, None, &[], None).await?;
    assert!(
        iceberg_source(empty.as_ref())
            .read_strategy()
            .ordering()
            .is_none()
    );
    assert!(
        collect(empty.execute(0, state.task_ctx())?)
            .await?
            .is_empty()
    );
    Ok(())
}

fn config(partitions: usize) -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(128)
        .with_option_extension(IcebergPlanningConfig::default())
        .set_bool("iceberg_local.eager_file_planning", false)
        .set_bool("iceberg_local.preserve_data_ordering", true)
}

fn iceberg_source(plan: &dyn ExecutionPlan) -> &IcebergDataSource {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<IcebergDataSource>()
        .unwrap()
}

fn ids(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<Int32Type>().iter())
        .collect()
}
