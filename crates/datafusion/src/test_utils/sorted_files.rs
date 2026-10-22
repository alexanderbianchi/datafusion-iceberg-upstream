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

//! Small on-disk sorted files for exercising the real Iceberg planner and reader.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::{Result, exec_datafusion_err};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, NullOrder,
    PrimitiveType, Schema, SortDirection, SortField, SortOrder, Struct, Transform, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::arrow::ArrowWriter;

use super::IcebergTestHarness;
use crate::{IcebergStaticTableProvider, to_datafusion_error};

impl IcebergTestHarness {
    /// Register `sorted.public.events`: one nullable integer key, ascending with nulls first.
    /// Each slice becomes a real Parquet file. `None` omits that file's sort order.
    /// Callers retain `directory` for the duration of the query.
    pub async fn register_sorted_files(
        &self,
        directory: &Path,
        files: &[(Option<i32>, &[Option<i32>])],
    ) -> Result<Table> {
        let warehouse = directory.display().to_string();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "sorted",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.clone(),
                )]),
            )
            .await
            .map_err(to_datafusion_error)?;
        let namespace = NamespaceIdent::new("test".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .map_err(to_datafusion_error)?;
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .map_err(to_datafusion_error)?;
        let sort_order = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .transform(Transform::Identity)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .build(),
            )
            .build_unbound()
            .map_err(to_datafusion_error)?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("events".to_string())
                    .location(warehouse)
                    .schema(schema)
                    .sort_order(sort_order)
                    .build(),
            )
            .await
            .map_err(to_datafusion_error)?;
        let arrow_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(to_datafusion_error)?,
        );
        let mut data_files = Vec::new();
        for (index, (order, values)) in files.iter().enumerate() {
            let path = directory.join(format!("part-{index}.parquet"));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )?;
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(&path)?,
                arrow_schema.clone(),
                None,
            )?;
            writer.write(&batch)?;
            writer.close()?;
            let mut file = DataFileBuilder::default();
            file.content(DataContentType::Data)
                .file_path(path.display().to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(std::fs::metadata(path)?.len())
                .record_count(values.len() as u64)
                .partition_spec_id(0)
                .partition(Struct::empty());
            if let Some(order) = order {
                file.sort_order_id(*order);
            }
            data_files.push(
                file.build()
                    .map_err(|e| exec_datafusion_err!("Invalid sorted fixture: {e}"))?,
            );
        }
        let table = if data_files.is_empty() {
            table
        } else {
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(data_files)
                .apply(tx)
                .map_err(to_datafusion_error)?;
            tx.commit(&catalog).await.map_err(to_datafusion_error)?
        };
        let schema = Arc::new(MemorySchemaProvider::new());
        schema.register_table(
            "events".to_string(),
            Arc::new(
                IcebergStaticTableProvider::try_new_from_table(table.clone()).await?,
            ),
        )?;
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("public", schema)?;
        self.context().register_catalog("sorted", catalog);
        Ok(table)
    }
}
