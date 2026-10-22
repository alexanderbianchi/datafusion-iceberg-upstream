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
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionConfig;
use iceberg::Runtime;
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, Summary,
};
use iceberg::table::Table;
use iceberg_datafusion::IcebergCatalogTableProvider;
use iceberg_datafusion::test_utils::IcebergTestHarness;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use tempfile::TempDir;

/// A taxi table with its first data file's first row deleted.
/// Owns the copied files for the lifetime of scans against this table.
pub struct PositionDeleteFixture {
    pub table: Table,
    _directory: TempDir,
}

pub async fn taxi_with_position_delete() -> Result<PositionDeleteFixture, Box<dyn Error>>
{
    // Isolate the checked-in fixture: the test writes a new delete manifest and
    // snapshot, but must not modify the files used by other tests.
    let temporary = TempDir::new()?;
    let root = temporary.path().join("iceberg");
    copy_fixture(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/iceberg"),
        &root,
    )?;
    let harness =
        IcebergTestHarness::with_fixture_root(SessionConfig::new(), root).await?;
    let provider = harness.context().table_provider("taxi").await?;
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

    let new_snapshot = Snapshot::builder()
        .with_snapshot_id(next_snapshot_id)
        .with_parent_snapshot_id(Some(snapshot.snapshot_id()))
        .with_sequence_number(snapshot.sequence_number() + 1)
        .with_timestamp_ms(snapshot.timestamp_ms() + 1)
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Delete,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(snapshot.schema(table.metadata())?.schema_id())
        .build();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_branch_snapshot(new_snapshot, "main")?
        .build()?
        .metadata;
    let table = Table::builder()
        .file_io(table.file_io().clone())
        .identifier(table.identifier().clone())
        .metadata(metadata)
        .runtime(Runtime::try_current()?)
        .build()?;
    Ok(PositionDeleteFixture {
        table,
        _directory: temporary,
    })
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
