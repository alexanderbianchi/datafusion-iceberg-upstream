<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg DataFusion Integration

This crate contains the integration of Apache DataFusion and Apache Iceberg.

## Optional file planning before execution

By default, the Iceberg reader discovers files when execution starts. To select
`FileScanTask`s during `TableProvider::scan()` instead, register the planning
options on the DataFusion session before planning a query:

```rust
use datafusion::prelude::SessionConfig;
use iceberg_datafusion::IcebergPlanningConfig;

let config = SessionConfig::new()
    .with_option_extension(IcebergPlanningConfig::default())
    .set_bool("iceberg_local.eager_file_planning", true);
```

### Behavior

- Lazy planning is the default. Eager planning selects files before execution;
  it reads metadata, not Parquet data.
- Selected tasks are assigned round-robin to at most `target_partitions`.
  An empty scan has one empty partition. Each eager partition reads one file
  at a time when there are multiple output partitions.
- Retained tasks have an estimated reservation in the query memory pool.
  Exceeding the pool fails planning; there is no file-count cap or truncation.
- Both modes use the same reader implementation. Reader clones share the
  delete loader across partitions. Scan statistics remain unknown.

### Trade-offs

Eager planning completes file discovery and retains tasks even for queries
that stop early. Repeated provider scans repeat planning. The planning caller
must support metadata I/O: Iceberg's initial manifest-list read still runs on
that runtime, even when the table has a separate I/O runtime.

The `iceberg_local` prefix allows these options to coexist with DataFusion
Distributed's `iceberg` options.

## Optional ordered reads

Set `iceberg_local.preserve_data_ordering = true` on the same configuration
extension to select files eagerly and preserve compatible file ordering.
This implies eager planning even when `eager_file_planning` is false.

The same `IcebergDataSource` assigns files to output partitions before choosing
how to read them. `ReadStrategy::OrderedMerge` merges the files assigned to each
partition and advertises that partition's ordering to DataFusion. It constructs
reader handles synchronously from `planned_partition_tasks()`; file contents
are still read asynchronously when the merge stream is polled. A global
`ORDER BY` across partitions can still require a sort-preserving merge.
The strategy accepts only a compatible leading sequence of projected identity
sort fields, with matching types and safe nullability. Unsupported or missing
orders fall back to unordered reading of the already-planned tasks.

`iceberg_local.max_merge_files_per_partition` defaults to 64. It bounds
simultaneously merged files, not metadata discovery; larger groups retain the
same eager plan and read unordered. Ordered reads currently merge even when
no parent operator needs that ordering. File bounds and overlap analysis are
not part of this implementation.

Custom integrations can use `with_planned_files(...).await?.build()` for the same planning path
as the table providers. For in-process reconstruction, retain `planned_scan()`
and pass it to `with_planned_scan`: an optimized parent may depend on both
partitioning and ordering. This is not a distributed serialization API;
transporting tasks and restoring their properties on workers remains separate.
