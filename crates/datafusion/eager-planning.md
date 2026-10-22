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

# Eager planning across Iceberg, DataFusion, and distributed execution

This document describes the boundary proposed by
[datafusion-iceberg#67](https://github.com/apache/datafusion-iceberg/pull/67).
API names below are illustrative, not APIs provided by this change.

## One selection pipeline, two consumption times

```text
Table metadata → snapshot → manifest list → selected manifests
  → live entries → partition/metrics pruning → file tasks + delete references

Lazy:   execution starts → stream selected tasks → reader
Eager:  planning starts  → collect selected tasks → assign partitions
                        → execution starts → reader
```

The catalog does not return the complete file list. Reading the manifest list
and manifests is metadata I/O, even when Parquet reads are deferred. Eager
planning pays that cost before execution and retains tasks, allowing the
optimizer to see the selected work. Lazy planning can overlap discovery and
execution and avoid retaining the full task list. Both use Iceberg's pruning
and delete matching, and both retain residual DataFusion filters.

#67 keeps both modes, defaults to lazy, and uses the same local reader flow.
It exposes borrowed access to retained tasks, accounts for their estimated
memory, and assigns them round-robin to local partitions. It does not add
column statistics, distributed routing, or a file-count limit. Statistics
remain unknown in both modes.

## What the distributed stack adds

| Change | Responsibility |
| --- | --- |
| [DFD#773](https://github.com/datafusion-contrib/datafusion-distributed/pull/773) | Select and retain work during planning while preserving lazy execution. |
| [DFD#786](https://github.com/datafusion-contrib/datafusion-distributed/pull/786) | Distribute feed work across tasks before filling their local partitions. |
| [DFD#787](https://github.com/datafusion-contrib/datafusion-distributed/pull/787) | Use selected-file counts and whole-file bytes to size distributed work. |
| [DFD#788](https://github.com/datafusion-contrib/datafusion-distributed/pull/788) | Balance eager assignments by file size. |
| [DFD#789](https://github.com/datafusion-contrib/datafusion-distributed/pull/789) | Reconstruct statistics for selected files. |
| [DFD#790](https://github.com/datafusion-contrib/datafusion-distributed/pull/790) | Extend placement with affinity. |

Selection belongs in Iceberg; interpretation as DataFusion statistics belongs
in the integration; worker counts and placement belong in DFD. #67 carries
over the shared flow and ownership lessons without imposing distributed
scheduling on local readers.

## Upstream need 1: preserve access to selected-file metadata

At Iceberg revision `5b82ab99`, `ManifestEntryContext` holds the parsed manifest
entry, including `DataFile` column sizes, value/null/NaN counts, and lower/upper
bounds. `process_data_manifest_entry` applies pruning before
[`into_file_scan_task`](https://github.com/apache/iceberg-rust/blob/5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3/crates/iceberg/src/scan/context.rs#L140) constructs the public task. That conversion omits the
column metrics. `plan_files()` exposes only the resulting tasks.

The metadata may remain in the table's internal object cache, but consumers
cannot obtain it from the selected tasks. DFD's current workaround traverses
manifests again and matches selected paths; its direct manifest reader also
bypasses that cache. Exposing the cache alone would not eliminate the repeated
traversal and matching.

An additive API could return a stream of planning results:

```rust,ignore
// Illustrative API, not a definition in this crate.
impl PlannedFile {
    pub fn scan_task(&self) -> &FileScanTask;
    pub fn data_file(&self) -> &DataFile;
    pub fn into_scan_task(self) -> FileScanTask;
}
// TableScan::plan_files_with_metadata() -> stream of Result<PlannedFile>
```

Internally, a result can retain the existing `Arc<ManifestEntry>` beside its
task. The generic [`plan_data_files`](https://github.com/apache/iceberg-rust/blob/5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3/crates/iceberg/src/scan/mod.rs#L412) result builder already supports alternative
outputs; copy-on-write planning already preserves a `DataFile` alongside a
task. Reuse that pipeline rather than expose planning contexts or ask callers
to reproduce pruning. Keep `plan_files()` and task serialization unchanged.
The internal result builder would have this shape:

```rust,ignore
self.plan_data_files(|ctx| async move {
    let entry = Arc::clone(&ctx.manifest_entry);
    let task = ctx.into_file_scan_task().await?;
    Ok(PlannedFile { entry, task })
}).await
```

Document whether a future split produces multiple tasks for one file: consumers
must aggregate whole-file metrics once per file, not once per task.

A consumer can aggregate metrics as results arrive, retain only tasks and
aggregate totals, then release each metadata reference. This avoids the second
pass without forcing every scan to retain all metrics. Missing metrics must
remain unknown; file bounds/counts are not exact post-filter or post-delete row
statistics. Snapshot NDV cannot be summed across selected files and remains a
separate, inexact fallback. Iceberg should expose metadata; DataFusion should
own precision and aggregation semantics.

## Upstream need 2: portable reader settings

The [#2671 discussion](https://github.com/apache/iceberg-rust/pull/2671#issuecomment-6116801448)
identifies a separate risk: building a reader outside `TableScan::to_arrow()`
can lose scan-level batch size, file concurrency, row-group, row-selection,
and bloom-filter settings.

Expose a lightweight `ScanReaderOptions` value and an Iceberg-owned method to
apply it to a local `ArrowReaderBuilder`. Have `to_arrow()` use that same
mapping. Distributed consumers transport settings and tasks, then supply
worker-local `FileIO` and runtimes. They should not serialize a builder or
retain an entire `TableScan` merely to reconstruct a reader.

#67 currently uses default reader settings in both modes, with a deliberate
one-file-at-a-time setting per eager output partition. A single built reader
is cloned across those partitions so its delete loader is shared. That local
cache lifetime is distinct from portable options.

## Upstream runtime boundary

`plan_files()` awaits the initial manifest-list read on its caller before
spawning the remaining work onto the table's runtime. `Table::runtime()` is
private. Prefer making `plan_files()` execute that initial read on the table's
I/O runtime too, with cancellation on drop, rather than requiring each consumer
to duplicate runtime dispatch. Until then, #67 requires an I/O-capable planning
caller. Dropping direct collection releases its retained-task reservation;
this does not promise immediate cancellation of all Iceberg background work.

## Delivery order and validation

#67 can land using existing public Iceberg APIs. The metadata-result API then
lets selected-file statistics replace DFD's second-pass workaround. Portable
reader options prevent future configuration divergence and support remote
reader construction; they need not block the default-settings foundation.

Integration coverage should execute the same physical plan it inspects,
compare eager/lazy filtered results, verify empty scans and multi-partition
LIMIT, cover delete files, and check runtime and memory-pool behavior. The
current position-delete fixture checks correctness; equality deletes and
cross-partition delete-cache reuse need dedicated coverage before changing
the default. This proposal makes no measured performance claim.
