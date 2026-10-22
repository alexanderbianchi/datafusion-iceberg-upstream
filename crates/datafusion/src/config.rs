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

use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    /// Opt-in file planning during physical planning for local Iceberg scans.
    pub struct IcebergPlanningConfig {
        /// Discover and retain file tasks in TableProvider::scan instead of at execution.
        pub eager_file_planning: bool, default = false
        /// Plan files eagerly and merge each partition when their sort orders are compatible.
        /// Merging is performed even if no parent operator needs the order.
        pub preserve_data_ordering: bool, default = false
        /// Maximum simultaneously merged files in one partition; not a planning-file cap.
        /// Larger groups retain their planned files but read without an ordering guarantee.
        pub max_merge_files_per_partition: usize, default = 64
    }
}

impl ConfigExtension for IcebergPlanningConfig {
    // Distinct from DFD's `iceberg` options so both integrations can share a session.
    const PREFIX: &'static str = "iceberg_local";
}
