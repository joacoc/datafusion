<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Introduction

The library user guide explains how to use the DataFusion library as a
dependency in your Rust project and customize its behavior using its extension APIs.

Please check out the [user guide] for getting started using
DataFusion's SQL and DataFrame APIs, or the [contributor guide]
for details on how to contribute to DataFusion.

If you haven't reviewed the [architecture section in the docs][docs], it's a
useful place to get the lay of the land before starting down a specific path.

For guidance on running DataFusion with SQL from untrusted users, see
[Securing DataFusion](securing-datafusion.md).

DataFusion is designed to be extensible at all points, including

- [x] User Defined Functions (UDFs)
- [x] User Defined Aggregate Functions (UDAFs)
- [x] User Defined Table Source (`TableProvider`) for tables
- [x] User Defined `Optimizer` passes (plan rewrites)
- [x] User Defined `LogicalPlan` nodes
- [x] User Defined `ExecutionPlan` nodes

## Task execution

DataFusion uses Tokio for internal query tasks by default. Applications can
provide a `TaskSpawner` through `RuntimeEnvBuilder::with_task_spawner` to run
these tasks on another executor. The choice belongs to the runtime environment;
independent environments can use different executors in the same process.

```rust
use std::sync::Arc;

use datafusion::common::runtime::TaskSpawner;
use datafusion::error::Result;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

fn runtime(spawner: Arc<dyn TaskSpawner>) -> Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new()
        .with_task_spawner(spawner)
        .build_arc()
}
```

A spawner submits asynchronous futures and blocking closures without waiting for
them. Asynchronous tasks must make progress independently of the submitting task.
Blocking jobs need a separate pool so they cannot block the asynchronous tasks
that produce or consume their data. Accepted work must eventually run or be
dropped when the executor shuts down; rejected submissions return `SpawnError`.
DataFusion handles typed results, cancellation, and panic propagation.

Dropping a query stream requests cancellation of its tasks. Asynchronous
cancellation takes effect when the executor next polls the task. A blocking job
that has already started runs to completion; cancellation can prevent a queued
job from starting. Outside a Tokio runtime, cooperative streams use a local
progress budget to yield periodically. All existing `Send` and `Sync` requirements
remain in place.

The default local spill-file implementation uses the configured executor for
blocking reads when a custom spawner is supplied. An explicitly supplied
`DiskManager` retains its own executor; configure it with
`DiskManagerBuilder::with_task_executor` when necessary. Object stores, custom
sources, custom spill backends, and the FFI interface can have separate runtime
requirements. Supplying a spawner does not change those requirements or remove
Tokio from the dependency tree.

Custom operators should submit tasks through `TaskContext::task_executor` and
create `TaskSet` collections with that executor. `SpawnedTask::spawn` and
`JoinSet::new` remain conveniences for the current Tokio runtime. Task handles
now return `TaskError`; `into_join_error` recovers a native Tokio error when the
task used the default executor.

[user guide]: ../user-guide/example-usage.md
[contributor guide]: ../contributor-guide/index.md
[docs]: https://docs.rs/datafusion/latest/datafusion/#architecture
