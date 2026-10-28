// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Public API coverage for execution without an entered Tokio runtime.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{JsonReadOptions, ParquetReadOptions};
use datafusion_common::Result;
use datafusion_common_runtime::TaskSpawner;
use datafusion_physical_plan::{ExecutionPlan, collect, collect_partitioned};
use futures::executor::{ThreadPool, ThreadPoolBuilder, block_on};
use futures::future::BoxFuture;
use futures::task::{SpawnError, SpawnExt};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;

struct PoolSpawner {
    asynchronous: ThreadPool,
    blocking: ThreadPool,
    async_count: AtomicUsize,
    blocking_count: AtomicUsize,
}

impl PoolSpawner {
    fn new() -> Self {
        Self {
            asynchronous: ThreadPoolBuilder::new().pool_size(2).create().unwrap(),
            blocking: ThreadPoolBuilder::new().pool_size(2).create().unwrap(),
            async_count: AtomicUsize::new(0),
            blocking_count: AtomicUsize::new(0),
        }
    }
}

impl TaskSpawner for PoolSpawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.async_count.fetch_add(1, Ordering::Relaxed);
        self.asynchronous.spawn(async move {
            assert!(tokio::runtime::Handle::try_current().is_err());
            task.await;
        })
    }

    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send>) -> Result<(), SpawnError> {
        self.blocking_count.fetch_add(1, Ordering::Relaxed);
        self.blocking.spawn(async move {
            assert!(tokio::runtime::Handle::try_current().is_err());
            task();
        })
    }
}

fn runtime(spawner: &Arc<PoolSpawner>) -> Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new()
        .with_task_spawner(Arc::clone(spawner) as Arc<dyn TaskSpawner>)
        .build_arc()
}

fn register_table(context: &SessionContext) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let partitions = (0..4)
        .map(|partition| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(
                    (0..1024).map(|row| partition * 1024 + row),
                ))],
            )?;
            Ok(vec![batch])
        })
        .collect::<Result<Vec<_>>>()?;
    context.register_table("t", Arc::new(MemTable::try_new(schema, partitions)?))?;
    Ok(())
}

async fn scalar(context: &SessionContext, sql: &str) -> Result<i64> {
    let batches = context.sql(sql).await?.collect().await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0))
}

#[test]
fn partitioned_queries_use_the_configured_executor() -> Result<()> {
    assert!(tokio::runtime::Handle::try_current().is_err());
    block_on(async {
        let spawner = Arc::new(PoolSpawner::new());
        let environment = runtime(&spawner)?;
        let rebuilt = RuntimeEnvBuilder::from_runtime_env(&environment).build_arc()?;
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(4)
                .with_batch_size(128),
            rebuilt,
        );
        register_table(&context)?;
        assert_eq!(
            scalar(&context, "SELECT sum(id) FROM t").await?,
            4095 * 4096 / 2
        );
        assert_eq!(
            scalar(&context, "SELECT count(*) FROM t a JOIN t b ON a.id = b.id").await?,
            4096
        );
        context
            .sql("SET datafusion.sql_parser.dialect = 'PostgreSQL'")
            .await?
            .collect()
            .await?;
        assert_eq!(scalar(&context, "WITH q AS MATERIALIZED (SELECT * FROM t) SELECT count(*) FROM q a JOIN q b ON a.id = b.id").await?, 4096);

        let frame = context.table("t").await?;
        let plan = frame.create_physical_plan().await?;
        let partitions = collect_partitioned(plan, context.task_ctx()).await?;
        assert_eq!(partitions.len(), 4);
        assert_eq!(
            partitions
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4096
        );
        assert!(spawner.async_count.load(Ordering::Relaxed) > 0);
        Ok(())
    })
}

#[test]
fn streaming_json_uses_the_blocking_executor() -> Result<()> {
    block_on(async {
        let spawner = Arc::new(PoolSpawner::new());
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime(&spawner)?);
        let store = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("array.json"),
                Bytes::from_static(b"[{\"id\":1},{\"id\":2}]").into(),
            )
            .await?;
        context.register_object_store(&Url::parse("memory://runtime").unwrap(), store);
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        context
            .register_json(
                "j",
                "memory://runtime/array.json",
                JsonReadOptions::default()
                    .schema(&schema)
                    .newline_delimited(false),
            )
            .await?;
        assert_eq!(scalar(&context, "SELECT sum(id) FROM j").await?, 3);
        assert!(spawner.async_count.load(Ordering::Relaxed) > 0);
        assert!(spawner.blocking_count.load(Ordering::Relaxed) > 0);
        Ok(())
    })
}

#[cfg(feature = "parquet")]
#[test]
fn nested_parquet_writers_use_the_query_executor() -> Result<()> {
    block_on(async {
        let spawner = Arc::new(PoolSpawner::new());
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new().with_target_partitions(4),
            runtime(&spawner)?,
        );
        register_table(&context)?;
        context.register_object_store(
            &Url::parse("memory://runtime").unwrap(),
            Arc::new(InMemory::new()),
        );
        context
            .sql("COPY t TO 'memory://runtime/out/' STORED AS PARQUET")
            .await?
            .collect()
            .await?;
        let frame = context
            .read_parquet("memory://runtime/out/", ParquetReadOptions::default())
            .await?;
        context.register_table("written", frame.into_view())?;
        assert_eq!(
            scalar(&context, "SELECT sum(id) FROM written").await?,
            4095 * 4096 / 2
        );
        Ok(())
    })
}

fn spill_count(plan: &dyn ExecutionPlan) -> usize {
    let local = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
    local
        + plan
            .children()
            .iter()
            .map(|child| spill_count(child.as_ref()))
            .sum::<usize>()
}

#[test]
fn local_spill_reads_do_not_require_tokio() -> Result<()> {
    block_on(async {
        let spawner = Arc::new(PoolSpawner::new());
        let environment = RuntimeEnvBuilder::new()
            .with_task_spawner(Arc::clone(&spawner) as Arc<dyn TaskSpawner>)
            .with_memory_limit(1024 * 1024, 1.0)
            .build_arc()?;
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(64)
                .with_sort_spill_reservation_bytes(256 * 1024)
                .with_sort_in_place_threshold_bytes(0),
            Arc::clone(&environment),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batches = (0..128)
            .map(|batch| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(
                            (0..64).map(|row| batch * 64 + row),
                        )),
                        Arc::new(StringArray::from_iter_values(
                            (0..64).map(|row| format!("{row:04}{}", "x".repeat(1024))),
                        )),
                    ],
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        context.register_table(
            "large",
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
        let plan = context
            .sql("SELECT * FROM large ORDER BY payload, id DESC")
            .await?
            .create_physical_plan()
            .await?;
        let result = collect(Arc::clone(&plan), context.task_ctx()).await?;
        assert_eq!(
            result.iter().map(RecordBatch::num_rows).sum::<usize>(),
            8192
        );
        assert!(
            spill_count(plan.as_ref()) > 0,
            "sort must exercise the spill path"
        );
        assert!(spawner.blocking_count.load(Ordering::Relaxed) > 0);
        drop(plan);
        assert_eq!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0
        );
        Ok(())
    })
}

#[test]
fn dropping_a_query_stream_cancels_its_partition_tasks() -> Result<()> {
    use arrow::datatypes::SchemaRef;
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion_physical_plan::{SendableRecordBatchStream, execute_stream};
    use std::sync::mpsc::{Sender, channel};
    use std::time::Duration;

    #[derive(Debug)]
    struct PendingPartition {
        schema: SchemaRef,
        started: Sender<()>,
        dropped: Sender<()>,
    }

    struct OnDrop(Sender<()>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    impl PartitionStream for PendingPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let started = self.started.clone();
            let dropped = self.dropped.clone();
            let stream = futures::stream::once(async move {
                let _guard = OnDrop(dropped);
                started.send(()).unwrap();
                futures::future::pending::<Result<RecordBatch>>().await
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            ))
        }
    }

    let spawner = Arc::new(PoolSpawner::new());
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new(), runtime(&spawner)?);
    let (started_tx, started_rx) = channel();
    let (dropped_tx, dropped_rx) = channel();
    let schema = Arc::new(Schema::empty());
    let partition: Arc<dyn PartitionStream> = Arc::new(PendingPartition {
        schema: Arc::clone(&schema),
        started: started_tx,
        dropped: dropped_tx,
    });
    let plan = Arc::new(StreamingTableExec::try_new(
        schema,
        vec![Arc::clone(&partition), partition],
        None,
        [],
        true,
        None,
    )?);
    let stream = execute_stream(plan, context.task_ctx())?;
    for _ in 0..2 {
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    drop(stream);
    for _ in 0..2 {
        dropped_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    Ok(())
}

#[test]
fn rejected_tasks_fail_the_query_and_do_not_affect_other_sessions() -> Result<()> {
    struct Reject;
    impl TaskSpawner for Reject {
        fn spawn(&self, _task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            Err(SpawnError::shutdown())
        }
        fn spawn_blocking(
            &self,
            _task: Box<dyn FnOnce() + Send>,
        ) -> Result<(), SpawnError> {
            Err(SpawnError::shutdown())
        }
    }

    block_on(async {
        let environment = RuntimeEnvBuilder::new()
            .with_task_spawner(Arc::new(Reject))
            .build_arc()?;
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), environment);
        register_table(&context)?;
        let error = context
            .sql("SELECT * FROM t")
            .await?
            .collect()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("could not spawn task"),
            "{error}"
        );

        let spawner = Arc::new(PoolSpawner::new());
        let other =
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime(&spawner)?);
        register_table(&other)?;
        assert_eq!(scalar(&other, "SELECT count(*) FROM t").await?, 4096);
        Ok(())
    })
}

#[test]
fn cooperative_streams_yield_without_tokio() {
    use datafusion_physical_plan::coop::cooperative;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{Future, StreamExt};
    use std::task::{Context, Poll};

    assert!(tokio::runtime::Handle::try_current().is_err());
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&schema));
    let input = RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter((0..1024).map(move |_| Ok(batch.clone()))),
    );
    let mut output = Box::pin(cooperative(input).collect::<Vec<_>>());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(matches!(output.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(block_on(output).len(), 1024);
}
