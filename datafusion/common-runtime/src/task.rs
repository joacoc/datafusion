// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Runtime-independent task submission and structured task ownership.

use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::channel::oneshot;
use futures::future::{AbortHandle, AbortRegistration, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::task::SpawnError;
use futures::{FutureExt, Stream};

use crate::{JoinSet, trace_block, trace_future};

/// Submits DataFusion tasks to an application's executor.
///
/// Tasks must be scheduled independently of the submitting future: executing a
/// task inline, or waiting for it before returning, can deadlock query execution.
/// Blocking jobs must run separately from asynchronous tasks so a blocked job
/// does not prevent the asynchronous producer or consumer it depends on from
/// making progress.
///
/// Returning an error rejects the task. Accepted tasks must either be driven to
/// completion or dropped during executor shutdown. Dropping a task notifies
/// DataFusion's task handle that the executor has shut down.
///
/// DataFusion supplies result delivery, panic propagation, and cancellation; the
/// implementation only needs to submit the supplied future or closure. All
/// futures and results retain their existing `Send` requirements. Implementors
/// are responsible for propagating any executor-specific context to new tasks.
pub trait TaskSpawner: Send + Sync + 'static {
    /// Schedule an asynchronous task without waiting for its completion.
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError>;

    /// Schedule a blocking job without blocking the calling thread.
    fn spawn_blocking(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), SpawnError>;
}

/// Creates tasks and task sets using Tokio or an application-supplied spawner.
///
/// The default preserves native Tokio task submission and task handles. A custom
/// spawner applies only to tasks submitted through this executor; it does not
/// change the runtime requirements of external I/O implementations.
#[derive(Clone, Default)]
pub struct TaskExecutor {
    spawner: Option<Arc<dyn TaskSpawner>>,
}

impl Debug for TaskExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskExecutor")
            .field("custom_spawner", &self.spawner.is_some())
            .finish()
    }
}

impl TaskExecutor {
    /// Create an executor using the supplied task spawner.
    pub fn new(spawner: Arc<dyn TaskSpawner>) -> Self {
        Self {
            spawner: Some(spawner),
        }
    }

    /// Whether this executor uses the default Tokio task implementation.
    pub fn is_tokio(&self) -> bool {
        self.spawner.is_none()
    }

    /// Spawn a task that is cancelled when its handle is dropped.
    pub fn spawn<F, T>(&self, task: F) -> SpawnedTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.spawner {
            None => SpawnedTask::spawn(task),
            Some(spawner) => SpawnedTask::custom(|sender, registration| {
                let task = trace_future(task);
                let task = async move {
                    let result = AssertUnwindSafe(task)
                        .catch_unwind()
                        .await
                        .map_err(TaskError::panic);
                    let _ = sender.send(result);
                };
                spawner.spawn(Abortable::new(task, registration).map(|_| ()).boxed())
            }),
        }
    }

    /// Spawn a blocking job. Cancellation can prevent a queued job from starting,
    /// but cannot stop a job that has already started.
    pub fn spawn_blocking<F, T>(&self, task: F) -> SpawnedTask<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        match &self.spawner {
            None => SpawnedTask::spawn_blocking(task),
            Some(spawner) => SpawnedTask::custom(|sender, registration| {
                let task = trace_block(task);
                spawner.spawn_blocking(Box::new(move || {
                    // This future contains no suspension points. Abortable checks
                    // cancellation before allowing a queued closure to start.
                    let task = async move {
                        let result = std::panic::catch_unwind(AssertUnwindSafe(task))
                            .map_err(TaskError::panic);
                        let _ = sender.send(result);
                    };
                    let _ = Abortable::new(task, registration).now_or_never();
                }))
            }),
        }
    }
}

/// Failure to submit or complete a task.
pub struct TaskError(TaskErrorKind);

enum TaskErrorKind {
    Tokio(tokio::task::JoinError),
    Cancelled,
    // Preserve the panic payload while keeping the error Send + Sync.
    Panic(Mutex<Box<dyn Any + Send + 'static>>),
    Spawn(SpawnError),
}

impl TaskError {
    fn panic(payload: Box<dyn Any + Send + 'static>) -> Self {
        Self(TaskErrorKind::Panic(Mutex::new(payload)))
    }

    /// Whether the task was cancelled or dropped by its executor.
    pub fn is_cancelled(&self) -> bool {
        match &self.0 {
            TaskErrorKind::Tokio(error) => error.is_cancelled(),
            TaskErrorKind::Cancelled => true,
            _ => false,
        }
    }

    /// Whether the task panicked.
    pub fn is_panic(&self) -> bool {
        match &self.0 {
            TaskErrorKind::Tokio(error) => error.is_panic(),
            TaskErrorKind::Panic(_) => true,
            _ => false,
        }
    }

    /// Recover the Tokio join error when this task used the default executor.
    /// Returns this error unchanged for application-supplied executors.
    pub fn into_join_error(self) -> Result<tokio::task::JoinError, Self> {
        match self.0 {
            TaskErrorKind::Tokio(error) => Ok(error),
            _ => Err(self),
        }
    }

    /// Extract the original panic payload.
    ///
    /// # Panics
    /// Panics if this error does not represent a task panic.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.0 {
            TaskErrorKind::Tokio(error) => error.into_panic(),
            TaskErrorKind::Panic(payload) => payload.into_inner().unwrap(),
            _ => panic!("TaskError does not contain a panic"),
        }
    }
}

impl Debug for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TaskError")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            TaskErrorKind::Tokio(error) => Display::fmt(error, f),
            TaskErrorKind::Cancelled => f.write_str("task was cancelled"),
            TaskErrorKind::Panic(_) => f.write_str("task panicked"),
            TaskErrorKind::Spawn(error) => write!(f, "could not spawn task: {error}"),
        }
    }
}

impl std::error::Error for TaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            TaskErrorKind::Tokio(error) => Some(error),
            TaskErrorKind::Spawn(error) => Some(error),
            _ => None,
        }
    }
}

impl From<tokio::task::JoinError> for TaskError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self(TaskErrorKind::Tokio(error))
    }
}

/// A task owned by its caller. Dropping the handle requests cancellation.
///
/// Cancellation takes effect when the executor next polls an asynchronous task.
/// A running blocking job cannot be forcibly cancelled.
#[must_use = "dropping a task handle cancels the task"]
#[derive(Debug)]
pub struct SpawnedTask<T> {
    inner: TaskInner<T>,
}

#[derive(Debug)]
enum TaskInner<T> {
    Tokio(tokio::task::JoinHandle<T>),
    Custom {
        receiver: oneshot::Receiver<Result<T, TaskError>>,
        abort: AbortHandle,
        finished: bool,
    },
    Rejected(Option<SpawnError>),
}

impl<T> SpawnedTask<T> {
    pub(crate) fn from_handle(task: tokio::task::JoinHandle<T>) -> Self {
        Self {
            inner: TaskInner::Tokio(task),
        }
    }

    fn custom(
        submit: impl FnOnce(
            oneshot::Sender<Result<T, TaskError>>,
            AbortRegistration,
        ) -> Result<(), SpawnError>,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();
        let (abort, registration) = AbortHandle::new_pair();
        let inner = match submit(sender, registration) {
            Ok(()) => TaskInner::Custom {
                receiver,
                abort,
                finished: false,
            },
            Err(error) => {
                abort.abort();
                TaskInner::Rejected(Some(error))
            }
        };
        Self { inner }
    }

    /// Spawn a task on the current Tokio runtime.
    pub fn spawn<F>(task: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        #[expect(clippy::disallowed_methods)]
        Self::from_handle(tokio::task::spawn(trace_future(task)))
    }

    /// Spawn a blocking job on the current Tokio runtime.
    pub fn spawn_blocking<F>(task: F) -> Self
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        #[expect(clippy::disallowed_methods)]
        Self::from_handle(tokio::task::spawn_blocking(trace_block(task)))
    }

    /// Wait for task completion.
    pub async fn join(self) -> Result<T, TaskError> {
        self.await
    }

    /// Join this task, resuming unwinding if it panicked.
    pub async fn join_unwind(mut self) -> Result<T, TaskError> {
        self.join_unwind_mut().await
    }

    /// Join by mutable reference, resuming the original panic if the task panicked.
    pub async fn join_unwind_mut(&mut self) -> Result<T, TaskError> {
        match self.await {
            Err(error) if error.is_panic() => {
                std::panic::resume_unwind(error.into_panic())
            }
            result => result,
        }
    }
}

impl<T> Future for SpawnedTask<T> {
    type Output = Result<T, TaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            TaskInner::Tokio(task) => Pin::new(task).poll(cx).map_err(TaskError::from),
            TaskInner::Custom {
                receiver, finished, ..
            } => {
                if *finished {
                    return Poll::Ready(Err(TaskError(TaskErrorKind::Cancelled)));
                }
                let result = futures::ready!(Pin::new(receiver).poll(cx));
                *finished = true;
                Poll::Ready(
                    result.unwrap_or_else(|_| Err(TaskError(TaskErrorKind::Cancelled))),
                )
            }
            TaskInner::Rejected(error) => Poll::Ready(Err(TaskError(
                error
                    .take()
                    .map(TaskErrorKind::Spawn)
                    .unwrap_or(TaskErrorKind::Cancelled),
            ))),
        }
    }
}

impl<T> Drop for SpawnedTask<T> {
    fn drop(&mut self) {
        match &self.inner {
            TaskInner::Tokio(task) => task.abort(),
            TaskInner::Custom { abort, .. } => abort.abort(),
            TaskInner::Rejected(_) => {}
        }
    }
}

/// A collection of tasks that are cancelled when the collection is dropped.
///
/// Unlike [`JoinSet`], this collection can use an application-supplied executor.
/// Results are returned in completion order.
#[derive(Debug)]
pub struct TaskSet<T> {
    inner: TaskSetInner<T>,
}

#[derive(Debug)]
enum TaskSetInner<T> {
    Tokio(JoinSet<T>),
    Custom {
        executor: TaskExecutor,
        tasks: FuturesUnordered<SpawnedTask<T>>,
    },
}

impl<T> TaskSet<T> {
    /// Create an empty task collection using the supplied executor.
    pub fn new(executor: TaskExecutor) -> Self {
        let inner = if executor.spawner.is_some() {
            TaskSetInner::Custom {
                executor,
                tasks: FuturesUnordered::new(),
            }
        } else {
            TaskSetInner::Tokio(JoinSet::new())
        };
        Self { inner }
    }

    /// Number of tasks whose completion has not yet been joined.
    pub fn len(&self) -> usize {
        match &self.inner {
            TaskSetInner::Tokio(tasks) => tasks.len(),
            TaskSetInner::Custom { tasks, .. } => tasks.len(),
        }
    }

    /// Whether all submitted tasks have been joined.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Default for TaskSet<T> {
    fn default() -> Self {
        Self::new(TaskExecutor::default())
    }
}

impl<T: Send + 'static> TaskSet<T> {
    /// Submit a task to the collection's executor.
    pub fn spawn<F: Future<Output = T> + Send + 'static>(&mut self, task: F) {
        match &mut self.inner {
            TaskSetInner::Tokio(tasks) => {
                tasks.spawn(task);
            }
            TaskSetInner::Custom { executor, tasks } => tasks.push(executor.spawn(task)),
        }
    }

    /// Submit a blocking job to the collection's executor.
    pub fn spawn_blocking<F: FnOnce() -> T + Send + 'static>(&mut self, task: F) {
        match &mut self.inner {
            TaskSetInner::Tokio(tasks) => {
                tasks.spawn_blocking(task);
            }
            TaskSetInner::Custom { executor, tasks } => {
                tasks.push(executor.spawn_blocking(task))
            }
        }
    }

    /// Explicitly submit a task to the supplied Tokio runtime.
    pub fn spawn_on<F: Future<Output = T> + Send + 'static>(
        &mut self,
        task: F,
        handle: &tokio::runtime::Handle,
    ) {
        match &mut self.inner {
            TaskSetInner::Tokio(tasks) => {
                tasks.spawn_on(task, handle);
            }
            TaskSetInner::Custom { tasks, .. } => {
                let inner = handle.spawn(trace_future(task));
                tasks.push(SpawnedTask::from_handle(inner));
            }
        }
    }

    /// Explicitly submit a blocking job to the supplied Tokio runtime.
    pub fn spawn_blocking_on<F: FnOnce() -> T + Send + 'static>(
        &mut self,
        task: F,
        handle: &tokio::runtime::Handle,
    ) {
        match &mut self.inner {
            TaskSetInner::Tokio(tasks) => {
                tasks.spawn_blocking_on(task, handle);
            }
            TaskSetInner::Custom { tasks, .. } => {
                let inner = handle.spawn_blocking(trace_block(task));
                tasks.push(SpawnedTask::from_handle(inner));
            }
        }
    }

    /// Wait for the next completion, or return `None` when the set is empty.
    pub async fn join_next(&mut self) -> Option<Result<T, TaskError>> {
        futures::future::poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// Poll for the next task completion.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, TaskError>>> {
        match &mut self.inner {
            TaskSetInner::Tokio(tasks) => tasks
                .poll_join_next(cx)
                .map(|result| result.map(|r| r.map_err(TaskError::from))),
            TaskSetInner::Custom { tasks, .. } => Pin::new(tasks).poll_next(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::task::noop_waker_ref;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct ManualSpawner {
        tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
        jobs: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    }

    impl TaskSpawner for ManualSpawner {
        fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            self.tasks.lock().unwrap().push(task);
            Ok(())
        }

        fn spawn_blocking(
            &self,
            task: Box<dyn FnOnce() + Send>,
        ) -> Result<(), SpawnError> {
            self.jobs.lock().unwrap().push(task);
            Ok(())
        }
    }

    impl ManualSpawner {
        fn poll_tasks(&self) {
            let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
            for mut task in tasks {
                if task
                    .as_mut()
                    .poll(&mut Context::from_waker(noop_waker_ref()))
                    .is_pending()
                {
                    self.tasks.lock().unwrap().push(task);
                }
            }
        }

        fn run_jobs(&self) {
            let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
            for job in jobs {
                job();
            }
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn custom_tasks_and_blocking_jobs_without_tokio() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let task = executor.spawn(async { 41 });
        let job = executor.spawn_blocking(|| 42);
        spawner.poll_tasks();
        spawner.run_jobs();
        assert_eq!(block_on(task).unwrap(), 41);
        assert_eq!(block_on(job).unwrap(), 42);
    }

    #[test]
    fn dropping_a_running_task_releases_its_resources() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = DropFlag(Arc::clone(&dropped));
        let task = executor.spawn(async move {
            let _flag = flag;
            futures::future::pending::<()>().await;
        });
        spawner.poll_tasks();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(task);
        spawner.poll_tasks();
        assert!(dropped.load(Ordering::SeqCst));
        assert!(spawner.tasks.lock().unwrap().is_empty());
    }

    #[test]
    fn cancellation_prevents_queued_work_from_starting() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let started = Arc::new(AtomicBool::new(false));
        let task_started = Arc::clone(&started);
        let task =
            executor.spawn(async move { task_started.store(true, Ordering::SeqCst) });
        drop(task);
        spawner.poll_tasks();
        assert!(!started.load(Ordering::SeqCst));
        let job_started = Arc::clone(&started);
        let job =
            executor.spawn_blocking(move || job_started.store(true, Ordering::SeqCst));
        drop(job);
        spawner.run_jobs();
        assert!(!started.load(Ordering::SeqCst));
    }

    #[test]
    fn running_blocking_job_finishes_after_handle_is_dropped() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let job_finished = Arc::clone(&finished);
        let task = executor.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            job_finished.store(true, Ordering::SeqCst);
        });
        let worker = std::thread::spawn(move || spawner.run_jobs());
        started_rx.recv().unwrap();
        drop(task);
        finish_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(finished.load(Ordering::SeqCst));
    }

    #[test]
    fn executor_shutdown_reports_cancellation() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let task = executor.spawn(futures::future::pending::<()>());
        spawner.tasks.lock().unwrap().clear();
        assert!(block_on(task).unwrap_err().is_cancelled());
        let job = executor.spawn_blocking(|| 42);
        spawner.jobs.lock().unwrap().clear();
        assert!(block_on(job).unwrap_err().is_cancelled());
    }

    #[test]
    fn panic_payloads_survive_task_boundaries() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let task = executor.spawn(async { std::panic::panic_any(42_u32) });
        let job = executor.spawn_blocking(|| std::panic::panic_any(43_u32));
        spawner.poll_tasks();
        spawner.run_jobs();
        let error = block_on(task).unwrap_err();
        assert!(error.is_panic());
        assert_eq!(*error.into_panic().downcast::<u32>().unwrap(), 42);
        let panic =
            block_on(AssertUnwindSafe(job.join_unwind()).catch_unwind()).unwrap_err();
        assert_eq!(*panic.downcast::<u32>().unwrap(), 43);
    }

    #[test]
    fn rejected_submission_is_a_task_failure() {
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
        let executor = TaskExecutor::new(Arc::new(Reject));
        for error in [
            block_on(executor.spawn(async {})).unwrap_err(),
            block_on(executor.spawn_blocking(|| ())).unwrap_err(),
        ] {
            assert!(!error.is_panic());
            assert!(!error.is_cancelled());
            assert!(error.to_string().contains("could not spawn task"));
        }
    }

    #[test]
    fn task_set_joins_results_and_cancels_on_drop() {
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        let mut tasks = TaskSet::new(executor.clone());
        tasks.spawn(async { 41 });
        tasks.spawn_blocking(|| 42);
        assert_eq!(tasks.len(), 2);
        spawner.poll_tasks();
        spawner.run_jobs();
        let mut results = block_on(async {
            let mut results = Vec::new();
            while let Some(result) = tasks.join_next().await {
                results.push(result.unwrap());
            }
            results
        });
        results.sort_unstable();
        assert_eq!(results, vec![41, 42]);
        assert!(tasks.is_empty());

        let dropped = Arc::new(AtomicBool::new(false));
        let flag = DropFlag(Arc::clone(&dropped));
        tasks.spawn(async move {
            let _flag = flag;
            futures::future::pending::<i32>().await
        });
        drop(tasks);
        spawner.poll_tasks();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn errors_can_cross_thread_boundaries() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TaskError>();
    }

    #[test]
    fn tracing_captures_the_submitting_context() {
        use std::cell::Cell;

        thread_local! {
            static SUBMITTING: Cell<bool> = const { Cell::new(false) };
            static CAPTURED: Cell<usize> = const { Cell::new(0) };
        }

        struct Tracer;
        impl crate::JoinSetTracer for Tracer {
            fn trace_future(
                &self,
                task: BoxFuture<'static, Box<dyn Any + Send>>,
            ) -> BoxFuture<'static, Box<dyn Any + Send>> {
                if SUBMITTING.get() {
                    CAPTURED.set(CAPTURED.get() + 1);
                }
                task
            }

            fn trace_block(
                &self,
                job: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
            ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
                if SUBMITTING.get() {
                    CAPTURED.set(CAPTURED.get() + 1);
                }
                job
            }
        }

        crate::set_join_set_tracer(&Tracer).unwrap();
        let spawner = Arc::new(ManualSpawner::default());
        let executor = TaskExecutor::new(Arc::clone(&spawner) as Arc<dyn TaskSpawner>);
        SUBMITTING.set(true);
        let task = executor.spawn(async { 41 });
        let job = executor.spawn_blocking(|| 42);
        SUBMITTING.set(false);
        assert_eq!(CAPTURED.get(), 2);
        spawner.poll_tasks();
        spawner.run_jobs();
        assert_eq!(block_on(task).unwrap(), 41);
        assert_eq!(block_on(job).unwrap(), 42);
    }
}
