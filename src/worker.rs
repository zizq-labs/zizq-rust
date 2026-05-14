// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Long-running job consumer — the [`Worker`] orchestrates a
//! taking jobs from the server, dispatching them to a handler with
//! bounded concurrency, batches successful acknowledgements, and
//! reports failures.
//!
//! The worker auto-reconnects on transient disconnects (with
//! exponential backoff up to 30s, capped), and retries
//! acknowledgements / failure reports indefinitely on transport and
//! 5xx errors. 4xx responses are treated as terminal: the affected
//! batch / report is dropped and the worker continues.
//!
//! Shutdown is bounded: the worker waits up to
//! [`WorkerBuilder::shutdown_timeout`] (default 60s) for in-flight
//! handlers and pending acks to drain. Anything still running when
//! the deadline fires is aborted; in-flight jobs that haven't been
//! acked will be redelivered by the server for other workers to
//! process.
//!
//! [`WorkerBuilder::shutdown_timeout`]: crate::WorkerBuilder::shutdown_timeout

use std::any::type_name;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::client::Client;
use crate::error::ZizqError;
use crate::resources::Job;
use crate::take::TakeStream;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);

/// Captured error info from a handler invocation.
///
/// Built from the handler's concrete error type via
/// [`std::any::type_name`] and [`Display`](std::fmt::Display); the
/// `type_name` is monomorphised at the handler's call site so it
/// reflects the actual error type chosen by the user (e.g.
/// `"myapp::errors::WorkerError"`), unless the handler returns a
/// type-erased error wrapper like `anyhow::Error` — in which case
/// the wrapper's chain is captured in `message` and `type_name` is
/// the wrapper itself.
///
/// Note: backtraces are not captured yet. Doing so generically across
/// error types requires `std::error::Error::provide` /
/// `std::error::request_ref`, which are gated behind the unstable
/// `error_generic_member_access` feature
/// (<https://github.com/rust-lang/rust/issues/99301>). Once that
/// stabilises we can add a `backtrace` field that picks up traces
/// from `anyhow`, `eyre`, and `thiserror`-derived types
/// automatically.
#[derive(Debug, Clone)]
pub struct HandlerError {
    /// Static type name of the handler's error type, as seen at the
    /// handler's call site.
    pub type_name: &'static str,

    /// Human-readable error message from `Display`. For wrappers that
    /// format chains (anyhow, eyre), this includes the full chain.
    pub message: String,
}

impl HandlerError {
    /// Build a `HandlerError` from any `std::error::Error`. Captures
    /// the static type name of `E` and the `Display` form of the
    /// value. `pub(crate)` so the [`Router`] can reuse it for its
    /// own synthesised errors.
    ///
    /// [`Router`]: crate::Router
    pub(crate) fn from_typed<E: StdError + Send + Sync + 'static>(e: E) -> Self {
        Self {
            type_name: type_name::<E>(),
            message: e.to_string(),
        }
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.type_name)
    }
}

impl StdError for HandlerError {}

/// Trait implemented by anything that can process a [`Job`] and
/// return a `Result`.
///
/// A blanket impl is provided for any closure of the shape
/// `Fn(Job) -> impl Future<Output = Result<(), E>>`, where `E` is
/// any `std::error::Error` that's `Send + Sync + 'static`. So most
/// users never name this trait directly — they hand a closure to
/// [`WorkerBuilder::handler`] and the blanket impl applies.
///
/// Both common closure spellings work:
///
/// ```no_run
/// # use std::convert::Infallible;
/// # use zizq::Job;
/// // Classic form — outer closure returns an `async move` block.
/// let h1 = move |_job: Job| async move { Ok::<(), Infallible>(()) };
///
/// // Async closure (stable since Rust 1.85).
/// let h2 = async |_job: Job| { Ok::<(), Infallible>(()) };
/// ```
///
/// The trait exists so future composable handlers (e.g. the planned
/// `Router`) can be plugged in alongside closures.
pub trait JobHandler: Send + Sync + 'static {
    /// Process one [`Job`]. Returns `Ok(())` on success (the worker
    /// will handle acknowledgement) or `Err` with a [`HandlerError`]
    /// (the worker notify of the failure so any backoff can be
    /// applied).
    fn handle(&self, job: Job) -> Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>;
}

impl<F, Fut, E> JobHandler for F
where
    F: Fn(Job) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: StdError + Send + Sync + 'static,
{
    fn handle(&self, job: Job) -> Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>> {
        let fut = self(job);
        Box::pin(async move { fut.await.map_err(HandlerError::from_typed) })
    }
}

/// Long-running consumer that streams jobs from the server and
/// dispatches them to a [`JobHandler`].
///
/// Construct via [`Worker::builder`] then start with [`Worker::run`].
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use zizq::{Client, Worker};
/// # use zizq::Job;
/// # use std::convert::Infallible;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::builder().url("http://127.0.0.1:7890").build()?;
///
/// let worker = Worker::builder()
///     .client(client.clone())
///     .concurrency(16)
///     .queues(["emails"])
///     .prefetch(32)
///     .handler(|_job: Job| async move {
///         // ... do the work ...
///         Ok::<(), Infallible>(())
///     })
///     .build()?;
///
/// // Shutdown after 30 seconds; in production you'd hook
/// // `tokio::signal::ctrl_c()` or a `CancellationToken` here.
/// let shutdown = tokio::time::sleep(Duration::from_secs(30));
/// worker.run(shutdown).await?;
/// # Ok(()) }
/// ```
pub struct Worker {
    /// Shared client that the worker uses to speak to the server.
    client: Client,
    /// Generic handler implementation to process each job.
    handler: Arc<dyn JobHandler>,
    /// Maximum number of in-flight handler tasks.
    concurrency: usize,
    /// Which queues to listen for work on. Empty means all queues.
    queues: Vec<String>,
    /// The maximum number of in-flight, unacknowledged jobs the server
    /// will send before pausing. Resolved at build time — defaults to
    /// `concurrency` if not explicitly set, so dispatch isn't
    /// accidentally serialised by the server's own default of 1.
    prefetch: u32,
    /// True if the worker should automatically reconnect on transient
    /// connection failures.
    reconnect: bool,
    /// The deadline after which graceful shutdown is aborted.
    shutdown_timeout: Duration,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("concurrency", &self.concurrency)
            .field("queues", &self.queues)
            .field("prefetch", &self.prefetch)
            .field("reconnect", &self.reconnect)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl Worker {
    /// Start building a new worker. Equivalent to
    /// [`WorkerBuilder::default`].
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::default()
    }

    /// Start the worker. Resolves when:
    ///
    /// - the `shutdown` future resolves (graceful shutdown), or
    /// - the initial take connect returns a 4xx (hard config error,
    ///   returned as `Err`), or
    /// - reconnect is disabled and the take stream ends or errors.
    ///
    /// In all cases the worker drains in-flight handlers and pending
    /// acks before returning, bounded by [`WorkerBuilder::shutdown_timeout`].
    /// Handlers still running after the deadline are aborted; their
    /// jobs will be redelivered by the server for other workers to process.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> Result<(), ZizqError> {
        let mut shutdown = Box::pin(shutdown);
        let mut inflight: JoinSet<()> = JoinSet::new();
        let (ack_tx, ack_rx) = mpsc::channel::<String>(self.concurrency * 2);
        let ack_handle = tokio::spawn(ack_processor(self.client.clone(), ack_rx));

        let (stream, result) = self
            .dispatch_loop(&mut inflight, &ack_tx, shutdown.as_mut())
            .await;

        // Bounded drain. For graceful shutdown the take stream is
        // held open (Some) while in-flight handlers complete and
        // their acks flush via the http2 pool — if we dropped it
        // first, the server would see the disconnect and start
        // requeueing the still-in-flight jobs before our acks
        // landed. The stream is dropped immediately after the drain
        // completes (or after we abort on timeout).
        //
        // For the disconnect / reconnect-disabled paths the stream
        // is already gone (None) — we rely on the ack processor's
        // retry loop and the server's in-flight timeout to keep
        // things consistent there.
        let drain = async {
            while inflight.join_next().await.is_some() {}
            drop(ack_tx);
            let _ = ack_handle.await;
        };
        if tokio::time::timeout(self.shutdown_timeout, drain)
            .await
            .is_err()
        {
            log::warn!(
                "zizq: worker shutdown deadline ({:?}) exceeded; aborting remaining handlers",
                self.shutdown_timeout
            );
            inflight.abort_all();
        }

        drop(stream);

        result
    }

    /// Outer reconnect loop. Returns the still-live take stream
    /// (`Some`) alongside the result for the graceful-shutdown case —
    /// `run()` holds it open while it drains in-flight handlers and
    /// acks, then drops it. For all other exit paths the stream is
    /// already gone (`None`).
    async fn dispatch_loop<F: Future<Output = ()>>(
        &self,
        inflight: &mut JoinSet<()>,
        ack_tx: &mpsc::Sender<String>,
        mut shutdown: Pin<&mut F>,
    ) -> (Option<TakeStream>, Result<(), ZizqError>) {
        let mut backoff = INITIAL_BACKOFF;

        loop {
            // Open the take stream.
            let take = self
                .client
                .take()
                .queues(self.queues.clone())
                .prefetch(self.prefetch);

            let mut stream = match take.await {
                Ok(s) => {
                    backoff = INITIAL_BACKOFF;
                    s
                }
                Err(ZizqError::Response { status, message }) if (400..500).contains(&status) => {
                    return (None, Err(ZizqError::Response { status, message }));
                }
                Err(e) => {
                    if !self.reconnect {
                        return (None, Err(e));
                    }
                    log::warn!("zizq: take connect failed ({e}); reconnecting in {backoff:?}");
                    if sleep_or_shutdown(backoff, shutdown.as_mut()).await {
                        return (None, Ok(()));
                    }
                    backoff = next_backoff(backoff);
                    continue;
                }
            };

            // Run one session against this stream. The session
            // borrows the stream; on graceful shutdown we hand it
            // back to the caller so it can keep the connection
            // alive during the drain.
            match self
                .dispatch_session(&mut stream, inflight, ack_tx, shutdown.as_mut())
                .await
            {
                SessionEnd::Shutdown => return (Some(stream), Ok(())),
                SessionEnd::StreamEnded => {
                    // The stream's already done — drop it.
                    drop(stream);
                    if !self.reconnect {
                        return (None, Ok(()));
                    }
                    log::info!("zizq: take stream ended; reconnecting in {backoff:?}");
                    if sleep_or_shutdown(backoff, shutdown.as_mut()).await {
                        return (None, Ok(()));
                    }
                    backoff = next_backoff(backoff);
                }
                SessionEnd::TransportError(e) => {
                    drop(stream);
                    if !self.reconnect {
                        return (None, Err(e));
                    }
                    log::warn!("zizq: take stream error ({e}); reconnecting in {backoff:?}");
                    if sleep_or_shutdown(backoff, shutdown.as_mut()).await {
                        return (None, Ok(()));
                    }
                    backoff = next_backoff(backoff);
                }
            }
        }
    }

    /// One stream's lifecycle. Pulls jobs from the stream and spawns
    /// handlers into the shared `inflight` set. The set persists
    /// across sessions, so reconnects don't wait for in-flight
    /// handlers to complete. Borrows the stream rather than taking
    /// ownership so the caller controls when it gets dropped — the
    /// graceful-shutdown path keeps it alive past this call.
    async fn dispatch_session<F: Future<Output = ()>>(
        &self,
        stream: &mut TakeStream,
        inflight: &mut JoinSet<()>,
        ack_tx: &mpsc::Sender<String>,
        mut shutdown: Pin<&mut F>,
    ) -> SessionEnd {
        loop {
            // Block while at capacity, but honour shutdown so we
            // don't pull from the stream after shutdown is requested
            // even if the inflight set later drains.
            while inflight.len() >= self.concurrency {
                tokio::select! {
                    biased;
                    _ = shutdown.as_mut() => return SessionEnd::Shutdown,
                    _ = inflight.join_next() => {}
                }
            }

            let job = tokio::select! {
                biased;
                _ = shutdown.as_mut() => return SessionEnd::Shutdown,
                next = stream.try_next() => match next {
                    Ok(Some(j)) => j,
                    Ok(None) => return SessionEnd::StreamEnded,
                    Err(e) => return SessionEnd::TransportError(e),
                }
            };

            inflight.spawn(handle_job(
                self.client.clone(),
                self.handler.clone(),
                ack_tx.clone(),
                job,
            ));
        }
    }
}

enum SessionEnd {
    Shutdown,
    StreamEnded,
    TransportError(ZizqError),
}

/// Fluent builder for a [`Worker`].
///
/// `client` and `handler` are required; everything else has a
/// sensible default.
///
/// # Examples
///
/// ```no_run
/// # use std::convert::Infallible;
/// # use std::time::Duration;
/// # use zizq::{Client, Job, Worker};
/// # fn build(client: Client) -> Result<Worker, zizq::ZizqError> {
/// Worker::builder()
///     .client(client)
///     .concurrency(8)
///     .queues(["emails", "urgent"])
///     .prefetch(16)
///     .shutdown_timeout(Duration::from_secs(30))
///     .handler(|_job: Job| async move { Ok::<(), Infallible>(()) })
///     .build()
/// # }
/// ```
pub struct WorkerBuilder {
    /// The shared client to which all worker requests are sent.
    client: Option<Client>,
    /// Generic async handler for each job.
    handler: Option<Arc<dyn JobHandler>>,
    /// Maximum number of in-flight handler tasks.
    concurrency: usize,
    /// Which queues to listen to. Empty means all queues.
    queues: Vec<String>,
    /// Maximum number of jobs the server can send without
    /// acknowledgement. The default is equal to the
    /// concurrency. This should never be set to a value
    /// lower than the concurrency as doing so holds back
    /// jobs.
    prefetch: Option<u32>,
    /// True if the worker should auto-reconnect on transient
    /// connection failures.
    reconnect: bool,
    /// The deadline for a graceful shutdown.
    shutdown_timeout: Duration,
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        Self {
            client: None,
            handler: None,
            concurrency: 1,
            queues: Vec::new(),
            prefetch: None,
            reconnect: true,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

impl WorkerBuilder {
    /// Set the shared [`Client`] used for all worker-specific requests.
    /// Required.
    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the generic async job handler. Required. Accepts any value
    /// implementing [`JobHandler`] — including closures of the form
    /// `Fn(Job) -> impl Future<Output = Result<(), E>>` via the
    /// blanket impl.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::convert::Infallible;
    /// # use zizq::{Client, Job, Worker};
    /// # fn build(client: Client) -> Result<(), zizq::ZizqError> {
    /// // Classic form — outer closure returns an `async move` block.
    /// Worker::builder()
    ///     .client(client.clone())
    ///     .handler(|_job: Job| async move {
    ///         // ... do the work ...
    ///         Ok::<(), Infallible>(())
    ///     })
    ///     .build()?;
    ///
    /// // Async closure (stable since Rust 1.85).
    /// Worker::builder()
    ///     .client(client)
    ///     .handler(async |_job: Job| {
    ///         Ok::<(), Infallible>(())
    ///     })
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn handler<H: JobHandler>(mut self, handler: H) -> Self {
        self.handler = Some(Arc::new(handler));
        self
    }

    /// Maximum number of jobs processed concurrently. Defaults to 1
    /// if not set (a strict serial worker). Values of 0 are clamped
    /// to 1.
    ///
    /// Also sets [`prefetch`](Self::prefetch) to `n` if no explicit
    /// prefetch has been configured — otherwise the server's own
    /// default of 1 would silently serialise dispatch regardless of
    /// this setting. Call `.prefetch(...)` explicitly to override the
    /// prefetch limit if required.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        if self.prefetch.is_none() {
            self.prefetch = Some(self.concurrency as u32);
        }
        self
    }

    /// Restrict the underlying take stream to the given queues.
    /// Empty (the default) means all queues.
    pub fn queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Server-side prefetch limit — the maximum number of in-flight,
    /// unacknowledged jobs the server will send before waiting for
    /// acknowledgment.
    ///
    /// Defaults to the configured [`concurrency`](Self::concurrency)
    /// if not set explicitly. This value should not be set lower than
    /// `concurrency` as doing so effectively holds back work. Setting
    /// this value higher than `concurrency` holds a buffer of jobs in
    /// memory so work is immediately available without an I/O trip
    /// after each task completes.
    pub fn prefetch(mut self, n: u32) -> Self {
        self.prefetch = Some(n);
        self
    }

    /// Whether the worker should automatically reconnect when the
    /// stream ends or errors. Defaults to `true` — disable for
    /// tests / one-shot consumers that want a single session.
    ///
    /// Note: this only governs the stream reconnect loop.
    /// Ack and failure-report retries are unaffected and always
    /// retry on transient errors.
    pub fn reconnect(mut self, enabled: bool) -> Self {
        self.reconnect = enabled;
        self
    }

    /// Maximum time the worker will wait for in-flight handlers and
    /// pending acks to drain during shutdown. Defaults to 60 seconds.
    ///
    /// Handlers still running when the deadline fires are aborted;
    /// pending acks not yet sent are dropped. The server will
    /// redeliver any jobs that weren't acknowledged before its own
    /// in-flight timeout fires.
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = d;
        self
    }

    /// Finalise the builder.
    ///
    /// Returns [`ZizqError::MissingClient`] if no client was set, or
    /// [`ZizqError::MissingHandler`] if no handler was set.
    pub fn build(self) -> Result<Worker, ZizqError> {
        let client = self.client.ok_or(ZizqError::MissingClient)?;
        let handler = self.handler.ok_or(ZizqError::MissingHandler)?;
        // Concurrency is always >= 1 from Default / .concurrency(). The
        // prefetch fallback catches the "neither .concurrency() nor
        // .prefetch() was called" case — when .concurrency(n) is called
        // it eagerly sets prefetch=Some(n).
        let prefetch = self.prefetch.unwrap_or(self.concurrency as u32);

        Ok(Worker {
            client,
            handler,
            concurrency: self.concurrency,
            queues: self.queues,
            prefetch,
            reconnect: self.reconnect,
            shutdown_timeout: self.shutdown_timeout,
        })
    }
}

// --- Internal task functions ---

async fn handle_job(
    client: Client,
    handler: Arc<dyn JobHandler>,
    ack_tx: mpsc::Sender<String>,
    job: Job,
) {
    let job_id = job.id.clone();
    match handler.handle(job).await {
        Ok(()) => {
            // Best-effort send. The ack processor batches and retries;
            // if the channel is closed (worker has shut down past the
            // drain phase) the job won't be acked and the server will
            // redeliver it.
            if ack_tx.send(job_id).await.is_err() {
                log::warn!("zizq: ack channel closed before send");
            }
        }
        Err(handler_err) => {
            // Inline retry — keeps the JoinSet slot held until the
            // failure report is durable. Server-down means the take
            // stream is also down, so we wouldn't be dispatching new
            // jobs into the slot anyway; in steady state nacks succeed
            // immediately and the slot frees right away.
            let mut backoff = INITIAL_BACKOFF;
            loop {
                let req = client
                    .report_failure(job_id.clone(), handler_err.message.clone())
                    .error_type(handler_err.type_name);
                match req.await {
                    Ok(_) => break,
                    Err(ZizqError::Response { status, message })
                        if (400..500).contains(&status) =>
                    {
                        log::error!(
                            "zizq: failure report for {job_id} rejected as 4xx ({status}): {message}; dropping"
                        );
                        break;
                    }
                    Err(e) => {
                        log::warn!(
                            "zizq: failure report for {job_id} failed ({e}); retrying after {backoff:?}"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = next_backoff(backoff);
                    }
                }
            }
        }
    }
}

async fn ack_processor(client: Client, mut rx: mpsc::Receiver<String>) {
    loop {
        // Block until at least one id is available, or the channel
        // closes.
        let first = match rx.recv().await {
            Some(id) => id,
            None => return,
        };
        let mut batch = vec![first];
        // Drain whatever else is immediately available — this is the
        // batching mechanism: any ids that arrived while we were
        // blocked on `recv` come out in one bulk request.
        while let Ok(id) = rx.try_recv() {
            batch.push(id);
        }

        // Retry indefinitely on transient / 5xx errors. Treat 4xx as
        // terminal (the request itself is unacceptable; retrying
        // won't help).
        let mut backoff = INITIAL_BACKOFF;
        loop {
            match client.report_success_bulk(batch.clone()).await {
                Ok(()) => break,
                Err(ZizqError::Response { status, message }) if (400..500).contains(&status) => {
                    log::error!(
                        "zizq: bulk ack rejected as 4xx ({status}): {message}; dropping batch of {} ids",
                        batch.len()
                    );
                    break;
                }
                Err(e) => {
                    log::warn!("zizq: bulk ack failed ({e}); retrying after {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = next_backoff(backoff);
                }
            }
        }
    }
}

fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(MAX_BACKOFF)
}

async fn sleep_or_shutdown<F: Future<Output = ()>>(
    duration: Duration,
    shutdown: Pin<&mut F>,
) -> bool {
    tokio::select! {
        biased;
        _ = shutdown => true,
        _ = tokio::time::sleep(duration) => false,
    }
}
