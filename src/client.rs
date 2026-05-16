// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! The [`Client`] handle and its [`ClientBuilder`].
//!
//! A [`Client`] wraps an [`Arc`]-shared inner state — including the
//! configured base URL, API [`Format`], and the underlying
//! [`reqwest::Client`] connection pools — so it's cheap to clone and
//! safe to share across tasks.

use std::sync::Arc;
use std::time::Duration;

use reqwest::IntoUrl;
use serde::de::DeserializeOwned;
use serde::Serialize;
use url::Url;

use crate::bulk_enqueue::BulkEnqueueBuilder;
use crate::count_jobs::CountJobsBuilder;
use crate::enqueue::EnqueueBuilder;
use crate::error::ZizqError;
use crate::failure::FailureBuilder;
use crate::format::Format;
use crate::job::JobKind;
use crate::list_jobs::ListJobsBuilder;
use crate::resources::{BackoffConfig, Job, RetentionConfig};
use crate::take::{TakeBuilder, TakeStream};
use crate::unique_key::UniqueScope;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Connection handle to a Zizq server.
///
/// Construct with [`Client::builder`]. The handle is internally an
/// [`Arc`]-backed shared state, so cloning is cheap and the clones all
/// share the same connection pools. Spawn it into tasks freely.
///
/// # Examples
///
/// ```
/// use zizq::Client;
///
/// let client = Client::builder()
///     .url("http://127.0.0.1:7890")
///     .build()
///     .expect("valid url");
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Start building a new client. Equivalent to
    /// [`ClientBuilder::default`].
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Begin enqueueing a job. Returns an [`EnqueueBuilder`] that can
    /// chain per-job overrides ([`EnqueueBuilder::queue`],
    /// [`EnqueueBuilder::priority`], etc) before being awaited to send
    /// the request.
    ///
    /// The payload type must implement [`JobKind`], which supplies the
    /// API-level type name and any per-type defaults.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use serde::{Deserialize, Serialize};
    /// use zizq::{Client, JobKind};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct SendEmail { to: String }
    ///
    /// impl JobKind for SendEmail {
    ///     const NAME: &'static str = "send_email";
    ///     const QUEUE: &'static str = "emails";
    /// }
    ///
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let job = client
    ///     .enqueue(SendEmail { to: "alice@example.com".into() })
    ///     .priority(50)
    ///     .await?;
    ///
    /// println!("enqueued {} on {}", job.id, job.queue);
    /// # Ok(()) }
    /// ```
    pub fn enqueue<T: JobKind>(&self, payload: T) -> EnqueueBuilder<'_, T> {
        EnqueueBuilder::new(self, payload)
    }

    /// Begin a bulk enqueue. Returns a [`BulkEnqueueBuilder`] that
    /// collects per-job [`EnqueueBuilder`]s via [`BulkEnqueueBuilder::add`]
    /// (chainable, consuming) or [`BulkEnqueueBuilder::push`] (mutating,
    /// loop-friendly), and is finalised by awaiting it to send a single
    /// `POST /jobs/bulk` request.
    ///
    /// Mixed [`JobKind`]s in one batch are fine — each per-job builder
    /// is resolved (defaults applied, payload serialised) at the moment
    /// it's added, then type-erased into the batched request body.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use serde::{Deserialize, Serialize};
    /// use zizq::{Client, JobKind};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct SendEmail { to: String }
    /// impl JobKind for SendEmail {
    ///     const NAME: &'static str = "send_email";
    /// }
    ///
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// // Inline / chained:
    /// let jobs = client
    ///     .enqueue_bulk()
    ///     .add(client.enqueue(SendEmail { to: "a@x".into() }).priority(50))
    ///     .add(client.enqueue(SendEmail { to: "b@x".into() }))
    ///     .await?;
    /// assert_eq!(jobs.len(), 2);
    ///
    /// // Loop-style:
    /// let mut batch = client.enqueue_bulk();
    /// for i in 0..100 {
    ///     batch.push(client.enqueue(SendEmail { to: format!("u{i}@x") }));
    /// }
    /// let jobs = batch.await?;
    /// assert_eq!(jobs.len(), 100);
    /// # Ok(()) }
    /// ```
    pub fn enqueue_bulk(&self) -> BulkEnqueueBuilder<'_> {
        BulkEnqueueBuilder::new(self)
    }

    /// Acknowledge a job as successfully completed.
    ///
    /// Until a job is acknowledged (or failed via [`Client::report_failure`])
    /// the server does not send any new jobs to the connected Worker as the
    /// job remains in the in-flight set, which counts against the worker's
    /// prefetch limit and the server's global in-flight limit. Both the
    /// durable storage and the at-least-once delivery model mean the same job
    /// will be automatically redelivered if the client disconnects before
    /// acknowledging — handlers should be idempotent by design.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::Client;
    /// # async fn run(client: &Client, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// // After successfully processing a job, acknowledge it so the
    /// // server stops considering it in-flight and resumes sending new
    /// // work to this worker.
    /// client.report_success(job_id).await?;
    /// # Ok(()) }
    /// ```
    pub async fn report_success(&self, id: &str) -> Result<(), ZizqError> {
        let url = self.url(&["jobs", id, "success"]);
        let response = self.send(reqwest::Method::POST, url, None).await?;
        self.expect_status(response, &[reqwest::StatusCode::NO_CONTENT])
            .await
    }

    /// Acknowledge multiple jobs as successfully completed in one
    /// request.
    ///
    /// The server may answer 204 (all acknowledged) or 422 (some IDs not
    /// found, typically already acked or purged); both are treated as
    /// success as there is not further action for the client to take, so
    /// retries of this operation are safe and idempotent. Other statuses are
    /// surfaced as [`ZizqError::Response`].
    ///
    /// Workers are advised to use this method for acknowledgement when under
    /// high throughput where ack's are being generated rapidly, as this can
    /// significantly improve throughput by reducing request traffic and LSM
    /// database transaction volume on the Zizq backend.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::Client;
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// // Acknowledge a batch of completed jobs in one request.
    /// client.report_success_bulk(["job-1", "job-2", "job-3"]).await?;
    ///
    /// // Also works with owned strings, e.g. accumulated from previous responses.
    /// let ids: Vec<String> = vec!["job-4".into(), "job-5".into()];
    /// client.report_success_bulk(ids).await?;
    /// # Ok(()) }
    /// ```
    pub async fn report_success_bulk<I, S>(&self, ids: I) -> Result<(), ZizqError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let body = BulkSuccessBody {
            ids: ids.into_iter().map(Into::into).collect(),
        };
        let bytes = encode_body(&body, self.inner.format)?;
        let url = self.url(&["jobs", "success"]);
        let response = self.send(reqwest::Method::POST, url, Some(bytes)).await?;
        self.expect_status(
            response,
            &[
                reqwest::StatusCode::NO_CONTENT,
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            ],
        )
        .await
    }

    /// Begin reporting a job failure. Returns a [`FailureBuilder`] that
    /// chains optional error details (error type, backtrace, forced
    /// retry time, kill flag) and is awaited to send the request.
    ///
    /// The response is the updated [`Job`] with its new attempt count
    /// and status — either back to `Scheduled` for another attempt, or
    /// `Dead` if the retry limit was exhausted (or the `kill` flag was
    /// set).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// # use zizq::Client;
    /// # async fn run(client: &Client, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// // Minimal — just a message; the server applies the job's backoff
    /// // and reschedules until the retry budget is exhausted.
    /// client.report_failure(job_id, "smtp timeout").await?;
    ///
    /// // With richer context and a forced retry time that bypasses
    /// // backoff.
    /// client
    ///     .report_failure(job_id, "rate limited by downstream")
    ///     .error_type("RateLimitError")
    ///     .backtrace("...stack trace from the worker...")
    ///     .retry_in(Duration::from_secs(60))
    ///     .await?;
    ///
    /// // Permanent failure — kill the job immediately regardless of
    /// // retry budget.
    /// client
    ///     .report_failure(job_id, "payload references deleted entity")
    ///     .kill()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn report_failure(
        &self,
        id: impl Into<String>,
        message: impl Into<String>,
    ) -> FailureBuilder<'_> {
        FailureBuilder::new(self, id.into(), message.into())
    }

    /// Fetch a single job by id.
    ///
    /// Returns the [`Job`] on success. A job that doesn't exist (e.g.
    /// already deleted, purged, or never enqueued) surfaces as
    /// [`ZizqError::Response`] with `status: 404`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::{Client, ZizqError};
    /// # async fn run(client: &Client, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// match client.get_job(job_id).await {
    ///     Ok(job) => println!("found {} on {}", job.id, job.queue),
    ///     Err(ZizqError::Response { status: 404, .. }) => println!("not found"),
    ///     Err(e) => return Err(e.into()),
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn get_job(&self, id: &str) -> Result<Job, ZizqError> {
        let url = self.url(&["jobs", id]);
        let response = self.send(reqwest::Method::GET, url, None).await?;
        self.parse_job_response(response).await
    }

    /// Begin a `GET /jobs` listing. Returns a [`ListJobsBuilder`]
    /// that accumulates filter / paging parameters and is awaited to
    /// fetch a single [`JobPage`]. Subsequent pages are followed via
    /// [`Client::get_page`] using the [`PageLinks::next`] /
    /// [`PageLinks::prev`] paths the server emits.
    ///
    /// [`JobPage`]: crate::JobPage
    /// [`PageLinks::next`]: crate::PageLinks::next
    /// [`PageLinks::prev`]: crate::PageLinks::prev
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::{Client, JobPage, JobStatus, Order};
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut page = client
    ///     .list_jobs()
    ///     .status([JobStatus::Ready])
    ///     .queue(["emails"])
    ///     .order(Order::Asc)
    ///     .limit(100)
    ///     .await?;
    ///
    /// loop {
    ///     for job in &page.jobs {
    ///         // ... handle each job ...
    ///         let _ = &job.id;
    ///     }
    ///     let Some(next) = page.pages.next.clone() else { break };
    ///     page = client.get_page::<JobPage>(&next).await?;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn list_jobs(&self) -> ListJobsBuilder<'_> {
        ListJobsBuilder::new(self)
    }

    /// Begin a `GET /jobs/count`. Returns a [`CountJobsBuilder`] that
    /// accumulates job-selection filters and is awaited to fetch the
    /// number of matching jobs. With no filters, counts all jobs.
    ///
    /// Shares the filter set (`status`, `queue`, `type`, `id`,
    /// `filter`) with [`Client::list_jobs`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::{Client, JobStatus};
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let ready = client.count_jobs().status([JobStatus::Ready]).await?;
    /// println!("{ready} ready jobs");
    /// # Ok(()) }
    /// ```
    pub fn count_jobs(&self) -> CountJobsBuilder<'_> {
        CountJobsBuilder::new(self)
    }

    /// Follow a server-emitted pagination path (e.g.
    /// [`PageLinks::next`]) and decode the response as `T`.
    ///
    /// The server returns pagination cursors as paths only (no host),
    /// so the path is resolved against the configured base URL. The
    /// resolved URL's host and port are validated to match the base —
    /// a malformed or protocol-relative path can't redirect the
    /// client to a different host.
    ///
    /// `T` is anything that implements [`DeserializeOwned`], typically
    /// [`JobPage`] for `GET /jobs` results. Future paginated resources
    /// (e.g. error listings) reuse this same method.
    ///
    /// [`PageLinks::next`]: crate::PageLinks::next
    /// [`JobPage`]: crate::JobPage
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::{Client, JobPage};
    /// # async fn run(client: &Client, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// // Type can be inferred from the binding…
    /// let next: JobPage = client.get_page(path).await?;
    /// // …or supplied via turbofish.
    /// let _ = client.get_page::<JobPage>(path).await?;
    /// # let _ = next;
    /// # Ok(()) }
    /// ```
    pub async fn get_page<T: DeserializeOwned>(&self, path: &str) -> Result<T, ZizqError> {
        let url = self.resolve_page_path(path)?;
        self.get_decoded(url).await
    }

    /// Permanently remove a job from the server.
    ///
    /// A job that doesn't exist surfaces as [`ZizqError::Response`] with
    /// `status: 404`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zizq::Client;
    /// # async fn run(client: &Client, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// client.delete_job(job_id).await?;
    /// # Ok(()) }
    /// ```
    pub async fn delete_job(&self, id: &str) -> Result<(), ZizqError> {
        let url = self.url(&["jobs", id]);
        let response = self.send(reqwest::Method::DELETE, url, None).await?;
        self.expect_status(response, &[reqwest::StatusCode::NO_CONTENT])
            .await
    }

    /// Begin streaming jobs from the server. Returns a [`TakeBuilder`]
    /// that chains optional filters (`.queues(...)`, `.prefetch(...)`)
    /// and is awaited to open the connection.
    ///
    /// The returned [`TakeStream`] implements
    /// [`futures_core::Stream`]; iterate via `.next().await` and stop
    /// by dropping the stream (or via `tokio::select!` for explicit
    /// cancellation). Heartbeats from the server are filtered out
    /// transparently and never reach the caller.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_util::TryStreamExt;
    /// # use zizq::Client;
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stream = client
    ///     .take()
    ///     .queues(["emails", "urgent"])
    ///     .prefetch(16)
    ///     .await?;
    ///
    /// // `try_next` returns Result<Option<Job>, ZizqError>; `?`
    /// // propagates a transport / decode error out of the loop.
    /// while let Some(job) = stream.try_next().await? {
    ///     // ... process the job ...
    ///     client.report_success(&job.id).await?;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn take(&self) -> TakeBuilder<'_> {
        TakeBuilder::new(self)
    }

    /// Submit a resolved enqueue request and return the resulting
    /// [`Job`].
    ///
    /// This is the transport-layer entry point used by
    /// [`EnqueueBuilder`]; it knows nothing about [`JobKind`] or
    /// per-type defaults — those have already been folded into the
    /// supplied [`EnqueueRequest`]. Non-generic by design so the
    /// resulting future is `Send` without imposing constraints on the
    /// user's payload type.
    pub(crate) async fn enqueue_raw(&self, req: EnqueueRequest) -> Result<Job, ZizqError> {
        let bytes = encode_body(&req, self.inner.format)?;
        let url = self.url(&["jobs"]);
        let response = self.send(reqwest::Method::POST, url, Some(bytes)).await?;
        self.parse_job_response(response).await
    }

    /// Submit a resolved bulk enqueue request and return the resulting
    /// `Vec` of [`Job`]s.
    ///
    /// This is the transport-layer entry point for `POST /jobs/bulk`.
    /// Used by [`BulkEnqueueBuilder`]; resolved per-job defaults and
    /// payload serialisation happen upstream so this method only deals
    /// in fully-owned [`EnqueueRequest`] values.
    pub(crate) async fn enqueue_bulk_raw(
        &self,
        req: BulkEnqueueRequest,
    ) -> Result<Vec<Job>, ZizqError> {
        let bytes = encode_body(&req, self.inner.format)?;
        let url = self.url(&["jobs", "bulk"]);
        let response = self.send(reqwest::Method::POST, url, Some(bytes)).await?;

        let status = response.status();
        let format = self.response_format(&response);
        let body_bytes = response.bytes().await?;
        if !status.is_success() {
            let message = extract_error_message(&body_bytes, format);
            return Err(ZizqError::Response {
                status: status.as_u16(),
                message,
            });
        }
        let resp: BulkEnqueueResponse = decode_body(&body_bytes, format)?;
        Ok(resp.jobs)
    }

    /// Submit a resolved failure request and return the updated
    /// [`Job`]. Transport-layer entry point used by [`FailureBuilder`].
    pub(crate) async fn report_failure_raw(
        &self,
        id: &str,
        req: FailureRequest,
    ) -> Result<Job, ZizqError> {
        let bytes = encode_body(&req, self.inner.format)?;
        let url = self.url(&["jobs", id, "failure"]);
        let response = self.send(reqwest::Method::POST, url, Some(bytes)).await?;
        self.parse_job_response(response).await
    }

    /// Open the streaming `/jobs/take` connection and return a
    /// [`TakeStream`] yielding decoded jobs.
    ///
    /// Uses the HTTP/1.1 pool so the long-lived stream doesn't share
    /// a multiplexed HTTP/2 connection with request/response traffic.
    /// The framing decoder is picked from the server's `Content-Type`
    /// (not the requested format).
    pub(crate) async fn take_raw(
        &self,
        queues: Vec<String>,
        prefetch: Option<usize>,
    ) -> Result<TakeStream, ZizqError> {
        let mut url = self.url(&["jobs", "take"]);
        // Only touch query_pairs_mut when we have params to add;
        // calling it unconditionally leaves a trailing `?` on the URL
        // even when no pairs are appended.
        if !queues.is_empty() || prefetch.is_some() {
            let mut q = url.query_pairs_mut();
            // The server's take endpoint takes a single comma-delimited
            // `queue` param (a `CommaSet`), not a repeated `queues`
            // pair — and rejects unknown fields outright.
            if !queues.is_empty() {
                q.append_pair("queue", &queues.join(","));
            }
            if let Some(p) = prefetch {
                q.append_pair("prefetch", &p.to_string());
            }
        }

        let response = self
            .inner
            .http1
            .get(url)
            .header(
                reqwest::header::ACCEPT,
                self.inner.format.stream_content_type(),
            )
            .send()
            .await?;

        let status = response.status();
        let format = self.response_format(&response);

        if !status.is_success() {
            let body_bytes = response.bytes().await?;
            let message = extract_error_message(&body_bytes, format);
            return Err(ZizqError::Response {
                status: status.as_u16(),
                message,
            });
        }

        Ok(TakeStream::new(Box::pin(response.bytes_stream()), format))
    }

    /// Build an absolute URL by appending the given path segments to
    /// the configured base URL. Segments are percent-encoded by the
    /// `url` crate, so IDs with reserved characters are handled
    /// correctly. Appends rather than overrides, so a base URL with a
    /// path prefix (e.g. `http://host/api`) keeps that prefix.
    pub(crate) fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.inner.base_url.clone();
        url.path_segments_mut()
            .expect("base URL is http(s)")
            .pop_if_empty()
            .extend(segments);
        url
    }

    /// GET the given URL and decode the response body as `T`.
    pub(crate) async fn get_decoded<T: DeserializeOwned>(&self, url: Url) -> Result<T, ZizqError> {
        let response = self.send(reqwest::Method::GET, url, None).await?;
        let status = response.status();
        let format = self.response_format(&response);
        let body_bytes = response.bytes().await?;
        if !status.is_success() {
            let message = extract_error_message(&body_bytes, format);
            return Err(ZizqError::Response {
                status: status.as_u16(),
                message,
            });
        }
        decode_body(&body_bytes, format)
    }

    /// Resolve a server-emitted pagination path against the
    /// configured base URL. Validates that the resolved URL's host
    /// and port match the base — guards against a malformed or
    /// protocol-relative path (e.g. `//evil.com/jobs`) redirecting
    /// the client at a different host.
    fn resolve_page_path(&self, path: &str) -> Result<Url, ZizqError> {
        let resolved = self.inner.base_url.join(path)?;
        let base = &self.inner.base_url;
        if resolved.host_str() != base.host_str()
            || resolved.port_or_known_default() != base.port_or_known_default()
        {
            return Err(ZizqError::Decode(format!(
                "page path resolved to a different host than the configured base URL: {resolved}"
            )));
        }
        Ok(resolved)
    }

    /// Issue an HTTP request with the configured format negotiation.
    /// `body` is pre-encoded bytes; pass `None` for endpoints that
    /// don't send a request body.
    async fn send(
        &self,
        method: reqwest::Method,
        url: Url,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::Response, ZizqError> {
        let mut req = self
            .inner
            .http2
            .request(method, url)
            .header(reqwest::header::ACCEPT, self.inner.format.content_type());
        if let Some(bytes) = body {
            req = req
                .header(
                    reqwest::header::CONTENT_TYPE,
                    self.inner.format.content_type(),
                )
                .body(bytes);
        }
        Ok(req.send().await?)
    }

    /// Consume a response that's expected to have one of the given
    /// status codes and no decodable body. Maps any other status to
    /// [`ZizqError::Response`] with the server-supplied message.
    async fn expect_status(
        &self,
        response: reqwest::Response,
        ok: &[reqwest::StatusCode],
    ) -> Result<(), ZizqError> {
        let status = response.status();
        if ok.contains(&status) {
            return Ok(());
        }
        let format = self.response_format(&response);
        let body_bytes = response.bytes().await?;
        let message = extract_error_message(&body_bytes, format);
        Err(ZizqError::Response {
            status: status.as_u16(),
            message,
        })
    }

    /// Consume a response that's expected to contain a [`Job`] body on
    /// success. Errors are decoded via [`extract_error_message`].
    async fn parse_job_response(&self, response: reqwest::Response) -> Result<Job, ZizqError> {
        let status = response.status();
        let format = self.response_format(&response);
        let body_bytes = response.bytes().await?;
        if !status.is_success() {
            let message = extract_error_message(&body_bytes, format);
            return Err(ZizqError::Response {
                status: status.as_u16(),
                message,
            });
        }
        decode_body::<Job>(&body_bytes, format)
    }

    /// Pick the [`Format`] to decode a response in based on its
    /// `Content-Type` header. Falls back to the configured format when
    /// the header is missing or unrecognised.
    ///
    /// Honouring `Content-Type` rather than blindly assuming the format
    /// we asked for covers servers that respond with the wrong type by
    /// mistake, and 406 Not Acceptable replies which must come back in
    /// JSON regardless of `Accept`.
    fn response_format(&self, response: &reqwest::Response) -> Format {
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(Format::from_content_type)
            .unwrap_or(self.inner.format)
    }
}

/// Fluent builder for a [`Client`].
///
/// All settings are optional except the URL. Defaults:
///
/// - Format: [`Format::MessagePack`]
/// - Connect timeout: 10s
/// - Read timeout: 30s — per-read, reset by each incoming chunk
/// - TCP keep-alive: 60s — to preserve NAT/firewall state on idle
///   connections
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use zizq::{Client, Format};
///
/// let client = Client::builder()
///     .url("http://127.0.0.1:7890")
///     .format(Format::Json)
///     .connect_timeout(Duration::from_secs(5))
///     .build()
///     .expect("valid url");
/// ```
#[derive(Default)]
pub struct ClientBuilder {
    /// Base URL of the Zizq API (e.g. "http://localhost:7890")
    url: Option<String>,

    /// Request/response format selection.
    format: Option<Format>,

    /// Deadline after which initial connection fails.
    connect_timeout: Option<Duration>,

    /// Deadline after which reads fail if no data is received.
    read_timeout: Option<Duration>,
}

impl ClientBuilder {
    /// Set the base URL of the Zizq server. Required.
    ///
    /// Accepts anything that implements [`reqwest::IntoUrl`], including
    /// `&str`, `String`, and [`url::Url`].
    pub fn url(mut self, url: impl IntoUrl) -> Self {
        self.url = Some(url.as_str().to_string());
        self
    }

    /// Override the API request/response format. Defaults to
    /// [`Format::MessagePack`].
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Override the connect timeout — how long a TCP dial may take
    /// before being abandoned. Defaults to 10 seconds.
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = Some(d);
        self
    }

    /// Override the per-read timeout — the maximum time between
    /// consecutive bytes received from the server. Reset by each
    /// incoming chunk, so heartbeats on long-lived streams keep it
    /// fresh. Defaults to 30 seconds, which is comfortably wider than
    /// any reasonable heartbeat cadence (default heartbeat iterval is
    /// 3 seconds).
    pub fn read_timeout(mut self, d: Duration) -> Self {
        self.read_timeout = Some(d);
        self
    }

    /// Finalise the builder and produce a [`Client`].
    ///
    /// Returns [`ZizqError::MissingUrl`] if no URL was supplied,
    /// [`ZizqError::InvalidUrl`] if the URL fails to parse, or
    /// [`ZizqError::Transport`] if the underlying HTTP client cannot
    /// be constructed.
    pub fn build(self) -> Result<Client, ZizqError> {
        let raw_url = self.url.ok_or(ZizqError::MissingUrl)?;
        let base_url = Url::parse(&raw_url)?;

        let connect_timeout = self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let read_timeout = self.read_timeout.unwrap_or(DEFAULT_READ_TIMEOUT);

        let http2 = reqwest::Client::builder()
            .http2_prior_knowledge()
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .tcp_keepalive(Some(DEFAULT_TCP_KEEPALIVE))
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .build()?;

        // HTTP/1.1 pool for the long-lived take stream. We force HTTP/1.1
        // with `http1_only()` rather than relying on the default — over
        // TLS, ALPN would otherwise upgrade us to HTTP/2, which adds
        // framing overhead that we've measured as a net negative on a
        // single long-lived stream. Liveness knobs match http2.
        let http1 = reqwest::Client::builder()
            .http1_only()
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .tcp_keepalive(Some(DEFAULT_TCP_KEEPALIVE))
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .build()?;

        Ok(Client {
            inner: Arc::new(Inner {
                base_url,
                format: self.format.unwrap_or_default(),
                http2,
                http1,
            }),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    /// The server base URL e.g. "http://localhost:7890"
    pub(crate) base_url: Url,

    /// Whether or not we're using Json or MessagePack.
    pub(crate) format: Format,

    /// Persistent multiplexing HTTP/2 client used for request/response
    /// endpoints (enqueue, success, failure, etc). Supports h2c so
    /// HTTP/2 can be used without TLS.
    pub(crate) http2: reqwest::Client,

    /// Separate HTTP/1.1 client used for the long-lived `/jobs/take`
    /// streaming endpoint. Sharing the http2 pool would mean the take
    /// stream and ack traffic competed on the same multiplexed
    /// connection, which is undesirable.
    pub(crate) http1: reqwest::Client,
}

/// Raw API format body for a single enqueue request.
///
/// Constructed by [`EnqueueBuilder`] from its resolved per-job
/// parameters and handed to [`Client::enqueue_raw`]. The payload is
/// pre-serialised by the builder into a [`serde_json::Value`] so this
/// struct is fully owned, non-generic, and `Send` — which keeps the
/// returned future `Send` regardless of the original payload type.
///
/// Field names match the API's snake_case shape; `Option` fields are
/// omitted from the resulting payload when `None`.
#[derive(Serialize)]
pub(crate) struct EnqueueRequest {
    /// The raw job type used in the API.
    #[serde(rename = "type")]
    pub(crate) job_type: &'static str,

    /// Queue this job is placed on.
    pub(crate) queue: String,

    /// Type-erased payload — boxed at builder time so each item walks
    /// through serde exactly once when the outer body is encoded,
    /// without an intermediate `serde_json::Value` tree. The vtable
    /// indirection is essentially free compared to the cost of an
    /// extra walk + Value-tree allocation per job.
    pub(crate) payload: Box<dyn erased_serde::Serialize + Send>,

    /// Job priority. Lower values run sooner.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) priority: Option<u32>,

    /// Optional timestamp at which this job becomes ready to run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ready_at: Option<i64>,

    /// Optional retry limit after which the job is killed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry_limit: Option<u32>,

    /// Optional backoff policy for this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) backoff: Option<BackoffConfig>,

    /// Optional retention policy for this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retention: Option<RetentionConfig>,

    /// Optional unique key for enqueue-time deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unique_key: Option<String>,

    /// Optional scope for the unique key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unique_while: Option<UniqueScope>,
}

/// Raw API format body for reporting a job failure.
///
/// Constructed by [`FailureBuilder`] and handed to
/// [`Client::report_failure_raw`]. All fields are owned, so the
/// resulting future is `Send` without further constraints.
///
/// `kill` is only emitted when `true`. Passing `false` does nothing.
#[derive(Serialize)]
pub(crate) struct FailureRequest {
    /// Arbitrary error message to be captured with the failure.
    pub(crate) message: String,

    /// Details of the type of error that caused the failure (i.e. the
    /// name of the type).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_type: Option<String>,

    /// Optional backtrace, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) backtrace: Option<String>,

    /// Optional override for when the job should be retried, overriding
    /// the job's backoff policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry_at: Option<i64>,

    /// Kill flag, set to `true` to explicitly prevent further retries.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) kill: bool,
}

/// Raw API format body for a bulk enqueue request — wraps a slice of
/// per-job [`EnqueueRequest`]s. Matches the server's expected
/// `{"jobs": [...]}` envelope.
#[derive(Serialize)]
pub(crate) struct BulkEnqueueRequest {
    pub(crate) jobs: Vec<EnqueueRequest>,
}

/// Server response shape for a bulk enqueue — the created jobs in the
/// order they were submitted.
#[derive(serde::Deserialize)]
struct BulkEnqueueResponse {
    jobs: Vec<Job>,
}

/// Raw API format body for a bulk-success (bulk ack) request.
#[derive(Serialize)]
struct BulkSuccessBody {
    ids: Vec<String>,
}

/// Server-emitted error body. The API guarantees the `error` field is
/// always present on error responses; richer structured fields (e.g.
/// `supported` on 406) are intentionally discarded — if the user needs
/// them later we can swap the model out.
#[derive(serde::Deserialize)]
struct ApiError {
    error: String,
}

/// Encode a serializable body using the configured [`Format`].
pub(crate) fn encode_body<B: Serialize>(body: &B, format: Format) -> Result<Vec<u8>, ZizqError> {
    match format {
        Format::Json => serde_json::to_vec(body).map_err(|e| ZizqError::Encode(e.to_string())),
        Format::MessagePack => {
            // Use struct-map serialization so field names are preserved on the wire,
            // matching the JSON shape and what the other clients send.
            let mut buf = Vec::new();
            let mut ser = rmp_serde::Serializer::new(&mut buf)
                .with_struct_map()
                .with_human_readable();
            body.serialize(&mut ser)
                .map_err(|e| ZizqError::Encode(e.to_string()))?;
            Ok(buf)
        }
    }
}

/// Decode response bytes using the configured [`Format`].
pub(crate) fn decode_body<R: DeserializeOwned>(
    bytes: &[u8],
    format: Format,
) -> Result<R, ZizqError> {
    match format {
        Format::Json => serde_json::from_slice(bytes).map_err(|e| ZizqError::Decode(e.to_string())),
        Format::MessagePack => {
            rmp_serde::from_slice(bytes).map_err(|e| ZizqError::Decode(e.to_string()))
        }
    }
}

/// Extract a human-readable error message from a non-2xx response body.
///
/// Tries to decode `{ "error": "..." }` in the configured format first;
/// on failure falls back to JSON (covers 406 Not Acceptable, which the
/// server must reply to in JSON since the client asked for a format
/// the server doesn't support); finally falls back to a lossy UTF-8
/// rendering of the raw bytes so we always surface *something*.
fn extract_error_message(body: &[u8], format: Format) -> String {
    // Try the assumed format.
    if let Ok(e) = decode_body::<ApiError>(body, format) {
        return e.error;
    }
    // Fallback on trying JSON.
    if format != Format::Json {
        if let Ok(e) = decode_body::<ApiError>(body, Format::Json) {
            return e.error;
        }
    }
    // Fallback on extracting UTF-8.
    String::from_utf8_lossy(body).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_requires_url() {
        let err = Client::builder().build().unwrap_err();
        assert!(matches!(err, ZizqError::MissingUrl));
    }

    #[test]
    fn build_rejects_garbage_url() {
        let err = Client::builder().url("not a url").build().unwrap_err();
        assert!(matches!(err, ZizqError::InvalidUrl(_)));
    }

    #[test]
    fn build_defaults_to_messagepack() {
        let c = Client::builder()
            .url("http://localhost:7890")
            .build()
            .unwrap();
        assert_eq!(c.inner.format, Format::MessagePack);
    }

    #[test]
    fn build_accepts_explicit_format() {
        let c = Client::builder()
            .url("http://localhost:7890")
            .format(Format::Json)
            .build()
            .unwrap();
        assert_eq!(c.inner.format, Format::Json);
    }
}
