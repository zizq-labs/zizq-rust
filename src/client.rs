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

use crate::enqueue::EnqueueBuilder;
use crate::error::ZizqError;
use crate::format::Format;
use crate::job::JobKind;
use crate::resources::{BackoffConfig, Job, RetentionConfig};
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

#[derive(Debug)]
pub(crate) struct Inner {
    /// The server base URL e.g. "http://localhost:7890"
    pub(crate) base_url: Url,

    /// Whether or not we're using Json or MessagePack.
    pub(crate) format: Format,

    /// Persistent multiplexing HTTP/2 client.
    /// Supports h2c so HTTP/2 can be used without TLS.
    pub(crate) http2: reqwest::Client,

    // Placeholder for a separate HTTP/1.1 pool, used later by the
    // streaming /jobs/take endpoint. When wired up it will share the
    // same connect_timeout / read_timeout / tcp_keepalive values as
    // http2 — read_timeout is the idle-detection knob for the
    // long-lived stream, kept fresh by the server's heartbeats.
    #[allow(dead_code)]
    pub(crate) http1: Option<reqwest::Client>,
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
    pub fn enqueue<T: JobKind>(&self, payload: T) -> EnqueueBuilder<'_, T> {
        EnqueueBuilder::new(self, payload)
    }

    /// Submit a resolved enqueue request and return the resulting
    /// [`Job`]. Owns URL construction, body encoding, headers, dispatch,
    /// status handling, and response decoding.
    ///
    /// This is the transport-layer entry point used by
    /// [`EnqueueBuilder`]; it knows nothing about [`JobKind`] or
    /// per-type defaults — those have already been folded into the
    /// supplied [`EnqueueRequest`]. Non-generic by design so the
    /// resulting future is `Send` without imposing constraints on the
    /// user's payload type.
    pub(crate) async fn enqueue_raw(&self, req: EnqueueRequest) -> Result<Job, ZizqError> {
        let bytes = encode_body(&req, self.inner.format)?;

        let url = self
            .inner
            .base_url
            .join("/jobs")
            .map_err(ZizqError::InvalidUrl)?;
        let response = self
            .inner
            .http2
            .post(url)
            .header(
                reqwest::header::CONTENT_TYPE,
                self.inner.format.content_type(),
            )
            .header(reqwest::header::ACCEPT, self.inner.format.content_type())
            .body(bytes)
            .send()
            .await?;

        let status = response.status();
        let response_format = self.response_format(&response);
        let body_bytes = response.bytes().await?;

        if !status.is_success() {
            let message = extract_error_message(&body_bytes, response_format);
            return Err(ZizqError::Response {
                status: status.as_u16(),
                message,
            });
        }

        decode_body::<Job>(&body_bytes, response_format)
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

/// Server-emitted error body. The API guarantees the `error` field is
/// always present on error responses; richer structured fields (e.g.
/// `supported` on 406) are intentionally discarded — if the user needs
/// them later we can swap the model out.
#[derive(serde::Deserialize)]
struct ApiError {
    error: String,
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

/// Raw API format body for a single enqueue request.
///
/// Constructed by [`EnqueueBuilder`] from its resolved per-job
/// parameters and handed to [`Client::enqueue_raw`]. The payload is
/// pre-serialised by the builder into a [`serde_json::Value`] so this
/// struct is fully owned, non-generic, and `Send` — which keeps the
/// returned future `Send` regardless of the original payload type.
///
/// Field names match the API's snake_case shape; `Option` fields are
/// omitted from the wire when `None`.
#[derive(Serialize)]
pub(crate) struct EnqueueRequest {
    /// The raw job type used in the API.
    #[serde(rename = "type")]
    pub(crate) job_type: &'static str,

    /// Queue this job is placed on.
    pub(crate) queue: String,

    /// Arbitrary application-defined JSON-compatible payload.
    pub(crate) payload: serde_json::Value,

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

        Ok(Client {
            inner: Arc::new(Inner {
                base_url,
                format: self.format.unwrap_or_default(),
                http2,
                http1: None,
            }),
        })
    }
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
