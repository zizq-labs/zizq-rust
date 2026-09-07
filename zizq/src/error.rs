// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Error type returned by every fallible client operation.

use thiserror::Error;

/// The unified error type for the Zizq client.
///
/// Most variants wrap an underlying source error (URL parsing, HTTP
/// transport, encoding/decoding). [`ZizqError::Response`] is produced
/// when the server returns a non-2xx status — the raw body is captured
/// for diagnostics.
///
/// Every server error response is the same variant rather than a family of
/// per-status ones. The predicates are conveniences over pattern matching
/// and comparing the status yourself: [`is_conflict`], [`is_forbidden`],
/// [`is_retryable`], etc.
///
/// [`is_conflict`]: ZizqError::is_conflict
/// [`is_forbidden`]: ZizqError::is_forbidden
/// [`is_retryable`]: ZizqError::is_retryable
#[derive(Debug, Error)]
pub enum ZizqError {
    /// The builder was finalised without calling [`ClientBuilder::url`].
    ///
    /// [`ClientBuilder::url`]: crate::ClientBuilder::url
    #[error("client URL is required")]
    MissingUrl,

    /// The supplied URL failed to parse.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// The underlying HTTP transport returned an error — dial failure,
    /// timeout, connection reset, TLS error, and so on.
    #[error("HTTP transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// The server returned a non-2xx response. `message` is the raw
    /// body as UTF-8 lossy text.
    #[error("server returned HTTP {status}: {message}")]
    Response {
        /// HTTP status code returned by the server.
        status: u16,
        /// Response body, as UTF-8 lossy text.
        message: String,
    },

    /// The request body could not be serialised in the configured
    /// [`Format`].
    ///
    /// [`Format`]: crate::Format
    #[error("failed to serialize request: {0}")]
    Encode(String),

    /// The response body could not be deserialised in the configured
    /// [`Format`].
    ///
    /// [`Format`]: crate::Format
    #[error("failed to deserialize response: {0}")]
    Decode(String),

    /// The [`WorkerBuilder`] was finalised without calling
    /// [`WorkerBuilder::client`].
    ///
    /// [`WorkerBuilder`]: crate::WorkerBuilder
    /// [`WorkerBuilder::client`]: crate::WorkerBuilder::client
    #[error("worker client is required")]
    MissingClient,

    /// The [`WorkerBuilder`] was finalised without calling
    /// [`WorkerBuilder::handler`].
    ///
    /// [`WorkerBuilder`]: crate::WorkerBuilder
    /// [`WorkerBuilder::handler`]: crate::WorkerBuilder::handler
    #[error("worker handler is required")]
    MissingHandler,

    /// A bulk [`PatchJobsBuilder`] was awaited without calling
    /// [`PatchJobsBuilder::patch`] to supply the update to apply.
    ///
    /// [`PatchJobsBuilder`]: crate::PatchJobsBuilder
    /// [`PatchJobsBuilder::patch`]: crate::PatchJobsBuilder::patch
    #[error("bulk patch requires a patch — call .patch(...) before awaiting")]
    MissingPatch,

    /// A TLS root certificate or client identity could not be loaded
    /// — typically malformed PEM passed to the client builder's TLS
    /// configuration.
    #[error("TLS configuration error: {0}")]
    Tls(String),
}

/// Classifying a failure.
///
/// Every server error response is a single [`ZizqError::Response`] variant
/// carrying its status, so these predicates exist to save you matching
/// on magic numbers.
impl ZizqError {
    /// The HTTP status, for an error response. `None` for every other
    /// variant, since those never reached the server or never came
    /// back from it.
    ///
    /// ```
    /// # use zizq::ZizqError;
    /// let err = ZizqError::Response { status: 409, message: String::new() };
    /// assert_eq!(err.status(), Some(409));
    /// assert_eq!(ZizqError::MissingUrl.status(), None);
    /// ```
    pub fn status(&self) -> Option<u16> {
        match self {
            ZizqError::Response { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// A `404` — a resource was specified that does not exist.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(404)
    }

    /// A `409`.
    ///
    /// The server refused to process an operation because doing so would
    /// create an integrity issue (e.g. duplicate or orphaned resource).
    ///
    /// In some cases, such as where all that matters is existence, this may
    /// be interpreted as a success.
    pub fn is_conflict(&self) -> bool {
        self.status() == Some(409)
    }

    /// A `403` — the server refused a licensed feature.
    ///
    /// Unique jobs, batches, cron scheduling and budgets are all Pro
    /// features.
    pub fn is_forbidden(&self) -> bool {
        self.status() == Some(403)
    }

    /// A `400` or `422` — the server rejected the request itself.
    pub fn is_invalid_request(&self) -> bool {
        matches!(self.status(), Some(400 | 422))
    }

    /// A `406` or `415` — content negotiation failed.
    ///
    /// This indicates a client bug rather than bad input.
    pub fn is_unsupported_format(&self) -> bool {
        matches!(self.status(), Some(406 | 415))
    }

    /// Any `4xx`.
    pub fn is_client_error(&self) -> bool {
        matches!(self.status(), Some(400..=499))
    }

    /// Any `5xx`.
    pub fn is_server_error(&self) -> bool {
        matches!(self.status(), Some(500..=599))
    }

    /// The request never completed — connection refused, timeout, DNS
    /// failure, TLS handshake failure.
    pub fn is_transport(&self) -> bool {
        matches!(self, ZizqError::Transport(_))
    }

    /// Whether the same request is worth sending again.
    ///
    /// Transport failures and `5xx` are transient; everything else is
    /// permanent. A rejected request will be rejected identically no
    /// matter how many times it is sent, and a body that could not be
    /// serialised will not serialise on the second attempt either.
    ///
    /// ```
    /// # use zizq::ZizqError;
    /// let flaky = ZizqError::Response { status: 503, message: String::new() };
    /// let fatal = ZizqError::Response { status: 422, message: String::new() };
    /// assert!(flaky.is_retryable());
    /// assert!(!fatal.is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        self.is_transport() || self.is_server_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16) -> ZizqError {
        ZizqError::Response {
            status,
            message: String::new(),
        }
    }

    #[test]
    fn status_is_only_carried_by_response() {
        assert_eq!(response(404).status(), Some(404));
        assert_eq!(ZizqError::MissingUrl.status(), None);
        assert_eq!(ZizqError::Encode("bad".into()).status(), None);
    }

    #[test]
    fn named_statuses_match_only_themselves() {
        assert!(response(404).is_not_found());
        assert!(!response(409).is_not_found());

        assert!(response(409).is_conflict());
        assert!(!response(404).is_conflict());

        assert!(response(403).is_forbidden());
        assert!(!response(401).is_forbidden());
    }

    #[test]
    fn invalid_request_covers_both_rejection_statuses() {
        assert!(response(400).is_invalid_request());
        assert!(response(422).is_invalid_request());
        assert!(!response(409).is_invalid_request());
    }

    #[test]
    fn unsupported_format_covers_both_negotiation_statuses() {
        assert!(response(406).is_unsupported_format());
        assert!(response(415).is_unsupported_format());
        assert!(!response(400).is_unsupported_format());
    }

    #[test]
    fn error_classes_stop_at_their_boundaries() {
        assert!(!response(399).is_client_error());
        assert!(response(400).is_client_error());
        assert!(response(499).is_client_error());
        assert!(!response(500).is_client_error());

        assert!(!response(499).is_server_error());
        assert!(response(500).is_server_error());
        assert!(response(599).is_server_error());
        assert!(!response(600).is_server_error());
    }

    // The policy the worker applies to acknowledgements, and the same
    // one every other Zizq client uses: transient means transport or
    // 5xx, and nothing else.
    #[test]
    fn only_transport_and_server_errors_are_retryable() {
        assert!(response(500).is_retryable());
        assert!(response(503).is_retryable());

        assert!(!response(409).is_retryable());
        assert!(!response(422).is_retryable());
        assert!(!ZizqError::Encode("bad".into()).is_retryable());
        assert!(!ZizqError::Decode("bad".into()).is_retryable());
        assert!(!ZizqError::MissingUrl.is_retryable());
    }
}
