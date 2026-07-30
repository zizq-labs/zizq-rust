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
