// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Failure reporting — the [`FailureBuilder`] returned by
//! [`Client::report_failure`].
//!
//! The builder takes optional error details as chained method calls
//! and is finalised by `await`ing it (via [`IntoFuture`]). The actual
//! transport — URL, headers, encoding, dispatch — lives on
//! [`Client::report_failure_raw`].
//!
//! [`Client::report_failure`]: crate::Client::report_failure
//! [`Client::report_failure_raw`]: crate::Client::report_failure_raw

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use time::OffsetDateTime;

use crate::client::{Client, FailureRequest};
use crate::error::ZizqError;
use crate::resources::Job;
use crate::timestamp::to_ms_epoch;

/// Builder for a job-failure report.
///
/// Produced by [`Client::report_failure`]. Chain any of the optional
/// error-detail methods, then `.await` the builder to send the request
/// and receive the updated [`Job`].
///
/// [`Client::report_failure`]: crate::Client::report_failure
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use zizq::Client;
///
/// # async fn run(client: &Client, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
/// // Minimal — just a message.
/// client.report_failure(job_id, "send timed out").await?;
///
/// // With more context, forced retry, and the option to kill the job
/// // immediately (bypassing the retry budget).
/// client
///     .report_failure(job_id, "permanent failure")
///     .error_type("FatalError")
///     .backtrace("...stack trace...")
///     .retry_in(Duration::from_secs(30))
///     .kill()
///     .await?;
/// # Ok(()) }
/// ```
pub struct FailureBuilder<'a> {
    /// The client to which the failure report is ultimately sent.
    client: &'a Client,

    /// ID of the job being failed.
    job_id: String,

    /// Required human-readable failure message.
    message: String,

    /// Optional type name for the underlying error (e.g.
    /// `"TimeoutError"`).
    error_type: Option<String>,

    /// Optional stack trace / backtrace from the worker.
    backtrace: Option<String>,

    /// Optional forced retry time, as Unix ms. Bypasses the server's
    /// computed backoff.
    retry_at_ms: Option<i64>,

    /// When true, kills the job immediately regardless of retry limit.
    kill: bool,
}

impl<'a> FailureBuilder<'a> {
    /// Set the error type name (e.g. `"TimeoutError"`,
    /// `"DownstreamUnavailable"`).
    pub fn error_type(mut self, t: impl Into<String>) -> Self {
        self.error_type = Some(t.into());
        self
    }

    /// Attach a stack trace or backtrace to the failure report.
    pub fn backtrace(mut self, b: impl Into<String>) -> Self {
        self.backtrace = Some(b.into());
        self
    }

    /// Schedule the next retry at an absolute point in time, bypassing
    /// the server-computed backoff. Mutually exclusive with
    /// [`Self::retry_in`] — the last one set wins.
    pub fn retry_at(mut self, when: OffsetDateTime) -> Self {
        self.retry_at_ms = Some(to_ms_epoch(when));
        self
    }

    /// Schedule the next retry after the given duration from now.
    /// Mutually exclusive with [`Self::retry_at`] — the last one set
    /// wins.
    pub fn retry_in(mut self, delay: Duration) -> Self {
        self.retry_at_ms = Some(to_ms_epoch(OffsetDateTime::now_utc() + delay));
        self
    }

    /// Kill the job immediately regardless of its retry limit. Use
    /// for permanent failures the job cannot recover from.
    pub fn kill(mut self) -> Self {
        self.kill = true;
        self
    }

    /// Initialize the FailureBuilder with the given message.
    pub(crate) fn new(client: &'a Client, job_id: String, message: String) -> Self {
        Self {
            client,
            job_id,
            message,
            error_type: None,
            backtrace: None,
            retry_at_ms: None,
            kill: false,
        }
    }
}

impl<'a> IntoFuture for FailureBuilder<'a> {
    /// The final outcome of the Future — the updated [`Job`] with its
    /// new attempt count and status (re-scheduled, dead, etc).
    type Output = Result<Job, ZizqError>;

    /// The type of the Future itself.
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    /// Called to send the request when .await is invoked at the end of
    /// the builder chain.
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let req = FailureRequest {
                message: self.message,
                error_type: self.error_type,
                backtrace: self.backtrace,
                retry_at: self.retry_at_ms,
                kill: self.kill,
            };
            self.client.report_failure_raw(&self.job_id, req).await
        })
    }
}
