// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Bulk job patching — the [`PatchJobsBuilder`] returned by
//! [`Client::patch_all_jobs`].
//!
//! Accumulates job-selection filters, takes a [`JobPatch`] via
//! [`patch`](PatchJobsBuilder::patch), then awaits to apply that
//! patch to every matching job, returning the count patched. The
//! filters (`status`, `queue`, `type`, `id`, `filter`) are shared
//! with the list / count / delete endpoints via
//! [`crate::job_filter::JobFilter`].
//!
//! With no filters set it patches *every job on the server* — that's
//! why the method is named `patch_all_jobs` rather than something
//! narrower. Setting any filter to an explicitly empty set
//! short-circuits to patching nothing (see [`JobFilter`]). Awaiting
//! without supplying a patch is a usage error and returns
//! [`ZizqError::MissingPatch`].
//!
//! [`Client::patch_all_jobs`]: crate::Client::patch_all_jobs
//! [`JobFilter`]: crate::job_filter::JobFilter

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::Deserialize;
use url::Url;

use crate::client::Client;
use crate::error::ZizqError;
use crate::job_filter::{job_filter_setters, JobFilter};
use crate::job_patch::JobPatch;

/// Builder for the bulk `PATCH /jobs`.
///
/// Produced by [`Client::patch_all_jobs`]. Chain filter methods to narrow
/// what gets patched, call [`patch`](Self::patch) to supply the
/// [`JobPatch`] to apply, then `.await` to perform the patch and get
/// the number of jobs updated.
///
/// All filter options combine to narrow the patch (logically AND'ed).
///
/// **With no filters set, awaiting this patches every job on the
/// server.** A filter explicitly set to an empty set instead patches
/// nothing (and makes no request).
///
/// [`Client::patch_all_jobs`]: crate::Client::patch_all_jobs
///
/// # Examples
///
/// ```no_run
/// # use zizq::{Client, JobPatch, JobStatus};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// // Bump the retry limit on every ready job on the `emails` queue.
/// let patched = client
///     .patch_all_jobs()
///     .status([JobStatus::Ready])
///     .queue(["emails"])
///     .patch(JobPatch::new().retry_limit(10))
///     .await?;
/// println!("patched {patched} jobs");
/// # Ok(()) }
/// ```
pub struct PatchJobsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,
    /// Shared job-selection filters. Setters supplied by
    /// `job_filter_setters!`.
    filters: JobFilter,
    /// The patch to apply. `None` until [`patch`](Self::patch) is
    /// called; awaiting while `None` is a [`ZizqError::MissingPatch`].
    patch: Option<JobPatch>,
}

impl<'a> PatchJobsBuilder<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            filters: JobFilter::default(),
            patch: None,
        }
    }

    job_filter_setters!();

    /// Supply the [`JobPatch`] to apply to every matching job.
    ///
    /// Required — awaiting the builder without calling this returns
    /// [`ZizqError::MissingPatch`]. Calling it again replaces the
    /// previous patch.
    pub fn patch(mut self, patch: JobPatch) -> Self {
        self.patch = Some(patch);
        self
    }

    /// Build the request URL with filter query parameters.
    fn build_url(&self) -> Url {
        let mut url = self.client.url(&["jobs"]);
        // Only touch `query_pairs_mut` when there's something to add —
        // calling it unconditionally appends a stray trailing `?`.
        if self.filters.has_params() {
            let mut q = url.query_pairs_mut();
            self.filters.append_to(&mut q);
        }
        url
    }
}

/// Server response envelope for the bulk `PATCH /jobs`.
#[derive(Deserialize)]
struct PatchResponse {
    patched: u64,
}

impl<'a> IntoFuture for PatchJobsBuilder<'a> {
    type Output = Result<u64, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            // A missing patch is a usage error — surface it before
            // anything else, even an empty filter.
            let Some(patch) = self.patch.as_ref() else {
                return Err(ZizqError::MissingPatch);
            };
            // An explicitly empty filter set can match no jobs;
            // short-circuit to patching nothing with no server
            // round-trip — a guard against an accidental empty filter
            // becoming a patch-everything request.
            if self.filters.matches_nothing() {
                return Ok(0);
            }
            let url = self.build_url();
            let resp: PatchResponse = client.patch_decoded(url, patch).await?;
            Ok(resp.patched)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Format, JobStatus};

    fn client() -> Client {
        Client::builder()
            .url("http://127.0.0.1:7890")
            .format(Format::Json)
            .build()
            .unwrap()
    }

    #[test]
    fn unfiltered_builder_has_no_query() {
        // No filters → URL is the bare endpoint → patches everything.
        let c = client();
        let url = PatchJobsBuilder::new(&c).build_url();
        assert_eq!(url.path(), "/jobs");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn filters_appear_in_query() {
        let c = client();
        let url = PatchJobsBuilder::new(&c)
            .status([JobStatus::Ready])
            .queue(["emails"])
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("status=ready"));
        assert!(query.contains("queue=emails"));
    }

    #[tokio::test]
    async fn awaiting_without_patch_errors() {
        let c = client();
        let result = PatchJobsBuilder::new(&c).status([JobStatus::Ready]).await;
        assert!(matches!(result, Err(ZizqError::MissingPatch)));
    }

    #[tokio::test]
    async fn empty_filter_short_circuits_to_zero() {
        // An explicitly empty filter matches nothing — no request,
        // even though a patch was supplied.
        let c = client();
        let result = PatchJobsBuilder::new(&c)
            .status([])
            .patch(JobPatch::new().retry_limit(5))
            .await;
        assert!(matches!(result, Ok(0)));
    }

    #[tokio::test]
    async fn missing_patch_takes_precedence_over_empty_filter() {
        // Both wrong at once → the usage error wins.
        let c = client();
        let result = PatchJobsBuilder::new(&c).status([]).await;
        assert!(matches!(result, Err(ZizqError::MissingPatch)));
    }
}
