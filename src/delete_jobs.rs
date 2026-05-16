// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Bulk job deletion — the [`DeleteJobsBuilder`] returned by
//! [`Client::delete_all_jobs`].
//!
//! Accumulates job-selection filters, then awaits to permanently
//! delete every matching job, returning the count removed. The
//! filters (`status`, `queue`, `type`, `id`, `filter`) are shared
//! with the list / count / patch endpoints via
//! [`crate::job_filter::JobFilter`].
//!
//! **This is a destructive operation.** With no filters set it
//! deletes *every job on the server* — that's why the method is named
//! `delete_all_jobs`. Setting any filter to an explicitly empty set
//! short-circuits to deleting nothing (see [`JobFilter`]).
//!
//! [`Client::delete_all_jobs`]: crate::Client::delete_all_jobs
//! [`JobFilter`]: crate::job_filter::JobFilter

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::Deserialize;
use url::Url;

use crate::client::Client;
use crate::error::ZizqError;
use crate::job_filter::{job_filter_setters, JobFilter};

/// Builder for `DELETE /jobs`.
///
/// Produced by [`Client::delete_all_jobs`]. Chain filter methods to
/// narrow what gets deleted, then `.await` to perform the delete and
/// get the number of jobs removed.
///
/// All filter options combine to narrow the delete (logically AND'ed).
///
/// **With no filters set, awaiting this deletes every job on the
/// server.** A filter explicitly set to an empty set instead deletes
/// nothing (and makes no request).
///
/// [`Client::delete_all_jobs`]: crate::Client::delete_all_jobs
///
/// # Examples
///
/// ```no_run
/// # use zizq::{Client, JobStatus};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// // Delete all dead jobs on the `emails` queue.
/// let removed = client
///     .delete_all_jobs()
///     .status([JobStatus::Dead])
///     .queue(["emails"])
///     .await?;
/// println!("deleted {removed} jobs");
/// # Ok(()) }
/// ```
pub struct DeleteJobsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,
    /// Shared job-selection filters. Setters supplied by
    /// `job_filter_setters!`.
    filters: JobFilter,
}

impl<'a> DeleteJobsBuilder<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            filters: JobFilter::default(),
        }
    }

    job_filter_setters!();

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

/// Server response envelope for `DELETE /jobs`.
#[derive(Deserialize)]
struct DeleteResponse {
    deleted: u64,
}

impl<'a> IntoFuture for DeleteJobsBuilder<'a> {
    type Output = Result<u64, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            // An explicitly empty filter set can match no jobs;
            // short-circuit to deleting nothing with no server
            // round-trip — a guard against an accidental empty filter
            // becoming a delete-everything request.
            if self.filters.matches_nothing() {
                return Ok(0);
            }
            let url = self.build_url();
            let resp: DeleteResponse = client.delete_decoded(url).await?;
            Ok(resp.deleted)
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
        // No filters → URL is the bare endpoint → deletes everything.
        let c = client();
        let url = DeleteJobsBuilder::new(&c).build_url();
        assert_eq!(url.path(), "/jobs");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn filters_appear_in_query() {
        let c = client();
        let url = DeleteJobsBuilder::new(&c)
            .status([JobStatus::Dead])
            .queue(["emails"])
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("status=dead"));
        assert!(query.contains("queue=emails"));
    }
}
