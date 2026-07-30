// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Counting jobs — the [`CountJobsBuilder`] returned by
//! [`Client::count_jobs`].
//!
//! Accumulates job-selection filters, then awaits to fetch the number
//! of matching jobs. The filters (`status`, `queue`, `type`, `id`,
//! `filter`) are shared with the list / delete / patch endpoints via
//! [`crate::job_filter::JobFilter`].
//!
//! [`Client::count_jobs`]: crate::Client::count_jobs

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::Deserialize;
use url::Url;

use crate::client::Client;
use crate::error::ZizqError;
use crate::job_filter::{job_filter_setters, JobFilter};

/// Builder for `GET /jobs/count`.
///
/// Produced by [`Client::count_jobs`]. Chain filter methods, then
/// `.await` to get the number of matching jobs. With no filters set,
/// counts every job on the server.
///
/// All filter options combine to narrow the count (logically AND'ed).
///
/// [`Client::count_jobs`]: crate::Client::count_jobs
///
/// # Examples
///
/// ```no_run
/// # use zizq::{Client, JobStatus};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let dead = client
///     .count_jobs()
///     .status([JobStatus::Dead])
///     .queue(["emails"])
///     .await?;
/// println!("{dead} dead jobs on the emails queue");
/// # Ok(()) }
/// ```
pub struct CountJobsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,
    /// Shared job-selection filters. Setters supplied by
    /// `job_filter_setters!`.
    filters: JobFilter,
}

impl<'a> CountJobsBuilder<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            filters: JobFilter::default(),
        }
    }

    job_filter_setters!();

    /// Build the request URL with filter query parameters.
    fn build_url(&self) -> Url {
        let mut url = self.client.url(&["jobs", "count"]);
        // Only touch `query_pairs_mut` when there's something to add —
        // calling it unconditionally appends a stray trailing `?`.
        if self.filters.has_params() {
            let mut q = url.query_pairs_mut();
            self.filters.append_to(&mut q);
        }
        url
    }
}

/// Server response envelope for `GET /jobs/count`.
#[derive(Deserialize)]
struct CountResponse {
    count: u64,
}

impl<'a> IntoFuture for CountJobsBuilder<'a> {
    type Output = Result<u64, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            // An explicitly empty filter set can match no jobs;
            // short-circuit to a count of 0 with no server round-trip
            // rather than sending a request the server would read as
            // "no filter" (i.e. count everything).
            if self.filters.matches_nothing() {
                return Ok(0);
            }
            let url = self.build_url();
            let resp: CountResponse = client.get_decoded(url).await?;
            Ok(resp.count)
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
    fn empty_builder_has_no_query() {
        let c = client();
        let url = CountJobsBuilder::new(&c).build_url();
        assert_eq!(url.path(), "/jobs/count");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn filters_appear_in_query() {
        let c = client();
        let url = CountJobsBuilder::new(&c)
            .status([JobStatus::Dead])
            .queue(["emails"])
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("status=dead"));
        assert!(query.contains("queue=emails"));
    }
}
