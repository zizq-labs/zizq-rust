// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Listing jobs — the [`ListJobsBuilder`] returned by [`Client::list_jobs`].
//!
//! Accumulates filter / paging parameters, then awaits to fetch a
//! single [`JobPage`]. Pagination is handled by following the
//! [`PageLinks::next`] / [`PageLinks::prev`] links on each page using
//! [`Client::get_page`].
//!
//! The job-selection filters (`status`, `queue`, `type`, `id`,
//! `filter`) are shared with the count / delete / patch endpoints via
//! [`crate::job_filter::JobFilter`]; the paging params (`from`,
//! `order`, `limit`) are specific to this builder.
//!
//! [`Client::list_jobs`]: crate::Client::list_jobs
//! [`Client::get_page`]: crate::Client::get_page
//! [`PageLinks::next`]: crate::PageLinks::next
//! [`PageLinks::prev`]: crate::PageLinks::prev

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use futures_core::Stream;
use futures_util::stream::try_unfold;
use url::Url;

use crate::client::Client;
use crate::error::ZizqError;
use crate::job_filter::{job_filter_setters, JobFilter};
use crate::resources::{Job, JobPage, Order};

/// Builder for [`Client::list_jobs`].
///
/// Produced by [`Client::list_jobs`]. Chain filter / paging methods,
/// then `.await` to fetch a single [`JobPage`].
///
/// All filter options combine to narrow the search (logically AND'ed).
///
/// [`Client::list_jobs`]: crate::Client::list_jobs
///
/// # Examples
///
/// ```no_run
/// # use zizq::{Client, JobStatus, Order};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let page = client
///     .list_jobs()
///     .status([JobStatus::Ready, JobStatus::Scheduled])
///     .queue(["emails"])
///     .order(Order::Desc)
///     .limit(100)
///     .await?;
///
/// for job in &page.jobs {
///     println!("{} on {}", job.id, job.queue);
/// }
///
/// if let Some(next) = page.pages.next.as_deref() {
///     let _next_page: zizq::JobPage = client.get_page(next).await?;
/// }
/// # Ok(()) }
/// ```
pub struct ListJobsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,
    /// Shared job-selection filters (`status`, `queue`, `type`, `id`,
    /// `filter`). Setters supplied by `job_filter_setters!`.
    filters: JobFilter,
    /// Optional pagination cursor (exclusive). Callers should generally
    /// just follow the links on each page instead.
    from: Option<String>,
    /// Return jobs in ascending (default) or descending order by `id`.
    order: Option<Order>,
    /// Maximum number of records to return on the page.
    limit: Option<u16>,
}

impl<'a> ListJobsBuilder<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            filters: JobFilter::default(),
            from: None,
            order: None,
            limit: None,
        }
    }

    job_filter_setters!();

    /// Start the page *after* the given job id (exclusive cursor).
    /// Normally you'd use [`Client::get_page`] with one of the
    /// server-emitted [`PageLinks`] paths instead of constructing the
    /// cursor manually.
    ///
    /// [`Client::get_page`]: crate::Client::get_page
    /// [`PageLinks`]: crate::PageLinks
    pub fn from(mut self, id: impl Into<String>) -> Self {
        self.from = Some(id.into());
        self
    }

    /// Sort order by `id`. Defaults to the server's default ([`Order::Asc`]).
    pub fn order(mut self, order: Order) -> Self {
        self.order = Some(order);
        self
    }

    /// Maximum number of jobs to return on this page. Valid range is
    /// 1–2000; the server's default is 50.
    ///
    /// When used with [`Self::stream`], this is the *per-page* fetch
    /// size — larger pages mean fewer round trips.
    pub fn limit(mut self, n: u16) -> Self {
        self.limit = Some(n);
        self
    }

    /// Stream every matching job, fetching pages lazily.
    ///
    /// Returns a [`Stream`] of `Result<Job, ZizqError>` that fetches
    /// the first page on first poll and follows each page's `next`
    /// link until the result set is exhausted. Only one page is held
    /// in memory at a time. A transport / decode error ends the
    /// stream after yielding the error.
    ///
    /// If a filter is explicitly set to an empty set, the stream
    /// yields nothing and makes no request — consistent with
    /// `.await`ing the builder.
    ///
    /// Consuming the stream needs [`futures_util::StreamExt`] /
    /// `TryStreamExt` in scope.
    ///
    /// [`futures_util::StreamExt`]: https://docs.rs/futures-util/latest/futures_util/stream/trait.StreamExt.html
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_util::TryStreamExt;
    /// # use zizq::{Client, JobStatus};
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut jobs = client
    ///     .list_jobs()
    ///     .status([JobStatus::Dead])
    ///     .limit(2000)
    ///     .stream();
    ///
    /// while let Some(job) = jobs.try_next().await? {
    ///     println!("{} on {}", job.id, job.queue);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn stream(self) -> Pin<Box<dyn Stream<Item = Result<Job, ZizqError>> + Send + 'a>> {
        let cursor = if self.filters.matches_nothing() {
            PageCursor::Done
        } else {
            PageCursor::First(self.build_url())
        };
        let state = JobStreamState {
            client: self.client,
            buffer: Vec::new().into_iter(),
            cursor,
        };
        // Boxed so the returned stream is `Unpin` — `try_unfold`'s
        // own type holds a non-`Unpin` future, which would otherwise
        // force callers to `pin!` it before using `StreamExt`.
        Box::pin(try_unfold(state, |mut state| async move {
            loop {
                // Yield buffered jobs from the current page first.
                if let Some(job) = state.buffer.next() {
                    return Ok(Some((job, state)));
                }
                // Buffer drained — fetch the next page (or stop).
                // The std::mem::replace here is so we can atomically get
                // the current cursor as an owned value. It is replaced
                // with Done largely arbitrarily simply to push the
                // existing value out. We set a new cursor shortly after.
                let page = match std::mem::replace(&mut state.cursor, PageCursor::Done) {
                    PageCursor::First(url) => state.client.get_decoded::<JobPage>(url).await?,
                    PageCursor::Next(path) => state.client.get_page::<JobPage>(&path).await?,
                    PageCursor::Done => return Ok(None),
                };
                state.buffer = page.jobs.into_iter();
                state.cursor = match page.pages.next {
                    Some(path) => PageCursor::Next(path),
                    None => PageCursor::Done,
                };
            }
        }))
    }

    /// Build the request URL with filter / paging query parameters.
    fn build_url(&self) -> Url {
        let mut url = self.client.url(&["jobs"]);
        let has_params = self.from.is_some()
            || self.order.is_some()
            || self.limit.is_some()
            || self.filters.has_params();
        // Only touch `query_pairs_mut` when there's something to add —
        // calling it unconditionally appends a stray trailing `?`.
        if has_params {
            let mut q = url.query_pairs_mut();
            if let Some(from) = &self.from {
                q.append_pair("from", from);
            }
            if let Some(order) = self.order {
                q.append_pair("order", order.as_str());
            }
            if let Some(limit) = self.limit {
                q.append_pair("limit", &limit.to_string());
            }
            self.filters.append_to(&mut q);
        }
        url
    }
}

impl<'a> IntoFuture for ListJobsBuilder<'a> {
    type Output = Result<JobPage, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            // An explicitly empty filter set can match no jobs;
            // short-circuit to an empty page with no server round-trip
            // rather than sending a request the server would read as
            // "no filter" (i.e. match everything).
            if self.filters.matches_nothing() {
                return Ok(JobPage::empty());
            }
            let url = self.build_url();
            client.get_decoded::<JobPage>(url).await
        })
    }
}

/// Where the job stream fetches its next page from.
enum PageCursor {
    /// First page — fetch this fully-built URL (filters + paging).
    First(Url),
    /// Subsequent page — follow this server-emitted `next` path.
    Next(String),
    /// Result set exhausted (or an empty filter matched nothing).
    Done,
}

/// `try_unfold` state for the job stream: the client handle, the
/// remaining jobs of the current page, and the cursor to the next.
struct JobStreamState<'a> {
    client: &'a Client,
    buffer: std::vec::IntoIter<Job>,
    cursor: PageCursor,
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
        let url = ListJobsBuilder::new(&c).build_url();
        assert_eq!(url.path(), "/jobs");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn paging_params_appear_in_query() {
        let c = client();
        let url = ListJobsBuilder::new(&c)
            .from("cursor-9")
            .order(Order::Desc)
            .limit(25)
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("from=cursor-9"));
        assert!(query.contains("order=desc"));
        assert!(query.contains("limit=25"));
    }

    #[test]
    fn shared_filters_are_comma_delimited() {
        let c = client();
        let url = ListJobsBuilder::new(&c)
            .status([JobStatus::Ready, JobStatus::Scheduled])
            .queue(["emails", "webhooks"])
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("status=ready%2Cscheduled"));
        assert!(query.contains("queue=emails%2Cwebhooks"));
    }
}
