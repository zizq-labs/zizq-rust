// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Listing a job's error history — the [`ListErrorsBuilder`] returned
//! by [`Client::list_errors`].
//!
//! Accumulates paging parameters, then awaits to fetch a single
//! [`ErrorPage`], or [`stream`](ListErrorsBuilder::stream)s every
//! record across pages. Pagination follows the [`PageLinks::next`] /
//! [`PageLinks::prev`] links via [`Client::get_page`].
//!
//! [`Client::list_errors`]: crate::Client::list_errors
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
use crate::resources::{ErrorPage, ErrorRecord, Order};

/// Builder for [`Client::list_errors`].
///
/// Produced by [`Client::list_errors`]. Chain paging methods, then
/// `.await` to fetch a single [`ErrorPage`], or call
/// [`stream`](Self::stream) to iterate every record across pages.
///
/// [`Client::list_errors`]: crate::Client::list_errors
///
/// # Examples
///
/// ```no_run
/// # use zizq::{Client, Order};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let page = client
///     .list_errors("job-123")
///     .order(Order::Desc)
///     .limit(100)
///     .await?;
///
/// for err in &page.errors {
///     println!("attempt {}: {}", err.attempt, err.message);
/// }
/// # Ok(()) }
/// ```
pub struct ListErrorsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,
    /// Id of the job whose error history is being listed.
    job_id: String,
    /// Optional pagination cursor — start after this attempt number
    /// (exclusive). Normally you'd follow page links instead.
    from: Option<u32>,
    /// Return records oldest-first (default) or newest-first.
    order: Option<Order>,
    /// Maximum number of records to return on the page.
    limit: Option<u16>,
}

impl<'a> ListErrorsBuilder<'a> {
    pub(crate) fn new(client: &'a Client, job_id: String) -> Self {
        Self {
            client,
            job_id,
            from: None,
            order: None,
            limit: None,
        }
    }

    /// Start the page *after* the given attempt number (exclusive
    /// cursor). Normally you'd use [`Client::get_page`] with one of
    /// the server-emitted [`PageLinks`] paths instead.
    ///
    /// [`Client::get_page`]: crate::Client::get_page
    /// [`PageLinks`]: crate::PageLinks
    pub fn from(mut self, attempt: u32) -> Self {
        self.from = Some(attempt);
        self
    }

    /// Sort order by attempt number. Defaults to the server's
    /// default ([`Order::Asc`]).
    pub fn order(mut self, order: Order) -> Self {
        self.order = Some(order);
        self
    }

    /// Maximum number of error records to return on this page. Valid
    /// range is 1–200; the server's default is 50.
    ///
    /// When used with [`Self::stream`], this is the *per-page* fetch
    /// size — larger pages mean fewer round trips.
    pub fn limit(mut self, n: u16) -> Self {
        self.limit = Some(n);
        self
    }

    /// Stream every error record for the job, fetching pages lazily.
    ///
    /// Returns a [`Stream`] of `Result<ErrorRecord, ZizqError>` that
    /// fetches the first page on first poll and follows each page's
    /// `next` link until the history is exhausted. Only one page is
    /// held in memory at a time. A transport / decode error ends the
    /// stream after yielding the error.
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
    /// # use zizq::Client;
    /// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut errors = client.list_errors("job-123").limit(200).stream();
    ///
    /// while let Some(err) = errors.try_next().await? {
    ///     println!("attempt {}: {}", err.attempt, err.message);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn stream(self) -> Pin<Box<dyn Stream<Item = Result<ErrorRecord, ZizqError>> + Send + 'a>> {
        let state = ErrorStreamState {
            client: self.client,
            buffer: Vec::new().into_iter(),
            cursor: PageCursor::First(self.build_url()),
        };
        // Boxed so the returned stream is `Unpin` — `try_unfold`'s
        // own type holds a non-`Unpin` future, which would otherwise
        // force callers to `pin!` it before using `StreamExt`.
        Box::pin(try_unfold(state, |mut state| async move {
            loop {
                // Yield buffered records from the current page first.
                if let Some(record) = state.buffer.next() {
                    return Ok(Some((record, state)));
                }
                // Buffer drained — fetch the next page (or stop).
                let page = match std::mem::replace(&mut state.cursor, PageCursor::Done) {
                    PageCursor::First(url) => state.client.get_decoded::<ErrorPage>(url).await?,
                    PageCursor::Next(path) => state.client.get_page::<ErrorPage>(&path).await?,
                    PageCursor::Done => return Ok(None),
                };
                state.buffer = page.errors.into_iter();
                state.cursor = match page.pages.next {
                    Some(path) => PageCursor::Next(path),
                    None => PageCursor::Done,
                };
            }
        }))
    }

    /// Build the request URL with paging query parameters.
    fn build_url(&self) -> Url {
        let mut url = self.client.url(&["jobs", self.job_id.as_str(), "errors"]);
        // Only touch `query_pairs_mut` when there's something to add —
        // calling it unconditionally appends a stray trailing `?`.
        if self.from.is_some() || self.order.is_some() || self.limit.is_some() {
            let mut q = url.query_pairs_mut();
            if let Some(from) = self.from {
                q.append_pair("from", &from.to_string());
            }
            if let Some(order) = self.order {
                q.append_pair("order", order.as_str());
            }
            if let Some(limit) = self.limit {
                q.append_pair("limit", &limit.to_string());
            }
        }
        url
    }
}

impl<'a> IntoFuture for ListErrorsBuilder<'a> {
    type Output = Result<ErrorPage, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            let url = self.build_url();
            client.get_decoded::<ErrorPage>(url).await
        })
    }
}

/// Where the error stream fetches its next page from.
enum PageCursor {
    /// First page — fetch this fully-built URL.
    First(Url),
    /// Subsequent page — follow this server-emitted `next` path.
    Next(String),
    /// History exhausted.
    Done,
}

/// `try_unfold` state for the error stream: the client handle, the
/// remaining records of the current page, and the cursor to the next.
struct ErrorStreamState<'a> {
    client: &'a Client,
    buffer: std::vec::IntoIter<ErrorRecord>,
    cursor: PageCursor,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Format;

    fn client() -> Client {
        Client::builder()
            .url("http://127.0.0.1:7890")
            .format(Format::Json)
            .build()
            .unwrap()
    }

    #[test]
    fn bare_builder_has_no_query() {
        let c = client();
        let url = ListErrorsBuilder::new(&c, "job-1".into()).build_url();
        assert_eq!(url.path(), "/jobs/job-1/errors");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn paging_params_appear_in_query() {
        let c = client();
        let url = ListErrorsBuilder::new(&c, "job-1".into())
            .from(3)
            .order(Order::Desc)
            .limit(25)
            .build_url();
        let query = url.query().unwrap();
        assert!(query.contains("from=3"));
        assert!(query.contains("order=desc"));
        assert!(query.contains("limit=25"));
    }

    #[test]
    fn job_id_is_percent_encoded() {
        let c = client();
        // A slash in the id would split routing on the server side.
        let url = ListErrorsBuilder::new(&c, "ns/abc".into()).build_url();
        assert_eq!(url.path(), "/jobs/ns%2Fabc/errors");
    }
}
