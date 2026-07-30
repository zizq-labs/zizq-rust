// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Bulk job enqueueing — the [`BulkEnqueueBuilder`] returned by
//! [`Client::enqueue_bulk`].
//!
//! Collects per-job [`EnqueueBuilder`]s and dispatches them as a single
//! `POST /jobs/bulk` request. Per-job builders are resolved into
//! type-erased [`EnqueueRequest`]s when added, so a single batch can
//! mix arbitrary [`JobKind`]s.
//!
//! Two add APIs exist for the two natural call sites:
//!
//! - [`BulkEnqueueBuilder::add`] consumes `self` and returns `Self`, so
//!   it chains in expression position
//!   (`client.enqueue_bulk().add(...).add(...).await?`).
//! - [`BulkEnqueueBuilder::push`] takes `&mut self`, so it composes in
//!   loops without `batch = batch.add(...)` reassignment.
//!
//! [`Client::enqueue_bulk`]: crate::Client::enqueue_bulk
//! [`EnqueueRequest`]: crate::client::EnqueueRequest

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use crate::client::{BulkEnqueueRequest, Client, EnqueueRequest};
use crate::enqueue::EnqueueBuilder;
use crate::error::ZizqError;
use crate::job::JobKind;
use crate::resources::Job;

/// Builder for a bulk enqueue request.
///
/// Produced by [`Client::enqueue_bulk`]. Add per-job [`EnqueueBuilder`]s
/// with [`Self::add`] (chainable) or [`Self::push`] (mutating), then
/// `.await` the builder to send a single `POST /jobs/bulk` request.
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
/// let jobs = client
///     .enqueue_bulk()
///     .add(client.enqueue(SendEmail { to: "a@x".into() }).priority(50))
///     .add(client.enqueue(SendEmail { to: "b@x".into() }))
///     .await?;
/// assert_eq!(jobs.len(), 2);
/// # Ok(()) }
/// ```
pub struct BulkEnqueueBuilder<'a> {
    client: &'a Client,
    /// Accumulated per-job requests, or the first error encountered
    /// while resolving one. Once an error is recorded, subsequent
    /// `.add` / `.push` calls are silent no-ops and the error surfaces
    /// when the builder is awaited. First-error-wins keeps `.add`
    /// chainable; surfacing only at `.await` matches the single-enqueue
    /// behaviour where serialisation also runs at send time.
    state: Result<Vec<EnqueueRequest>, ZizqError>,
}

impl<'a> BulkEnqueueBuilder<'a> {
    /// Initialise an empty batch bound to the given [`Client`].
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            state: Ok(Vec::new()),
        }
    }

    /// Add a per-job [`EnqueueBuilder`] to the batch. Chainable — see
    /// [`Self::push`] for the mutating variant suited to loops.
    ///
    /// Resolves the per-job builder eagerly (defaults applied, payload
    /// serialised). Any serialisation error is captured and surfaced
    /// when the batch is awaited.
    // clippy::should_implement_trait flags this because `add` collides
    // with `std::ops::Add::add`. Implementing `Add` isn't a fit here —
    // it would force `Add<EnqueueBuilder<T>>` for every payload `T`,
    // and the analogous stdlib pairing (`String + &str` consuming
    // vs `String::push_str` mutating) is exactly what we're modelling.
    #[allow(clippy::should_implement_trait)]
    pub fn add<T: JobKind>(mut self, builder: EnqueueBuilder<'_, T>) -> Self {
        self.push(builder);
        self
    }

    /// Add a per-job [`EnqueueBuilder`] to the batch in place.
    ///
    /// Suited to loops where rebinding `self` (`batch = batch.add(...)`)
    /// would be noisy.
    pub fn push<T: JobKind>(&mut self, builder: EnqueueBuilder<'_, T>) {
        let Ok(reqs) = &mut self.state else {
            return;
        };
        match builder.into_request() {
            Ok(req) => reqs.push(req),
            Err(e) => self.state = Err(e),
        }
    }

    /// Number of jobs currently in the batch. Returns 0 if the builder
    /// is in an error state (no jobs will be sent on `.await`).
    pub fn len(&self) -> usize {
        self.state.as_ref().map(|v| v.len()).unwrap_or(0)
    }

    /// True when no jobs have been added.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> IntoFuture for BulkEnqueueBuilder<'a> {
    type Output = Result<Vec<Job>, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            let jobs = self.state?;
            client.enqueue_bulk_raw(BulkEnqueueRequest { jobs }).await
        })
    }
}

impl std::fmt::Debug for BulkEnqueueBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkEnqueueBuilder")
            .field("len", &self.len())
            .field("error", &self.state.is_err())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Format;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct SendEmail {
        to: String,
    }
    impl JobKind for SendEmail {
        const NAME: &'static str = "send_email";
        const QUEUE: &'static str = "emails";
        const PRIORITY: Option<u16> = Some(50);
    }

    #[derive(Serialize, Deserialize)]
    struct ProcessReport {
        report_id: u64,
    }
    impl JobKind for ProcessReport {
        const NAME: &'static str = "process_report";
    }

    fn client() -> Client {
        Client::builder()
            .url("http://127.0.0.1:7890")
            .format(Format::Json)
            .build()
            .unwrap()
    }

    #[test]
    fn empty_builder_has_zero_len() {
        let c = client();
        let b = c.enqueue_bulk();
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
    }

    #[test]
    fn add_is_chainable_and_accumulates() {
        let c = client();
        let b = c
            .enqueue_bulk()
            .add(c.enqueue(SendEmail { to: "a@x".into() }))
            .add(c.enqueue(SendEmail { to: "b@x".into() }))
            .add(c.enqueue(ProcessReport { report_id: 7 }));
        assert_eq!(b.len(), 3);
        assert!(!b.is_empty());
    }

    #[test]
    fn push_mutates_in_place_for_loop_usage() {
        let c = client();
        let mut b = c.enqueue_bulk();
        for i in 0..5u64 {
            b.push(c.enqueue(ProcessReport { report_id: i }));
        }
        assert_eq!(b.len(), 5);
    }

    #[test]
    fn add_and_push_can_be_mixed() {
        let c = client();
        let mut b = c
            .enqueue_bulk()
            .add(c.enqueue(SendEmail { to: "a@x".into() }));
        b.push(c.enqueue(SendEmail { to: "b@x".into() }));
        let b = b.add(c.enqueue(ProcessReport { report_id: 1 }));
        assert_eq!(b.len(), 3);
    }
}
