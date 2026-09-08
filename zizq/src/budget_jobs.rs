// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Bulk budget rebinding — the [`BudgetJobsBuilder`] returned by
//! [`Client::bind_all_jobs_budget`] and its four siblings.
//!
//! One builder covers all five operations because they differ only in
//! HTTP method, path and body; the filter accumulation, the
//! empty-filter short-circuit and the [`BudgetChange`] decode are
//! identical for each.
//!
//! **These are bulk operations.** With no filters set they act on
//! *every job on the server*. Setting any filter to an explicitly
//! empty set instead turns the operation into a no-op (see [`JobFilter`]).
//!
//! [`Client::bind_all_jobs_budget`]: crate::Client::bind_all_jobs_budget
//! [`JobFilter`]: crate::job_filter::JobFilter

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use url::Url;

use crate::budget::{BindingBody, BudgetBindingInput, BudgetChange, CostBody};
use crate::client::Client;
use crate::error::ZizqError;
use crate::job_filter::{job_filter_setters, JobFilter};

/// Which budget change to apply to the matched jobs.
enum Operation {
    /// `POST /jobs/budgets/{key}` — refuses jobs already bound.
    Bind(BudgetBindingInput),

    /// `PUT /jobs/budgets/{key}` — replaces an existing binding.
    Rebind(BudgetBindingInput),

    /// `PATCH /jobs/budgets/{key}` — changes the cost only.
    SetCost { key: String, cost: u32 },

    /// `DELETE /jobs/budgets/{key}` — removes one binding.
    Unbind { key: String },

    /// `DELETE /jobs/budgets` — removes every binding.
    Clear,
}

/// Builder for the bulk budget-binding endpoints.
///
/// Produced by [`Client::bind_all_jobs_budget`],
/// [`Client::rebind_all_jobs_budget`],
/// [`Client::set_all_jobs_budget_cost`],
/// [`Client::unbind_all_jobs_budget`] and
/// [`Client::clear_all_jobs_budgets`].
///
/// Chain filter methods to narrow what is affected, then `.await` for a
/// [`BudgetChange`].
///
/// All filter options combine to narrow the selection (logically
/// AND'ed).
///
/// **With no filters set, awaiting this changes every job on the
/// server.** A filter explicitly set to an empty set instead changes
/// nothing (and makes no request).
///
/// [`Client::bind_all_jobs_budget`]: crate::Client::bind_all_jobs_budget
/// [`Client::rebind_all_jobs_budget`]: crate::Client::rebind_all_jobs_budget
/// [`Client::set_all_jobs_budget_cost`]: crate::Client::set_all_jobs_budget_cost
/// [`Client::unbind_all_jobs_budget`]: crate::Client::unbind_all_jobs_budget
/// [`Client::clear_all_jobs_budgets`]: crate::Client::clear_all_jobs_budgets
///
/// # Examples
///
/// ```no_run
/// # use zizq::{BudgetBindingInput, Client, JobStatus};
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let change = client
///     .bind_all_jobs_budget(BudgetBindingInput::new("stripe").cost(2))
///     .queue(["billing"])
///     .status([JobStatus::Ready])
///     .await?;
/// # Ok(()) }
/// ```
pub struct BudgetJobsBuilder<'a> {
    /// The client reference to which the await'ed request is sent.
    client: &'a Client,

    /// Shared job-selection filters. Setters supplied by
    /// `job_filter_setters!`.
    filters: JobFilter,

    /// The change to apply.
    operation: Operation,
}

impl<'a> BudgetJobsBuilder<'a> {
    job_filter_setters!();

    pub(crate) fn bind(client: &'a Client, binding: BudgetBindingInput) -> Self {
        Self::new(client, Operation::Bind(binding))
    }

    pub(crate) fn rebind(client: &'a Client, binding: BudgetBindingInput) -> Self {
        Self::new(client, Operation::Rebind(binding))
    }

    pub(crate) fn set_cost(client: &'a Client, key: String, cost: u32) -> Self {
        Self::new(client, Operation::SetCost { key, cost })
    }

    pub(crate) fn unbind(client: &'a Client, key: String) -> Self {
        Self::new(client, Operation::Unbind { key })
    }

    pub(crate) fn clear(client: &'a Client) -> Self {
        Self::new(client, Operation::Clear)
    }

    fn new(client: &'a Client, operation: Operation) -> Self {
        Self {
            client,
            filters: JobFilter::default(),
            operation,
        }
    }

    /// Build the request URL with filter query parameters. Every
    /// operation but [`Operation::Clear`] names its budget in the
    /// path.
    fn build_url(&self) -> Url {
        let mut url = match &self.operation {
            Operation::Bind(binding) | Operation::Rebind(binding) => {
                self.client.url(&["jobs", "budgets", &binding.key])
            }
            Operation::SetCost { key, .. } | Operation::Unbind { key } => {
                self.client.url(&["jobs", "budgets", key])
            }
            Operation::Clear => self.client.url(&["jobs", "budgets"]),
        };
        // Only touch `query_pairs_mut` when there's something to add —
        // calling it unconditionally appends a stray trailing `?`.
        if self.filters.has_params() {
            let mut q = url.query_pairs_mut();
            self.filters.append_to(&mut q);
        }
        url
    }
}

impl<'a> IntoFuture for BudgetJobsBuilder<'a> {
    type Output = Result<BudgetChange, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            // An explicitly empty filter set can match no jobs;
            // short-circuit with no server round-trip, so an
            // accidentally empty filter cannot become a
            // change-everything request.
            if self.filters.matches_nothing() {
                return Ok(BudgetChange {
                    changed: 0,
                    blocked: Vec::new(),
                });
            }

            let url = self.build_url();
            match &self.operation {
                Operation::Bind(binding) => {
                    client
                        .send_body_decoded(reqwest::Method::POST, url, BindingBody::from(binding))
                        .await
                }
                Operation::Rebind(binding) => {
                    client
                        .send_body_decoded(reqwest::Method::PUT, url, BindingBody::from(binding))
                        .await
                }
                Operation::SetCost { cost, .. } => {
                    client
                        .send_body_decoded(reqwest::Method::PATCH, url, CostBody { cost: *cost })
                        .await
                }
                Operation::Unbind { .. } | Operation::Clear => client.delete_decoded(url).await,
            }
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
    fn the_budget_key_travels_in_the_path() {
        let c = client();
        let url = BudgetJobsBuilder::bind(&c, BudgetBindingInput::new("emails")).build_url();
        assert_eq!(url.path(), "/jobs/budgets/emails");
        assert_eq!(url.query(), None);
    }

    // Clearing every binding names no budget, so there is no key
    // segment to append.
    #[test]
    fn clearing_targets_the_collection_itself() {
        let c = client();
        let url = BudgetJobsBuilder::clear(&c).build_url();
        assert_eq!(url.path(), "/jobs/budgets");
    }

    #[test]
    fn filters_appear_in_the_query() {
        let c = client();
        let url = BudgetJobsBuilder::unbind(&c, "emails".to_string())
            .status([JobStatus::Ready])
            .queue(["billing"])
            .budgets_key(["emails"])
            .build_url();

        let query = url.query().unwrap();
        assert!(query.contains("status=ready"));
        assert!(query.contains("queue=billing"));
        assert!(query.contains("budgets.key=emails"));
    }

    #[test]
    fn a_key_needing_escaping_is_encoded_in_the_path() {
        let c = client();
        let url = BudgetJobsBuilder::unbind(&c, "emails/acme".to_string()).build_url();
        assert_eq!(url.path(), "/jobs/budgets/emails%2Facme");
    }
}
