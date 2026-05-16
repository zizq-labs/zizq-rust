// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Shared job-selection filter — the query parameters common to the
//! `list`, `count`, bulk-`delete` and bulk-`patch` job endpoints
//! (`status`, `queue`, `type`, `id`, and a jq payload expression).
//!
//! [`JobFilter`] is an internal type: each public endpoint builder
//! owns one privately and exposes the setters via the
//! [`job_filter_setters`] macro. The builders stay independent — no
//! shared *public* type to become a breaking-change liability if the
//! endpoints' parameter sets ever diverge — while the param-encoding
//! logic lives in exactly one place.
//!
//! # Empty-set semantics
//!
//! Each list filter is `Option<Vec<_>>`, which preserves the
//! three-way distinction the server's query string can't:
//!
//! - **`None`** — the setter was never called. No query parameter is
//!   sent; the server treats the absence as "no filter on this axis"
//!   (i.e. all values match).
//! - **`Some(non-empty)`** — a normal filter.
//! - **`Some(empty)`** — the setter was called with an empty
//!   collection. Under the endpoints' AND semantics an empty set on
//!   any axis makes the whole result empty, so builders short-circuit
//!   to an empty response ([`matches_nothing`](JobFilter::matches_nothing))
//!   rather than sending a request the server would interpret as "no
//!   filter" — which would match *everything* and is a genuine
//!   footgun on bulk delete / patch.

use url::form_urlencoded;

use crate::resources::JobStatus;

/// Job-selection filter params shared across listing-style endpoints.
/// Internal — embedded privately in each endpoint builder.
#[derive(Debug, Default, Clone)]
pub(crate) struct JobFilter {
    pub(crate) status: Option<Vec<JobStatus>>,
    pub(crate) queue: Option<Vec<String>>,
    pub(crate) job_type: Option<Vec<String>>,
    pub(crate) id: Option<Vec<String>>,
    pub(crate) jq: Option<String>,
}

/// True when `opt` is `Some` and the contained list is empty.
fn is_explicitly_empty<T>(opt: &Option<Vec<T>>) -> bool {
    matches!(opt, Some(v) if v.is_empty())
}

/// True when `opt` is `Some` and the contained list is non-empty.
fn is_present<T>(opt: &Option<Vec<T>>) -> bool {
    matches!(opt, Some(v) if !v.is_empty())
}

impl JobFilter {
    /// True if any list filter was explicitly set to an empty list.
    ///
    /// Filters AND together, so an empty set on any axis means the
    /// whole result set is empty. Builders check this first and
    /// return an empty response without contacting the server.
    pub(crate) fn matches_nothing(&self) -> bool {
        is_explicitly_empty(&self.status)
            || is_explicitly_empty(&self.queue)
            || is_explicitly_empty(&self.job_type)
            || is_explicitly_empty(&self.id)
    }

    /// True if at least one filter contributes a query parameter.
    ///
    /// A `Some(empty)` list contributes nothing — but
    /// [`matches_nothing`](Self::matches_nothing) will have
    /// short-circuited the request before this is reached.
    pub(crate) fn has_params(&self) -> bool {
        is_present(&self.status)
            || is_present(&self.queue)
            || is_present(&self.job_type)
            || is_present(&self.id)
            || self.jq.is_some()
    }

    /// Append the set filter params to a query-string serializer.
    /// Multi-value filters are comma-delimited, matching the server's
    /// `CommaSet` parsing. `None` and `Some(empty)` lists append
    /// nothing.
    pub(crate) fn append_to<T: form_urlencoded::Target>(
        &self,
        q: &mut form_urlencoded::Serializer<'_, T>,
    ) {
        if let Some(status) = self.status.as_ref().filter(|v| !v.is_empty()) {
            let joined = status
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(",");
            q.append_pair("status", &joined);
        }
        if let Some(queue) = self.queue.as_ref().filter(|v| !v.is_empty()) {
            q.append_pair("queue", &queue.join(","));
        }
        if let Some(job_type) = self.job_type.as_ref().filter(|v| !v.is_empty()) {
            q.append_pair("type", &job_type.join(","));
        }
        if let Some(id) = self.id.as_ref().filter(|v| !v.is_empty()) {
            q.append_pair("id", &id.join(","));
        }
        if let Some(jq) = &self.jq {
            q.append_pair("filter", jq);
        }
    }
}

/// Generate the inherent filter-setter methods (`status`, `queue`,
/// `job_type`, `id`, `filter`) on an endpoint builder.
///
/// The builder must have a `filters: JobFilter` field. The methods
/// consume and return `Self` for chaining. Generating them as
/// *inherent* methods (rather than a shared trait) means callers
/// don't need to import anything to use them.
macro_rules! job_filter_setters {
    () => {
        /// Filter by lifecycle state.
        ///
        /// Passing an empty iterator means "match no statuses" — the
        /// request short-circuits to an empty result. To not filter
        /// by status at all, simply don't call this method.
        pub fn status<I: IntoIterator<Item = $crate::JobStatus>>(mut self, statuses: I) -> Self {
            self.filters.status = Some(statuses.into_iter().collect());
            self
        }

        /// Filter by queue name.
        ///
        /// Passing an empty iterator means "match no queues" — see
        /// [`Self::status`] for the empty-set semantics.
        pub fn queue<I, S>(mut self, queues: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: Into<String>,
        {
            self.filters.queue = Some(queues.into_iter().map(Into::into).collect());
            self
        }

        /// Filter by job-type name (the `JobKind::NAME` associated
        /// constant).
        ///
        /// Passing an empty iterator means "match no types" — see
        /// [`Self::status`] for the empty-set semantics.
        pub fn job_type<I, S>(mut self, types: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: Into<String>,
        {
            self.filters.job_type = Some(types.into_iter().map(Into::into).collect());
            self
        }

        /// Filter by known job id.
        ///
        /// Passing an empty iterator means "match no ids" — see
        /// [`Self::status`] for the empty-set semantics.
        pub fn id<I, S>(mut self, ids: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: Into<String>,
        {
            self.filters.id = Some(ids.into_iter().map(Into::into).collect());
            self
        }

        /// Server-side jq expression evaluated against each job's
        /// payload for filtering (e.g. `".user_id == 42"`).
        pub fn filter(mut self, expr: impl Into<String>) -> Self {
            self.filters.jq = Some(expr.into());
            self
        }
    };
}

pub(crate) use job_filter_setters;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filter_matches_nothing_is_false() {
        // No setters called — every axis is `None`, so nothing is
        // explicitly empty.
        let f = JobFilter::default();
        assert!(!f.matches_nothing());
        assert!(!f.has_params());
    }

    #[test]
    fn explicitly_empty_list_matches_nothing() {
        let f = JobFilter {
            status: Some(vec![]),
            ..Default::default()
        };
        assert!(f.matches_nothing());
        // An empty set contributes no query parameter.
        assert!(!f.has_params());
    }

    #[test]
    fn populated_filter_has_params_and_matches_something() {
        let f = JobFilter {
            queue: Some(vec!["emails".into()]),
            ..Default::default()
        };
        assert!(!f.matches_nothing());
        assert!(f.has_params());
    }

    #[test]
    fn jq_only_filter_has_params() {
        let f = JobFilter {
            jq: Some(".x == 1".into()),
            ..Default::default()
        };
        assert!(!f.matches_nothing());
        assert!(f.has_params());
    }
}
