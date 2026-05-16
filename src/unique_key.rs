// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Uniqueness keys used to deduplicate jobs at enqueue-time.
//!
//! Unique jobs requires a [Pro license](https://zizq.io/pricing) on
//! the server.
//!
//! A [`UniqueKey`] is an arbitrary string the server uses to reject
//! duplicate enqueues while the matching [`UniqueScope`] applies. Keys
//! can be supplied explicitly at the enqueue call site or derived from
//! the payload via [`JobKind::unique_key`].
//!
//! [`JobKind::unique_key`]: crate::JobKind::unique_key

use serde::{Deserialize, Serialize};

/// A uniqueness key sent with an enqueue request.
///
/// Constructed with [`UniqueKey::raw`] when you already have a string,
/// or via the [`From<&str>`] / [`From<String>`] conversions when
/// passing a literal into [`EnqueueBuilder::unique_key`].
///
/// [`EnqueueBuilder::unique_key`]: crate::EnqueueBuilder::unique_key
///
/// # Examples
///
/// ```
/// use zizq::{UniqueKey, UniqueScope};
///
/// let key = UniqueKey::raw("user:42").scope(UniqueScope::Queued);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueKey {
    /// Raw string key.
    pub key: String,

    /// Lifecycle scope for which the uniqueness is enforced.
    pub scope: Option<UniqueScope>,
}

impl UniqueKey {
    /// Wrap a pre-built key string. No hashing or transformation is
    /// applied — the string is sent verbatim to the server.
    pub fn raw(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            scope: None,
        }
    }

    /// Attach a [`UniqueScope`] to this key. Returns `self` for
    /// chaining; with no scope set, the server applies the default
    /// [`UniqueScope::Queued`].
    pub fn scope(mut self, scope: UniqueScope) -> Self {
        self.scope = Some(scope);
        self
    }
}

impl From<&str> for UniqueKey {
    fn from(s: &str) -> Self {
        Self::raw(s)
    }
}

impl From<String> for UniqueKey {
    fn from(s: String) -> Self {
        Self::raw(s)
    }
}

/// The lifecycle window during which a [`UniqueKey`] prevents
/// duplicate enqueues.
///
/// The scope is taken from any successfully enqueued job. Subsequent
/// enqueues that conflict are de-duplicated (the server returns the
/// existing job with the `duplicate` flag set to `true`).
///
/// Serialised in the API as snake_case: `queued`, `active`, `exists`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UniqueScope {
    /// Prevent duplicates while this job is in the [`Scheduled`] or
    /// [`Ready`] states. As soon as the job is dequeued, a subsequent
    /// enqueue is permitted.
    ///
    /// [`Scheduled`]: crate::JobStatus::Scheduled
    /// [`Ready`]: crate::JobStatus::Ready
    Queued,

    /// Prevent duplicate enqueues while this job is in the [`Scheduled`],
    /// [`Ready`] or [`InFlight`] states. As soon as the job completes or
    /// exceeds its retry limit, a subsequent enqueue is permitted.
    ///
    /// [`Scheduled`]: crate::JobStatus::Scheduled
    /// [`Ready`]: crate::JobStatus::Ready
    /// [`InFlight`]: crate::JobStatus::InFlight
    Active,

    /// Prevent duplicate enqueues for as long as this job exists on
    /// the server (i.e. until it is reaped, according to the retention
    /// policy).
    Exists,

    /// A scope this client version doesn't recognise — e.g. a newer
    /// server introduced a scope the client predates.
    ///
    /// The catch-all keeps an unknown scope from failing the whole
    /// [`Job`] decode, so older clients keep working against newer
    /// servers. Treat it as opaque — don't round-trip it back as a
    /// uniqueness scope on a new enqueue.
    ///
    /// [`Job`]: crate::Job
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_constructor_stores_key_without_scope() {
        let k = UniqueKey::raw("abc");
        assert_eq!(k.key, "abc");
        assert_eq!(k.scope, None);
    }

    #[test]
    fn from_str_uses_raw() {
        let k: UniqueKey = "abc".into();
        assert_eq!(k.key, "abc");
    }

    #[test]
    fn scope_attaches() {
        let k = UniqueKey::raw("abc").scope(UniqueScope::Exists);
        assert_eq!(k.scope, Some(UniqueScope::Exists));
    }

    #[test]
    fn known_scope_deserialises() {
        let s: UniqueScope = serde_json::from_str("\"active\"").unwrap();
        assert_eq!(s, UniqueScope::Active);
    }

    #[test]
    fn unknown_scope_falls_back_to_unknown() {
        // A scope this client version doesn't know must not fail the
        // decode — it lands on the `Unknown` catch-all.
        let s: UniqueScope = serde_json::from_str("\"some_future_scope\"").unwrap();
        assert_eq!(s, UniqueScope::Unknown);
    }
}
