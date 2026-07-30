// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Batched-job configuration — the [`BatchConfig`] carried on an
//! enqueue request that opts the job into server-side folding.
//!
//! Batched jobs require a [Pro license](https://zizq.io/pricing) on
//! the server.
//!
//! When a batched enqueue arrives, the server looks for an existing
//! enqueued job with the same [`BatchConfig::key`]; if found, it
//! evaluates [`BatchConfig::when`] against the existing and incoming
//! payloads to decide whether to fold, and [`BatchConfig::fold`] to
//! produce the merged payload. When the predicate returns falsy the
//! existing batch is sealed and a fresh job is created from this
//! enqueue instead.
//!
//! For end-to-end context see [`JobKind::batch`] and
//! [`EnqueueBuilder::batch`].
//!
//! [`JobKind::batch`]: crate::JobKind::batch
//! [`EnqueueBuilder::batch`]: crate::EnqueueBuilder::batch

use serde::{Deserialize, Serialize};

/// A batched-job configuration attached to an enqueue request.
///
/// The three fields map directly to the server API — see the [module
/// docs](self) for the semantics of each. Applications will generally
/// use the fluent builder methods rather than constructing the config
/// directly.
///
/// # Examples
///
/// ```
/// use zizq::BatchConfig;
///
/// let cfg = BatchConfig {
///     key: "push:tenant-42".into(),
///     when: "($existing.notifications + $new.notifications) | length <= 100".into(),
///     fold: "$existing | .notifications += ($new | .notifications)".into(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Identifies the batch. Only one unsealed batch exists on the
    /// server per key at a time.
    pub key: String,

    /// `jq` predicate that runs with `$existing` bound to the current
    /// enqueued payload and `$new` bound to the incoming payload.
    /// When the expression evaluates truthy, the incoming payload folds
    /// into the existing batch's payload; when it evaluates falsy, the
    /// existing batch is sealed and a new one is started by creating a
    /// new job with the incoming payload.
    pub when: String,

    /// `jq` expression that runs with `$existing` and `$new` bound
    /// (same as `when`) and produces the merged payload for the batch.
    pub fold: String,
}
