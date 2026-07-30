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
/// Direct construction — full control over `when` and `fold`:
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
///
/// Templated via the fluent builder — cap `.notifications` at 100
/// entries per batch:
///
/// ```
/// use zizq::BatchConfig;
///
/// let cfg = BatchConfig::at(".notifications", 100).keyed_by("push:tenant-42");
/// assert_eq!(cfg.key, "push:tenant-42");
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

impl BatchConfig {
    /// Start a fluent builder that targets an array at `path` within
    /// the payload and caps the combined length at `limit`. Complete
    /// the builder with [`BatchConfigAt::keyed_by`] to attach a batch
    /// key and produce a [`BatchConfig`].
    ///
    /// Pass `.` as `path` to batch the entire payload (assumed to be
    /// an array).
    ///
    /// The generated `when` and `fold` use `$existing | <path>` /
    /// `$new | <path>` pipe access, so the same template shape works
    /// for the root case (`.`) and any nested path.
    ///
    /// # Examples
    ///
    /// ```
    /// use zizq::BatchConfig;
    ///
    /// // Cap `.deviceIds` at 100 entries per batch.
    /// let cfg = BatchConfig::at(".deviceIds", 100).keyed_by("push:apple");
    ///
    /// // Whole-payload batch (payload is an array of events).
    /// let cfg = BatchConfig::at(".", 1000).keyed_by("audit");
    ///
    /// // Dedup entries as they accumulate.
    /// let cfg = BatchConfig::at(".deviceIds", 100)
    ///     .dedup()
    ///     .keyed_by("push:apple");
    /// ```
    pub fn at(path: impl Into<String>, limit: usize) -> BatchConfigAt {
        BatchConfigAt {
            path: path.into(),
            limit,
            mode: FoldMode::Append,
        }
    }
}

/// Fluent builder returned by [`BatchConfig::at`]. Templates the
/// `when`/`fold` expressions around a jq path and a length cap, and
/// completes into a [`BatchConfig`] via [`Self::keyed_by`].
///
/// The default fold appends new entries onto the existing batch;
/// [`Self::dedup`] and [`Self::sorted`] switch to the deduplicated or
/// sorted variants respectively.
pub struct BatchConfigAt {
    path: String,
    limit: usize,
    mode: FoldMode,
}

/// How [`BatchConfigAt`] combines the existing and incoming batch
/// values in the generated `fold` expression.
enum FoldMode {
    /// `$existing | <path> += ($new | <path>)`.
    Append,
    /// `$existing | <path> = ((<path>) + ($new | <path>) | unique)`.
    /// `unique` also sorts, so this subsumes [`Self::Sorted`].
    Dedup,
    /// `$existing | <path> = ((<path>) + ($new | <path>) | sort)`.
    Sorted,
}

impl BatchConfigAt {
    /// Switch the fold to deduplicate entries via jq's `unique`.
    /// Because `unique` also sorts, this subsumes [`Self::sorted`] —
    /// setting both keeps the dedup form.
    pub fn dedup(mut self) -> Self {
        self.mode = FoldMode::Dedup;
        self
    }

    /// Switch the fold to sort entries via jq's `sort`. Ignored if
    /// [`Self::dedup`] was also called.
    pub fn sorted(mut self) -> Self {
        // Preserve dedup precedence: only downgrade if we're still on
        // the default Append mode.
        if matches!(self.mode, FoldMode::Append) {
            self.mode = FoldMode::Sorted;
        }
        self
    }

    /// Finish the builder with a batch key. Only one unsealed batch
    /// exists on the server per key at a time.
    pub fn keyed_by(self, key: impl Into<String>) -> BatchConfig {
        let path = &self.path;
        let limit = self.limit;
        let when = format!("(($existing | {path}) + ($new | {path})) | length <= {limit}",);
        let fold = match self.mode {
            FoldMode::Append => format!("$existing | {path} += ($new | {path})"),
            FoldMode::Dedup => {
                format!("$existing | {path} = (({path}) + ($new | {path}) | unique)")
            }
            FoldMode::Sorted => {
                format!("$existing | {path} = (({path}) + ($new | {path}) | sort)")
            }
        };
        BatchConfig {
            key: key.into(),
            when,
            fold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn at_produces_append_when_and_fold_for_nested_path() {
        let cfg = BatchConfig::at(".deviceIds", 100).keyed_by("push:apple");
        assert_eq!(cfg.key, "push:apple");
        assert_eq!(
            cfg.when,
            "(($existing | .deviceIds) + ($new | .deviceIds)) | length <= 100",
        );
        assert_eq!(cfg.fold, "$existing | .deviceIds += ($new | .deviceIds)");
    }

    #[test]
    fn at_root_path_works_for_whole_payload() {
        let cfg = BatchConfig::at(".", 1000).keyed_by("audit");
        assert_eq!(cfg.when, "(($existing | .) + ($new | .)) | length <= 1000");
        assert_eq!(cfg.fold, "$existing | . += ($new | .)");
    }

    #[test]
    fn dedup_switches_fold_to_unique_form() {
        let cfg = BatchConfig::at(".deviceIds", 100)
            .dedup()
            .keyed_by("push:apple");
        assert_eq!(
            cfg.fold,
            "$existing | .deviceIds = ((.deviceIds) + ($new | .deviceIds) | unique)",
        );
    }

    #[test]
    fn sorted_switches_fold_to_sort_form() {
        let cfg = BatchConfig::at(".deviceIds", 100)
            .sorted()
            .keyed_by("push:apple");
        assert_eq!(
            cfg.fold,
            "$existing | .deviceIds = ((.deviceIds) + ($new | .deviceIds) | sort)",
        );
    }

    #[test]
    fn dedup_takes_precedence_over_sorted_regardless_of_call_order() {
        // dedup then sorted — sorted is a no-op.
        let a = BatchConfig::at(".x", 10).dedup().sorted().keyed_by("k");
        assert!(a.fold.ends_with("unique)"));

        // sorted then dedup — dedup wins.
        let b = BatchConfig::at(".x", 10).sorted().dedup().keyed_by("k");
        assert!(b.fold.ends_with("unique)"));
    }
}
