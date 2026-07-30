// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! The patch payload shared by the single-job and bulk `PATCH /jobs`
//! endpoints — [`JobPatch`] and its nested [`RetentionPatch`].
//!
//! # Three-state fields
//!
//! A JSON Merge Patch distinguishes three things a field can express,
//! which a plain `Option` cannot:
//!
//! - **keep** — the field is absent from the request; the server
//!   leaves it unchanged.
//! - **clear** — the field is present as `null`; the server resets it
//!   to its default.
//! - **set** — the field carries a new value.
//!
//! [`JobPatch`] exposes a method per state instead — `field(v)`
//! to set, `clear_field()` to clear, `keep_field()` to reset back to
//! keep (useful when conditional builder logic needs to undo an
//! earlier choice). Fields the server forbids clearing (`queue`,
//! `priority`) get only the set / keep pair.

use serde::{Serialize, Serializer};

use crate::resources::BackoffConfig;

/// Three-state value for a JSON Merge Patch field.
///
/// See the [module docs](self) for the keep / clear / set model.
#[derive(Debug, Clone, Default)]
enum Field<T> {
    /// Absent from the request body — leave the field unchanged.
    #[default]
    Keep,

    /// Present as `null` — reset the field to its server default.
    Clear,

    /// Present with a new value.
    Set(T),
}

impl<T> Field<T> {
    /// True for [`Field::Keep`]. Used as the struct fields'
    /// `skip_serializing_if` predicate so a kept field emits nothing.
    fn is_keep(&self) -> bool {
        matches!(self, Field::Keep)
    }
}

impl<T: Serialize> Serialize for Field<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            // Every `Field` struct member is tagged
            // `skip_serializing_if = "Field::is_keep"`, so a `Keep`
            // never reaches serialization. Reaching here means some
            // field is missing that attribute — a bug. Panic rather
            // than emit `null`, which the server reads as Clear and
            // would wipe the field the caller asked to leave alone.
            Field::Keep => {
                unreachable!("Field::Keep must be skipped via skip_serializing_if")
            }
            Field::Clear => serializer.serialize_none(),
            Field::Set(value) => serializer.serialize_some(value),
        }
    }
}

/// A set of mutable-field changes to apply to one or more jobs.
///
/// Built standalone, then handed to [`Client::patch_job`] (single
/// job) or [`PatchJobsBuilder::patch`] (bulk). A field a setter never
/// touched is left unchanged on the server; a freshly constructed
/// `JobPatch` therefore changes nothing.
///
/// [`Client::patch_job`]: crate::Client::patch_job
/// [`PatchJobsBuilder::patch`]: crate::PatchJobsBuilder::patch
///
/// # Examples
///
/// ```
/// use zizq::JobPatch;
///
/// let patch = JobPatch::new()
///     .priority(10)          // set priority
///     .retry_limit(5)        // set retry limit
///     .clear_backoff()       // reset backoff to the server default
///     .ready_now();          // make the job ready immediately
/// ```
#[derive(Debug, Clone, Default, Serialize)]
pub struct JobPatch {
    /// Move the job to a different queue. The server forbids a null
    /// queue, so this field is only ever `Keep` or `Set` — there is
    /// no `clear_queue`.
    #[serde(skip_serializing_if = "Field::is_keep")]
    queue: Field<String>,

    /// New priority. The server forbids a null priority, so this
    /// field is only ever `Keep` or `Set` — there is no
    /// `clear_priority`.
    #[serde(skip_serializing_if = "Field::is_keep")]
    priority: Field<u16>,

    /// New ready-at time, in Unix milliseconds.
    #[serde(skip_serializing_if = "Field::is_keep")]
    ready_at: Field<u64>,

    /// New retry limit.
    #[serde(skip_serializing_if = "Field::is_keep")]
    retry_limit: Field<u32>,

    /// New backoff configuration.
    #[serde(skip_serializing_if = "Field::is_keep")]
    backoff: Field<BackoffConfig>,

    /// Retention changes — itself a sub-field merge-patch.
    #[serde(skip_serializing_if = "Field::is_keep")]
    retention: Field<RetentionPatch>,
}

impl JobPatch {
    /// A patch that changes nothing. Chain setters to fill it in.
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the job to `queue`.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Field::Set(queue.into());
        self
    }

    /// Move the job to `queue`. A readable alias for [`Self::queue`].
    pub fn move_to_queue(self, queue: impl Into<String>) -> Self {
        self.queue(queue)
    }

    /// Reset the queue change — leave the job's queue unchanged.
    pub fn keep_queue(mut self) -> Self {
        self.queue = Field::Keep;
        self
    }

    /// Set the job's priority. Lower values run sooner; valid range
    /// is 0 to 65535.
    pub fn priority(mut self, priority: u16) -> Self {
        self.priority = Field::Set(priority);
        self
    }

    /// Reset the priority change — leave the job's priority unchanged.
    pub fn keep_priority(mut self) -> Self {
        self.priority = Field::Keep;
        self
    }

    /// Set the job's ready-at time, in Unix milliseconds. A future
    /// time moves a ready job to `Scheduled`.
    pub fn ready_at(mut self, ready_at_ms: u64) -> Self {
        self.ready_at = Field::Set(ready_at_ms);
        self
    }

    /// Clear the ready-at time, making the job ready immediately.
    pub fn clear_ready_at(mut self) -> Self {
        self.ready_at = Field::Clear;
        self
    }

    /// Make the job ready immediately. An alias for
    /// [`Self::clear_ready_at`].
    pub fn ready_now(self) -> Self {
        self.clear_ready_at()
    }

    /// Reset the ready-at change — leave it unchanged.
    pub fn keep_ready_at(mut self) -> Self {
        self.ready_at = Field::Keep;
        self
    }

    /// Set the job's retry limit.
    pub fn retry_limit(mut self, retry_limit: u32) -> Self {
        self.retry_limit = Field::Set(retry_limit);
        self
    }

    /// Clear the retry limit, resetting it to the server default.
    pub fn clear_retry_limit(mut self) -> Self {
        self.retry_limit = Field::Clear;
        self
    }

    /// Reset the retry-limit change — leave it unchanged.
    pub fn keep_retry_limit(mut self) -> Self {
        self.retry_limit = Field::Keep;
        self
    }

    /// Set the job's backoff configuration.
    pub fn backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = Field::Set(backoff);
        self
    }

    /// Clear the backoff configuration, resetting it to the server
    /// default.
    pub fn clear_backoff(mut self) -> Self {
        self.backoff = Field::Clear;
        self
    }

    /// Reset the backoff change — leave it unchanged.
    pub fn keep_backoff(mut self) -> Self {
        self.backoff = Field::Keep;
        self
    }

    /// Apply a retention merge-patch — see [`RetentionPatch`].
    pub fn retention(mut self, retention: RetentionPatch) -> Self {
        self.retention = Field::Set(retention);
        self
    }

    /// Clear the retention configuration entirely, resetting it to
    /// the server default.
    pub fn clear_retention(mut self) -> Self {
        self.retention = Field::Clear;
        self
    }

    /// Reset the retention change — leave it unchanged.
    pub fn keep_retention(mut self) -> Self {
        self.retention = Field::Keep;
        self
    }
}

/// A merge-patch for a job's retention configuration.
///
/// Passed to [`JobPatch::retention`]. Each sub-field is independently
/// three-state: set it, clear it (reset to the server default), or
/// leave it unchanged. To clear retention as a whole instead, use
/// [`JobPatch::clear_retention`].
///
/// # Examples
///
/// ```
/// use zizq::{JobPatch, RetentionPatch};
///
/// let patch = JobPatch::new().retention(
///     RetentionPatch::new()
///         .completed_ms(86_400_000)   // keep completed jobs 1 day
///         .clear_dead_ms(),           // reset dead retention to default
/// );
/// ```
#[derive(Debug, Clone, Default, Serialize)]
pub struct RetentionPatch {
    /// How long completed jobs are retained, in milliseconds.
    #[serde(skip_serializing_if = "Field::is_keep")]
    completed_ms: Field<u64>,

    /// How long dead jobs are retained, in milliseconds.
    #[serde(skip_serializing_if = "Field::is_keep")]
    dead_ms: Field<u64>,
}

impl RetentionPatch {
    /// A retention patch that changes nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set how long completed jobs are retained, in milliseconds.
    pub fn completed_ms(mut self, completed_ms: u64) -> Self {
        self.completed_ms = Field::Set(completed_ms);
        self
    }

    /// Clear the completed-job retention, resetting it to the server
    /// default.
    pub fn clear_completed_ms(mut self) -> Self {
        self.completed_ms = Field::Clear;
        self
    }

    /// Reset the completed-job retention change — leave it unchanged.
    pub fn keep_completed_ms(mut self) -> Self {
        self.completed_ms = Field::Keep;
        self
    }

    /// Set how long dead jobs are retained, in milliseconds.
    pub fn dead_ms(mut self, dead_ms: u64) -> Self {
        self.dead_ms = Field::Set(dead_ms);
        self
    }

    /// Clear the dead-job retention, resetting it to the server
    /// default.
    pub fn clear_dead_ms(mut self) -> Self {
        self.dead_ms = Field::Clear;
        self
    }

    /// Reset the dead-job retention change — leave it unchanged.
    pub fn keep_dead_ms(mut self) -> Self {
        self.dead_ms = Field::Keep;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_patch_serializes_to_empty_object() {
        // Every field is `Keep` — all skipped.
        assert_eq!(serde_json::to_value(JobPatch::new()).unwrap(), json!({}));
    }

    #[test]
    fn set_fields_serialize_their_values() {
        let patch = JobPatch::new().priority(10).retry_limit(5).ready_at(1234);
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({ "priority": 10, "retry_limit": 5, "ready_at": 1234 }),
        );
    }

    #[test]
    fn cleared_fields_serialize_as_null() {
        let patch = JobPatch::new().clear_retry_limit().clear_backoff();
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({ "retry_limit": null, "backoff": null }),
        );
    }

    #[test]
    fn ready_now_is_a_null_ready_at() {
        assert_eq!(
            serde_json::to_value(JobPatch::new().ready_now()).unwrap(),
            json!({ "ready_at": null }),
        );
    }

    #[test]
    fn keep_reverses_an_earlier_set() {
        // A `keep_*` call after a set undoes it — the field drops out.
        let patch = JobPatch::new().priority(99).keep_priority();
        assert_eq!(serde_json::to_value(patch).unwrap(), json!({}));
    }

    #[test]
    fn keep_reverses_an_earlier_clear() {
        let patch = JobPatch::new().clear_retry_limit().keep_retry_limit();
        assert_eq!(serde_json::to_value(patch).unwrap(), json!({}));
    }

    #[test]
    fn move_to_queue_is_an_alias_for_queue() {
        assert_eq!(
            serde_json::to_value(JobPatch::new().move_to_queue("q")).unwrap(),
            json!({ "queue": "q" }),
        );
    }

    #[test]
    fn retention_is_a_nested_merge_patch() {
        let patch =
            JobPatch::new().retention(RetentionPatch::new().completed_ms(1000).clear_dead_ms());
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({ "retention": { "completed_ms": 1000, "dead_ms": null } }),
        );
    }

    #[test]
    fn clear_retention_nulls_the_whole_field() {
        assert_eq!(
            serde_json::to_value(JobPatch::new().clear_retention()).unwrap(),
            json!({ "retention": null }),
        );
    }
}
