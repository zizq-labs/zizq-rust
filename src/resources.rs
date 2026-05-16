// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Server-side resource types returned in API responses.

use serde::Deserialize;

use crate::unique_key::{UniqueKey, UniqueScope};

/// The lifecycle state of a job on the server.
///
/// Serialised to/from snake_case for the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Eligible for dequeueing by a worker.
    Ready,

    /// Dequeued by a worker; awaiting acknowledgment or failure.
    InFlight,

    /// Scheduled to become ready at a future time.
    ///
    /// Also used for retries and backoff.
    Scheduled,

    /// Finished successfully.
    Completed,

    /// Exhausted its retry budget without succeeding.
    Dead,

    /// A status this client version doesn't recognise — e.g. a newer
    /// server introduced a status the client predates.
    ///
    /// The catch-all keeps an unknown status from failing the whole
    /// [`Job`] decode (and cascading to a failed `list_jobs` page or
    /// take-stream frame), so older clients keep working against
    /// newer servers.
    #[serde(other)]
    Unknown,
}

impl JobStatus {
    /// API-serialized format name. Used when building query strings for the
    /// list/count endpoints, which accept status filters as a
    /// comma-delimited list of these names.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::InFlight => "in_flight",
            Self::Scheduled => "scheduled",
            Self::Completed => "completed",
            Self::Dead => "dead",
            // `Unknown` only ever arises from deserialising a status
            // this client doesn't know — it should never be used as a
            // filter. If one somehow is, the server rejects the query
            // loudly rather than silently mis-filtering.
            Self::Unknown => "unknown",
        }
    }
}

/// One page of jobs returned by [`Client::list_jobs`].
///
/// Follow [`pages.next`](PageLinks::next) (or `.prev`) via
/// [`Client::get_page`] to navigate.
///
/// [`Client::list_jobs`]: crate::Client::list_jobs
/// [`Client::get_page`]: crate::Client::get_page
#[derive(Debug, Clone, Deserialize)]
pub struct JobPage {
    /// Jobs on this page, in the requested order.
    pub jobs: Vec<Job>,

    /// Pagination cursors as server-emitted paths.
    pub pages: PageLinks,
}

impl JobPage {
    /// A synthetic empty page. Returned by [`Client::list_jobs`] when
    /// a filter is explicitly set to an empty set — which can match no
    /// jobs — so no request is sent. `pages.next` / `pages.prev` are
    /// `None`.
    ///
    /// [`Client::list_jobs`]: crate::Client::list_jobs
    pub(crate) fn empty() -> Self {
        Self {
            jobs: Vec::new(),
            pages: PageLinks {
                next: None,
                prev: None,
            },
        }
    }
}

/// Pagination links emitted by listing endpoints — server-side paths
/// (no host) suitable for [`Client::get_page`].
///
/// [`Client::get_page`]: crate::Client::get_page
#[derive(Debug, Clone, Deserialize)]
pub struct PageLinks {
    /// Path to follow for the next page, or `None` at the end of the
    /// result set.
    pub next: Option<String>,

    /// Path to follow for the previous page, or `None` at the start.
    pub prev: Option<String>,
}

/// Per-job backoff configuration controlling the retry delay curve.
///
/// The delay applied before a retry is computed as:
///
/// ```text
/// t = base_ms + (attempts ** exponent) + (attempts * random() * jitter_ms)
/// ```
///
/// The random jitter spreads out clustered failures so a group of jobs
/// that all failed together do not all retry at the same moment.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, Deserialize)]
pub struct BackoffConfig {
    /// Base delay in milliseconds, applied to every retry.
    pub base_ms: u64,

    /// Backoff curve steepness — multiplied into `attempts ** exponent`.
    pub exponent: f64,

    /// Maximum random jitter in milliseconds per attempt multiplier.
    pub jitter_ms: u64,
}

/// Per-job retention configuration controlling how long jobs in
/// terminal states ([`JobStatus::Completed`] / [`JobStatus::Dead`])
/// remain visible before being reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, Deserialize)]
pub struct RetentionConfig {
    /// How long completed jobs remain visible (ms). When set to `None`
    /// the server’s default value applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_ms: Option<u64>,

    /// How long dead jobs remain visible (ms). When set to `None`
    /// the server’s default value applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_ms: Option<u64>,
}

/// A job record as returned by the server.
///
/// Timestamps are Unix milliseconds since the epoch. Fields typed as
/// `Option<_>` are only present in some lifecycle states (e.g.
/// `completed_at` is only set once the job has completed). `payload`
/// is omitted on metadata-only responses.
#[derive(Debug, Clone, Deserialize)]
#[serde(from = "JobFromApi")]
pub struct Job {
    /// Server-assigned identifier.
    pub id: String,

    /// The job type (derived from `JobKind::NAME`) registered when the
    /// job was enqueued.
    pub job_type: String,

    /// Queue this job belongs to.
    pub queue: String,

    /// Current lifecycle state.
    pub status: JobStatus,

    /// Priority within the queue. Lower values run sooner. Valid range
    /// is 0 to 65536; the server default is 32768.
    pub priority: u32,

    /// Arbitrary payload supplied by the enqueuer.
    ///
    /// `None` on metadata-only responses; otherwise the round-tripped
    /// JSON shape of whatever was originally enqueued. Payloads are
    /// guaranteed to be JSON-compatible — raw binary is not permitted
    /// — so this works equally for both wire formats.
    pub payload: Option<serde_json::Value>,

    /// Earliest time at which the job becomes ready, as Unix ms.
    ///
    /// Once this time is reached the job transitions automatically
    /// from [`JobStatus::Scheduled`] to [`JobStatus::Ready`].
    pub ready_at: i64,

    /// Number of attempts so far.
    pub attempts: u32,

    /// Maximum attempts permitted before the job is considered dead on
    /// failure. `None` means the server default applies.
    pub retry_limit: Option<u32>,

    /// Per-job backoff configuration. `None` means the server default
    /// applies.
    pub backoff: Option<BackoffConfig>,

    /// Time the job was most recently acquired by a worker, as Unix ms.
    pub dequeued_at: Option<i64>,

    /// Time the job last failed, as Unix ms.
    pub failed_at: Option<i64>,

    /// Time the job completed, as Unix ms.
    pub completed_at: Option<i64>,

    /// Per-job retention configuration. `None` means the server
    /// default applies.
    pub retention: Option<RetentionConfig>,

    /// Time at which the reaper will hard-delete this job, as Unix ms.
    pub purge_at: Option<i64>,

    /// Uniqueness constraint that this job carries, if any.
    pub unique_key: Option<UniqueKey>,

    /// `Some(true)` when this job was returned as a duplicate by an
    /// enqueue call that hit an existing uniqueness constraint. Only
    /// set on responses to enqueue requests; `None` everywhere else
    /// (e.g. when reading the job via `GET /jobs/{id}`).
    pub duplicate: Option<bool>,
}

// Raw shape of a Job as returned by the API. Flat `unique_key` /
// `unique_while` fields get folded into the public `Option<UniqueKey>`
// via the `From` impl below.
#[derive(Deserialize)]
struct JobFromApi {
    /// Server-assigned identifier.
    id: String,

    /// The job type registered when the job was enqueued.
    #[serde(rename = "type")]
    job_type: String,

    /// Queue this job belongs to.
    queue: String,

    /// Current lifecycle state.
    status: JobStatus,

    /// Priority within the queue. Lower values run sooner.
    priority: u32,
    #[serde(default)]

    /// Arbitrary payload supplied by the enqueuer.
    payload: Option<serde_json::Value>,

    /// Earliest time at which the job becomes ready, as Unix ms.
    ready_at: i64,

    /// Number of attempts so far.
    attempts: u32,

    /// Maximum attempts permitted before the job is considered dead.
    #[serde(default)]
    retry_limit: Option<u32>,

    /// Per-job backoff configuration.
    #[serde(default)]
    backoff: Option<BackoffConfig>,

    /// Time the job was most recently acquired by a worker, as Unix ms.
    #[serde(default)]
    dequeued_at: Option<i64>,

    /// Time the job last failed, as Unix ms.
    #[serde(default)]
    failed_at: Option<i64>,

    /// Time the job completed, as Unix ms.
    #[serde(default)]
    completed_at: Option<i64>,

    /// Per-job retention configuration.
    #[serde(default)]
    retention: Option<RetentionConfig>,

    /// Time at which the reaper will hard-delete this job, as Unix ms.
    #[serde(default)]
    purge_at: Option<i64>,

    /// The unique key use to prevent duplicate enqueues, if any.
    #[serde(default)]
    unique_key: Option<String>,

    /// The lifecycle scope for which the `unique_key` applies.
    #[serde(default)]
    unique_while: Option<UniqueScope>,

    /// `Some(true)` when this job was returned as a duplicate by an enqueue.
    #[serde(default)]
    duplicate: Option<bool>,
}

impl From<JobFromApi> for Job {
    fn from(w: JobFromApi) -> Self {
        let unique_key = w.unique_key.map(|key| UniqueKey {
            key,
            scope: w.unique_while,
        });

        Self {
            id: w.id,
            job_type: w.job_type,
            queue: w.queue,
            status: w.status,
            priority: w.priority,
            payload: w.payload,
            ready_at: w.ready_at,
            attempts: w.attempts,
            retry_limit: w.retry_limit,
            backoff: w.backoff,
            dequeued_at: w.dequeued_at,
            failed_at: w.failed_at,
            completed_at: w.completed_at,
            retention: w.retention,
            purge_at: w.purge_at,
            unique_key,
            duplicate: w.duplicate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_status_deserialises() {
        let s: JobStatus = serde_json::from_str("\"in_flight\"").unwrap();
        assert_eq!(s, JobStatus::InFlight);
    }

    #[test]
    fn unknown_status_falls_back_to_unknown() {
        // A status this client version doesn't know lands on the
        // `Unknown` catch-all instead of erroring.
        let s: JobStatus = serde_json::from_str("\"blocked\"").unwrap();
        assert_eq!(s, JobStatus::Unknown);
    }

    #[test]
    fn job_with_unknown_status_still_decodes() {
        // The catch-all must keep an unknown status from failing the
        // whole `Job` decode.
        let job: Job = serde_json::from_value(serde_json::json!({
            "id": "job-1",
            "type": "send_email",
            "queue": "emails",
            "status": "blocked",
            "priority": 50,
            "ready_at": 0,
            "attempts": 0,
        }))
        .unwrap();
        assert_eq!(job.status, JobStatus::Unknown);
        assert_eq!(job.id, "job-1");
    }
}
