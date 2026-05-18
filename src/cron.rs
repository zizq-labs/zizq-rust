// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Cron scheduling — recurring jobs grouped under a named schedule.
//!
//! A *cron group* is a named set of *entries*; each entry pairs a cron
//! expression with a job template that the server enqueues on each
//! tick.
//!
//! Cron requires a [Pro license](https://zizq.io/pricing) on the server.
//! Calls made against a server without a Pro license surface as
//! [`ZizqError::Response`] with `status: 403`.
//!
//! # Writing vs reading
//!
//! Entries are constructed for adding to the schedule using  [`CronEntry`]
//! which embeds an [`EnqueueBuilder`] to specify the job to be enqueued,
//! exactly as you would ordinarily enqueue a job.
//!
//! [`CronEntryRecord`] and [`JobTemplate`] are the equivalent types returned
//! from the server.
//!
//! [`ZizqError::Response`]: crate::ZizqError::Response

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::client::{Client, EnqueueRequest};
use crate::enqueue::EnqueueBuilder;
use crate::error::ZizqError;
use crate::job::JobKind;
use crate::resources::{BackoffConfig, RetentionConfig};
use crate::unique_key::UniqueScope;

// --- Read types ------------------------------------------------------------

/// The job template stored on a cron entry — the shape of an enqueue
/// request, as read back from the server.
///
/// This is the read-side counterpart of an [`EnqueueBuilder`]: the
/// fields a job carries when the server enqueues it on each tick.
#[derive(Debug, Clone, Deserialize)]
pub struct JobTemplate {
    /// The job type (`JobKind::NAME`).
    #[serde(rename = "type")]
    pub job_type: String,

    /// Queue the job is enqueued onto.
    pub queue: String,

    /// Arbitrary payload, as round-tripped JSON.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,

    /// Priority. `None` means the server default applies.
    #[serde(default)]
    pub priority: Option<u16>,

    /// Maximum attempts before the job is considered dead. `None`
    /// means the server default applies.
    #[serde(default)]
    pub retry_limit: Option<u32>,

    /// Backoff configuration. `None` means the server default applies.
    #[serde(default)]
    pub backoff: Option<BackoffConfig>,

    /// Retention configuration. `None` means the server default
    /// applies.
    #[serde(default)]
    pub retention: Option<RetentionConfig>,

    /// Uniqueness key carried by each enqueued job, if any.
    #[serde(default)]
    pub unique_key: Option<String>,

    /// Lifecycle scope the `unique_key` applies for, if any.
    #[serde(default)]
    pub unique_while: Option<UniqueScope>,
}

/// A cron entry as returned by the server — the definition plus
/// scheduling runtime state.
#[derive(Debug, Clone, Deserialize)]
pub struct CronEntryRecord {
    /// Entry name, unique within its group.
    pub name: String,

    /// Cron expression (e.g. `"*/15 * * * *"`).
    pub expression: String,

    /// IANA timezone the expression is evaluated in. `None` means the
    /// server's local timezone.
    #[serde(default)]
    pub timezone: Option<String>,

    /// Whether this entry is currently paused.
    pub paused: bool,

    /// When the entry was last paused, as Unix ms.
    #[serde(default)]
    pub paused_at: Option<u64>,

    /// When the entry was last resumed, as Unix ms.
    #[serde(default)]
    pub resumed_at: Option<u64>,

    /// The job template enqueued on each tick.
    pub job: JobTemplate,

    /// Next scheduled enqueue time, as Unix ms.
    #[serde(default)]
    pub next_enqueue_at: Option<u64>,

    /// When this entry last enqueued a job, as Unix ms.
    #[serde(default)]
    pub last_enqueue_at: Option<u64>,
}

/// A cron group as returned by the server.
#[derive(Debug, Clone, Deserialize)]
pub struct CronGroup {
    /// Group name.
    pub name: String,

    /// Whether the whole group is paused.
    pub paused: bool,

    /// When the group was last paused, as Unix ms.
    #[serde(default)]
    pub paused_at: Option<u64>,

    /// When the group was last resumed, as Unix ms.
    #[serde(default)]
    pub resumed_at: Option<u64>,

    /// The entries in the group.
    pub entries: Vec<CronEntryRecord>,
}

// --- Write side ------------------------------------------------------------

/// API serialized form of a cron entry — the body sent to the server. Built
/// from a [`CronEntry`] once its job template is resolved.
#[derive(Serialize)]
pub(crate) struct CronEntryBody {
    /// Application-defined name for the entry.
    pub(crate) name: String,

    /// 5 or 6 field cron expression.
    pub(crate) expression: String,

    /// Optional time zone in which the expression is evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    timezone: Option<String>,

    /// True if the entry is paused.
    #[serde(skip_serializing_if = "Option::is_none")]
    paused: Option<bool>,

    /// Details of what gets enqueued for this entry.
    job: EnqueueRequest,
}

/// A cron entry to create or replace.
///
/// Build one from a job — the same [`EnqueueBuilder`] you would use
/// to enqueue that job — and hand it to [`Client::add_cron_entry`],
/// [`Client::put_cron_entry`], or [`ReplaceCronBuilder::entry`].
///
/// The server rejects a job template that sets a ready-at time
/// (`.delay()` / `.ready_at()` / `.run_at()` on the builder) — a cron
/// entry's schedule comes from its expression, not the job.
///
/// [`Client::add_cron_entry`]: crate::Client::add_cron_entry
/// [`Client::put_cron_entry`]: crate::Client::put_cron_entry
///
/// # Examples
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use zizq::{Client, CronEntry, JobKind};
/// # #[derive(Serialize, Deserialize)]
/// # struct Cleanup { older_than_days: u32 }
/// # impl JobKind for Cleanup { const NAME: &'static str = "cleanup"; }
/// # fn build(client: &Client) -> CronEntry {
/// CronEntry::new(
///     "nightly-cleanup",
///     "0 0 * * *",
///     client.enqueue(Cleanup { older_than_days: 30 }),
/// )
/// .timezone("Australia/Melbourne")
/// # }
/// ```
pub struct CronEntry {
    /// Application-defined name for the entry.
    name: String,

    /// 5 or 6 field cron expression.
    expression: String,

    /// Optional time zone in which the expression is evaluated.
    timezone: Option<String>,

    /// True if the entry is paused.
    paused: Option<bool>,

    /// Resolved job template, or a deferred serialisation error — the
    /// error surfaces when the entry is submitted, mirroring how
    /// `BulkEnqueueBuilder` defers per-job errors.
    job: Result<EnqueueRequest, ZizqError>,
}

impl CronEntry {
    /// Define a cron entry: a `name` (unique within its group), a cron
    /// `expression`, and the `job` to enqueue on each tick.
    pub fn new<T: JobKind>(
        name: impl Into<String>,
        expression: impl Into<String>,
        job: EnqueueBuilder<'_, T>,
    ) -> Self {
        Self {
            name: name.into(),
            expression: expression.into(),
            timezone: None,
            paused: None,
            job: job.into_request(),
        }
    }

    /// Evaluate the cron expression in the given IANA timezone (e.g.
    /// `"Australia/Melbourne"`). Defaults to the server's local
    /// timezone.
    pub fn timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = Some(timezone.into());
        self
    }

    /// Create the entry paused. Defaults to running (not paused).
    pub fn paused(mut self, paused: bool) -> Self {
        self.paused = Some(paused);
        self
    }

    /// Resolve into the API request body, surfacing any deferred job
    /// serialisation error.
    pub(crate) fn into_body(self) -> Result<CronEntryBody, ZizqError> {
        Ok(CronEntryBody {
            name: self.name,
            expression: self.expression,
            timezone: self.timezone,
            paused: self.paused,
            job: self.job?,
        })
    }
}

/// Request body for `PUT /crons/{name}`.
#[derive(Serialize)]
pub(crate) struct ReplaceCronGroupBody {
    /// True if this group is paused.
    #[serde(skip_serializing_if = "Option::is_none")]
    paused: Option<bool>,

    /// List of entries on the schedule.
    entries: Vec<CronEntryBody>,
}

/// Builder for [`Client::replace_cron`].
///
/// Produced by [`Client::replace_cron`]. Chain [`entry`](Self::entry)
/// to add cron entries, then `.await` to atomically replace the
/// group's entire entry set, returning the resulting [`CronGroup`].
///
/// Entries absent from the replace are removed. Awaiting with no
/// entries added empties the group.
///
/// [`Client::replace_cron`]: crate::Client::replace_cron
///
/// # Examples
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use zizq::{Client, CronEntry, JobKind};
/// # #[derive(Serialize, Deserialize)]
/// # struct Cleanup;
/// # impl JobKind for Cleanup { const NAME: &'static str = "cleanup"; }
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let group = client
///     .replace_cron("maintenance")
///     .entry(CronEntry::new("cleanup", "0 0 * * *", client.enqueue(Cleanup)))
///     .await?;
/// println!("{} has {} entries", group.name, group.entries.len());
/// # Ok(()) }
/// ```
pub struct ReplaceCronBuilder<'a> {
    /// Client to which the final request is sent.
    client: &'a Client,

    /// Application-defined name for this cron group.
    name: String,

    /// True if the group is paused.
    paused: Option<bool>,

    /// Accumulated entry bodies, or the first deferred error from a
    /// [`CronEntry`] whose job failed to serialise.
    entries: Result<Vec<CronEntryBody>, ZizqError>,
}

impl<'a> ReplaceCronBuilder<'a> {
    pub(crate) fn new(client: &'a Client, name: String) -> Self {
        Self {
            client,
            name,
            paused: None,
            entries: Ok(Vec::new()),
        }
    }

    /// Add an entry to the group. Call once per entry.
    pub fn entry(mut self, entry: CronEntry) -> Self {
        if let Ok(entries) = &mut self.entries {
            match entry.into_body() {
                Ok(body) => entries.push(body),
                Err(e) => self.entries = Err(e),
            }
        }
        self
    }

    /// Set whether the whole group is paused. Omitted preserves the
    /// existing state (or defaults to running for a new group).
    pub fn paused(mut self, paused: bool) -> Self {
        self.paused = Some(paused);
        self
    }
}

impl<'a> IntoFuture for ReplaceCronBuilder<'a> {
    type Output = Result<CronGroup, ZizqError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    /// The .await implementation.
    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            let entries = self.entries?;
            let body = ReplaceCronGroupBody {
                paused: self.paused,
                entries,
            };
            let url = client.url(&["crons", self.name.as_str()]);
            client
                .send_body_decoded(reqwest::Method::PUT, url, body)
                .await
        })
    }
}
