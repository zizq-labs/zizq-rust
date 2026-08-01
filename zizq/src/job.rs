// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! The [`JobKind`] trait — how user types describe themselves to the
//! queue.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::batch::BatchConfig;
use crate::resources::{BackoffConfig, RetentionConfig};
use crate::unique_key::UniqueKey;

/// Describes a job type — its type name in the API and its enqueue-time
/// defaults.
///
/// Implement this on the payload struct for each job your application
/// produces. Most types can be built with the [`#[derive(JobKind)]`][derive]
/// proc-macro via `#[zizq(...)]` attributes; a hand-written `impl` is
/// available for cases the derive doesn't cover (generics, defaults
/// computed at runtime, `unique_key` / `batch` bodies that need custom
/// logic).
///
/// [`Self::NAME`] is the only required item — either the `name`
/// attribute is set, or the derive falls back to the struct's
/// identifier. Everything else has a default that can still be
/// overridden per-job at the call site via [`EnqueueBuilder`].
///
/// [derive]: derive@crate::JobKind
/// [`EnqueueBuilder`]: crate::EnqueueBuilder
///
/// # Examples
///
/// The recommended path — `#[derive(JobKind)]` with `#[zizq(...)]`:
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use zizq::JobKind;
///
/// #[derive(Serialize, Deserialize, JobKind)]
/// #[zizq(name = "send_email", queue = "emails", priority = 50)]
/// struct SendEmail {
///     to: String,
/// }
/// ```
///
/// See the `#[derive(JobKind)]` docs for the full attribute grammar
/// — `backoff(...)`, `retention(...)`, `unique(...)`, and `batch(...)`
/// are all supported.
///
/// Or the same shape written by hand — useful when you need behaviour
/// the attribute grammar can't express, and the form you'll produce if
/// you disable the `derive` feature:
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use zizq::{JobKind, UniqueKey};
///
/// #[derive(Serialize, Deserialize)]
/// struct SendEmail {
///     to: String,
/// }
///
/// impl JobKind for SendEmail {
///     const NAME: &'static str = "send_email";
///     const QUEUE: &'static str = "emails";
///     const PRIORITY: Option<u16> = Some(50);
///
///     fn unique_key(&self) -> Option<UniqueKey> {
///         Some(UniqueKey::tagged_hash_of(Self::NAME, &self.to))
///     }
/// }
/// ```
///
/// Deriving on some job types and hand-implementing on others is fine
/// — they coexist in the same worker and router without issue. But
/// Rust's coherence rule means each individual type is either fully
/// derived or fully hand-implemented; you can't split a single trait
/// impl across the two forms.
pub trait JobKind: Serialize + DeserializeOwned + Send + 'static {
    /// The underlying name of the job type used in the Zizq API.
    ///
    /// This is what the server stores as the job `type` and what the
    /// worker uses to dispatch to the correct handler.
    const NAME: &'static str;

    /// Queue this job is placed on when none is specified at the call
    /// site. Defaults to `"default"`.
    const QUEUE: &'static str = "default";

    /// Default priority used when none is specified at the call site.
    /// Lower values run sooner. Valid range is 0 to 65535; `None` lets
    /// the server apply its own default (typically `32768`).
    const PRIORITY: Option<u16> = None;

    /// Default retry budget used when none is specified at the call
    /// site. `None` lets the server apply its own default.
    const RETRY_LIMIT: Option<u32> = None;

    /// Default backoff configuration used when none is specified at
    /// the call site. When set to `None` the server’s default applies.
    const BACKOFF: Option<BackoffConfig> = None;

    /// Default retention configuration used when none is specified at
    /// the call site. When set to `None` the server’s default applies.
    const RETENTION: Option<RetentionConfig> = None;

    /// Derive a uniqueness key from this payload.
    ///
    /// Requires a [Pro license](https://zizq.io/pricing) on the server.
    ///
    /// Override this to produce a key from the payload's fields; the
    /// server will reject duplicate enqueues that match the same key
    /// within the chosen [`UniqueScope`]. Returning `None` (the
    /// default) means no default uniqueness is applied — but a key can
    /// still be supplied explicitly via [`EnqueueBuilder::unique_key`].
    ///
    /// [`UniqueScope`]: crate::UniqueScope
    /// [`EnqueueBuilder::unique_key`]: crate::EnqueueBuilder::unique_key
    fn unique_key(&self) -> Option<UniqueKey> {
        None
    }

    /// Derive a batched-job configuration from this payload.
    ///
    /// Requires a [Pro license](https://zizq.io/pricing) on the server.
    ///
    /// Override this to opt every enqueue of this type into server-side
    /// folding; the server groups pending enqueues by
    /// [`BatchConfig::key`] and evaluates [`BatchConfig::when`] and
    /// [`BatchConfig::fold`] to merge each incoming payload into the
    /// existing batch. Returning `None` (the default) means no default
    /// batching — a config can still be supplied per-call via
    /// [`EnqueueBuilder::batch`].
    ///
    /// [`EnqueueBuilder::batch`]: crate::EnqueueBuilder::batch
    fn batch(&self) -> Option<BatchConfig> {
        None
    }
}
