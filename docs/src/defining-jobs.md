# Defining Jobs

Every job your application produces or consumes is a Rust type that implements
the `JobKind` trait. The type *is* the payload — its fields are what gets
serialized and stored on the server.

The recommended way to implement `JobKind` is via `#[derive(JobKind)]` with
`#[zizq(...)]` attributes:

> Rust:
>
> ```rust
> use serde::{Deserialize, Serialize};
> use zizq::JobKind;
> 
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "send_email")]
> struct SendEmail {
>     to: String,
>     subject: String,
> }
> ```

The payload type must also derive (or implement) `serde::Serialize` and
`serde::Deserialize` — the client serializes it when enqueuing and
deserializes it when a worker receives it.

The derive lives behind the crate's `derive` feature, which is on by default.
If you'd rather skip the proc-macro compile-time cost, disable defaults
(`default-features = false`) and [implement the trait by
hand](#manual-jobkind-impl) instead.

## The job type name

`name` is the only field that never has a default — either the attribute is
set, or the derive falls back to the struct's identifier. It is the API-level
type name the server stores as the job's `type`, and what a `Router` uses to
dispatch a received job back to the correct handler.

The name must be stable: changing it after jobs have been enqueued means the
worker will no longer recognise the in-flight jobs.

## Per-type defaults

Everything else is optional. When absent, the trait defaults apply; when
present, they win over the trait but can *still* be overridden per-job at the
call site (see [Enqueuing Jobs](./enqueuing-jobs.md)).

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(
>     name = "send_email",
>     queue = "emails",
>     priority = 50,
>     retry_limit = 10,
> )]
> struct SendEmail {
>     to: String,
> }
> ```

<table>
    <thead>
        <tr><th>Attribute</th><th>Description</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>queue = "..."</code></td>
            <td>
                The queue this job is placed on. Defaults to <code>"default"</code>.
                Queues need not be created — they exist implicitly once a job
                is enqueued to them.
            </td>
        </tr>
        <tr>
            <td><code>priority = N</code></td>
            <td>
                Priority within the queue — lower values run sooner, valid
                range 0–65535. Absent means the server default applies
                (typically 32768).
            </td>
        </tr>
        <tr>
            <td><code>retry_limit = N</code></td>
            <td>
                Maximum attempts before a failing job is considered dead.
                Absent means the server default applies.
            </td>
        </tr>
        <tr>
            <td><code>backoff(base_ms = ..., exponent = ..., jitter_ms = ...)</code></td>
            <td>
                Retry-delay curve. All three fields are required when this
                attribute is present. Absent means the server default.
            </td>
        </tr>
        <tr>
            <td><code>retention(completed_ms = ..., dead_ms = ...)</code></td>
            <td>
                How long completed and dead jobs remain visible. At least one
                field is required when this attribute is present; the other
                falls through to the server default.
            </td>
        </tr>
    </tbody>
</table>

Numeric fields accept any const-evaluable expression, so
`retention(dead_ms = 7 * 24 * 60 * 60 * 1000)` reads naturally.

## Uniqueness and batching

Two attributes generate trait *methods* rather than associated constants:

- `#[zizq(unique(...))]` — generates `fn unique_key(&self)`. Enables
  server-side deduplication of enqueues. See [Unique Jobs](./unique-jobs.md).
- `#[zizq(batch(...))]` — generates `fn batch(&self)`. Enables server-side
  folding of successive enqueues into a single pending job. See
  [Batched Jobs](./batched-jobs.md).

Both dedicated pages walk through the attribute grammar in detail.

## Scalar payloads

A job with no meaningful payload can be a unit struct, and a job whose payload
*is* a scalar can use a `#[serde(transparent)]` newtype:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "heartbeat")]
> struct Heartbeat;
> 
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "process_report")]
> #[serde(transparent)]
> struct ProcessReport(u64);
> ```

## Manual JobKind impl

The derive covers most cases. But a hand-written `impl JobKind for T` may
occasionally be required if you need something the attribute grammar doesn't
express, such as generic payload types, defaults computed at runtime, or a
`unique_key` / `batch` that applies custom logic on `&self` in ways the
attribute form can't capture.

Every attribute has a corresponding associated constant or trait method:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> use zizq::{BackoffConfig, JobKind, RetentionConfig, UniqueKey};
> 
> # #[derive(Serialize, Deserialize)]
> # struct SendEmail { user_id: u64, to: String }
> impl JobKind for SendEmail {
>     const NAME: &'static str = "send_email";
>     const QUEUE: &'static str = "emails";
>     const PRIORITY: Option<u16> = Some(50);
>     const RETRY_LIMIT: Option<u32> = Some(10);
>     const BACKOFF: Option<BackoffConfig> = Some(BackoffConfig {
>         base_ms: 1000,
>         exponent: 2.0,
>         jitter_ms: 500,
>     });
>     const RETENTION: Option<RetentionConfig> = Some(RetentionConfig {
>         completed_ms: Some(60_000),
>         dead_ms: None,
>     });
> 
>     fn unique_key(&self) -> Option<UniqueKey> {
>         Some(UniqueKey::tagged_hash_of(Self::NAME, &self.user_id))
>     }
> }
> ```

Mixing is fine — a job type you define by hand and another via derive coexist
in the same router and worker without issue. However it is not possibly for a
single job to be partially derived and partially hand-implemented.
