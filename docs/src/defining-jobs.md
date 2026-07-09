# Defining Jobs

Every job your application produces or consumes is a Rust type that implements
the `JobKind` trait. The type *is* the payload — its fields are what gets
serialized and stored on the server.

> Rust:
>
> ```rust
> use serde::{Deserialize, Serialize};
> use zizq::JobKind;
> 
> #[derive(Serialize, Deserialize)]
> struct SendEmail {
>     to: String,
>     subject: String,
> }
> 
> impl JobKind for SendEmail {
>     const NAME: &'static str = "send_email";
> }
> ```

The payload type must derive (or implement) `serde::Serialize` and
`serde::Deserialize` — the client serializes it when enqueuing and
deserializes it when a worker receives it.

## The job type name

`NAME` is the only **required** associated constant. It is the API-level type
name the server stores as the job's `type`, and what a `Router` uses to
dispatch a received job back to the correct handler.

It must be stable: changing `NAME` after jobs have been enqueued means the
worker will no longer recognise the in-flight jobs.

## Per-type defaults

Everything other than `NAME` has a default that you may override on the trait,
and which can *still* be overridden per-job at the call site (see
[Enqueuing Jobs](./enqueuing-jobs.md)).

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> use zizq::{BackoffConfig, JobKind};
> 
> # #[derive(Serialize, Deserialize)]
> # struct SendEmail { to: String }
> impl JobKind for SendEmail {
>     const NAME: &'static str = "send_email";
>     const QUEUE: &'static str = "emails";
>     const PRIORITY: Option<u16> = Some(50);
>     const RETRY_LIMIT: Option<u32> = Some(10);
> }
> ```

<table>
    <thead>
        <tr><th>Constant</th><th>Description</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>QUEUE</code></td>
            <td>
                The queue this job is placed on. Defaults to <code>"default"</code>.
                Queues need not be created — they exist implicitly once a job
                is enqueued to them.
            </td>
        </tr>
        <tr>
            <td><code>PRIORITY</code></td>
            <td>
                Priority within the queue — lower values run sooner, valid
                range 0–65535. <code>None</code> lets the server apply its
                default (typically 32768).
            </td>
        </tr>
        <tr>
            <td><code>RETRY_LIMIT</code></td>
            <td>
                Maximum attempts before a failing job is considered dead.
                <code>None</code> uses the server default.
            </td>
        </tr>
        <tr>
            <td><code>BACKOFF</code></td>
            <td>
                A <code>BackoffConfig</code> controlling the retry delay
                curve. <code>None</code> uses the server default.
            </td>
        </tr>
        <tr>
            <td><code>RETENTION</code></td>
            <td>
                A <code>RetentionConfig</code> controlling how long completed
                and dead jobs remain visible. <code>None</code> uses the
                server default.
            </td>
        </tr>
    </tbody>
</table>

## Deriving a unique key

Override the `unique_key` method to derive a deduplication key from a job's
payload — the server will then reject duplicate enqueues. See
[Unique Jobs](./unique-jobs.md) for the full detail.

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> use zizq::{JobKind, UniqueKey};
> 
> # #[derive(Serialize, Deserialize)]
> # struct SendEmail { to: String }
> impl JobKind for SendEmail {
>     const NAME: &'static str = "send_email";
> 
>     fn unique_key(&self) -> Option<UniqueKey> {
>         Some(UniqueKey::tagged_hash_of(Self::NAME, &self.to))
>     }
> }
> ```

## Scalar payloads

A job with no meaningful payload can be a unit struct, and a job whose payload
*is* a scalar can use a `#[serde(transparent)]` newtype:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize)]
> struct Heartbeat;
> 
> impl JobKind for Heartbeat {
>     const NAME: &'static str = "heartbeat";
> }
> 
> #[derive(Serialize, Deserialize)]
> #[serde(transparent)]
> struct ProcessReport(u64);
> 
> impl JobKind for ProcessReport {
>     const NAME: &'static str = "process_report";
> }
> ```
