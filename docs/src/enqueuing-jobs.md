# Enqueuing Jobs

Enqueue a job with `Client::enqueue`, passing a value of any type that
implements [`JobKind`](./defining-jobs.md). It returns an `EnqueueBuilder`
that you `.await` to send the request:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let job = client
>     .enqueue(SendEmail { to: "alice@example.com".into() })
>     .await?;
> 
> println!("enqueued {} on queue {}", job.id, job.queue);
> # Ok(()) }
> ```

Awaiting the builder returns the created `Job` — its server-assigned `id`,
resolved `queue`, `priority`, and so on.

## Per-job overrides

Before awaiting, chain methods to override the type's defaults for this one
call. Anything not overridden falls back to the [`JobKind`](./defining-jobs.md)
constant, then to the server default.

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use std::time::Duration;
> # use zizq::{Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(SendEmail { to: "alice@example.com".into() })
>     .queue("priority-emails")
>     .priority(10)
>     .delay(Duration::from_secs(3600))
>     .retry_limit(3)
>     .await?;
> # Ok(()) }
> ```

<table>
    <thead>
        <tr><th>Method</th><th>Description</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>queue</code></td>
            <td>Override the queue this job is placed on.</td>
        </tr>
        <tr>
            <td><code>priority</code></td>
            <td>Override the priority (0–65535, lower runs sooner).</td>
        </tr>
        <tr>
            <td><code>delay</code></td>
            <td>
                Make the job ready after the given <code>Duration</code> from
                now — it sits in the <code>Scheduled</code> state until then.
            </td>
        </tr>
        <tr>
            <td><code>ready_at</code> / <code>run_at</code></td>
            <td>
                Schedule the job for an absolute <code>OffsetDateTime</code>.
                The two names are aliases.
            </td>
        </tr>
        <tr>
            <td><code>retry_limit</code></td>
            <td>Override the retry budget.</td>
        </tr>
        <tr>
            <td><code>backoff</code></td>
            <td>Override the <code>BackoffConfig</code> retry-delay curve.</td>
        </tr>
        <tr>
            <td><code>retention</code></td>
            <td>Override the <code>RetentionConfig</code>.</td>
        </tr>
        <tr>
            <td><code>unique_key</code></td>
            <td>
                Attach a deduplication key for this enqueue — see
                <a href="./unique-jobs.md">Unique Jobs</a>.
            </td>
        </tr>
        <tr>
            <td><code>batch</code></td>
            <td>
                Attach a <code>BatchConfig</code> to fold this enqueue into
                an existing pending job — see
                <a href="./batched-jobs.md">Batched Jobs</a>.
            </td>
        </tr>
    </tbody>
</table>

## Bulk enqueue

To enqueue many jobs in a single request, use `Client::enqueue_bulk`. It
returns a `BulkEnqueueBuilder` that collects per-job `EnqueueBuilder`s:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let jobs = client
>     .enqueue_bulk()
>     .add(client.enqueue(SendEmail { to: "a@example.com".into() }))
>     .add(client.enqueue(SendEmail { to: "b@example.com".into() }).priority(10))
>     .await?;
> 
> assert_eq!(jobs.len(), 2);
> # Ok(()) }
> ```

`.add` is chainable and consuming. For building a batch in a loop, use the
mutating `.push` instead:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let mut batch = client.enqueue_bulk();
> for n in 0..1000 {
>     batch.push(client.enqueue(SendEmail { to: format!("user{n}@example.com") }));
> }
> let jobs = batch.await?;
> # Ok(()) }
> ```

Different `JobKind`s can be mixed freely within a single batch. Each batch is
enqueued atomically on the server.

> [!TIP]
> For very large numbers of jobs, send them in chunks (e.g. 1,000 per call)
> rather than one enormous batch — this bounds memory and keeps the
> connection responsive.
