# Running Workers

A `Worker` streams jobs from the server, dispatches each to a handler with
bounded concurrency, batches acknowledgements, and reconnects on transient
failures. Build one with `Worker::builder()`:

```rust
# use serde::{Deserialize, Serialize};
# use std::convert::Infallible;
# use zizq::{Client, JobKind, Router, Worker};
# #[derive(Serialize, Deserialize)]
# struct SendEmail { to: String }
# impl JobKind for SendEmail { const NAME: &'static str = "send_email"; }
# async fn run(client: Client) -> Result<(), Box<dyn std::error::Error>> {
let worker = Worker::builder()
    .client(client)
    .concurrency(25)
    .queues(vec!["emails"])
    .handler(Router::new().route(async |job: SendEmail| {
        // your application logic here
        println!("emailing {}", job.to);
        Ok::<(), Infallible>(())
    }))
    .build()?;

worker.run(tokio::signal::ctrl_c()).await?;  // runs until Ctrl-C
# Ok(()) }
```

`worker.run(shutdown)` takes a *shutdown future* — when it resolves, the
worker shuts down gracefully (see [below](#graceful-shutdown)). Pass
`tokio::signal::ctrl_c()`, a `CancellationToken`, or
`std::future::pending()` to run until the process is killed.

## Handlers

The `handler` passed to a `Worker` is anything that implements the
`JobHandler` trait. The simplest handler is an async closure taking a `Job` —
the raw job, with its `id`, `job_type`, and `payload` (as JSON):

```rust
use zizq::{Client, Job, Worker};
# async fn run(client: Client) -> Result<(), Box<dyn std::error::Error>> {
let worker = Worker::builder()
    .client(client)
    .concurrency(25)
    .queues(vec!["emails"])
    .handler(|job: Job| async move {
        println!("processing {} ({})", job.id, job.job_type);
        // ... your application logic ...
        Ok::<(), std::convert::Infallible>(())
    })
    .build()?;
# let _ = worker;
# Ok(()) }
```

A handler that returns `Ok` acknowledges the job as complete; returning `Err`
fails it, and the server applies the job's retry/backoff policy.

A bare closure receives every job from the worker's queues regardless of type,
with the payload left as untyped JSON. For typed, per-type dispatch — the
common case — use a `Router`.

## Routing by job type

A `Router` is itself a `JobHandler`, so you pass it to `.handler()` exactly as
you would a closure. Rather than one function for every job, it dispatches
each job to a handler chosen by its type, deserializing the payload into the
right `JobKind` automatically. Register one handler per type with `.route`:

```rust
# use serde::{Deserialize, Serialize};
# use std::convert::Infallible;
# use zizq::Router;
# #[derive(Serialize, Deserialize)]
# struct SendEmail { to: String }
# impl zizq::JobKind for SendEmail { const NAME: &'static str = "send_email"; }
# #[derive(Serialize, Deserialize)]
# struct ProcessReport { id: u64 }
# impl zizq::JobKind for ProcessReport { const NAME: &'static str = "process_report"; }
let router = Router::new()
    .route(async |job: SendEmail| {
        // ... send the email ...
        Ok::<(), Infallible>(())
    })
    .route(async |job: ProcessReport| {
        // ... process the report ...
        Ok::<(), Infallible>(())
    });
```

A job whose type matches no route is failed with a routing error.

### Sharing state across handlers

When several handlers need the same resources (database pool, API clients,
config), build the router with `Router::with_state(...)`. Each handler that
needs the state takes a `State<S>` extractor as its first argument. The state
is cloned per invocation — wrap heavy state in `Arc<...>` so clones are cheap.

```rust
# use serde::{Deserialize, Serialize};
# use std::convert::Infallible;
# use std::sync::Arc;
# use zizq::{JobKind, Router, State};
# #[derive(Serialize, Deserialize)]
# struct SendEmail { to: String }
# impl JobKind for SendEmail { const NAME: &'static str = "send_email"; }
# #[derive(Serialize, Deserialize)]
# struct ProcessReport { id: u64 }
# impl JobKind for ProcessReport { const NAME: &'static str = "process_report"; }
#[derive(Clone)]
struct AppState {
    // pretend these are a database pool, mailer, etc.
    db: Arc<()>,
    mailer: Arc<()>,
}

let router = Router::with_state(AppState { db: Arc::new(()), mailer: Arc::new(()) })
    .route(async |State(ctx): State<AppState>, job: SendEmail| {
        let _ = (ctx.mailer, job.to);
        Ok::<(), Infallible>(())
    })
    .route(async |State(ctx): State<AppState>, job: ProcessReport| {
        let _ = (ctx.db, job.id);
        Ok::<(), Infallible>(())
    });
```

Stateless handlers (`Fn(T)`) are also accepted on a stateful router — they
just ignore the state, so an additional route that needs no resources can be
added without restructuring. For sub-state projection (one router, several
handlers each wanting a different slice), keep one combined state struct and
destructure it inside each handler — finer-grained extraction is on the
roadmap.

## Concurrency

`concurrency` sets how many job handlers may run at once. The default is `1`
— a purely sequential worker.

```rust
# use zizq::Worker;
# fn x(b: zizq::WorkerBuilder) -> zizq::WorkerBuilder {
b.concurrency(100)
# }
```

### Prefetch

By default the worker fetches as many jobs as its concurrency. Throughput can
improve by *prefetching* more — keeping a buffer of jobs ready so a handler
never waits on the network for its next job.

```rust
# use zizq::Worker;
# fn x(b: zizq::WorkerBuilder) -> zizq::WorkerBuilder {
b.concurrency(50).prefetch(60)  // 50 in flight, 10 buffered
# }
```

> [!WARNING]
> Do not set `prefetch` below `concurrency` — that starves the worker, since
> there are never enough buffered jobs to reach the desired concurrency.

## Which queues to process

`queues` selects which queues the worker pulls from. An empty list (the
default) means *all queues*. Queues need not be created — they exist
implicitly once jobs are enqueued to them.

```rust
# use zizq::Worker;
# fn x(b: zizq::WorkerBuilder) -> zizq::WorkerBuilder {
b.queues(vec!["emails", "invoicing"])
# }
```

## Graceful shutdown

When the shutdown future resolves, `run()` performs a patient shutdown: it
stops pulling new jobs, waits for in-flight handlers to finish, flushes any
pending acknowledgements, then closes the connection. `run()` returns once
that has drained. This is bounded by a shutdown timeout (30s by default).

Because acks are flushed before the connection closes, no completed work is
lost. If the worker is killed before its acks land, the server simply
re-dispatches those still-in-flight jobs to another worker — Zizq's delivery
is at-least-once, so **handlers should be idempotent**.

## Manual control

The `Worker` is the recommended consumer API, but the underlying primitives
are available for full manual control:

- `Client::take` opens a streaming connection and yields jobs as a
  `futures::Stream`.
- `Client::report_success` / `Client::report_success_bulk` acknowledge jobs.
- `Client::report_failure` fails a job, with optional retry timing and error
  detail.

```rust
use futures_util::TryStreamExt;
# use zizq::Client;
# async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
let mut stream = client.take().queues(["emails"]).prefetch(16).await?;

while let Some(job) = stream.try_next().await? {
    // ... process the job ...
    client.report_success(&job.id).await?;
}
# Ok(()) }
```
