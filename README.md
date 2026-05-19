# Zizq — Official Rust Client

Official Rust client for [Zizq](https://zizq.io), a high-performance,
self-contained job queue server.

[![CI](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/zizq.svg)](https://crates.io/crates/zizq)

## Installation

Add the crate with Cargo:

```shell
cargo add zizq
```

The minimum supported Rust version is **1.85**. Client and server share
version numbers — keep the client's major/minor at or below the server's.

### TLS

TLS support is behind a feature flag; enable exactly one. `rustls-tls` (the
default) is pure Rust with no system OpenSSL dependency; `native-tls` uses the
platform's native TLS library.

```toml
[dependencies]
zizq = { version = "0.3", default-features = false, features = ["native-tls"] }
```

## Features

- `Client` + builder, with configurable connect/read timeouts and
      TCP keep-alive
- JSON or MessagePack API formats (MessagePack by default)
- `JobKind` trait for per-type defaults (queue, priority, retry
      limit, backoff, retention, uniqueness key)
- Single-job enqueue via a builder that resolves trait
      defaults and per-call overrides
- Bulk enqueue — many jobs submitted in a single request
- `Worker` — long-running consumer with bounded concurrency,
      auto-reconnect, batched acks, retry-aware nack, and graceful
      shutdown
- `Router` — type-driven dispatch keyed by `JobKind::NAME`, so
      one worker can serve many job types
- Job queries: `get_job`, paginated `list_jobs`, and `count_jobs`
- Job mutation: single (`patch_job` / `delete_job`) and bulk
      (`patch_all_jobs` / `delete_all_jobs`), the bulk forms sharing a
      filter set (status / queue / type / id / jq payload expression)
- Per-job error history: `list_errors` (paginated, streamable)
      and `get_error`
- Server introspection: `health`, `server_version`, `list_queues`
- HTTPS / TLS — custom root CA and mutual-TLS client identity,
      with a `rustls-tls` (default) or `native-tls` feature flag
- Cron scheduling: `list_crons`, `get_cron`, `replace_cron`,
      `delete_cron`, per-group and per-entry pause/resume, and
      single-entry CRUD (`add`/`get`/`put`/`delete_cron_entry`)

## Sample Usage

Read the [docs](https://zizq.io/docs/clients/rust/) for complete documentation.

### Producer

```rust
use serde::{Deserialize, Serialize};
use zizq::{Client, JobKind};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

impl JobKind for SendEmail {
    const NAME: &'static str = "send_email";
    const QUEUE: &'static str = "emails";
    const PRIORITY: Option<u16> = Some(100);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .url("http://127.0.0.1:7890")
        .build()?;

    client
        .enqueue(SendEmail { to: "alice@example.com".into() })
        .priority(50)              // override the trait default
        .retry_limit(3)
        .await?;

    Ok(())
}
```

`JobKind` is the only required piece per job type — `NAME` is mandatory,
everything else has a default. Per-call overrides on the
`EnqueueBuilder` beat the trait defaults, and the future is finalised
by awaiting the builder.

### Consumer

A `Worker` connects to `/jobs/take`, dispatches each job to your
handler with bounded concurrency, batches acks, and reconnects on
transient failures. Pair it with a `Router` to dispatch by job type —
each `.route(...)` registers a typed handler keyed on
`JobKind::NAME`, so there's no string matching at the call site. The
same shape works for one job type or many.

```rust
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use zizq::{Client, JobKind, Router, Worker};

#[derive(Serialize, Deserialize)]
struct SendEmail { to: String }
impl JobKind for SendEmail {
    const NAME: &'static str = "send_email";
}

#[derive(Serialize, Deserialize)]
struct ProcessReport { report_id: String }
impl JobKind for ProcessReport {
    const NAME: &'static str = "process_report";
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .url("http://127.0.0.1:7890")
        .build()?;

    let worker = Worker::builder()
        .client(client)
        .concurrency(16)
        .handler(
            Router::new()
                .route(async |job: SendEmail| {
                    println!("sending email to {}", job.to);
                    Ok::<(), Infallible>(())
                })
                .route(async |job: ProcessReport| {
                    println!("processing report {}", job.report_id);
                    Ok::<(), Infallible>(())
                }),
        )
        .build()?;

    // Graceful shutdown on Ctrl-C — drains in-flight handlers and
    // acks (up to the configured shutdown_timeout) before returning.
    worker.run(async { let _ = tokio::signal::ctrl_c().await; }).await?;
    Ok(())
}
```

Returning `Ok(())` from a route acks the job; returning `Err(_)`
reports a failure and lets the server's retry policy apply. If a job
arrives for a type the router doesn't know about, or its payload
doesn't deserialise into the route's input, the worker reports it as
a failure on the same path as a handler error.

### Recurring jobs (cron)

`replace_cron` installs a cron schedule — each entry pairs a cron expression
with a job to enqueue on every tick. Cron requires a Pro license on the
server.

```rust
use serde::{Deserialize, Serialize};
use zizq::{Client, CronEntry, JobKind};

#[derive(Serialize, Deserialize)]
struct NightlyCleanup;
impl JobKind for NightlyCleanup {
    const NAME: &'static str = "nightly_cleanup";
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .url("http://127.0.0.1:7890")
        .build()?;

    client
        .replace_cron("maintenance")
        .entry(CronEntry::new(
            "cleanup",
            "0 0 * * *",                       // every day at midnight
            client.enqueue(NightlyCleanup),
        ))
        .await?;

    Ok(())
}
```

A `CronEntry` is built from the same `enqueue(...)` builder you'd use to
enqueue the job directly. The server then enqueues that job on schedule.

### Lower-level API

If you'd rather coordinate dispatch yourself, the worker is built on
two primitives you can use directly: `Client::take` returns a
`Stream<Item = Result<Job, ZizqError>>` and `Client::report_success` /
`Client::report_failure` ack individually. `Worker::builder().handler`
also accepts a closure of the form `Fn(Job) -> Fut` (the raw `Job`,
not a typed payload) if you want the worker's concurrency and
reconnect logic without `Router`'s type-driven dispatch.

## Resources

* [Rust Client Docs](https://zizq.io/docs/clients/rust/)
* [Getting Started Docs](https://zizq.io/docs/getting-started/)
* [Zizq Command Reference](https://zizq.io/docs/cli/)
* [Zizq Rust Client Source](https://github.com/zizq-labs/zizq-rust)
* [Zizq Source](https://github.com/zizq-labs/zizq)

## Support & Feedback

If you need help using Zizq,
[create an issue](https://github.com/zizq-labs/zizq-rust/issues) on the
[zizq-rust](https://github.com/zizq-labs/zizq-rust) repo. Feedback is very
welcome.

## License

MIT — see [LICENSE](LICENSE).
