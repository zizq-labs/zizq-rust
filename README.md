# Zizq — Official Rust Client

Official Rust client for [Zizq](https://zizq.io), a high-performance,
self-contained job queue server.

[![CI](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/zizq.svg)](https://crates.io/crates/zizq)

## Repository Layout

This repository is a Cargo workspace. You likely want to start with the
`zizq` crate; the surrounding directories exist to support development
and release of that crate.

| Path            | Contents                                                                                                                                                 |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zizq/`         | The `zizq` crate itself — the library published to crates.io. Source, tests, examples, CHANGELOG.                                                        |
| `zizq-derive/`  | Companion proc-macro crate that hosts `#[derive(JobKind)]`. Re-exported from `zizq` behind the `derive` feature; not depended on directly by end users.  |
| `docs/`         | The mdBook source for the [Rust Client Docs](https://zizq.io/docs/clients/rust/).                                                                        |
| `integration/`  | End-to-end integration tests that run against a real Zizq server (see `integration/run.sh`).                                                             |

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
zizq = { version = "0.6", default-features = false, features = ["native-tls"] }
```

## Features

- `Client` + builder, with configurable connect/read timeouts and
      TCP keep-alive
- JSON or MessagePack API formats (MessagePack by default)
- `#[derive(JobKind)]` — declarative per-type config via
      `#[zizq(...)]` attributes (name, queue, priority, retry limit,
      backoff, retention, uniqueness, batching). Manual
      `impl JobKind` remains available for cases the derive doesn't
      cover (generics, computed defaults, etc.)
- Single-job enqueue via a builder that resolves trait
      defaults and per-call overrides
- Bulk enqueue — many jobs submitted in a single request
- `Worker` — long-running consumer with bounded concurrency,
      auto-reconnect, batched acks, retry-aware nack, and graceful
      shutdown
- `Router` — type-driven dispatch keyed by `JobKind::NAME`, so
      one worker can serve many job types, with optional shared
      state (`Router::with_state` + a `State<S>` extractor)
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

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "send_email", queue = "emails", priority = 100)]
struct SendEmail {
    to: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .url("http://127.0.0.1:7890")
        .build()?;

    client
        .enqueue(SendEmail { to: "alice@example.com".into() })
        .priority(50)              // override the derive default
        .retry_limit(3)
        .await?;

    Ok(())
}
```

`JobKind` is the only required piece per job type — `#[derive(JobKind)]`
generates the trait impl from `#[zizq(...)]` attributes. `name`
defaults to the struct's identifier when absent; every other field is
optional. Per-call overrides on the `EnqueueBuilder` beat the derive
defaults, and the future is finalised by awaiting the builder.

If your job needs behaviour that doesn't fit the attribute grammar —
computed defaults, generic payloads, dynamic keys — implement
`JobKind` by hand instead. See [Manual JobKind
impl](#manual-jobkind-impl) below.

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

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "send_email")]
struct SendEmail { to: String }

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "process_report")]
struct ProcessReport { report_id: String }

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

#### Sharing state across handlers

When several handlers need the same resources (database pool, API
client, config), build the router with `Router::with_state(...)` and
take a `State<S>` extractor as the handler's first argument. State is
cloned per invocation — wrap heavy state in `Arc` so the clone is
cheap.

```rust
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use zizq::{JobKind, Router, State};

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "send_email")]
struct SendEmail { to: String }

#[derive(Clone)]
struct AppState {
    db: Arc<()>,     // your sqlx::Pool or similar
    mailer: Arc<()>, // your mailer handle
}

let _router = Router::with_state(AppState { db: Arc::new(()), mailer: Arc::new(()) })
    .route(async |State(ctx): State<AppState>, job: SendEmail| {
        let _ = (ctx.db, ctx.mailer, job.to);
        Ok::<(), Infallible>(())
    });
```

Stateless `Fn(T)` handlers also work on a stateful router (they
ignore the state), so you can mix the two shapes as routes are added.

### Recurring jobs (cron)

`replace_cron` installs a cron schedule — each entry pairs a cron expression
with a job to enqueue on every tick. Cron requires a Pro license on the
server.

```rust
use serde::{Deserialize, Serialize};
use zizq::{Client, CronEntry, JobKind};

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "nightly_cleanup")]
struct NightlyCleanup;

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

### Manual JobKind impl

The derive covers the common cases. When you need something the
attribute grammar doesn't express — generic payload types, defaults
computed at runtime, a `unique_key` that reaches into `&self` in
non-trivial ways — implement `JobKind` by hand. Every field that
has a `#[zizq(...)]` counterpart has a corresponding associated
constant or trait method:

```rust
use serde::{Deserialize, Serialize};
use zizq::{JobKind, UniqueKey};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    user_id: u64,
    body: String,
}

impl JobKind for SendEmail {
    const NAME: &'static str = "send_email";
    const QUEUE: &'static str = "emails";
    const PRIORITY: Option<u16> = Some(100);

    fn unique_key(&self) -> Option<UniqueKey> {
        Some(UniqueKey::tagged_hash_of(Self::NAME, &self.user_id))
    }
}
```

Mixing is fine — a job type you define by hand and another via
derive coexist in the same router and worker without issue.

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
* [Changelog](zizq/CHANGELOG.md)

## Support & Feedback

If you need help using Zizq,
[create an issue](https://github.com/zizq-labs/zizq-rust/issues) on the
[zizq-rust](https://github.com/zizq-labs/zizq-rust) repo. Feedback is very
welcome.

## License

MIT — see [LICENSE](zizq/LICENSE).
