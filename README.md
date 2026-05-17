# Zizq — Official Rust Client

Official Rust client for [Zizq](https://zizq.io), a high-performance,
self-contained job queue server.

> **Status: active development.** This crate is incomplete and the API
> is expected to change. It is **not yet ready for public use**. There
> is no release on crates.io, and breaking changes can land at any time.

[![CI](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zizq-labs/zizq-rust/actions/workflows/ci.yml)

## What's working

- [x] `Client` + builder, with configurable connect/read timeouts and
      TCP keep-alive
- [x] HTTP/2 transport (h2c) for request/response endpoints, with
      multiplexing on a single connection; dedicated HTTP/1.1 pool for
      the long-lived take stream
- [x] JSON or MessagePack wire formats (MessagePack by default)
- [x] `JobKind` trait for per-type defaults (queue, priority, retry
      limit, backoff, retention, uniqueness key)
- [x] Single-job enqueue via a fluent builder that resolves trait
      defaults and per-call overrides
- [x] Bulk enqueue — many jobs submitted in a single request
- [x] Streaming `/jobs/take` with NDJSON and length-prefixed
      MessagePack framing; heartbeats filtered transparently
- [x] Job acknowledgement: `report_success`, `report_success_bulk`
      (batched ack for high-throughput workers), and `report_failure`
      with retry / kill controls
- [x] Structured error decoding that honours the server's
      `Content-Type` (handles 406 Not Acceptable correctly)
- [x] `Worker` — long-running consumer with bounded concurrency,
      auto-reconnect, batched acks, retry-aware nack, and graceful
      shutdown
- [x] `Router` — type-driven dispatch keyed by `JobKind::NAME`, so
      one worker can serve many job types
- [x] Job queries: `get_job`, paginated `list_jobs`, and `count_jobs`
- [x] Job mutation: single (`patch_job` / `delete_job`) and bulk
      (`patch_all_jobs` / `delete_all_jobs`), the bulk forms sharing a
      filter set (status / queue / type / id / jq payload expression)
- [x] Per-job error history: `list_errors` (paginated, streamable)
      and `get_error`
- [x] Server introspection: `health`, `server_version`, `list_queues`
- [x] HTTPS / TLS — custom root CA and mutual-TLS client identity,
      with a `rustls-tls` (default) or `native-tls` feature flag

## What's not done yet

- [ ] Cron entry management

## Taster

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

### Lower-level API

If you'd rather coordinate dispatch yourself, the worker is built on
two primitives you can use directly: `Client::take` returns a
`Stream<Item = Result<Job, ZizqError>>` and `Client::report_success` /
`Client::report_failure` ack individually. `Worker::builder().handler`
also accepts a closure of the form `Fn(Job) -> Fut` (the raw `Job`,
not a typed payload) if you want the worker's concurrency and
reconnect logic without `Router`'s type-driven dispatch.

## Resources

* [Rust Client Docs](https://zizq.io/docs/clients/rust/) - TODO
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
