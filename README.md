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
- [x] HTTP/2 transport (h2c), with multiplexing on a single connection
- [x] JSON or MessagePack wire formats (MessagePack by default)
- [x] `JobKind` trait for per-type defaults (queue, priority, retry
      limit, backoff, retention, uniqueness key)
- [x] Single-job enqueue via a fluent builder that resolves trait
      defaults and per-call overrides
- [x] Structured error decoding that honours the server's
      `Content-Type` (handles 406 Not Acceptable correctly)

## What's not done yet

- [ ] Bulk enqueue
- [ ] Streaming `/jobs/take` and the Worker / `Router` API
- [ ] TLS (rustls and native-tls feature flags)
- [ ] Other admin endpoints (PATCH/DELETE/GET, error queries, etc.)
- [ ] Cron entry management

## Taster

### Enqueueing (working today)

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
    const PRIORITY: Option<u32> = Some(100);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .url("http://127.0.0.1:7890")
        .build()?;

    let job = client
        .enqueue(SendEmail { to: "alice@example.com".into() })
        .priority(50)              // override the trait default
        .retry_limit(3)
        .await?;

    println!("enqueued {} on {}", job.id, job.queue);
    Ok(())
}
```

`JobKind` is the only required piece per job type — `NAME` is mandatory,
everything else has a default. Per-call overrides on the
`EnqueueBuilder` beat the trait defaults, and the future is finalised
by awaiting the builder.

### Workers (planned, design subject to change)

The same `JobKind` types are reused on the worker side. A `Router`
registers handlers keyed by `JobKind::NAME`, so dispatch is
type-driven — no string matching at the call site.

```rust
use zizq::{Router, Worker};

// SendEmail and a second job type, both implementing JobKind.
#[derive(Serialize, Deserialize)]
struct ProcessReport {
    report_id: String,
}

impl JobKind for ProcessReport {
    const NAME: &'static str = "process_report";
}

let worker = Worker::builder()
    .client(client.clone())
    .concurrency(16)
    .handler(
        Router::new()
            .route(async move |job: SendEmail| {
                // do the work — send the email
            })
            .route(async move |job: ProcessReport| {
                // handle the other job type
            }),
    )
    .build()?;

worker.run().await?;
```

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
