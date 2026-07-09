# Introduction

Zizq is a simple single-binary persistent job queue server with clients in
various programming languages. All official Zizq clients are **MIT licensed**.

This documentation covers using Zizq from Rust with the official Zizq Rust
client, published on [crates.io](https://crates.io/crates/zizq) as the
[`zizq`](https://crates.io/crates/zizq) crate.

The Rust client is fully asynchronous, built on [`tokio`](https://tokio.rs)
and [`reqwest`](https://github.com/seanmonstar/reqwest) with HTTP/2 for
request/response traffic. A single `Worker` runs N concurrent job handlers,
which is efficient for I/O-bound work.

> [!NOTE]
> If you have not yet installed the Zizq server, follow the
> [Getting Started](/docs/getting-started/) guide first.

## Issues & Source

All client source code is
[available on GitHub](https://github.com/zizq-labs/zizq-rust). Issues can be
raised on the [issue tracker](https://github.com/zizq-labs/zizq-rust/issues).

## High-Level Structure

The Rust client has three core pieces:

1. A `Client` — a cheaply-cloneable handle that wraps the Zizq server's HTTP
   API. Construct it once and share it across tasks.
2. The `JobKind` trait — implemented on each payload type, it declares the
   API-level job type name and any per-type defaults (queue, priority, retry
   limit, and so on).
3. A `Worker` — streams jobs from the server, dispatches each to a handler
   with bounded concurrency, batches acknowledgements, and reconnects on
   transient failures. A `Router` dispatches multiple `JobKind`s through a
   single worker by type.

Unlike the dynamically-typed clients, the Rust client leans on the type
system: each job is a concrete Rust type, so payloads are checked at compile
time on both the producer and consumer side.

## Example

A minimal producer and consumer look like this:

> Rust:
>
> ```rust
> use serde::{Deserialize, Serialize};
> use std::convert::Infallible;
> use zizq::{Client, JobKind, Router, Worker};
> 
> #[derive(Serialize, Deserialize)]
> struct SendEmail {
>     to: String,
> }
> 
> impl JobKind for SendEmail {
>     const NAME: &'static str = "send_email";
>     const QUEUE: &'static str = "emails";
> }
> 
> #[tokio::main]
> async fn main() -> Result<(), Box<dyn std::error::Error>> {
>     let client = Client::builder().url("http://127.0.0.1:7890").build()?;
> 
>     // Producer — enqueue a job.
>     client
>         .enqueue(SendEmail { to: "alice@example.com".into() })
>         .await?;
> 
>     // Consumer — run a worker until Ctrl-C.
>     let worker = Worker::builder()
>         .client(client)
>         .concurrency(16)
>         .handler(Router::new().route(async |job: SendEmail| {
>             println!("emailing {}", job.to);
>             Ok::<(), Infallible>(())
>         }))
>         .build()?;
> 
>     worker.run(tokio::signal::ctrl_c()).await?;
>     Ok(())
> }
> ```

The rest of this guide works through each piece in turn.
