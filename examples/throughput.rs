// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Crude but effective throughput benchmark — enqueue N jobs, then run
//! a worker until the Nth job is seen, which works because jobs are
//! processed in FIFO order.
//!
//! Configuration is via env vars:
//!
//! - `ZIZQ_URL` — server URL (default `http://127.0.0.1:7890`)
//! - `ZIZQ_FORMAT` — `json` or `msgpack` (default `json`)
//! - `JOB_COUNT` — number of jobs (default 5000)
//! - `CONCURRENCY` — worker concurrency (default 25)
//! - `PREFETCH` — worker prefetch (default `CONCURRENCY * 2`)
//!
//! Run with:
//!
//! ```text
//! cargo run --release --example throughput
//! ```
//!
//! Assumes a happy-path plain-HTTP connection to a clean server. If
//! there are leftover jobs on `rust/bench` from a previous run, the
//! worker will see them first and shutdown may trigger before all N
//! new jobs have been processed.

use std::convert::Infallible;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use zizq::{Client, Format, JobKind, Router, Worker};

#[derive(Serialize, Deserialize)]
struct Bench {
    n: u64,
}

impl JobKind for Bench {
    const NAME: &'static str = "bench";
    const QUEUE: &'static str = "rust/bench";
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = env::var("ZIZQ_URL").unwrap_or_else(|_| "http://127.0.0.1:7890".into());

    let format = match env::var("ZIZQ_FORMAT").as_deref() {
        Ok("msgpack") => Format::MessagePack,
        Ok("json") | Err(_) => Format::Json,
        Ok(other) => return Err(format!("unknown ZIZQ_FORMAT: {other:?}").into()),
    };

    let job_count: u64 = env::var("JOB_COUNT").as_deref().unwrap_or("5000").parse()?;

    let concurrency: usize = env::var("CONCURRENCY").as_deref().unwrap_or("25").parse()?;

    let prefetch: u32 = match env::var("PREFETCH") {
        Ok(s) => s.parse()?,
        Err(_) => (concurrency * 2) as u32,
    };

    let client = Client::builder().url(&url).format(format).build()?;

    // --- Enqueue phase ---
    //
    // Sequential enqueue so jobs land in strict FIFO order. The
    // dequeue phase relies on this to know that seeing `n == JOB_COUNT`
    // means every prior job has already been served to a worker.

    let enqueue_start = Instant::now();
    for n in 1..=job_count {
        client.enqueue(Bench { n }).await?;
    }

    let enqueue_elapsed = enqueue_start.elapsed().as_secs_f64();
    let enqueue_rate = (job_count as f64 / enqueue_elapsed).round() as u64;
    println!(
        "Enqueued {} jobs in {:.2}s ({} jobs/sec)",
        job_count, enqueue_elapsed, enqueue_rate,
    );

    // --- Dequeue phase ---
    //
    // Shutdown signal: a single `Arc<Notify>` shared between the
    // worker handler and the future passed to `run()`. The handler
    // clones the Arc per invocation (closure is `Fn`, so we can't
    // move the Arc into the async block — we clone outside, move the
    // clone in) and calls `notify_one()` when it sees the Nth job.
    // `run()` blocks on `notified()`, then drains any in-flight
    // handlers and pending acks before returning.

    let shutdown = Arc::new(Notify::new());
    let dequeue_start = Instant::now();

    let worker = Worker::builder()
        .client(client.clone())
        .concurrency(concurrency)
        .prefetch(prefetch)
        .handler(Router::new().route({
            let shutdown = shutdown.clone();
            move |job: Bench| {
                let shutdown = shutdown.clone();
                async move {
                    if job.n == job_count {
                        shutdown.notify_one();
                    }
                    Ok::<(), Infallible>(())
                }
            }
        }))
        .build()?;

    worker.run(async move { shutdown.notified().await }).await?;

    let dequeue_elapsed = dequeue_start.elapsed().as_secs_f64();
    let dequeue_rate = (job_count as f64 / dequeue_elapsed).round() as u64;
    println!(
        "Dequeued {} jobs in {:.2}s ({} jobs/sec)",
        job_count, dequeue_elapsed, dequeue_rate,
    );

    Ok(())
}
