// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Official Rust client for the [Zizq] job queue.
//!
//! [Zizq]: https://zizq.io
//!
//! Zizq is a fast and durable job queue system based on an internal
//! LSM database, not on Redis or on your RDBMS. It provides multiple
//! producer, multiple consumer functionality across an entire stack
//! with producers and consumers written in any language.
//!
//! This client exposes a small, builder-driven API for enqueueing,
//! processing and managing jobs on the Zizq server. At its core:
//!
//! - [`Client`] is the cheaply-clonable handle. Configure it once with
//!   [`Client::builder`] and share it across tasks.
//! - [`JobKind`] is a trait you implement on each payload struct. It
//!   declares the API-level job type name and any per-type defaults
//!   (queue, priority, retry limit, uniqueness).
//! - [`Client::enqueue`] returns an [`EnqueueBuilder`] that chains
//!   per-job overrides and awaits to send the request.
//! - [`Worker`] is the recommended consumer API — it streams jobs,
//!   dispatches them to a [`JobHandler`] with bounded concurrency,
//!   batches acks, and reconnects on transient failures. Use [`Router`]
//!   to dispatch multiple [`JobKind`]s through one worker, optionally
//!   built with [`Router::with_state`] to share a database pool, API
//!   client, or config across handlers. For full manual control,
//!   [`Client::take`] + [`Client::report_success`] /
//!   [`Client::report_failure`] are the underlying primitives.
//!
//! The API serialization format defaults to [`Format::MessagePack`];
//! switch to [`Format::Json`] if you prefer a human-readable payload.
//! Both the MessagePack and JSON formats are compatible with one another.
//!
//! # Getting started
//!
//! Define a [`JobKind`] for each payload type your application
//! produces or consumes. The producer enqueues; the consumer runs a
//! [`Worker`] that calls your handler for each job.
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use std::convert::Infallible;
//! use zizq::{Client, JobKind, Router, Worker};
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail {
//!     to: String,
//! }
//!
//! impl JobKind for SendEmail {
//!     const NAME: &'static str = "send_email";
//!     const QUEUE: &'static str = "emails";
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct ProcessReport {
//!     report_id: String,
//! }
//!
//! impl JobKind for ProcessReport {
//!     const NAME: &'static str = "process_report";
//! }
//!
//! /// Producer side — enqueue work.
//! async fn produce(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
//!     client
//!         .enqueue(SendEmail { to: "alice@example.com".into() })
//!         .priority(100)
//!         .await?;
//!     Ok(())
//! }
//!
//! /// Consumer side — run a worker with a router that dispatches
//! /// by job type. For a single job type, pass a closure to
//! /// `.handler(...)` directly instead of a `Router`.
//! async fn consume(client: Client) -> Result<(), Box<dyn std::error::Error>> {
//!     let worker = Worker::builder()
//!         .client(client)
//!         .concurrency(16)
//!         .handler(
//!             Router::new()
//!                 .route(async |job: SendEmail| {
//!                     // ... send the email ...
//!                     Ok::<(), Infallible>(())
//!                 })
//!                 .route(async |job: ProcessReport| {
//!                     // ... process the report ...
//!                     Ok::<(), Infallible>(())
//!                 }),
//!         )
//!         .build()?;
//!
//!     // In production, wire `shutdown` to `tokio::signal::ctrl_c()`
//!     // or a `CancellationToken`. `pending()` here means "run until
//!     // the take stream ends or the process is killed".
//!     worker.run(std::future::pending::<()>()).await?;
//!     Ok(())
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .url("http://127.0.0.1:7890")
//!     .build()?;
//!
//! produce(&client).await?;
//! consume(client).await?;
//! # Ok(()) }
//! ```
//!
//! # Cargo features
#![doc = document_features::document_features!()]

mod batch;
mod bulk_enqueue;
mod client;
mod count_jobs;
mod cron;
mod delete_jobs;
mod enqueue;
mod error;
mod failure;
mod format;
mod hashable;
mod job;
mod job_filter;
mod job_patch;
mod jq_expr;
mod jq_path;
mod list_errors;
mod list_jobs;
mod patch_jobs;
mod range_filter;
mod resources;
mod router;
mod take;
mod timestamp;
mod unique_key;
mod worker;

pub use batch::BatchConfig;
pub use bulk_enqueue::BulkEnqueueBuilder;
pub use client::{Client, ClientBuilder};
pub use count_jobs::CountJobsBuilder;
pub use cron::{CronEntry, CronEntryRecord, CronGroup, JobTemplate, ReplaceCronBuilder};
pub use delete_jobs::DeleteJobsBuilder;
pub use enqueue::EnqueueBuilder;
pub use error::ZizqError;
pub use failure::FailureBuilder;
pub use format::Format;
pub use job::JobKind;
// Re-export the derive macro under the same name — Rust looks up
// derives in the macro namespace, so it doesn't collide with the
// trait. Users write `use zizq::JobKind;` and get both.
pub use job_patch::{JobPatch, RetentionPatch};
pub use jq_expr::{jq_array_prefix_eq, jq_contains, jq_eq};
pub use list_errors::ListErrorsBuilder;
pub use list_jobs::ListJobsBuilder;
pub use patch_jobs::PatchJobsBuilder;
pub use range_filter::RangeFilter;
pub use resources::{
    BackoffConfig, ErrorPage, ErrorRecord, Job, JobPage, JobStatus, Order, PageLinks,
    RetentionConfig,
};
pub use router::{Router, State};
pub use take::{TakeBuilder, TakeStream};
pub use unique_key::{UniqueKey, UniqueScope};
pub use worker::{HandlerError, JobHandler, Worker, WorkerBuilder};
#[cfg(feature = "derive")]
pub use zizq_derive::JobKind;
