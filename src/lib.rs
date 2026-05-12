// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Official Rust client for the [Zizq] job queue.
//!
//! [Zizq]: https://zizq.io
//!
//! The client exposes a small, builder-driven API for talking to a
//! Zizq server. At its core:
//!
//! - [`Client`] is the cheaply-clonable handle. Configure it once with
//!   [`Client::builder`] and share it across tasks.
//! - [`JobKind`] is a trait you implement on each payload struct. It
//!   declares the API-level job type name and any per-type defaults
//!   (queue, priority, retry limit, uniqueness).
//! - [`Client::enqueue`] returns an [`EnqueueBuilder`] that chains
//!   per-job overrides and awaits to send the request.
//!
//! The API serialization format defaults to [`Format::MessagePack`];
//! switch to [`Format::Json`] if you prefer a human-readable payload.
//! Both the MessagePack and JSON formats are compatible with one another.
//!
//! # Getting started
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use zizq::{Client, JobKind};
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
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .url("http://127.0.0.1:7890")
//!     .build()?;
//!
//! let job = client
//!     .enqueue(SendEmail { to: "alice@example.com".into() })
//!     .priority(100)
//!     .await?;
//!
//! println!("enqueued {}", job.id);
//! # Ok(()) }
//! ```

mod client;
mod enqueue;
mod error;
mod format;
mod job;
mod resources;
mod unique_key;

pub use client::{Client, ClientBuilder};
pub use enqueue::EnqueueBuilder;
pub use error::ZizqError;
pub use format::Format;
pub use job::JobKind;
pub use resources::{BackoffConfig, Job, JobStatus, RetentionConfig};
pub use unique_key::{UniqueKey, UniqueScope};
