// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Type-driven job dispatch — a [`Router`] groups multiple typed
//! handlers behind a single [`JobHandler`], looking up the right
//! one by [`JobKind::NAME`] on each incoming [`Job`].
//!
//! Typical use sits on top of a worker:
//!
//! ```no_run
//! # use serde::{Deserialize, Serialize};
//! # use std::convert::Infallible;
//! # use zizq::{Client, JobKind, Router, Worker};
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail { to: String }
//! impl JobKind for SendEmail {
//!     const NAME: &'static str = "send_email";
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct ProcessReport { report_id: String }
//! impl JobKind for ProcessReport {
//!     const NAME: &'static str = "process_report";
//! }
//!
//! # fn build(client: Client) -> Result<Worker, zizq::ZizqError> {
//! Worker::builder()
//!     .client(client)
//!     .concurrency(16)
//!     .handler(
//!         Router::new()
//!             .route(async |_job: SendEmail| Ok::<(), Infallible>(()))
//!             .route(async |_job: ProcessReport| Ok::<(), Infallible>(())),
//!     )
//!     .build()
//! # }
//! ```
//!
//! When the router handles a job, it looks at the `Job::job_type`
//! and finds the corresponding route handler. It then deserializes
//! the `Job::payload` into the correct type (the `JobKind`) before
//! calling the route handler with that payload.
//!
//! If no matching route exists, a `NoRouteError` is reported to the
//! server and job will backoff and retry. Similarly, if the payload
//! cannot be deserialized into the type, a
//! `PayloadDeserializeError` is reported to the server and the job
//! will backoff and retry.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;

use crate::job::JobKind;
use crate::resources::Job;
use crate::worker::{HandlerError, JobHandler};

/// Type-erased per-route handler. Each closure takes a [`Job`],
/// deserialises its payload into the route's typed input, calls the
/// user's handler, and maps the resulting error (if any) back into a
/// [`HandlerError`] preserving the user's original error type name.
type ErasedRouteHandler = Box<
    dyn Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>> + Send + Sync,
>;

/// Marker type for "no route registered for this job type" — exists
/// only so the synthesised [`HandlerError::type_name`] resolves to a
/// stable path (`"zizq::router::NoRouteError"`).
#[derive(Debug)]
struct NoRouteError {
    job_type: String,
}

impl std::fmt::Display for NoRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no route registered for job type {:?}", self.job_type)
    }
}

impl StdError for NoRouteError {}

/// Marker type for "payload didn't deserialise into the route's
/// typed input" — exists for the same reason as [`NoRouteError`].
#[derive(Debug)]
struct PayloadDeserializeError {
    target: &'static str,
    source: serde_json::Error,
}

impl std::fmt::Display for PayloadDeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to deserialise job payload as {}: {}",
            self.target, self.source,
        )
    }
}

impl StdError for PayloadDeserializeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

/// Dispatch table keyed by [`JobKind::NAME`].
///
/// Each [`route`](Router::route) call registers a typed handler for
/// one [`JobKind`]. When the router receives a [`Job`], it looks up
/// the handler for `job.job_type`, deserialises the payload into the
/// route's input type, and calls the handler. Implements
/// [`JobHandler`] so it can be passed to [`WorkerBuilder::handler`].
///
/// Registering the same `JobKind::NAME` twice overwrites the previous
/// entry — last call wins.
///
/// [`WorkerBuilder::handler`]: crate::WorkerBuilder::handler
///
/// # Examples
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use std::convert::Infallible;
/// # use zizq::{JobKind, Router};
/// #[derive(Serialize, Deserialize)]
/// struct SendEmail { to: String }
/// impl JobKind for SendEmail {
///     const NAME: &'static str = "send_email";
/// }
///
/// let router = Router::new().route(|job: SendEmail| async move {
///     println!("sending email to {}", job.to);
///     Ok::<(), Infallible>(())
/// });
/// # let _ = router;
/// ```
pub struct Router {
    routes: HashMap<&'static str, ErasedRouteHandler>,
}

impl Router {
    /// Create an empty router with no routes registered.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    /// Register a typed handler for one [`JobKind`].
    ///
    /// The handler receives `T` (deserialised from `job.payload`) and
    /// returns a `Result<(), E>` where `E` is any error that can be
    /// boxed into `Box<dyn Error + Send + Sync + 'static>` — same
    /// shape as the worker's [`JobHandler`] blanket impl, just with
    /// `T` in the input position instead of [`Job`]. Typed errors
    /// (e.g. via `thiserror`), `anyhow::Error`, and
    /// `Box<dyn Error + Send + Sync>` itself all satisfy this. Returns
    /// `self` for chaining.
    pub fn route<T, F, Fut, E>(mut self, handler: F) -> Self
    where
        T: JobKind,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<Box<dyn StdError + Send + Sync + 'static>> + 'static,
    {
        let erased: ErasedRouteHandler = Box::new(move |job: Job| {
            let payload = job.payload.unwrap_or(serde_json::Value::Null);
            let typed: T = match serde_json::from_value(payload) {
                Ok(t) => t,
                Err(e) => {
                    let err = HandlerError::from_typed(PayloadDeserializeError {
                        target: std::any::type_name::<T>(),
                        source: e,
                    });
                    return Box::pin(async move { Err(err) });
                }
            };
            let fut = handler(typed);
            Box::pin(async move { fut.await.map_err(HandlerError::from_typed) })
        });
        self.routes.insert(T::NAME, erased);
        self
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut routes: Vec<&str> = self.routes.keys().copied().collect();
        routes.sort();
        f.debug_struct("Router").field("routes", &routes).finish()
    }
}

impl JobHandler for Router {
    fn handle(&self, job: Job) -> Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>> {
        match self.routes.get(job.job_type.as_str()) {
            Some(handler) => handler(job),
            None => {
                let err = HandlerError::from_typed(NoRouteError {
                    job_type: job.job_type,
                });
                Box::pin(async move { Err(err) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Job, JobStatus};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SendEmail {
        to: String,
    }
    impl JobKind for SendEmail {
        const NAME: &'static str = "send_email";
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct ProcessReport {
        report_id: String,
    }
    impl JobKind for ProcessReport {
        const NAME: &'static str = "process_report";
    }

    fn make_job(job_type: &str, payload: serde_json::Value) -> Job {
        Job {
            id: "test-job".into(),
            job_type: job_type.to_string(),
            queue: "default".into(),
            status: JobStatus::Ready,
            priority: 0,
            payload: Some(payload),
            ready_at: 0,
            attempts: 0,
            retry_limit: None,
            backoff: None,
            dequeued_at: None,
            failed_at: None,
            completed_at: None,
            retention: None,
            purge_at: None,
            unique_key: None,
            duplicate: None,
        }
    }

    #[tokio::test]
    async fn dispatches_to_matching_route_and_passes_typed_payload() {
        let router = Router::new().route(|job: SendEmail| async move {
            assert_eq!(job.to, "alice@example.com");
            Ok::<(), Infallible>(())
        });

        let job = make_job("send_email", json!({ "to": "alice@example.com" }));
        router.handle(job).await.unwrap();
    }

    #[tokio::test]
    async fn dispatches_each_job_type_to_its_own_route() {
        let emails = Arc::new(AtomicUsize::new(0));
        let reports = Arc::new(AtomicUsize::new(0));
        let ec = emails.clone();
        let rc = reports.clone();

        let router = Router::new()
            .route(move |_job: SendEmail| {
                let ec = ec.clone();
                async move {
                    ec.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .route(move |_job: ProcessReport| {
                let rc = rc.clone();
                async move {
                    rc.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            });

        router
            .handle(make_job("send_email", json!({ "to": "a" })))
            .await
            .unwrap();
        router
            .handle(make_job("send_email", json!({ "to": "b" })))
            .await
            .unwrap();
        router
            .handle(make_job("process_report", json!({ "report_id": "r1" })))
            .await
            .unwrap();

        assert_eq!(emails.load(Ordering::SeqCst), 2);
        assert_eq!(reports.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unmatched_job_type_returns_no_route_error() {
        let router = Router::new().route(|_job: SendEmail| async move { Ok::<(), Infallible>(()) });

        let err = router
            .handle(make_job("unknown_type", json!({})))
            .await
            .unwrap_err();
        assert!(err.type_name.ends_with("NoRouteError"));
        assert!(
            err.message.contains("unknown_type"),
            "expected unknown type in message: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn payload_mismatch_returns_deserialize_error() {
        let router = Router::new().route(|_job: SendEmail| async move { Ok::<(), Infallible>(()) });

        // SendEmail expects `to`; we send a payload without it.
        let err = router
            .handle(make_job("send_email", json!({})))
            .await
            .unwrap_err();
        assert!(err.type_name.ends_with("PayloadDeserializeError"));
    }

    #[tokio::test]
    async fn handler_error_propagates_with_original_type_name() {
        use std::fmt;
        #[derive(Debug)]
        struct WorkerSpecificError(&'static str);
        impl fmt::Display for WorkerSpecificError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl StdError for WorkerSpecificError {}

        let router = Router::new().route(|_job: SendEmail| async move {
            Err::<(), _>(WorkerSpecificError("payload had an unprocessable field"))
        });

        let err = router
            .handle(make_job("send_email", json!({ "to": "x" })))
            .await
            .unwrap_err();
        assert!(
            err.type_name.ends_with("WorkerSpecificError"),
            "got {}",
            err.type_name,
        );
        assert_eq!(err.message, "payload had an unprocessable field");
    }

    #[tokio::test]
    async fn handler_returning_box_dyn_error_is_accepted() {
        // Adopter pattern: many error sources unified by returning a
        // boxed trait object. The relaxed bound on `route` accepts
        // this directly — no wrapper struct required.
        let router = Router::new().route(|_job: SendEmail| async move {
            let e: Box<dyn StdError + Send + Sync> = "something went wrong".to_string().into();
            Err::<(), _>(e)
        });

        let err = router
            .handle(make_job("send_email", json!({ "to": "x" })))
            .await
            .unwrap_err();
        assert_eq!(err.message, "something went wrong");
        // Type-erased return: type_name reflects the box itself.
        assert!(
            err.type_name.contains("Box<"),
            "expected boxed type name, got {}",
            err.type_name,
        );
    }

    #[tokio::test]
    async fn re_registering_a_route_overwrites_the_previous_handler() {
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let f = first.clone();
        let s = second.clone();

        let router = Router::new()
            .route(move |_job: SendEmail| {
                let f = f.clone();
                async move {
                    f.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .route(move |_job: SendEmail| {
                let s = s.clone();
                async move {
                    s.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            });

        router
            .handle(make_job("send_email", json!({ "to": "x" })))
            .await
            .unwrap();
        assert_eq!(first.load(Ordering::SeqCst), 0);
        assert_eq!(second.load(Ordering::SeqCst), 1);
    }
}
