// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use common::MockServer;
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;
use zizq::{Client, Job, Worker};

fn fake_job(id: &str) -> serde_json::Value {
    json!({
        "id": id,
        "type": "send_email",
        "queue": "emails",
        "status": "ready",
        "priority": 50,
        "ready_at": 0,
        "attempts": 0,
    })
}

fn msgpack_frame(payload: &serde_json::Value) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut buf)
        .with_struct_map()
        .with_human_readable();
    payload.serialize(&mut ser).unwrap();
    let mut out = Vec::with_capacity(4 + buf.len());
    out.extend_from_slice(&(buf.len() as u32).to_be_bytes());
    out.extend_from_slice(&buf);
    out
}

#[tokio::test]
async fn worker_handles_jobs_and_bulk_acks_them() {
    let server = MockServer::start().await;

    // /jobs/take returns three jobs then the stream closes.
    let mut stream_body = Vec::new();
    for id in ["job-1", "job-2", "job-3"] {
        stream_body.extend_from_slice(&msgpack_frame(&fake_job(id)));
    }
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;

    // /jobs/success (bulk ack) returns 204 No Content.
    server
        .set_response_for(
            "POST",
            "/jobs/success",
            204,
            "application/msgpack",
            Vec::new(),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();

    // Capture the handler invocations in order of completion.
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let processed_for_handler = processed.clone();
    let handler = move |job: Job| {
        let processed = processed_for_handler.clone();
        async move {
            processed.lock().await.push(job.id);
            Ok::<(), Infallible>(())
        }
    };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .concurrency(4)
        .queues(["emails"])
        .reconnect(false)
        .build()
        .unwrap();

    // Take stream ends after the 3 jobs, which triggers the worker's
    // drain path. We pass `pending()` for shutdown — we don't need to
    // exercise the signal path here, since stream-end runs through
    // the same drain logic.
    worker.run(std::future::pending::<()>()).await.unwrap();

    let processed = processed.lock().await.clone();
    let mut sorted = processed.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            "job-1".to_string(),
            "job-2".to_string(),
            "job-3".to_string()
        ]
    );

    // Aggregate every ack request body — the ack processor may have
    // sent the ids in one batch or split across multiple, depending on
    // how quickly handlers completed relative to the request flight.
    let ack_requests = server.requests_for("POST", "/jobs/success").await;
    assert!(!ack_requests.is_empty(), "expected at least one bulk ack");

    let mut acked_ids: Vec<String> = Vec::new();
    for req in &ack_requests {
        let body: serde_json::Value = rmp_serde::from_slice(&req.body).unwrap();
        for id in body["ids"].as_array().unwrap() {
            acked_ids.push(id.as_str().unwrap().to_string());
        }
    }
    acked_ids.sort();
    assert_eq!(
        acked_ids,
        vec![
            "job-1".to_string(),
            "job-2".to_string(),
            "job-3".to_string()
        ]
    );
}

#[tokio::test]
async fn worker_reports_failure_when_handler_errors() {
    use std::fmt;

    #[derive(Debug)]
    struct DemoError(&'static str);
    impl fmt::Display for DemoError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for DemoError {}

    let server = MockServer::start().await;

    // Stream one job.
    let stream_body = msgpack_frame(&fake_job("job-fail"));
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;

    // /jobs/job-fail/failure returns 200 with the updated job.
    let mut failure_response = Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut failure_response)
        .with_struct_map()
        .with_human_readable();
    fake_job("job-fail").serialize(&mut ser).unwrap();
    server
        .set_response_for(
            "POST",
            "/jobs/job-fail/failure",
            200,
            "application/msgpack",
            failure_response,
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();

    let handler =
        move |_job: Job| async move { Err::<(), DemoError>(DemoError("simulated failure")) };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .reconnect(false)
        .build()
        .unwrap();

    worker.run(std::future::pending::<()>()).await.unwrap();

    // The failure should have been reported with the expected fields.
    let failure_requests = server.requests_for("POST", "/jobs/job-fail/failure").await;
    assert_eq!(failure_requests.len(), 1);
    let body: serde_json::Value = rmp_serde::from_slice(&failure_requests[0].body).unwrap();
    assert_eq!(body["message"], "simulated failure");
    // type_name<E> with E = DemoError. Fully-qualified inside the test
    // module; assert it ends in "DemoError" rather than pinning the
    // exact path which can vary with anonymous test-fn naming.
    let type_name = body["error_type"].as_str().expect("error_type present");
    assert!(
        type_name.ends_with("DemoError"),
        "unexpected error_type {type_name}"
    );

    // And it should NOT have been bulk-acked.
    let ack_requests = server.requests_for("POST", "/jobs/success").await;
    assert!(
        ack_requests.is_empty(),
        "failed job should not be bulk-acked",
    );
}

#[tokio::test]
async fn worker_shutdown_signal_stops_dispatch_and_drains() {
    let server = MockServer::start().await;

    // A larger batch of jobs so we can interrupt mid-stream.
    let mut stream_body = Vec::new();
    for i in 0..20 {
        stream_body.extend_from_slice(&msgpack_frame(&fake_job(&format!("job-{i}"))));
    }
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;
    server
        .set_response_for(
            "POST",
            "/jobs/success",
            204,
            "application/msgpack",
            Vec::new(),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();

    let handler = move |_job: Job| async move {
        // Tiny delay so a few jobs are mid-flight when shutdown fires.
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok::<(), Infallible>(())
    };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .concurrency(2)
        .build()
        .unwrap();

    // Shutdown after a short time — should stop dispatch but still
    // drain in-flight and flush acks cleanly.
    let shutdown = tokio::time::sleep(Duration::from_millis(25));
    worker.run(shutdown).await.unwrap();

    // We don't assert *how many* jobs ran (timing-dependent), only
    // that the run completed without error and acks were flushed
    // before exit.
    let ack_requests = server.requests_for("POST", "/jobs/success").await;
    let mut acked = 0usize;
    for req in &ack_requests {
        let body: serde_json::Value = rmp_serde::from_slice(&req.body).unwrap();
        acked += body["ids"].as_array().unwrap().len();
    }
    // At least one job processed and acked before shutdown.
    assert!(acked >= 1, "expected at least one ack, got {acked}");
}

#[tokio::test]
async fn worker_builder_rejects_missing_client_and_handler() {
    use zizq::ZizqError;

    let err = Worker::builder().build().unwrap_err();
    assert!(matches!(err, ZizqError::MissingClient));

    let server = MockServer::start().await;
    let client = Client::builder().url(&server.url).build().unwrap();
    let err = Worker::builder().client(client).build().unwrap_err();
    assert!(matches!(err, ZizqError::MissingHandler));
}

// Paused time so the 500ms backoff between sessions doesn't cost real
// wall-clock time. Tokio auto-advances to the next pending timer when
// the runtime is otherwise idle — the mock server's I/O happens in
// real time as usual, only the sleep is virtual.
#[tokio::test(start_paused = true)]
async fn worker_reconnects_after_stream_end_and_processes_new_jobs() {
    let server = MockServer::start().await;

    // First /jobs/take returns one job; second returns another. After
    // the sequence is exhausted the mock falls through to the default
    // (empty body), but we shut the worker down before getting there.
    let stream_a = msgpack_frame(&fake_job("job-a"));
    let stream_b = msgpack_frame(&fake_job("job-b"));
    server
        .set_response_sequence_for(
            "GET",
            "/jobs/take",
            vec![
                (200, "application/vnd.zizq.msgpack-stream", stream_a),
                (200, "application/vnd.zizq.msgpack-stream", stream_b),
            ],
        )
        .await;
    server
        .set_response_for(
            "POST",
            "/jobs/success",
            204,
            "application/msgpack",
            Vec::new(),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();

    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());

    let processed_for_handler = processed.clone();
    let done_for_handler = done.clone();
    let handler = move |job: Job| {
        let processed = processed_for_handler.clone();
        let done = done_for_handler.clone();
        async move {
            let mut p = processed.lock().await;
            p.push(job.id);
            if p.len() == 2 {
                done.notify_one();
            }
            Ok::<(), Infallible>(())
        }
    };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .concurrency(2)
        .build()
        .unwrap();

    // Shutdown fires once both jobs (one from each session) have been
    // handled.
    let done_for_shutdown = done.clone();
    let shutdown = async move {
        done_for_shutdown.notified().await;
    };
    worker.run(shutdown).await.unwrap();

    let processed = processed.lock().await.clone();
    let mut sorted = processed.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["job-a".to_string(), "job-b".to_string()],
        "expected both jobs to be processed across the two sessions",
    );

    // At least two distinct take requests proves we reconnected.
    let take_requests = server.requests_for("GET", "/jobs/take").await;
    assert!(
        take_requests.len() >= 2,
        "expected at least 2 take requests across sessions, got {}",
        take_requests.len(),
    );
}

#[tokio::test(start_paused = true)]
async fn ack_retries_on_transient_error() {
    let server = MockServer::start().await;

    let stream_body = msgpack_frame(&fake_job("job-ack"));
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;

    // First bulk-ack returns 500 (transient), second returns 204.
    server
        .set_response_sequence_for(
            "POST",
            "/jobs/success",
            vec![
                (
                    500,
                    "application/json",
                    json!({"error": "transient"}).to_string().into_bytes(),
                ),
                (204, "application/msgpack", Vec::new()),
            ],
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let handler = move |_job: Job| async move { Ok::<(), Infallible>(()) };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .reconnect(false)
        .build()
        .unwrap();

    worker.run(std::future::pending::<()>()).await.unwrap();

    let ack_requests = server.requests_for("POST", "/jobs/success").await;
    assert_eq!(
        ack_requests.len(),
        2,
        "expected one failed attempt and one successful retry",
    );
    // Both attempts carry the same payload.
    for req in &ack_requests {
        let body: serde_json::Value = rmp_serde::from_slice(&req.body).unwrap();
        assert_eq!(body["ids"], json!(["job-ack"]));
    }
}

#[tokio::test]
async fn ack_drops_batch_on_4xx() {
    let server = MockServer::start().await;

    let stream_body = msgpack_frame(&fake_job("job-bad-ack"));
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;

    // Bulk-ack returns 400 (terminal). 422 doesn't count — we treat
    // it as success in the client. Use 400 to exercise the drop path.
    server
        .set_response_for(
            "POST",
            "/jobs/success",
            400,
            "application/json",
            json!({"error": "bad request"}).to_string().into_bytes(),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let handler = move |_job: Job| async move { Ok::<(), Infallible>(()) };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .reconnect(false)
        .build()
        .unwrap();

    // Should complete cleanly even though the ack was rejected.
    worker.run(std::future::pending::<()>()).await.unwrap();

    let ack_requests = server.requests_for("POST", "/jobs/success").await;
    assert_eq!(
        ack_requests.len(),
        1,
        "expected exactly one attempt — 4xx should not retry",
    );
}

#[tokio::test(start_paused = true)]
async fn nack_retries_on_transient_error() {
    use std::fmt;

    #[derive(Debug)]
    struct DemoError;
    impl fmt::Display for DemoError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "demo error")
        }
    }
    impl std::error::Error for DemoError {}

    let server = MockServer::start().await;

    let stream_body = msgpack_frame(&fake_job("job-nack"));
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;

    // Pre-serialise the success response for the retry.
    let mut job_response = Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut job_response)
        .with_struct_map()
        .with_human_readable();
    fake_job("job-nack").serialize(&mut ser).unwrap();

    server
        .set_response_sequence_for(
            "POST",
            "/jobs/job-nack/failure",
            vec![
                (
                    500,
                    "application/json",
                    json!({"error": "transient"}).to_string().into_bytes(),
                ),
                (200, "application/msgpack", job_response),
            ],
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let handler = move |_job: Job| async move { Err::<(), DemoError>(DemoError) };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .reconnect(false)
        .build()
        .unwrap();

    worker.run(std::future::pending::<()>()).await.unwrap();

    let failure_requests = server.requests_for("POST", "/jobs/job-nack/failure").await;
    assert_eq!(
        failure_requests.len(),
        2,
        "expected one failed attempt and one successful retry",
    );
}

#[tokio::test]
async fn worker_aborts_handlers_after_shutdown_timeout() {
    let server = MockServer::start().await;

    let stream_body = msgpack_frame(&fake_job("job-slow"));
    server
        .set_response_for(
            "GET",
            "/jobs/take",
            200,
            "application/vnd.zizq.msgpack-stream",
            stream_body,
        )
        .await;
    server
        .set_response_for(
            "POST",
            "/jobs/success",
            204,
            "application/msgpack",
            Vec::new(),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();

    // Handler that takes far longer than the shutdown deadline.
    let handler = move |_job: Job| async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok::<(), Infallible>(())
    };

    let worker = Worker::builder()
        .client(client.clone())
        .handler(handler)
        .shutdown_timeout(Duration::from_millis(100))
        .build()
        .unwrap();

    let start = std::time::Instant::now();
    let shutdown = tokio::time::sleep(Duration::from_millis(50));
    worker.run(shutdown).await.unwrap();
    let elapsed = start.elapsed();

    // Shutdown at ~50ms + drain timeout of 100ms ≈ 150ms.
    // Asserting < 1s gives plenty of headroom for CI variance.
    assert!(
        elapsed < Duration::from_secs(1),
        "worker took too long to force-exit: {elapsed:?}",
    );
}

#[tokio::test]
async fn worker_propagates_hard_4xx_on_initial_connect() {
    use zizq::ZizqError;

    let server = MockServer::start().await;
    server
        .set_response_json(403, json!({ "error": "forbidden" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let handler = move |_job: Job| async move { Ok::<(), Infallible>(()) };

    let worker = Worker::builder()
        .client(client)
        .handler(handler)
        .build()
        .unwrap();

    // Hard 4xx on the initial connect should propagate immediately —
    // reconnect setting doesn't apply to hard config errors.
    let err = worker.run(std::future::pending::<()>()).await.unwrap_err();
    match err {
        ZizqError::Response { status, .. } => assert_eq!(status, 403),
        other => panic!("expected Response error, got {other:?}"),
    }
}
