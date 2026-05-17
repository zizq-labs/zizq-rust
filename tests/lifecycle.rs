// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use std::time::Duration;

use common::MockServer;
use serde_json::json;
use time::OffsetDateTime;
use zizq::{Client, JobPatch, JobStatus, ZizqError};

fn fake_job_response(id: &str, queue: &str, status: &str, attempts: u32) -> serde_json::Value {
    json!({
        "id": id,
        "type": "send_email",
        "queue": queue,
        "status": status,
        "priority": 50,
        "ready_at": 0,
        "attempts": attempts,
        "retry_limit": 25
    })
}

fn msgpack_decode(bytes: &[u8]) -> serde_json::Value {
    rmp_serde::from_slice(bytes).unwrap()
}

// --- report_success -------------------------------------------------------

#[tokio::test]
async fn report_success_posts_to_job_success_path() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client.report_success("job-1").await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs/job-1/success");
    assert!(req.body.is_empty());
}

#[tokio::test]
async fn report_success_percent_encodes_job_id() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    // The id contains a slash, which would otherwise split the path
    // and break routing on the server side.
    client.report_success("ns/abc").await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.path, "/jobs/ns%2Fabc/success");
}

#[tokio::test]
async fn report_success_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.report_success("missing").await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- report_success_bulk --------------------------------------------------

#[tokio::test]
async fn report_success_bulk_sends_ids_array() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .report_success_bulk(["job-a", "job-b", "job-c"])
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs/success");
    let parsed = msgpack_decode(&req.body);
    assert_eq!(parsed["ids"], json!(["job-a", "job-b", "job-c"]));
}

#[tokio::test]
async fn report_success_bulk_accepts_owned_strings() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let ids = vec!["job-a".to_string(), "job-b".to_string()];
    client.report_success_bulk(ids).await.unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["ids"], json!(["job-a", "job-b"]));
}

#[tokio::test]
async fn report_success_bulk_treats_422_as_success() {
    let server = MockServer::start().await;
    server
        .set_response_json(422, json!({ "error": "some ids not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    // 422 means some IDs weren't acknowledgeable — the bulk operation
    // is still considered successful (already acked / purged).
    client.report_success_bulk(["already-acked"]).await.unwrap();
}

#[tokio::test]
async fn report_success_bulk_surfaces_server_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(500, json!({ "error": "internal" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.report_success_bulk(["job-a"]).await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 500);
            assert_eq!(message, "internal");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- report_failure -------------------------------------------------------

#[tokio::test]
async fn report_failure_minimal_sends_only_message() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-1", "emails", "scheduled", 1))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client.report_failure("job-1", "boom").await.unwrap();

    assert_eq!(job.id, "job-1");
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.attempts, 1);

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs/job-1/failure");

    let parsed = msgpack_decode(&req.body);
    assert_eq!(parsed["message"], "boom");
    // No optional fields, no kill flag.
    assert!(parsed.get("error_type").is_none());
    assert!(parsed.get("backtrace").is_none());
    assert!(parsed.get("retry_at").is_none());
    assert!(parsed.get("kill").is_none());
}

#[tokio::test]
async fn report_failure_sends_all_optional_fields_when_set() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-2", "emails", "dead", 5))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .report_failure("job-2", "permanent failure")
        .error_type("FatalError")
        .backtrace("frame 1\nframe 2")
        .kill()
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["message"], "permanent failure");
    assert_eq!(parsed["error_type"], "FatalError");
    assert_eq!(parsed["backtrace"], "frame 1\nframe 2");
    assert_eq!(parsed["kill"], true);
}

#[tokio::test]
async fn report_failure_retry_at_serializes_to_unix_ms() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-3", "emails", "scheduled", 2))
        .await;

    let when = OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_000_000_000).unwrap();
    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .report_failure("job-3", "transient")
        .retry_at(when)
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["retry_at"], 1_700_000_000_000_i64);
}

#[tokio::test]
async fn report_failure_retry_in_uses_now_plus_duration() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-4", "emails", "scheduled", 2))
        .await;

    let now_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .report_failure("job-4", "transient")
        .retry_in(Duration::from_secs(30))
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    let retry_at = parsed["retry_at"].as_i64().expect("retry_at missing");
    let lo = now_ms + 29_000;
    let hi = now_ms + 31_000;
    assert!(
        retry_at >= lo && retry_at <= hi,
        "retry_at {retry_at} not within ±1s of now+30s window [{lo}, {hi}]",
    );
}

#[tokio::test]
async fn report_failure_does_not_emit_kill_when_false() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-5", "emails", "scheduled", 1))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .report_failure("job-5", "transient")
        .error_type("TimeoutError")
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert!(parsed.get("kill").is_none());
}

// --- get_job --------------------------------------------------------------

#[tokio::test]
async fn get_job_returns_the_job_on_200() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-g1", "emails", "ready", 0))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client.get_job("job-g1").await.unwrap();

    assert_eq!(job.id, "job-g1");
    assert_eq!(job.queue, "emails");
    assert_eq!(job.status, JobStatus::Ready);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs/job-g1");
    assert!(req.body.is_empty());
}

#[tokio::test]
async fn get_job_percent_encodes_job_id() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("ns/abc", "emails", "ready", 0))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    // Slash in the id would split routing on the server side without
    // percent-encoding.
    client.get_job("ns/abc").await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.path, "/jobs/ns%2Fabc");
}

#[tokio::test]
async fn get_job_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.get_job("missing").await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- delete_job -----------------------------------------------------------

#[tokio::test]
async fn delete_job_returns_unit_on_204() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client.delete_job("job-d1").await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/jobs/job-d1");
    assert!(req.body.is_empty());
}

#[tokio::test]
async fn delete_job_percent_encodes_job_id() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/msgpack", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client.delete_job("ns/abc").await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.path, "/jobs/ns%2Fabc");
}

#[tokio::test]
async fn delete_job_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.delete_job("missing").await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- patch_job ------------------------------------------------------------

#[tokio::test]
async fn patch_job_returns_the_updated_job() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-p1", "emails", "ready", 0))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client
        .patch_job("job-p1", JobPatch::new().priority(10))
        .await
        .unwrap();

    assert_eq!(job.id, "job-p1");
    assert_eq!(job.status, JobStatus::Ready);

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/jobs/job-p1");
}

#[tokio::test]
async fn patch_job_sends_only_changed_fields() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-p2", "urgent", "ready", 0))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .patch_job(
            "job-p2",
            JobPatch::new()
                .move_to_queue("urgent") // set
                .clear_backoff() // clear → null
                .keep_priority(), // keep → absent
        )
        .await
        .unwrap();

    // Set field carries its value, cleared field is null, kept field
    // is omitted entirely.
    let req = server.last_request().await;
    let body = msgpack_decode(&req.body);
    assert_eq!(body, json!({ "queue": "urgent", "backoff": null }));
}

#[tokio::test]
async fn patch_job_percent_encodes_job_id() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("ns/abc", "emails", "ready", 0))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    // Slash in the id would split routing on the server side without
    // percent-encoding.
    client
        .patch_job("ns/abc", JobPatch::new().priority(1))
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.path, "/jobs/ns%2Fabc");
}

#[tokio::test]
async fn patch_job_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client
        .patch_job("missing", JobPatch::new().priority(1))
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- get_error ------------------------------------------------------------

#[tokio::test]
async fn get_error_returns_the_record() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(
            200,
            &json!({
                "attempt": 2,
                "message": "connection timed out",
                "error_type": "TimeoutError",
                "dequeued_at": 1000,
                "failed_at": 2000,
            }),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.get_error("job-e1", 2).await.unwrap();

    assert_eq!(err.attempt, 2);
    assert_eq!(err.message, "connection timed out");
    assert_eq!(err.error_type.as_deref(), Some("TimeoutError"));
    // `backtrace` was absent — decodes to `None`.
    assert!(err.backtrace.is_none());

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs/job-e1/errors/2");
}

#[tokio::test]
async fn get_error_percent_encodes_job_id() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(
            200,
            &json!({ "attempt": 1, "message": "x", "dequeued_at": 0, "failed_at": 0 }),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    // Slash in the id would split routing on the server side without
    // percent-encoding.
    client.get_error("ns/abc", 1).await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.path, "/jobs/ns%2Fabc/errors/1");
}

#[tokio::test]
async fn get_error_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "error record not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.get_error("job-e1", 99).await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "error record not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn report_failure_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client
        .report_failure("missing", "whatever")
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}
