// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use std::time::Duration;

use common::MockServer;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;
use zizq::{
    BackoffConfig, Client, Format, JobKind, JobStatus, RetentionConfig, UniqueKey, UniqueScope,
    ZizqError,
};

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

impl JobKind for SendEmail {
    const NAME: &'static str = "send_email";
    const QUEUE: &'static str = "emails";
    const PRIORITY: Option<u32> = Some(50);
}

#[derive(Debug, Serialize, Deserialize)]
struct PingJob;

impl JobKind for PingJob {
    const NAME: &'static str = "ping";
}

fn fake_job_response(id: &str, queue: &str, status: &str) -> serde_json::Value {
    json!({
        "id": id,
        "type": "send_email",
        "queue": queue,
        "status": status,
        "priority": 50,
        "ready_at": 0,
        "attempts": 0,
        "retry_limit": 25
    })
}

fn msgpack_decode(bytes: &[u8]) -> serde_json::Value {
    rmp_serde::from_slice(bytes).unwrap()
}

#[tokio::test]
async fn enqueues_via_h2c_with_messagepack_by_default() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-1", "emails", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap();

    assert_eq!(job.id, "job-1");
    assert_eq!(job.queue, "emails");
    assert_eq!(job.status, JobStatus::Ready);

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs");
    assert_eq!(req.content_type.as_deref(), Some("application/msgpack"));
    assert_eq!(req.accept.as_deref(), Some("application/msgpack"));

    // Wire body uses snake_case + the trait defaults for queue / priority.
    let parsed = msgpack_decode(&req.body);
    assert_eq!(parsed["type"], "send_email");
    assert_eq!(parsed["queue"], "emails");
    assert_eq!(parsed["priority"], 50);
    assert_eq!(parsed["payload"]["to"], "a@b");
    assert!(parsed.get("ready_at").is_none());
    assert!(parsed.get("unique_key").is_none());
}

#[tokio::test]
async fn enqueues_via_h2c_with_json_when_configured() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, fake_job_response("job-2", "emails", "ready"))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let job = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap();

    assert_eq!(job.id, "job-2");

    let req = server.last_request().await;
    assert_eq!(req.content_type.as_deref(), Some("application/json"));
    let parsed: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(parsed["type"], "send_email");
}

#[tokio::test]
async fn builder_overrides_beat_trait_defaults() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-3", "high", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .enqueue(SendEmail { to: "a@b".into() })
        .queue("high")
        .priority(10)
        .retry_limit(3)
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["queue"], "high");
    assert_eq!(parsed["priority"], 10);
    assert_eq!(parsed["retry_limit"], 3);
}

#[tokio::test]
async fn delay_sets_ready_at_in_the_future() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-4", "emails", "scheduled"))
        .await;

    let now_ms = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .enqueue(SendEmail { to: "a@b".into() })
        .delay(Duration::from_secs(60))
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    let ready_at = parsed["ready_at"].as_i64().expect("ready_at missing");
    let expected_min = (now_ms + 59_000) as i64;
    let expected_max = (now_ms + 61_000) as i64;
    assert!(
        ready_at >= expected_min && ready_at <= expected_max,
        "ready_at {ready_at} not within ±1s of now+60s window [{expected_min}, {expected_max}]",
    );
}

#[tokio::test]
async fn unique_key_override_with_scope() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-5", "emails", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .enqueue(SendEmail { to: "a@b".into() })
        .unique_key(UniqueKey::raw("user:42").scope(UniqueScope::Exists))
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["unique_key"], "user:42");
    assert_eq!(parsed["unique_while"], "exists");
}

#[tokio::test]
async fn trait_default_queue_used_when_not_overridden() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-6", "default", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client.enqueue(PingJob).await.unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["type"], "ping");
    assert_eq!(parsed["queue"], "default");
    assert!(parsed.get("priority").is_none());
}

#[tokio::test]
async fn response_decoded_per_content_type_not_requested_format() {
    // Client asks for msgpack but the server (incorrectly) replies with
    // JSON and Content-Type: application/json. We should honour the
    // server's Content-Type and decode as JSON.
    let server = MockServer::start().await;
    let body = serde_json::to_vec(&fake_job_response("job-ct", "emails", "ready")).unwrap();
    server.set_response_raw(200, "application/json", body).await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap();

    assert_eq!(job.id, "job-ct");
    assert_eq!(job.queue, "emails");
    assert_eq!(job.status, JobStatus::Ready);
}

#[tokio::test]
async fn structured_error_message_extracted_in_messagepack() {
    let server = MockServer::start().await;
    let mut buf = Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut buf)
        .with_struct_map()
        .with_human_readable();
    json!({ "error": "queue does not exist" })
        .serialize(&mut ser)
        .unwrap();
    server
        .set_response_raw(404, "application/msgpack", buf)
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "queue does not exist");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn structured_error_falls_back_to_json_when_format_mismatches() {
    // Simulates a 406 Not Acceptable: client asked for msgpack but the
    // server can only reply in JSON.
    let server = MockServer::start().await;
    let body = serde_json::to_vec(&json!({ "error": "format not acceptable" })).unwrap();
    server.set_response_raw(406, "application/json", body).await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 406);
            assert_eq!(message, "format not acceptable");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn server_error_surfaces_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_raw(500, "text/plain", b"oh no".to_vec())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 500);
            assert!(message.contains("oh no"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn payload_unique_key_from_trait_method() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Uniq {
        id: u64,
    }
    impl JobKind for Uniq {
        const NAME: &'static str = "uniq";
        fn unique_key(&self) -> Option<UniqueKey> {
            Some(UniqueKey::raw(format!("uniq:{}", self.id)))
        }
    }

    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-7", "default", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client.enqueue(Uniq { id: 99 }).await.unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["unique_key"], "uniq:99");
}

#[tokio::test]
async fn backoff_and_retention_overrides_serialize_on_the_wire() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-br", "emails", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .enqueue(SendEmail { to: "a@b".into() })
        .backoff(BackoffConfig {
            base_ms: 1_000,
            exponent: 2.0,
            jitter_ms: 500,
        })
        .retention(RetentionConfig {
            completed_ms: Some(60_000),
            dead_ms: None,
        })
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["backoff"]["base_ms"], 1_000);
    assert_eq!(parsed["backoff"]["exponent"], 2.0);
    assert_eq!(parsed["backoff"]["jitter_ms"], 500);
    assert_eq!(parsed["retention"]["completed_ms"], 60_000);
    assert!(parsed["retention"].get("dead_ms").is_none());
}

#[tokio::test]
async fn job_response_folds_unique_key_and_includes_payload() {
    let server = MockServer::start().await;
    let mut response = fake_job_response("job-uk", "emails", "ready");
    response["payload"] = json!({ "to": "alice@example.com" });
    response["unique_key"] = json!("user:42");
    response["unique_while"] = json!("exists");
    response["duplicate"] = json!(true);
    server.set_response_msgpack(200, &response).await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let job = client
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap();

    let uk = job.unique_key.expect("unique_key folded");
    assert_eq!(uk.key, "user:42");
    assert_eq!(uk.scope, Some(UniqueScope::Exists));
    assert_eq!(job.payload.as_ref().unwrap()["to"], "alice@example.com");
    assert_eq!(job.duplicate, Some(true));
}

#[tokio::test]
async fn explicit_unique_key_overrides_payload_method() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Uniq;
    impl JobKind for Uniq {
        const NAME: &'static str = "uniq";
        fn unique_key(&self) -> Option<UniqueKey> {
            Some(UniqueKey::raw("from-trait"))
        }
    }

    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &fake_job_response("job-8", "default", "ready"))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    client
        .enqueue(Uniq)
        .unique_key("from-builder")
        .await
        .unwrap();

    let parsed = msgpack_decode(&server.last_request().await.body);
    assert_eq!(parsed["unique_key"], "from-builder");
}
