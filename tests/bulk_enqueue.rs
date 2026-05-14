// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde::{Deserialize, Serialize};
use serde_json::json;
use zizq::{Client, Format, JobKind, JobStatus, ZizqError};

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
struct ProcessReport {
    report_id: u64,
}

impl JobKind for ProcessReport {
    const NAME: &'static str = "process_report";
    const QUEUE: &'static str = "reports";
}

fn fake_job(id: &str, job_type: &str, queue: &str) -> serde_json::Value {
    json!({
        "id": id,
        "type": job_type,
        "queue": queue,
        "status": "ready",
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
async fn bulk_enqueues_via_h2c_with_messagepack_by_default() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(
            200,
            &json!({
                "jobs": [
                    fake_job("job-1", "send_email", "emails"),
                    fake_job("job-2", "send_email", "emails"),
                ]
            }),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let jobs = client
        .enqueue_bulk()
        .add(client.enqueue(SendEmail { to: "a@x".into() }))
        .add(client.enqueue(SendEmail { to: "b@x".into() }))
        .await
        .unwrap();

    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].id, "job-1");
    assert_eq!(jobs[1].id, "job-2");
    assert_eq!(jobs[0].status, JobStatus::Ready);

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs/bulk");
    assert_eq!(req.content_type.as_deref(), Some("application/msgpack"));

    let parsed = msgpack_decode(&req.body);
    let jobs_arr = parsed["jobs"].as_array().unwrap();
    assert_eq!(jobs_arr.len(), 2);
    assert_eq!(jobs_arr[0]["type"], "send_email");
    assert_eq!(jobs_arr[0]["payload"]["to"], "a@x");
    assert_eq!(jobs_arr[1]["payload"]["to"], "b@x");
}

#[tokio::test]
async fn bulk_supports_mixed_job_kinds() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            json!({
                "jobs": [
                    fake_job("j1", "send_email", "emails"),
                    fake_job("j2", "process_report", "reports"),
                ]
            }),
        )
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let jobs = client
        .enqueue_bulk()
        .add(client.enqueue(SendEmail { to: "a@x".into() }))
        .add(client.enqueue(ProcessReport { report_id: 42 }))
        .await
        .unwrap();

    assert_eq!(jobs.len(), 2);

    let parsed: serde_json::Value =
        serde_json::from_slice(&server.last_request().await.body).unwrap();
    let arr = parsed["jobs"].as_array().unwrap();
    assert_eq!(arr[0]["type"], "send_email");
    assert_eq!(arr[0]["queue"], "emails");
    assert_eq!(arr[1]["type"], "process_report");
    assert_eq!(arr[1]["queue"], "reports");
    assert_eq!(arr[1]["payload"]["report_id"], 42);
}

#[tokio::test]
async fn bulk_preserves_per_job_overrides() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            json!({ "jobs": [fake_job("j1", "send_email", "high")] }),
        )
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    client
        .enqueue_bulk()
        .add(
            client
                .enqueue(SendEmail { to: "vip@x".into() })
                .queue("high")
                .priority(1)
                .retry_limit(7),
        )
        .await
        .unwrap();

    let parsed: serde_json::Value =
        serde_json::from_slice(&server.last_request().await.body).unwrap();
    let job = &parsed["jobs"][0];
    assert_eq!(job["queue"], "high");
    assert_eq!(job["priority"], 1);
    assert_eq!(job["retry_limit"], 7);
}

#[tokio::test]
async fn bulk_loop_style_with_push() {
    let server = MockServer::start().await;
    let response_jobs: Vec<serde_json::Value> = (0..5)
        .map(|i| fake_job(&format!("j{i}"), "send_email", "emails"))
        .collect();
    server
        .set_response_json(200, json!({ "jobs": response_jobs }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();

    let mut batch = client.enqueue_bulk();
    for i in 0..5 {
        batch.push(client.enqueue(SendEmail {
            to: format!("u{i}@x"),
        }));
    }
    assert_eq!(batch.len(), 5);
    let jobs = batch.await.unwrap();
    assert_eq!(jobs.len(), 5);

    let parsed: serde_json::Value =
        serde_json::from_slice(&server.last_request().await.body).unwrap();
    let arr = parsed["jobs"].as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr[0]["payload"]["to"], "u0@x");
    assert_eq!(arr[4]["payload"]["to"], "u4@x");
}

#[tokio::test]
async fn bulk_empty_sends_empty_array() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "jobs": [] })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let jobs = client.enqueue_bulk().await.unwrap();
    assert!(jobs.is_empty());

    let parsed: serde_json::Value =
        serde_json::from_slice(&server.last_request().await.body).unwrap();
    let arr = parsed["jobs"].as_array().unwrap();
    assert!(arr.is_empty());
}

#[tokio::test]
async fn bulk_error_response_surfaces_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(400, json!({ "error": "jobs[1]: queue must not be empty" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client
        .enqueue_bulk()
        .add(client.enqueue(SendEmail { to: "a@x".into() }))
        .add(client.enqueue(SendEmail { to: "b@x".into() }))
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 400);
            assert!(message.contains("queue must not be empty"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}
