// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::json;
use zizq::{Client, Format, JobStatus, ZizqError};

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

fn ndjson_line(value: &serde_json::Value) -> Vec<u8> {
    let mut v = serde_json::to_vec(value).unwrap();
    v.push(b'\n');
    v
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

fn msgpack_heartbeat() -> [u8; 4] {
    [0, 0, 0, 0]
}

// --- Connection-establishment ---------------------------------------------

#[tokio::test]
async fn take_request_uses_get_and_correct_path_with_query_params() {
    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/vnd.zizq.msgpack-stream", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let _stream = client
        .take()
        .queues(["emails", "urgent"])
        .prefetch(16)
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert!(
        req.path.starts_with("/jobs/take"),
        "expected /jobs/take path, got {}",
        req.path
    );
    // Multiple queues serialise as a single comma-delimited `queue`
    // param (the server's `CommaSet`); the comma is URL-encoded.
    assert!(req.path.contains("queue=emails%2Curgent"));
    assert!(req.path.contains("prefetch=16"));
    // Streaming endpoint uses its own Accept type, distinct from the
    // request/response one.
    assert_eq!(
        req.accept.as_deref(),
        Some("application/vnd.zizq.msgpack-stream"),
    );
}

#[tokio::test]
async fn take_with_no_filters_sends_no_query_params() {
    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/vnd.zizq.msgpack-stream", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let _stream = client.take().await.unwrap();

    let req = server.last_request().await;
    // Path should be just /jobs/take, no trailing ?.
    assert_eq!(req.path, "/jobs/take");
}

#[tokio::test]
async fn take_surfaces_non_2xx_on_connect_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(403, json!({ "error": "forbidden" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let err = client.take().await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 403);
            assert_eq!(message, "forbidden");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

// --- Iteration: NDJSON ----------------------------------------------------

#[tokio::test]
async fn iterates_jobs_from_ndjson_stream_with_heartbeats() {
    let mut body = Vec::new();
    body.push(b'\n'); // heartbeat
    body.extend_from_slice(&ndjson_line(&fake_job("job-1")));
    body.push(b'\n'); // heartbeat
    body.extend_from_slice(&ndjson_line(&fake_job("job-2")));
    body.extend_from_slice(&ndjson_line(&fake_job("job-3")));
    body.push(b'\n'); // heartbeat

    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/x-ndjson", body)
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();

    let mut stream = client.take().await.unwrap();
    let mut ids = Vec::new();
    while let Some(job) = stream.next().await {
        let job = job.unwrap();
        assert_eq!(job.status, JobStatus::Ready);
        ids.push(job.id);
    }

    assert_eq!(ids, vec!["job-1", "job-2", "job-3"]);
}

// --- Iteration: MessagePack ----------------------------------------------

#[tokio::test]
async fn iterates_jobs_from_msgpack_stream_with_heartbeats() {
    let mut body = Vec::new();
    body.extend_from_slice(&msgpack_heartbeat());
    body.extend_from_slice(&msgpack_frame(&fake_job("job-1")));
    body.extend_from_slice(&msgpack_heartbeat());
    body.extend_from_slice(&msgpack_frame(&fake_job("job-2")));

    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/vnd.zizq.msgpack-stream", body)
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let mut stream = client.take().await.unwrap();

    let mut ids = Vec::new();
    while let Some(job) = stream.next().await {
        ids.push(job.unwrap().id);
    }

    assert_eq!(ids, vec!["job-1", "job-2"]);
}

// --- Content-Type wins over requested format ------------------------------

#[tokio::test]
async fn parser_is_selected_from_response_content_type_not_request() {
    // Client asks for MessagePack-stream but the server replies with
    // the NDJSON content-type instead. The client should honour the
    // server's Content-Type and parse as NDJSON.
    let mut body = Vec::new();
    body.extend_from_slice(&ndjson_line(&fake_job("job-1")));

    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/x-ndjson", body)
        .await;

    // Default format is MessagePack — so we'd be asking for the
    // msgpack-stream Accept type but the server replies with ndjson.
    let client = Client::builder().url(&server.url).build().unwrap();
    let mut stream = client.take().await.unwrap();

    let job = stream.next().await.expect("at least one job").unwrap();
    assert_eq!(job.id, "job-1");
}

// --- Clean end / partial-frame handling -----------------------------------

#[tokio::test]
async fn empty_stream_terminates_with_none() {
    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/vnd.zizq.msgpack-stream", Vec::new())
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let mut stream = client.take().await.unwrap();

    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn trailing_partial_frame_surfaces_as_decode_error() {
    // Header says 99 bytes of payload but the server only sends 2.
    let body = vec![0, 0, 0, 99, 1, 2];

    let server = MockServer::start().await;
    server
        .set_response_raw(200, "application/vnd.zizq.msgpack-stream", body)
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let mut stream = client.take().await.unwrap();

    let err = stream.next().await.expect("error").unwrap_err();
    assert!(matches!(err, ZizqError::Decode(_)));
    // After surfacing the error, the stream terminates.
    assert!(stream.next().await.is_none());
}
