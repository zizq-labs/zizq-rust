// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde_json::json;
use zizq::{Client, Format, JobStatus, ZizqError};

#[tokio::test]
async fn count_jobs_with_no_filters() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "count": 17 })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let count = client.count_jobs().await.unwrap();
    assert_eq!(count, 17);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs/count");
}

#[tokio::test]
async fn count_jobs_serialises_filters() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "count": 3 })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let count = client
        .count_jobs()
        .status([JobStatus::Ready, JobStatus::Dead])
        .queue(["emails", "webhooks"])
        .job_type(["send_email"])
        .id(["a", "b"])
        .filter(".user_id == 42")
        .await
        .unwrap();
    assert_eq!(count, 3);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert!(req.path.starts_with("/jobs/count?"));
    assert!(req.path.contains("status=ready%2Cdead"));
    assert!(req.path.contains("queue=emails%2Cwebhooks"));
    assert!(req.path.contains("type=send_email"));
    assert!(req.path.contains("id=a%2Cb"));
    assert!(req.path.contains("filter="));
}

#[tokio::test]
async fn count_jobs_surfaces_400_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(400, json!({ "error": "invalid query parameters" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client
        .count_jobs()
        .filter("not valid jq (")
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 400);
            assert!(message.contains("invalid query parameters"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn count_jobs_empty_filter_returns_zero_without_request() {
    let server = MockServer::start().await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Explicitly empty status set → matches nothing.
    let count = client.count_jobs().status([]).await.unwrap();
    assert_eq!(count, 0);

    assert!(
        server.requests().await.is_empty(),
        "empty-filter count_jobs should short-circuit without a request",
    );
}

#[tokio::test]
async fn count_jobs_works_over_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &json!({ "count": 99 }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let count = client.count_jobs().await.unwrap();
    assert_eq!(count, 99);
}
