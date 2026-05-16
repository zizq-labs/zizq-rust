// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde_json::json;
use zizq::{Client, Format, JobStatus, ZizqError};

#[tokio::test]
async fn delete_all_jobs_unfiltered_deletes_everything() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "deleted": 5000 }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let deleted = client.delete_all_jobs().await.unwrap();
    assert_eq!(deleted, 5000);

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    // No filters → bare path → server-side delete-everything.
    assert_eq!(req.path, "/jobs");
    assert!(req.body.is_empty());
}

#[tokio::test]
async fn delete_all_jobs_serialises_filters() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "deleted": 7 })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let deleted = client
        .delete_all_jobs()
        .status([JobStatus::Dead, JobStatus::Completed])
        .queue(["emails"])
        .job_type(["send_email"])
        .id(["a", "b"])
        .filter(".retries > 3")
        .await
        .unwrap();
    assert_eq!(deleted, 7);

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert!(req.path.starts_with("/jobs?"));
    assert!(req.path.contains("status=dead%2Ccompleted"));
    assert!(req.path.contains("queue=emails"));
    assert!(req.path.contains("type=send_email"));
    assert!(req.path.contains("id=a%2Cb"));
    assert!(req.path.contains("filter="));
}

#[tokio::test]
async fn delete_all_jobs_empty_filter_deletes_nothing_without_request() {
    let server = MockServer::start().await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Explicitly empty status set → matches nothing → must NOT fall
    // through to an unfiltered delete-everything request.
    let deleted = client.delete_all_jobs().status([]).await.unwrap();
    assert_eq!(deleted, 0);

    assert!(
        server.requests().await.is_empty(),
        "empty-filter delete must short-circuit without a request",
    );
}

#[tokio::test]
async fn delete_all_jobs_surfaces_500_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(500, json!({ "error": "internal" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client
        .delete_all_jobs()
        .status([JobStatus::Dead])
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 500);
            assert!(message.contains("internal"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_all_jobs_works_over_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &json!({ "deleted": 42 }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let deleted = client
        .delete_all_jobs()
        .status([JobStatus::Ready])
        .await
        .unwrap();
    assert_eq!(deleted, 42);
}
