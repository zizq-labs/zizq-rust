// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde_json::json;
use zizq::{Client, Format, JobPatch, JobStatus, ZizqError};

#[tokio::test]
async fn patch_all_jobs_unfiltered_patches_everything() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "patched": 5000 }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let patched = client
        .patch_all_jobs()
        .patch(JobPatch::new().priority(10))
        .await
        .unwrap();
    assert_eq!(patched, 5000);

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    // No filters → bare path → server-side patch-everything.
    assert_eq!(req.path, "/jobs");
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({ "priority": 10 }));
}

#[tokio::test]
async fn patch_all_jobs_serialises_filters() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "patched": 7 })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let patched = client
        .patch_all_jobs()
        .status([JobStatus::Ready, JobStatus::Scheduled])
        .queue(["emails"])
        .job_type(["send_email"])
        .id(["a", "b"])
        .filter(".retries > 3")
        .patch(JobPatch::new().retry_limit(10))
        .await
        .unwrap();
    assert_eq!(patched, 7);

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert!(req.path.starts_with("/jobs?"));
    assert!(req.path.contains("status=ready%2Cscheduled"));
    assert!(req.path.contains("queue=emails"));
    assert!(req.path.contains("type=send_email"));
    assert!(req.path.contains("id=a%2Cb"));
    assert!(req.path.contains("filter="));
}

#[tokio::test]
async fn patch_all_jobs_sends_only_changed_fields() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "patched": 1 })).await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    client
        .patch_all_jobs()
        .patch(
            JobPatch::new()
                .priority(20) // set
                .clear_retry_limit() // clear → null
                .keep_backoff(), // keep → absent
        )
        .await
        .unwrap();

    // Set field carries its value, cleared field is null, kept field
    // is omitted entirely.
    let req = server.last_request().await;
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({ "priority": 20, "retry_limit": null }));
}

#[tokio::test]
async fn patch_all_jobs_without_patch_errors_and_makes_no_request() {
    let server = MockServer::start().await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client
        .patch_all_jobs()
        .status([JobStatus::Ready])
        .await
        .unwrap_err();

    assert!(matches!(err, ZizqError::MissingPatch));
    assert!(
        server.requests().await.is_empty(),
        "a builder awaited without .patch() must not contact the server",
    );
}

#[tokio::test]
async fn patch_all_jobs_empty_filter_patches_nothing_without_request() {
    let server = MockServer::start().await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Explicitly empty status set → matches nothing → must NOT fall
    // through to an unfiltered patch-everything request.
    let patched = client
        .patch_all_jobs()
        .status([])
        .patch(JobPatch::new().priority(1))
        .await
        .unwrap();
    assert_eq!(patched, 0);

    assert!(
        server.requests().await.is_empty(),
        "empty-filter patch must short-circuit without a request",
    );
}

#[tokio::test]
async fn patch_all_jobs_surfaces_422_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(422, json!({ "error": "cannot patch jobs in Dead status" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client
        .patch_all_jobs()
        .status([JobStatus::Dead])
        .patch(JobPatch::new().priority(1))
        .await
        .unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 422);
            assert!(message.contains("cannot patch jobs in Dead status"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn patch_all_jobs_works_over_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &json!({ "patched": 42 }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let patched = client
        .patch_all_jobs()
        .status([JobStatus::Ready])
        .patch(JobPatch::new().retry_limit(5))
        .await
        .unwrap();
    assert_eq!(patched, 42);

    // The request body is MessagePack — decoding it as such proves
    // the patch round-tripped over the binary format, not JSON.
    let req = server.last_request().await;
    let body: serde_json::Value = rmp_serde::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({ "retry_limit": 5 }));
}
