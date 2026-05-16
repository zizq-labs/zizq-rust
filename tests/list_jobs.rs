// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde_json::json;
use zizq::{Client, Format, JobPage, JobStatus, Order, ZizqError};

fn fake_job(id: &str, queue: &str, status: &str) -> serde_json::Value {
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

fn page_response(
    ids: &[(&str, &str, &str)],
    next: Option<&str>,
    prev: Option<&str>,
) -> serde_json::Value {
    json!({
        "jobs": ids.iter().map(|(id, q, s)| fake_job(id, q, s)).collect::<Vec<_>>(),
        "pages": {
            "self": "/jobs",
            "next": next,
            "prev": prev,
        }
    })
}

#[tokio::test]
async fn list_jobs_get_with_no_params() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            page_response(&[("job-1", "emails", "ready")], None, None),
        )
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let page = client.list_jobs().await.unwrap();

    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].id, "job-1");
    assert!(page.pages.next.is_none());
    assert!(page.pages.prev.is_none());

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs");
}

#[tokio::test]
async fn list_jobs_serialises_all_filters() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, page_response(&[], None, None))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    client
        .list_jobs()
        .from("cursor-id")
        .order(Order::Desc)
        .limit(25)
        .status([JobStatus::Ready, JobStatus::Scheduled])
        .queue(["emails", "webhooks"])
        .job_type(["send_email"])
        .id(["a", "b"])
        .filter(".user_id == 42")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    // Path always starts with `/jobs?` then query params in builder order.
    assert!(req.path.starts_with("/jobs?"));
    assert!(req.path.contains("from=cursor-id"));
    assert!(req.path.contains("order=desc"));
    assert!(req.path.contains("limit=25"));
    assert!(req.path.contains("status=ready%2Cscheduled"));
    assert!(req.path.contains("queue=emails%2Cwebhooks"));
    assert!(req.path.contains("type=send_email"));
    assert!(req.path.contains("id=a%2Cb"));
    assert!(req.path.contains("filter="));
}

#[tokio::test]
async fn list_jobs_surfaces_400_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(400, json!({ "error": "limit must be between 1 and 2000" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client.list_jobs().limit(0).await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 400);
            assert!(message.contains("limit"));
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn list_jobs_empty_filter_returns_empty_page_without_request() {
    let server = MockServer::start().await;
    // No response configured — if the client contacts the server the
    // test would still pass on the body, so we assert on request
    // count instead.

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Explicitly empty status set → matches nothing.
    let page = client
        .list_jobs()
        .status([])
        .queue(["emails"])
        .await
        .unwrap();

    assert!(page.jobs.is_empty());
    assert!(page.pages.next.is_none());
    assert!(page.pages.prev.is_none());

    // The request must NOT have been sent.
    assert!(
        server.requests().await.is_empty(),
        "empty-filter list_jobs should short-circuit without a request",
    );
}

#[tokio::test]
async fn get_page_follows_a_relative_path() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            page_response(
                &[("job-2", "emails", "ready")],
                Some("/jobs?from=job-2"),
                Some("/jobs"),
            ),
        )
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Server-emitted path with query string.
    let page: JobPage = client.get_page("/jobs?from=job-1").await.unwrap();

    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].id, "job-2");
    assert_eq!(page.pages.next.as_deref(), Some("/jobs?from=job-2"));

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs?from=job-1");
}

#[tokio::test]
async fn get_page_rejects_host_mismatch() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, page_response(&[], None, None))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    // Protocol-relative path → would resolve to a different host.
    let err = client
        .get_page::<JobPage>("//evil.example.com/jobs")
        .await
        .unwrap_err();

    match err {
        ZizqError::Decode(msg) => {
            assert!(
                msg.contains("different host"),
                "expected host-mismatch error message, got: {msg}",
            );
        }
        other => panic!("expected Decode error, got {other:?}"),
    }
}
