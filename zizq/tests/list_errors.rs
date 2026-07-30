// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use futures_util::TryStreamExt;
use serde_json::json;
use zizq::{Client, ErrorRecord, Format, Order, ZizqError};

fn fake_error(attempt: u32, message: &str) -> serde_json::Value {
    json!({
        "attempt": attempt,
        "message": message,
        "error_type": "TimeoutError",
        "dequeued_at": 1000,
        "failed_at": 2000,
    })
}

fn errors_page(
    records: &[(u32, &str)],
    next: Option<&str>,
    prev: Option<&str>,
) -> serde_json::Value {
    json!({
        "errors": records.iter().map(|(a, m)| fake_error(*a, m)).collect::<Vec<_>>(),
        "pages": {
            "self": "/jobs/job-1/errors",
            "next": next,
            "prev": prev,
        }
    })
}

#[tokio::test]
async fn list_errors_get_with_no_params() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, errors_page(&[(1, "boom")], None, None))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let page = client.list_errors("job-1").await.unwrap();

    assert_eq!(page.errors.len(), 1);
    assert_eq!(page.errors[0].attempt, 1);
    assert_eq!(page.errors[0].message, "boom");
    // `backtrace` was absent from the response — decodes to `None`.
    assert!(page.errors[0].backtrace.is_none());
    assert!(page.pages.next.is_none());

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/jobs/job-1/errors");
}

#[tokio::test]
async fn list_errors_serialises_paging_params() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, errors_page(&[], None, None))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    client
        .list_errors("job-1")
        .from(3)
        .order(Order::Desc)
        .limit(25)
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert!(req.path.starts_with("/jobs/job-1/errors?"));
    assert!(req.path.contains("from=3"));
    assert!(req.path.contains("order=desc"));
    assert!(req.path.contains("limit=25"));
}

#[tokio::test]
async fn list_errors_surfaces_404_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(404, json!({ "error": "job not found" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client.list_errors("missing").await.unwrap_err();

    match err {
        ZizqError::Response { status, message } => {
            assert_eq!(status, 404);
            assert_eq!(message, "job not found");
        }
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_paginates_across_pages() {
    let server = MockServer::start().await;
    // The mock routes by path only (query string ignored), so both the
    // first request and the `next` follow-up share the path
    // `/jobs/job-1/errors` — a response sequence serves page 1 then 2.
    server
        .set_response_sequence_for(
            "GET",
            "/jobs/job-1/errors",
            vec![
                (
                    200,
                    "application/json",
                    serde_json::to_vec(&errors_page(
                        &[(1, "first"), (2, "second")],
                        Some("/jobs/job-1/errors?from=2"),
                        None,
                    ))
                    .unwrap(),
                ),
                (
                    200,
                    "application/json",
                    serde_json::to_vec(&errors_page(
                        &[(3, "third")],
                        None,
                        Some("/jobs/job-1/errors"),
                    ))
                    .unwrap(),
                ),
            ],
        )
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let errors: Vec<ErrorRecord> = client
        .list_errors("job-1")
        .stream()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        errors.iter().map(|e| e.attempt).collect::<Vec<_>>(),
        [1, 2, 3],
    );

    // Two pages fetched: the first URL and the `next` path.
    let paths: Vec<String> = server
        .requests()
        .await
        .into_iter()
        .map(|r| r.path)
        .collect();
    assert_eq!(paths, ["/jobs/job-1/errors", "/jobs/job-1/errors?from=2"],);
}

#[tokio::test]
async fn list_errors_works_over_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &errors_page(&[(7, "mp")], None, None))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let page = client.list_errors("job-1").await.unwrap();

    assert_eq!(page.errors.len(), 1);
    assert_eq!(page.errors[0].attempt, 7);
    assert_eq!(page.errors[0].message, "mp");
}
