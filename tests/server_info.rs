// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde_json::json;
use zizq::{Client, Format, ZizqError};

#[tokio::test]
async fn health_returns_ok_on_200() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "status": "ok" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    client.health().await.unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/health");
}

#[tokio::test]
async fn health_surfaces_non_200_as_response_error() {
    let server = MockServer::start().await;
    server
        .set_response_json(503, json!({ "error": "unavailable" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let err = client.health().await.unwrap_err();

    match err {
        ZizqError::Response { status, .. } => assert_eq!(status, 503),
        other => panic!("expected Response error, got {other:?}"),
    }
}

#[tokio::test]
async fn server_version_returns_the_version_string() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "version": "0.3.1" }))
        .await;

    let client = Client::builder()
        .url(&server.url)
        .format(Format::Json)
        .build()
        .unwrap();
    let version = client.server_version().await.unwrap();
    assert_eq!(version, "0.3.1");

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/version");
}

#[tokio::test]
async fn server_version_works_over_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(200, &json!({ "version": "9.9.9" }))
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let version = client.server_version().await.unwrap();
    assert_eq!(version, "9.9.9");
}
