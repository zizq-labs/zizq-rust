// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use common::MockServer;
use serde::{Deserialize, Serialize};
use serde_json::json;
use zizq::{Client, CronEntry, Format, JobKind, ZizqError};

#[derive(Serialize, Deserialize)]
struct Cleanup(serde_json::Value);

impl JobKind for Cleanup {
    const NAME: &'static str = "cleanup";
    const QUEUE: &'static str = "maintenance";
}

fn json_client(url: &str) -> Client {
    Client::builder()
        .url(url)
        .format(Format::Json)
        .build()
        .unwrap()
}

fn job_json() -> serde_json::Value {
    json!({ "type": "cleanup", "queue": "maintenance", "payload": { "days": 30 } })
}

fn entry_json(name: &str, expression: &str) -> serde_json::Value {
    json!({
        "name": name,
        "expression": expression,
        "paused": false,
        "job": job_json(),
        "next_enqueue_at": 1_000,
    })
}

fn group_json(name: &str, entries: &[(&str, &str)]) -> serde_json::Value {
    json!({
        "name": name,
        "paused": false,
        "entries": entries.iter().map(|(n, e)| entry_json(n, e)).collect::<Vec<_>>(),
    })
}

#[tokio::test]
async fn list_crons_returns_group_names() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "crons": ["nightly", "hourly"] }))
        .await;

    let crons = json_client(&server.url).list_crons().await.unwrap();
    assert_eq!(crons, ["nightly", "hourly"]);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/crons");
}

#[tokio::test]
async fn get_cron_returns_group_and_entries() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, group_json("nightly", &[("cleanup", "0 0 * * *")]))
        .await;

    let group = json_client(&server.url).get_cron("nightly").await.unwrap();
    assert_eq!(group.name, "nightly");
    assert_eq!(group.entries.len(), 1);
    assert_eq!(group.entries[0].name, "cleanup");
    assert_eq!(group.entries[0].expression, "0 0 * * *");
    assert_eq!(group.entries[0].job.job_type, "cleanup");
    assert_eq!(group.entries[0].job.payload, Some(json!({ "days": 30 })));

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/crons/nightly");
}

#[tokio::test]
async fn replace_cron_sends_entries() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, group_json("nightly", &[("cleanup", "0 0 * * *")]))
        .await;

    let client = json_client(&server.url);
    let group = client
        .replace_cron("nightly")
        .entry(CronEntry::new(
            "cleanup",
            "0 0 * * *",
            client.enqueue(Cleanup(json!({ "days": 30 }))),
        ))
        .await
        .unwrap();
    assert_eq!(group.entries.len(), 1);

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/crons/nightly");
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["entries"][0]["name"], "cleanup");
    assert_eq!(body["entries"][0]["expression"], "0 0 * * *");
    assert_eq!(body["entries"][0]["job"]["type"], "cleanup");
    assert_eq!(body["entries"][0]["job"]["queue"], "maintenance");
    assert_eq!(body["entries"][0]["job"]["payload"], json!({ "days": 30 }));
}

#[tokio::test]
async fn replace_cron_includes_timezone_and_paused() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, group_json("nightly", &[]))
        .await;

    let client = json_client(&server.url);
    client
        .replace_cron("nightly")
        .paused(true)
        .entry(
            CronEntry::new("cleanup", "0 0 * * *", client.enqueue(Cleanup(json!({}))))
                .timezone("Australia/Melbourne")
                .paused(true),
        )
        .await
        .unwrap();

    let req = server.last_request().await;
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["paused"], true);
    assert_eq!(body["entries"][0]["timezone"], "Australia/Melbourne");
    assert_eq!(body["entries"][0]["paused"], true);
}

#[tokio::test]
async fn add_cron_entry_posts_and_decodes() {
    let server = MockServer::start().await;
    server
        .set_response_json(201, entry_json("hourly", "0 * * * *"))
        .await;

    let client = json_client(&server.url);
    let entry = client
        .add_cron_entry(
            "nightly",
            CronEntry::new("hourly", "0 * * * *", client.enqueue(Cleanup(json!({})))),
        )
        .await
        .unwrap();
    assert_eq!(entry.name, "hourly");
    assert_eq!(entry.expression, "0 * * * *");

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/crons/nightly/entries");
}

#[tokio::test]
async fn get_cron_entry_decodes() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, entry_json("cleanup", "0 0 * * *"))
        .await;

    let entry = json_client(&server.url)
        .get_cron_entry("nightly", "cleanup")
        .await
        .unwrap();
    assert_eq!(entry.name, "cleanup");
    assert_eq!(entry.job.job_type, "cleanup");

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/crons/nightly/entries/cleanup");
}

#[tokio::test]
async fn put_cron_entry_uses_entry_name_in_path() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, entry_json("cleanup", "0 6 * * *"))
        .await;

    let client = json_client(&server.url);
    client
        .put_cron_entry(
            "nightly",
            CronEntry::new("cleanup", "0 6 * * *", client.enqueue(Cleanup(json!({})))),
        )
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/crons/nightly/entries/cleanup");
}

#[tokio::test]
async fn delete_cron_and_entry_send_delete() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/json", Vec::new())
        .await;

    let client = json_client(&server.url);

    client.delete_cron("nightly").await.unwrap();
    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/crons/nightly");

    client
        .delete_cron_entry("nightly", "cleanup")
        .await
        .unwrap();
    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/crons/nightly/entries/cleanup");
}

#[tokio::test]
async fn pause_and_resume_cron_send_patch_with_paused_flag() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, group_json("nightly", &[]))
        .await;

    let client = json_client(&server.url);

    client.pause_cron("nightly").await.unwrap();
    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/crons/nightly");
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({ "paused": true }));

    client.resume_cron("nightly").await.unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&server.last_request().await.body).unwrap();
    assert_eq!(body, json!({ "paused": false }));
}

#[tokio::test]
async fn pause_cron_entry_targets_the_entry() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, entry_json("cleanup", "0 0 * * *"))
        .await;

    json_client(&server.url)
        .pause_cron_entry("nightly", "cleanup")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/crons/nightly/entries/cleanup");
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({ "paused": true }));
}

#[tokio::test]
async fn cron_surfaces_403_without_pro_license() {
    let server = MockServer::start().await;
    server
        .set_response_json(403, json!({ "error": "cron requires a Pro license" }))
        .await;

    let err = json_client(&server.url).list_crons().await.unwrap_err();
    match err {
        ZizqError::Response { status, .. } => assert_eq!(status, 403),
        other => panic!("expected Response error, got {other:?}"),
    }
}
