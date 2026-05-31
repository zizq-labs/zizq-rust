// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Integration scenarios for the Zizq Rust client.
//!
//! These exercise the *packaged* `zizq` crate (extracted from the
//! `.crate` artifact by `run.sh`) against a real Zizq server, whose
//! URL is supplied via the `ZIZQ_URL` environment variable.
//!
//! Scenarios run sequentially — `run.sh` passes `--test-threads=1` —
//! because each one wipes the server's entire job set on entry (via
//! [`fresh`]), which would race other scenarios under parallelism.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;
use zizq::{
    jq_contains, jq_eq, Client, CronEntry, JobKind, JobPatch, Router, Worker, ZizqError,
};

/// A job kind carrying an arbitrary JSON payload. The macro stamps
/// out one struct per API job-type name we need to tell apart in
/// queries — the payload shape is whatever each scenario passes.
macro_rules! job_kind {
    ($name:ident, $type:literal) => {
        #[derive(Serialize, Deserialize)]
        struct $name(serde_json::Value);

        impl JobKind for $name {
            const NAME: &'static str = $type;
            const QUEUE: &'static str = "integration";
        }
    };
}

job_kind!(Alpha, "alpha");
job_kind!(Beta, "beta");
job_kind!(Gamma, "gamma");

/// Connect to the server named by `ZIZQ_URL` and wipe every job and
/// cron group, so each scenario starts from a known-empty state.
async fn fresh() -> Client {
    let url = std::env::var("ZIZQ_URL").expect("ZIZQ_URL must be set (run via run.sh)");
    let client = Client::builder().url(&url).build().expect("build client");
    client.reset().await.expect("reset server");
    client
}

#[tokio::test]
async fn server_smoke() {
    let client = fresh().await;

    client.health().await.expect("server healthy");

    let version = client.server_version().await.expect("server version");
    assert!(!version.is_empty(), "version string should not be empty");

    // The call must succeed; the queue set may legitimately be empty.
    client.list_queues().await.expect("list queues");
}

#[tokio::test]
async fn enqueue_and_get_a_job() {
    let client = fresh().await;

    let job = client
        .enqueue(Alpha(json!({ "hello": "world" })))
        .await
        .expect("enqueue");

    assert!(!job.id.is_empty());
    assert_eq!(job.job_type, "alpha");
    assert_eq!(job.queue, "integration");

    let fetched = client.get_job(&job.id).await.expect("get_job");
    assert_eq!(fetched.id, job.id);
    assert_eq!(fetched.payload, Some(json!({ "hello": "world" })));
}

#[tokio::test]
async fn enqueue_bulk() {
    let client = fresh().await;

    let jobs = client
        .enqueue_bulk()
        .add(client.enqueue(Alpha(json!({ "n": 1 }))))
        .add(client.enqueue(Beta(json!({ "n": 2 }))))
        .add(client.enqueue(Gamma(json!({ "n": 3 }))))
        .await
        .expect("enqueue_bulk");

    assert_eq!(jobs.len(), 3);
    let types: Vec<&str> = jobs.iter().map(|j| j.job_type.as_str()).collect();
    assert_eq!(types, ["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn worker_processes_jobs_end_to_end() {
    let client = fresh().await;
    let count: u64 = 10;

    let mut batch = client.enqueue_bulk();
    for i in 0..count {
        batch.push(
            client
                .enqueue(Alpha(json!({ "index": i })))
                .queue("worker-integration"),
        );
    }
    batch.await.expect("enqueue batch");

    let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Arc::new(Notify::new());

    let worker = Worker::builder()
        .client(client.clone())
        .concurrency(5)
        .queues(vec!["worker-integration"])
        .handler(Router::new().route({
            let seen = seen.clone();
            let shutdown = shutdown.clone();
            move |job: Alpha| {
                let seen = seen.clone();
                let shutdown = shutdown.clone();
                async move {
                    let mut seen = seen.lock().unwrap();
                    seen.push(job.0["index"].as_u64().expect("index field"));
                    if seen.len() as u64 == count {
                        shutdown.notify_one();
                    }
                    Ok::<(), Infallible>(())
                }
            }
        }))
        .build()
        .expect("build worker");

    // Bound the run — a stuck worker should fail the test, not hang CI.
    tokio::time::timeout(
        Duration::from_secs(30),
        worker.run(async move { shutdown.notified().await }),
    )
    .await
    .expect("worker run timed out")
    .expect("worker run");

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, (0..count).collect::<Vec<_>>());
}

#[tokio::test]
async fn list_and_query_jobs() {
    let client = fresh().await;

    let job = client
        .enqueue(Alpha(json!({ "marker": "findme" })))
        .queue("query-integration")
        .await
        .expect("enqueue");

    let page = client
        .list_jobs()
        .queue(["query-integration"])
        .job_type(["alpha"])
        .await
        .expect("list_jobs");

    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].id, job.id);
    assert_eq!(page.jobs[0].payload, Some(json!({ "marker": "findme" })));
}

#[tokio::test]
async fn delete_a_job() {
    let client = fresh().await;

    let job = client.enqueue(Alpha(json!({}))).await.expect("enqueue");
    client.delete_job(&job.id).await.expect("delete_job");

    let err = client
        .get_job(&job.id)
        .await
        .expect_err("deleted job should be gone");
    assert!(
        matches!(err, ZizqError::Response { status: 404, .. }),
        "expected a 404 Response error, got {err:?}",
    );
}

#[tokio::test]
async fn count_jobs() {
    let client = fresh().await;
    assert_eq!(client.count_jobs().await.expect("count empty"), 0);

    client
        .enqueue_bulk()
        .add(client.enqueue(Alpha(json!({}))).queue("q1"))
        .add(client.enqueue(Alpha(json!({}))).queue("q1"))
        .add(client.enqueue(Beta(json!({}))).queue("q2"))
        .await
        .expect("enqueue_bulk");

    assert_eq!(client.count_jobs().await.expect("count all"), 3);
    assert_eq!(client.count_jobs().queue(["q1"]).await.expect("count q1"), 2);
    assert_eq!(client.count_jobs().queue(["q2"]).await.expect("count q2"), 1);
    assert_eq!(
        client
            .count_jobs()
            .job_type(["alpha"])
            .await
            .expect("count alpha"),
        2,
    );
    assert_eq!(
        client
            .count_jobs()
            .queue(["nonexistent"])
            .await
            .expect("count none"),
        0,
    );
}

#[tokio::test]
async fn query_with_jq_filters() {
    let client = fresh().await;

    client
        .enqueue_bulk()
        .add(client.enqueue(Alpha(json!({ "priority": "high", "region": "eu" }))))
        .add(client.enqueue(Alpha(json!({ "priority": "low", "region": "eu" }))))
        .add(client.enqueue(Alpha(json!({ "priority": "high", "region": "us" }))))
        .await
        .expect("enqueue_bulk");

    // Subset match via the generated jq expression — all high jobs.
    let high = client
        .list_jobs()
        .filter(jq_contains(&json!({ "priority": "high" })).expect("jq_contains"))
        .await
        .expect("list high");
    assert_eq!(high.jobs.len(), 2);

    // Exact match — one specific payload.
    let exact = client
        .list_jobs()
        .filter(jq_eq(&json!({ "priority": "high", "region": "eu" })).expect("jq_eq"))
        .await
        .expect("list exact");
    assert_eq!(exact.jobs.len(), 1);
    assert_eq!(
        exact.jobs[0].payload,
        Some(json!({ "priority": "high", "region": "eu" })),
    );

    // A hand-written jq expression works just the same.
    let raw = client
        .list_jobs()
        .filter(".region == \"us\"")
        .await
        .expect("list raw");
    assert_eq!(raw.jobs.len(), 1);
}

#[tokio::test]
async fn patch_a_job() {
    let client = fresh().await;

    let job = client
        .enqueue(Alpha(json!({})))
        .priority(100)
        .await
        .expect("enqueue");
    assert_eq!(job.priority, 100);

    let updated = client
        .patch_job(&job.id, JobPatch::new().priority(50))
        .await
        .expect("patch_job");
    assert_eq!(updated.priority, 50);

    let fetched = client.get_job(&job.id).await.expect("get_job");
    assert_eq!(fetched.priority, 50);
}

#[tokio::test]
async fn patch_all_jobs_by_filter() {
    let client = fresh().await;

    client
        .enqueue_bulk()
        .add(client.enqueue(Alpha(json!({}))).queue("q1").priority(100))
        .add(client.enqueue(Alpha(json!({}))).queue("q1").priority(100))
        .add(client.enqueue(Beta(json!({}))).queue("q2").priority(100))
        .await
        .expect("enqueue_bulk");

    let patched = client
        .patch_all_jobs()
        .queue(["q1"])
        .patch(JobPatch::new().priority(1))
        .await
        .expect("patch_all_jobs");
    assert_eq!(patched, 2);

    // The q1 jobs changed...
    let q1 = client.list_jobs().queue(["q1"]).await.expect("list q1");
    assert!(q1.jobs.iter().all(|j| j.priority == 1));

    // ...and q2 was left untouched.
    let q2 = client.list_jobs().queue(["q2"]).await.expect("list q2");
    assert!(q2.jobs.iter().all(|j| j.priority == 100));
}

#[tokio::test]
async fn delete_all_jobs_filtered_and_unfiltered() {
    let client = fresh().await;

    client
        .enqueue_bulk()
        .add(client.enqueue(Alpha(json!({}))).queue("q1"))
        .add(client.enqueue(Alpha(json!({}))).queue("q1"))
        .add(client.enqueue(Beta(json!({}))).queue("q2"))
        .await
        .expect("enqueue_bulk");

    // Filtered delete — only q1.
    let deleted = client.delete_all_jobs().queue(["q1"]).await.expect("delete q1");
    assert_eq!(deleted, 2);
    assert_eq!(client.count_jobs().await.expect("count"), 1);

    // Unfiltered delete — everything that remains.
    let deleted_all = client.delete_all_jobs().await.expect("delete all");
    assert_eq!(deleted_all, 1);
    assert_eq!(client.count_jobs().await.expect("count"), 0);
}

#[tokio::test]
async fn error_history() {
    let client = fresh().await;

    let job = client
        .enqueue(Alpha(json!({})))
        .queue("err-integration")
        .await
        .expect("enqueue");

    // Take the job so it's in-flight, then report it as failed.
    let mut stream = client
        .take()
        .queues(vec!["err-integration"])
        .await
        .expect("open take stream");
    let taken = stream
        .try_next()
        .await
        .expect("take stream")
        .expect("a job to take");
    assert_eq!(taken.id, job.id);

    client
        .report_failure(&job.id, "boom")
        .await
        .expect("report_failure");
    drop(stream);

    // The failure now shows up in the job's error history.
    let page = client.list_errors(&job.id).await.expect("list_errors");
    assert_eq!(page.errors.len(), 1);
    assert_eq!(page.errors[0].attempt, 1);
    assert_eq!(page.errors[0].message, "boom");

    let record = client.get_error(&job.id, 1).await.expect("get_error");
    assert_eq!(record.attempt, 1);
    assert_eq!(record.message, "boom");
}

#[tokio::test]
async fn cron_schedule_lifecycle() {
    let client = fresh().await;

    // Cron is a Pro-licensed feature — on a server without a Pro
    // license `replace_cron` answers 403, in which case we skip the
    // rest of the scenario (mirroring the Node/Ruby suites).
    let group = match client
        .replace_cron("integration-cron")
        .entry(CronEntry::new("a", "* * * * *", client.enqueue(Alpha(json!({})))))
        .entry(CronEntry::new("b", "*/5 * * * *", client.enqueue(Beta(json!({})))))
        .await
    {
        Ok(group) => group,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("replace_cron failed: {e:?}"),
    };
    assert_eq!(group.entries.len(), 2);

    // Re-fetch and confirm both entries are present.
    let fetched = client.get_cron("integration-cron").await.expect("get_cron");
    let mut names: Vec<&str> = fetched.entries.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["a", "b"]);

    // Pause then resume a single entry.
    client
        .pause_cron_entry("integration-cron", "a")
        .await
        .expect("pause entry");
    assert!(
        client
            .get_cron_entry("integration-cron", "a")
            .await
            .expect("get entry")
            .paused
    );
    client
        .resume_cron_entry("integration-cron", "a")
        .await
        .expect("resume entry");
    assert!(
        !client
            .get_cron_entry("integration-cron", "a")
            .await
            .expect("get entry")
            .paused
    );

    // The group shows up in the listing.
    assert!(client
        .list_crons()
        .await
        .expect("list_crons")
        .iter()
        .any(|g| g == "integration-cron"));

    // Clean up after ourselves.
    client
        .delete_cron("integration-cron")
        .await
        .expect("delete_cron");
}

#[tokio::test]
async fn delete_all_crons_wipes_every_group() {
    let client = fresh().await;

    // Create two cron groups (Pro-only feature — skip on free tier).
    for name in ["wipe-a", "wipe-b"] {
        match client
            .replace_cron(name)
            .entry(CronEntry::new("e", "* * * * *", client.enqueue(Alpha(json!({})))))
            .await
        {
            Ok(_) => {}
            Err(ZizqError::Response { status: 403, .. }) => return,
            Err(e) => panic!("replace_cron({name}) failed: {e:?}"),
        }
    }

    let deleted = client
        .delete_all_crons()
        .await
        .expect("delete_all_crons");
    assert_eq!(deleted, 2);

    let remaining = client.list_crons().await.expect("list_crons");
    assert!(remaining.is_empty(), "expected no cron groups after wipe");
}
