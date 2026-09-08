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
    jq_contains, jq_eq, BatchConfig, BudgetBindingInput, BudgetPatch, BudgetPolicy, BudgetStrategy,
    Client, CronEntry, JobKind, JobPatch, Router, UniqueKey, Worker, ZizqError,
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
job_kind!(AuditEvents, "audit.events");
job_kind!(Push, "push");
job_kind!(BatchedWorker, "batched_worker");

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

/// The group's timezone is the group's, not a copy smeared over every
/// entry, so a re-fetch still reports it and entries that set their own
/// keep it.
#[tokio::test]
async fn cron_group_timezone_round_trips() {
    let client = fresh().await;

    let group = match client
        .replace_cron("integration-cron")
        .timezone("Australia/Melbourne")
        .entry(CronEntry::new(
            "inherits",
            "0 9 * * *",
            client.enqueue(Alpha(json!({}))),
        ))
        .entry(
            CronEntry::new("scoped", "0 9 * * *", client.enqueue(Beta(json!({}))))
                .timezone("UTC"),
        )
        .await
    {
        Ok(group) => group,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("replace_cron failed: {e:?}"),
    };
    assert_eq!(group.timezone.as_deref(), Some("Australia/Melbourne"));

    let fetched = client.get_cron("integration-cron").await.expect("get_cron");
    assert_eq!(fetched.timezone.as_deref(), Some("Australia/Melbourne"));

    let entry = |name: &str| {
        fetched
            .entries
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("entry {name} missing"))
    };
    assert_eq!(entry("inherits").timezone, None);
    assert_eq!(entry("scoped").timezone.as_deref(), Some("UTC"));

    // And the group's timezone is what the inheriting entry actually runs
    // in: 9am in Melbourne is not 9am in UTC.
    assert_ne!(
        entry("inherits").next_enqueue_at,
        entry("scoped").next_enqueue_at
    );

    // A replace is a full replace, so a timezone left out goes.
    client
        .replace_cron("integration-cron")
        .entry(CronEntry::new(
            "inherits",
            "0 9 * * *",
            client.enqueue(Alpha(json!({}))),
        ))
        .await
        .expect("replace_cron without timezone");
    assert_eq!(
        client
            .get_cron("integration-cron")
            .await
            .expect("get_cron")
            .timezone,
        None
    );

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

// --- Batched jobs (Pro) ---
//
// Every batched enqueue is gated behind a Pro license on the server —
// on a free-tier server the enqueue returns 403, in which case we skip
// the rest of the scenario (mirroring the Node/Ruby suites).

#[tokio::test]
async fn batched_second_enqueue_folds_into_first_and_merges_payload() {
    let client = fresh().await;

    let r1 = match client
        .enqueue(AuditEvents(json!([{ "id": 1 }])))
        .queue("batched-integration")
        .batch(BatchConfig::at(".", 100).keyed_by("audit"))
        .await
    {
        Ok(job) => job,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("first enqueue failed: {e:?}"),
    };

    let r2 = client
        .enqueue(AuditEvents(json!([{ "id": 2 }, { "id": 3 }])))
        .queue("batched-integration")
        .batch(BatchConfig::at(".", 100).keyed_by("audit"))
        .await
        .expect("second enqueue");

    assert_eq!(r1.folded, Some(false));
    assert_eq!(r2.folded, Some(true));
    assert_eq!(r2.id, r1.id, "fold reuses the batch's job id");

    let fetched = client.get_job(&r1.id).await.expect("get_job");
    assert_eq!(
        fetched.payload,
        Some(json!([{ "id": 1 }, { "id": 2 }, { "id": 3 }])),
    );
    assert!(
        fetched.batch.is_some(),
        "batch config is visible on job reads",
    );
}

#[tokio::test]
async fn batched_different_batch_keys_do_not_fold() {
    let client = fresh().await;

    let r1 = match client
        .enqueue(Push(json!({ "deviceIds": ["a"], "platform": "apple" })))
        .queue("batched-integration")
        .batch(BatchConfig::at(".deviceIds", 100).keyed_by("push:apple"))
        .await
    {
        Ok(job) => job,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("first enqueue failed: {e:?}"),
    };

    let r2 = client
        .enqueue(Push(json!({ "deviceIds": ["b"], "platform": "android" })))
        .queue("batched-integration")
        .batch(BatchConfig::at(".deviceIds", 100).keyed_by("push:android"))
        .await
        .expect("second enqueue");

    assert_eq!(r1.folded, Some(false));
    assert_eq!(r2.folded, Some(false));
    assert_ne!(r1.id, r2.id);
}

#[tokio::test]
async fn batched_bulk_intra_fold_within_one_call() {
    let client = fresh().await;

    let bulk = client
        .enqueue_bulk()
        .add(
            client
                .enqueue(AuditEvents(json!([{ "id": 1 }])))
                .queue("batched-integration")
                .batch(BatchConfig::at(".", 100).keyed_by("audit")),
        )
        .add(
            client
                .enqueue(AuditEvents(json!([{ "id": 2 }])))
                .queue("batched-integration")
                .batch(BatchConfig::at(".", 100).keyed_by("audit")),
        )
        .add(
            client
                .enqueue(AuditEvents(json!([{ "id": 3 }])))
                .queue("batched-integration")
                .batch(BatchConfig::at(".", 100).keyed_by("audit")),
        )
        .await;

    let results = match bulk {
        Ok(jobs) => jobs,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("bulk enqueue failed: {e:?}"),
    };

    assert_eq!(results[0].folded, Some(false));
    assert_eq!(results[1].folded, Some(true));
    assert_eq!(results[2].folded, Some(true));
    assert_eq!(results[1].id, results[0].id);
    assert_eq!(results[2].id, results[0].id);

    let fetched = client.get_job(&results[0].id).await.expect("get_job");
    assert_eq!(
        fetched.payload,
        Some(json!([{ "id": 1 }, { "id": 2 }, { "id": 3 }])),
    );
}

#[tokio::test]
async fn batched_dedup_collapses_overlapping_items() {
    let client = fresh().await;

    let first = client
        .enqueue(AuditEvents(json!([{ "id": 1 }, { "id": 2 }])))
        .queue("batched-integration")
        .batch(BatchConfig::at(".", 100).dedup().keyed_by("audit"))
        .await;

    if let Err(ZizqError::Response { status: 403, .. }) = first {
        return;
    }
    first.expect("first enqueue");

    let r = client
        .enqueue(AuditEvents(json!([{ "id": 2 }, { "id": 3 }])))
        .queue("batched-integration")
        .batch(BatchConfig::at(".", 100).dedup().keyed_by("audit"))
        .await
        .expect("second enqueue");
    assert_eq!(r.folded, Some(true));

    let fetched = client.get_job(&r.id).await.expect("get_job");
    // `unique` in jq sorts as a side effect; assert on the sorted id set.
    let mut ids: Vec<i64> = fetched
        .payload
        .expect("payload")
        .as_array()
        .expect("array")
        .iter()
        .map(|v| v["id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3]);
}

#[tokio::test]
async fn batched_worker_receives_the_merged_payload() {
    let client = fresh().await;

    let first = client
        .enqueue(BatchedWorker(json!([{ "id": 1 }])))
        .queue("batched-worker-integration")
        .batch(BatchConfig::at(".", 100).keyed_by("batched-worker"))
        .await;

    if let Err(ZizqError::Response { status: 403, .. }) = first {
        return;
    }
    first.expect("first enqueue");

    for id in [2, 3] {
        client
            .enqueue(BatchedWorker(json!([{ "id": id }])))
            .queue("batched-worker-integration")
            .batch(BatchConfig::at(".", 100).keyed_by("batched-worker"))
            .await
            .expect("subsequent enqueue");
    }

    let received: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(Notify::new());

    let worker = Worker::builder()
        .client(client.clone())
        .concurrency(1)
        .queues(vec!["batched-worker-integration"])
        .handler(Router::new().route({
            let received = received.clone();
            let shutdown = shutdown.clone();
            move |job: BatchedWorker| {
                let received = received.clone();
                let shutdown = shutdown.clone();
                async move {
                    *received.lock().unwrap() = Some(job.0);
                    shutdown.notify_one();
                    Ok::<(), Infallible>(())
                }
            }
        }))
        .build()
        .expect("build worker");

    tokio::time::timeout(
        Duration::from_secs(10),
        worker.run(async move { shutdown.notified().await }),
    )
    .await
    .expect("worker run timed out")
    .expect("worker run");

    let payload = received.lock().unwrap().clone().expect("received payload");
    assert_eq!(payload, json!([{ "id": 1 }, { "id": 2 }, { "id": 3 }]));
}

#[tokio::test]
async fn batched_key_derived_from_payload_folds_together() {
    // Rust analog of the Node "function-valued key" test — the user
    // computes a batch key from payload data (via `UniqueKey::tagged_hash_of`
    // in this case) and passes it as a plain string.
    let client = fresh().await;

    let tenant_key = UniqueKey::tagged_hash_of("push", &42u64).key;

    let r1 = match client
        .enqueue(Push(json!({ "deviceIds": ["a"], "tenantId": 42 })))
        .queue("batched-integration")
        .batch(BatchConfig::at(".deviceIds", 100).keyed_by(tenant_key.clone()))
        .await
    {
        Ok(job) => job,
        Err(ZizqError::Response { status: 403, .. }) => return,
        Err(e) => panic!("first enqueue failed: {e:?}"),
    };

    let r2 = client
        .enqueue(Push(json!({ "deviceIds": ["b"], "tenantId": 42 })))
        .queue("batched-integration")
        .batch(BatchConfig::at(".deviceIds", 100).keyed_by(tenant_key.clone()))
        .await
        .expect("second enqueue");

    assert_eq!(r1.folded, Some(false));
    assert_eq!(r2.folded, Some(true));
    let batch = r1.batch.expect("batch config returned on enqueue");
    assert_eq!(batch.key, tenant_key);
}

#[tokio::test]
async fn batched_unique_key_plus_batch_is_rejected_with_400() {
    let client = fresh().await;

    let err = client
        .enqueue(Push(json!([{ "id": 1 }])))
        .queue("batched-integration")
        .unique_key(UniqueKey::raw("some-key"))
        .batch(BatchConfig::at(".", 100).keyed_by("push"))
        .await
        .expect_err("expected server to reject unique_key + batch");

    match err {
        ZizqError::Response { status: 403, .. } => {} // Pro not enabled — skip
        ZizqError::Response { status: 400, .. } => {} // expected outcome
        other => panic!("expected 400 or 403 Response, got {other:?}"),
    }
}

#[tokio::test]
async fn batched_invalid_jq_expression_is_rejected_with_422() {
    let client = fresh().await;

    let err = client
        .enqueue(Push(json!([{ "id": 1 }])))
        .queue("batched-integration")
        .batch(BatchConfig {
            key: "bad-expr".into(),
            when: ".[*]".into(), // syntactically invalid
            fold: "$existing + $new".into(),
        })
        .await
        .expect_err("expected server to reject the invalid expression");

    match err {
        ZizqError::Response { status: 403, .. } => {} // Pro not enabled — skip
        ZizqError::Response { status: 422, .. } => {} // expected outcome
        other => panic!("expected 422 or 403 Response, got {other:?}"),
    }
}

// --- Derive (`#[derive(JobKind)]`) ---
//
// These scenarios prove the derive-generated impl works end-to-end
// against a real server — the same way a downstream user would define
// their jobs. The manual `job_kind!` variants above cover the same
// shapes with hand-written `impl JobKind`; these add derive coverage
// on top.

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "derive.basic", queue = "integration", priority = 42)]
struct DerivedBasic {
    body: String,
}

#[tokio::test]
async fn derive_basic_round_trip() {
    let client = fresh().await;

    let job = client
        .enqueue(DerivedBasic {
            body: "hello".into(),
        })
        .await
        .expect("enqueue");

    assert_eq!(job.job_type, "derive.basic");
    assert_eq!(job.queue, "integration");
    assert_eq!(job.priority, 42);

    let fetched = client.get_job(&job.id).await.expect("get_job");
    assert_eq!(fetched.payload, Some(json!({ "body": "hello" })));
}

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(name = "derive.unique", queue = "integration", unique(only = [".user_id"]))]
struct DerivedUnique {
    user_id: u64,
    body: String,
}

#[tokio::test]
async fn derive_unique_key_dedups_by_only_fields() {
    let client = fresh().await;

    let first = match client
        .enqueue(DerivedUnique {
            user_id: 42,
            body: "hello".into(),
        })
        .await
    {
        Ok(job) => job,
        Err(ZizqError::Response { status: 403, .. }) => return, // Pro-only
        Err(e) => panic!("first enqueue failed: {e:?}"),
    };
    assert_eq!(first.duplicate, Some(false));

    // Same user_id, different body → derive's unique key sees only
    // user_id, so the second enqueue is rejected as a duplicate.
    let second = client
        .enqueue(DerivedUnique {
            user_id: 42,
            body: "goodbye".into(),
        })
        .await
        .expect("second enqueue");
    assert_eq!(second.duplicate, Some(true));
    assert_eq!(second.id, first.id);
}

#[derive(Serialize, Deserialize, JobKind)]
#[zizq(
    name = "derive.batch",
    queue = "integration",
    batch(path = ".events", limit = 100, key(only = [".tenant_id"]))
)]
struct DerivedBatch {
    tenant_id: u64,
    events: Vec<serde_json::Value>,
}

#[tokio::test]
async fn derive_batch_folds_by_only_fields() {
    let client = fresh().await;

    let first = match client
        .enqueue(DerivedBatch {
            tenant_id: 7,
            events: vec![json!({ "id": 1 })],
        })
        .await
    {
        Ok(job) => job,
        Err(ZizqError::Response { status: 403, .. }) => return, // Pro-only
        Err(e) => panic!("first enqueue failed: {e:?}"),
    };
    assert_eq!(first.folded, Some(false));

    // Second enqueue with the same tenant folds into the first.
    let second = client
        .enqueue(DerivedBatch {
            tenant_id: 7,
            events: vec![json!({ "id": 2 }), json!({ "id": 3 })],
        })
        .await
        .expect("second enqueue");
    assert_eq!(second.folded, Some(true));
    assert_eq!(second.id, first.id);

    let fetched = client.get_job(&first.id).await.expect("get_job");
    assert_eq!(
        fetched.payload,
        Some(json!({
            "tenant_id": 7,
            "events": [{ "id": 1 }, { "id": 2 }, { "id": 3 }],
        })),
    );
}

// --- Budgets (Pro) ---
//
// Every budget call is gated behind a Pro license on the server — on a
// free-tier server it returns 403, in which case we skip the rest of
// the scenario (mirroring the batched-job suites above).

job_kind!(Throttled, "throttled");

/// A job type declaring its budget as a per-type default, including
/// the policy to create it with. Exercises `#[zizq(budget(...))]` and
/// `JobKind::BUDGETS` against a real server.
#[derive(Serialize, Deserialize, JobKind)]
#[zizq(
    name = "declared_budget",
    queue = "integration",
    budget(key = "declared", cost = 2, create_with(allocation = 10, while_in_flight))
)]
struct DeclaredBudget {
    index: u64,
}

/// Create a budget, returning `None` when the server has no Pro
/// license so the caller can skip.
async fn budget_or_skip(
    client: &Client,
    key: &str,
    policy: BudgetPolicy,
) -> Option<zizq::Budget> {
    match client.create_budget(key, policy).await {
        Ok(budget) => Some(budget),
        Err(e) if e.is_forbidden() => None,
        Err(e) => panic!("create_budget failed: {e:?}"),
    }
}

#[tokio::test]
async fn budget_crud_round_trip() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::time_based(Duration::from_secs(60)));
    let Some(created) = budget_or_skip(&client, "crud", policy).await else {
        return;
    };

    assert_eq!(created.key, "crud");
    assert_eq!(created.allocation, 100);
    assert_eq!(
        created.strategy,
        BudgetStrategy::time_based(Duration::from_secs(60))
    );

    let fetched = client.get_budget("crud").await.expect("get_budget");
    assert_eq!(fetched, created);

    let listed = client.list_budgets().await.expect("list_budgets");
    assert!(listed.iter().any(|b| b.key == "crud"));

    // A merge patch touches one field within the strategy and leaves
    // the period alone.
    let patched = client
        .update_budget("crud", BudgetPatch::new().burst(5))
        .await
        .expect("update_budget");
    assert_eq!(
        patched.strategy,
        BudgetStrategy::TimeBased {
            duration: Duration::from_secs(60),
            burst: Some(5),
        }
    );

    // `burst` is the one field with a meaningful null.
    let cleared = client
        .update_budget("crud", BudgetPatch::new().clear_burst())
        .await
        .expect("clear burst");
    assert_eq!(
        cleared.strategy,
        BudgetStrategy::time_based(Duration::from_secs(60))
    );

    // A replace changes the policy, not the budget's identity.
    let replaced = client
        .put_budget("crud", BudgetPolicy::new(5, BudgetStrategy::WhileInFlight))
        .await
        .expect("put_budget");
    assert_eq!(replaced.strategy, BudgetStrategy::WhileInFlight);
    assert_eq!(replaced.allocation, 5);
    assert_eq!(
        replaced.created_at, created.created_at,
        "a replace must not re-create the budget"
    );

    client.delete_budget("crud").await.expect("delete_budget");

    let err = client.get_budget("crud").await.unwrap_err();
    assert!(err.is_not_found(), "unexpected error: {err}");
}

// The behaviour that lets every instance of an application declare its
// budgets on boot without coordinating.
#[tokio::test]
async fn creating_an_existing_budget_conflicts_and_leaves_it_alone() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "declare-race", policy).await else {
        return;
    };

    let err = client
        .create_budget(
            "declare-race",
            BudgetPolicy::new(1, BudgetStrategy::WhileInFlight),
        )
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "unexpected error: {err}");

    let stored = client.get_budget("declare-race").await.expect("get_budget");
    assert_eq!(
        stored.allocation, 100,
        "the losing declaration must not retune the budget"
    );
}

#[tokio::test]
async fn enqueue_binds_a_job_and_reports_it_back() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "bound", policy).await else {
        return;
    };

    let job = client
        .enqueue(Throttled(json!({ "n": 1 })))
        .budget(BudgetBindingInput::new("bound").cost(2))
        .await
        .expect("enqueue");

    assert_eq!(job.budgets.len(), 1);
    assert_eq!(job.budgets[0].key, "bound");
    assert_eq!(job.budgets[0].cost, 2);

    // And it survives a round trip through a read.
    let fetched = client.get_job(&job.id).await.expect("get_job");
    assert_eq!(fetched.budgets, job.budgets);
}

// One call creates the budget and the job together, so an application
// never needs a separate startup step.
#[tokio::test]
async fn a_binding_creates_its_budget_atomically() {
    let client = fresh().await;

    let job = match client
        .enqueue(Throttled(json!({ "n": 1 })))
        .budget(BudgetBindingInput::new("made-on-demand").create_with(BudgetPolicy::new(
            7,
            BudgetStrategy::WhileInFlight,
        )))
        .await
    {
        Ok(job) => job,
        Err(e) if e.is_forbidden() => return,
        Err(e) => panic!("enqueue failed: {e:?}"),
    };

    assert_eq!(job.budgets[0].key, "made-on-demand");

    let budget = client
        .get_budget("made-on-demand")
        .await
        .expect("budget was created by the enqueue");
    assert_eq!(budget.allocation, 7);
    assert_eq!(budget.strategy, BudgetStrategy::WhileInFlight);
}

// An existing budget's policy stays authoritative, so an enqueue can
// never quietly retune one.
#[tokio::test]
async fn create_with_is_ignored_when_the_budget_exists() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "already-there", policy).await else {
        return;
    };

    client
        .enqueue(Throttled(json!({ "n": 1 })))
        .budget(
            BudgetBindingInput::new("already-there")
                .create_with(BudgetPolicy::new(1, BudgetStrategy::WhileInFlight)),
        )
        .await
        .expect("enqueue");

    let stored = client.get_budget("already-there").await.expect("get_budget");
    assert_eq!(stored.allocation, 100);
}

#[tokio::test]
async fn a_derived_budget_default_applies_without_a_call_site_binding() {
    let client = fresh().await;

    let job = match client.enqueue(DeclaredBudget { index: 1 }).await {
        Ok(job) => job,
        Err(e) if e.is_forbidden() => return,
        Err(e) => panic!("enqueue failed: {e:?}"),
    };

    assert_eq!(job.budgets.len(), 1);
    assert_eq!(job.budgets[0].key, "declared");
    assert_eq!(job.budgets[0].cost, 2);

    // The `create_with` in the attribute made the budget too.
    let budget = client.get_budget("declared").await.expect("get_budget");
    assert_eq!(budget.allocation, 10);

    // And the call site can opt out of it entirely.
    let unthrottled = client
        .enqueue(DeclaredBudget { index: 2 })
        .clear_budgets()
        .await
        .expect("enqueue unthrottled");
    assert!(unthrottled.budgets.is_empty());
}

#[tokio::test]
async fn budgets_key_selects_what_is_bound() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "searchable", policy).await else {
        return;
    };

    client
        .enqueue(Throttled(json!({ "n": 1 })))
        .budget("searchable")
        .await
        .expect("bound enqueue");
    client
        .enqueue(Throttled(json!({ "n": 2 })))
        .await
        .expect("unbound enqueue");

    let count = client
        .count_jobs()
        .budgets_key(["searchable"])
        .await
        .expect("count_jobs");
    assert_eq!(count, 1);

    let page = client
        .list_jobs()
        .budgets_key(["searchable"])
        .await
        .expect("list_jobs");
    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].budgets[0].key, "searchable");
}

#[tokio::test]
async fn single_job_bindings_can_be_changed_after_enqueue() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "rebind-a", policy).await else {
        return;
    };
    client
        .create_budget(
            "rebind-b",
            BudgetPolicy::new(100, BudgetStrategy::WhileInFlight),
        )
        .await
        .expect("second budget");

    let job = client
        .enqueue(Throttled(json!({ "n": 1 })))
        .await
        .expect("enqueue");
    assert!(job.budgets.is_empty());

    let bound = client
        .bind_budget(&job.id, BudgetBindingInput::new("rebind-a").cost(2))
        .await
        .expect("bind_budget");
    assert_eq!(bound.budgets[0].cost, 2);

    // Binding the same budget twice is a conflict...
    let err = client
        .bind_budget(&job.id, "rebind-a")
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "unexpected error: {err}");

    // ...but rebinding replaces it.
    let rebound = client
        .rebind_budget(&job.id, BudgetBindingInput::new("rebind-a").cost(3))
        .await
        .expect("rebind_budget");
    assert_eq!(rebound.budgets[0].cost, 3);

    let recost = client
        .set_budget_cost(&job.id, "rebind-a", 5)
        .await
        .expect("set_budget_cost");
    assert_eq!(recost.budgets[0].cost, 5);

    let replaced = client
        .replace_budgets(
            &job.id,
            [
                BudgetBindingInput::new("rebind-a").cost(1),
                BudgetBindingInput::new("rebind-b").cost(4),
            ],
        )
        .await
        .expect("replace_budgets");
    let mut keys: Vec<_> = replaced.budgets.iter().map(|b| b.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["rebind-a", "rebind-b"]);

    let unbound = client
        .unbind_budget(&job.id, "rebind-a")
        .await
        .expect("unbind_budget");
    assert_eq!(unbound.budgets.len(), 1);
    assert_eq!(unbound.budgets[0].key, "rebind-b");

    let cleared = client
        .unbind_all_budgets(&job.id)
        .await
        .expect("unbind_all_budgets");
    assert!(cleared.budgets.is_empty());
}

// The sequence that drains a budget so it can be deleted — a budget
// cannot be removed while anything still draws on it.
#[tokio::test]
async fn bulk_rebinding_drains_a_budget_so_it_can_be_deleted() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "drain", policy).await else {
        return;
    };

    for n in 0..3 {
        client
            .enqueue(Throttled(json!({ "n": n })))
            .queue("drain-integration")
            .await
            .expect("enqueue");
    }

    let bound = client
        .bind_all_jobs_budget(BudgetBindingInput::new("drain").cost(2))
        .queue(["drain-integration"])
        .await
        .expect("bind_all_jobs_budget");
    assert_eq!(bound.changed, 3);
    assert!(bound.blocked.is_empty());

    // A bound budget cannot be deleted.
    let err = client.delete_budget("drain").await.unwrap_err();
    assert!(err.is_conflict(), "unexpected error: {err}");

    let recost = client
        .set_all_jobs_budget_cost("drain", 4)
        .budgets_key(["drain"])
        .await
        .expect("set_all_jobs_budget_cost");
    assert_eq!(recost.changed, 3);

    let unbound = client
        .unbind_all_jobs_budget("drain")
        .budgets_key(["drain"])
        .await
        .expect("unbind_all_jobs_budget");
    assert_eq!(unbound.changed, 3);

    assert_eq!(
        client
            .count_jobs()
            .budgets_key(["drain"])
            .await
            .expect("count_jobs"),
        0
    );

    // Now that nothing draws on it, it goes.
    client.delete_budget("drain").await.expect("delete_budget");
}

#[tokio::test]
async fn clearing_every_binding_leaves_jobs_unthrottled() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "clear-me", policy).await else {
        return;
    };

    for n in 0..2 {
        client
            .enqueue(Throttled(json!({ "n": n })))
            .queue("clear-integration")
            .budget("clear-me")
            .await
            .expect("enqueue");
    }

    let change = client
        .clear_all_jobs_budgets()
        .queue(["clear-integration"])
        .await
        .expect("clear_all_jobs_budgets");
    assert_eq!(change.changed, 2);

    assert_eq!(
        client
            .count_jobs()
            .budgets_key(["clear-me"])
            .await
            .expect("count_jobs"),
        0
    );
}

// The point of the whole feature: the server refuses to dispatch more
// than the allocation permits, and the worker never learns that it
// waited. With an allocation of 1 and a cost of 1, no two of these can
// be in flight at once however much concurrency the worker offers.
#[tokio::test]
async fn a_while_in_flight_budget_caps_concurrency_server_side() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(1, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "one-at-a-time", policy).await else {
        return;
    };

    let count: u64 = 4;
    for n in 0..count {
        client
            .enqueue(Throttled(json!({ "n": n })))
            .queue("throttle-integration")
            .budget("one-at-a-time")
            .await
            .expect("enqueue");
    }

    let in_flight = Arc::new(Mutex::new(0u32));
    let peak = Arc::new(Mutex::new(0u32));
    let done = Arc::new(Mutex::new(0u64));
    let shutdown = Arc::new(Notify::new());

    let worker = Worker::builder()
        .client(client.clone())
        .concurrency(4)
        .queues(vec!["throttle-integration"])
        .handler(Router::new().route({
            let in_flight = in_flight.clone();
            let peak = peak.clone();
            let done = done.clone();
            let shutdown = shutdown.clone();
            move |_job: Throttled| {
                let in_flight = in_flight.clone();
                let peak = peak.clone();
                let done = done.clone();
                let shutdown = shutdown.clone();
                async move {
                    {
                        let mut current = in_flight.lock().unwrap();
                        *current += 1;
                        let mut peak = peak.lock().unwrap();
                        *peak = (*peak).max(*current);
                    }

                    // Hold the token long enough that a server ignoring
                    // the budget would hand out the rest of the batch.
                    tokio::time::sleep(Duration::from_millis(300)).await;

                    *in_flight.lock().unwrap() -= 1;

                    let mut done = done.lock().unwrap();
                    *done += 1;
                    if *done == count {
                        shutdown.notify_one();
                    }
                    Ok::<(), Infallible>(())
                }
            }
        }))
        .build()
        .expect("build worker");

    tokio::time::timeout(
        Duration::from_secs(30),
        worker.run(async move { shutdown.notified().await }),
    )
    .await
    .expect("worker run timed out")
    .expect("worker run");

    assert_eq!(*done.lock().unwrap(), count, "every job should still run");
    assert_eq!(
        *peak.lock().unwrap(),
        1,
        "the budget allows only one in flight at a time",
    );
}

// Tokens are debited before dispatch, so a budget with none to give
// parks its jobs indefinitely rather than handing them to a worker.
#[tokio::test]
async fn an_exhausted_budget_parks_jobs_rather_than_dispatching_them() {
    let client = fresh().await;

    let policy = BudgetPolicy::new(1, BudgetStrategy::WhileInFlight);
    let Some(_) = budget_or_skip(&client, "parked", policy).await else {
        return;
    };

    // Cost 2 against an allocation of 1 would strand the job forever,
    // so the server refuses the binding outright.
    let err = client
        .enqueue(Throttled(json!({ "n": 1 })))
        .queue("parked-integration")
        .budget(BudgetBindingInput::new("parked").cost(2))
        .await
        .unwrap_err();
    assert!(
        err.is_invalid_request(),
        "a cost larger than the capacity must be refused: {err}"
    );
}
