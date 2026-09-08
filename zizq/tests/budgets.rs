// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

mod common;

use std::time::Duration;

use common::MockServer;
use serde::{Deserialize, Serialize};
use serde_json::json;
use zizq::{
    BudgetBinding, BudgetBindingInput, BudgetPatch, BudgetPolicy, BudgetStrategy, Client,
    CronEntry, Format, JobKind,
};

fn json_client(url: &str) -> Client {
    Client::builder()
        .url(url)
        .format(Format::Json)
        .build()
        .unwrap()
}

fn budget_json(key: &str, strategy: serde_json::Value) -> serde_json::Value {
    json!({
        "key": key,
        "allocation": 100,
        "strategy": strategy,
        "created_at": 1_700_000_000_000u64,
        "updated_at": 1_700_000_001_000u64,
    })
}

fn time_based() -> serde_json::Value {
    json!({ "type": "time_based", "duration_ms": 3_600_000 })
}

async fn body_of(server: &MockServer) -> serde_json::Value {
    serde_json::from_slice(&server.last_request().await.body).unwrap()
}

#[tokio::test]
async fn list_budgets_unwraps_the_envelope() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            json!({
                "budgets": [
                    budget_json("emails", time_based()),
                    budget_json("stripe", json!({ "type": "while_in_flight" })),
                ],
            }),
        )
        .await;

    let budgets = json_client(&server.url).list_budgets().await.unwrap();

    assert_eq!(budgets.len(), 2);
    assert_eq!(budgets[0].key, "emails");
    assert_eq!(
        budgets[0].strategy,
        BudgetStrategy::time_based(Duration::from_secs(3600))
    );
    assert_eq!(budgets[1].strategy, BudgetStrategy::WhileInFlight);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/budgets");
}

#[tokio::test]
async fn get_budget_decodes_the_policy_and_timestamps() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            budget_json(
                "emails",
                json!({ "type": "time_based", "duration_ms": 60_000, "burst": 5 }),
            ),
        )
        .await;

    let budget = json_client(&server.url).get_budget("emails").await.unwrap();

    assert_eq!(budget.key, "emails");
    assert_eq!(budget.allocation, 100);
    assert_eq!(
        budget.strategy,
        BudgetStrategy::TimeBased {
            duration: Duration::from_secs(60),
            burst: Some(5),
        }
    );
    assert_eq!(budget.created_at, 1_700_000_000_000);
    assert_eq!(budget.updated_at, 1_700_000_001_000);

    let req = server.last_request().await;
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/budgets/emails");
}

#[tokio::test]
async fn create_budget_posts_the_policy() {
    let server = MockServer::start().await;
    server
        .set_response_json(201, budget_json("emails", time_based()))
        .await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::time_based(Duration::from_secs(3600)));
    let budget = json_client(&server.url)
        .create_budget("emails", policy)
        .await
        .unwrap();

    assert_eq!(budget.key, "emails");

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/budgets/emails");
    assert_eq!(
        body_of(&server).await,
        json!({
            "allocation": 100,
            "strategy": { "type": "time_based", "duration_ms": 3_600_000 },
        })
    );
}

// The key lives in the path, so a caller declaring budgets on boot can
// treat this as success without unpacking the body.
#[tokio::test]
async fn create_budget_surfaces_an_existing_key_as_a_conflict() {
    let server = MockServer::start().await;
    server
        .set_response_json(409, json!({ "error": "budget 'emails' already exists" }))
        .await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::WhileInFlight);
    let err = json_client(&server.url)
        .create_budget("emails", policy)
        .await
        .unwrap_err();

    assert!(err.is_conflict());
    assert!(!err.is_retryable());
    assert_eq!(err.status(), Some(409));
    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn budgets_without_a_licence_surface_as_forbidden() {
    let server = MockServer::start().await;
    server
        .set_response_json(403, json!({ "error": "budgets require a Pro license" }))
        .await;

    let err = json_client(&server.url).list_budgets().await.unwrap_err();

    assert!(err.is_forbidden());
    assert!(!err.is_retryable());
    assert!(err.to_string().contains("Pro license"));
}

#[tokio::test]
async fn put_budget_replaces_the_policy() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            budget_json("stripe", json!({ "type": "while_in_flight" })),
        )
        .await;

    let policy = BudgetPolicy::new(3, BudgetStrategy::WhileInFlight);
    json_client(&server.url)
        .put_budget("stripe", policy)
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/budgets/stripe");
    assert_eq!(
        body_of(&server).await,
        json!({ "allocation": 3, "strategy": { "type": "while_in_flight" } })
    );
}

#[tokio::test]
async fn delete_budget_sends_no_body_and_accepts_no_content() {
    let server = MockServer::start().await;
    server
        .set_response_raw(204, "application/json", vec![])
        .await;

    json_client(&server.url)
        .delete_budget("emails")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/budgets/emails");
    assert!(req.body.is_empty());
}

// A budget cannot go while anything still draws on it, and the
// server calls that a conflict rather than a bad request.
#[tokio::test]
async fn delete_budget_surfaces_a_bound_job_as_a_conflict() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            409,
            json!({
                "error": "budget 'emails' is referenced by 3 unfinished jobs. Delete them or \
                          wait for them to finish before deleting it."
            }),
        )
        .await;

    let err = json_client(&server.url)
        .delete_budget("emails")
        .await
        .unwrap_err();

    assert!(err.is_conflict());
    assert!(!err.is_retryable());
}

// Merge patch: the fields no setter touched must be absent, not null,
// or the server reads them as a request to clear.
#[tokio::test]
async fn update_budget_sends_only_what_was_set() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, budget_json("emails", time_based()))
        .await;

    json_client(&server.url)
        .update_budget("emails", BudgetPatch::new().burst(500))
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/budgets/emails");
    assert_eq!(
        body_of(&server).await,
        json!({ "strategy": { "burst": 500 } })
    );
}

#[tokio::test]
async fn an_empty_patch_asks_for_nothing() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, budget_json("emails", time_based()))
        .await;

    json_client(&server.url)
        .update_budget("emails", BudgetPatch::new())
        .await
        .unwrap();

    assert_eq!(body_of(&server).await, json!({}));
}

// `burst` is the one field with a meaningful null — it clears the
// ceiling back to the allocation.
#[tokio::test]
async fn clearing_the_burst_sends_an_explicit_null() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, budget_json("emails", time_based()))
        .await;

    json_client(&server.url)
        .update_budget("emails", BudgetPatch::new().clear_burst())
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await,
        json!({ "strategy": { "burst": null } })
    );
}

#[tokio::test]
async fn patch_setters_accumulate_into_one_strategy() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, budget_json("emails", time_based()))
        .await;

    json_client(&server.url)
        .update_budget(
            "emails",
            BudgetPatch::new()
                .allocation(20_000)
                .duration(Duration::from_secs(60))
                .burst(5),
        )
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await,
        json!({
            "allocation": 20_000,
            "strategy": { "duration_ms": 60_000, "burst": 5 },
        })
    );
}

// Replacing the strategy states every field it implies, so the result
// is exactly what was asked for rather than a merge with what was
// there. A `TimeBased` with no burst therefore clears the stored one.
#[tokio::test]
async fn setting_a_time_based_strategy_clears_an_unstated_burst() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, budget_json("emails", time_based()))
        .await;

    json_client(&server.url)
        .update_budget(
            "emails",
            BudgetPatch::new().strategy(BudgetStrategy::time_based(Duration::from_secs(3600))),
        )
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await,
        json!({
            "strategy": { "type": "time_based", "duration_ms": 3_600_000, "burst": null },
        })
    );
}

// The server rejects either field alongside a switch to
// `while_in_flight` — it has no clock to set a period on and no drip
// to burst ahead of — and drops the stored ones itself.
#[tokio::test]
async fn switching_to_while_in_flight_sends_neither_duration_nor_burst() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            budget_json("emails", json!({ "type": "while_in_flight" })),
        )
        .await;

    json_client(&server.url)
        .update_budget(
            "emails",
            BudgetPatch::new().strategy(BudgetStrategy::WhileInFlight),
        )
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await,
        json!({ "strategy": { "type": "while_in_flight" } })
    );
}

#[tokio::test]
async fn a_budget_round_trips_through_messagepack() {
    let server = MockServer::start().await;
    server
        .set_response_msgpack(
            200,
            &json!({
                "budgets": [budget_json(
                    "emails",
                    json!({ "type": "time_based", "duration_ms": 60_000, "burst": 5 }),
                )],
            }),
        )
        .await;

    let client = Client::builder().url(&server.url).build().unwrap();
    let budgets = client.list_budgets().await.unwrap();

    assert_eq!(
        budgets[0].strategy,
        BudgetStrategy::TimeBased {
            duration: Duration::from_secs(60),
            burst: Some(5),
        }
    );
}

// --- Binding jobs to budgets ------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

impl JobKind for SendEmail {
    const NAME: &'static str = "send_email";
    const QUEUE: &'static str = "emails";
}

/// A type that declares its budgets as a per-type default, including
/// one that creates its budget on first use.
#[derive(Debug, Serialize, Deserialize)]
struct ChargeCard {
    invoice_id: String,
}

impl JobKind for ChargeCard {
    const NAME: &'static str = "charge_card";
    const QUEUE: &'static str = "billing";
    const BUDGETS: &'static [BudgetBindingInput] = &[
        BudgetBindingInput::new("stripe").cost(2),
        BudgetBindingInput::with_policy(
            "notifications",
            BudgetPolicy::new(3, BudgetStrategy::WhileInFlight),
        ),
    ];
}

fn job_response() -> serde_json::Value {
    json!({
        "id": "job-1",
        "type": "send_email",
        "queue": "emails",
        "status": "ready",
        "priority": 50,
        "ready_at": 0,
        "attempts": 0,
    })
}

#[tokio::test]
async fn an_unbudgeted_job_omits_the_field_entirely() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(SendEmail { to: "a@b".into() })
        .await
        .unwrap();

    assert_eq!(body_of(&server).await.get("budgets"), None);
}

#[tokio::test]
async fn budget_binds_by_key_alone() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(SendEmail { to: "a@b".into() })
        .budget("emails")
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{ "key": "emails" }])
    );
}

#[tokio::test]
async fn repeated_budget_calls_accumulate() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(SendEmail { to: "a@b".into() })
        .budget("emails")
        .budget(BudgetBindingInput::new("stripe").cost(2))
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{ "key": "emails" }, { "key": "stripe", "cost": 2 }])
    );
}

#[tokio::test]
async fn a_binding_can_create_its_budget_atomically() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    let policy = BudgetPolicy::new(100, BudgetStrategy::time_based(Duration::from_secs(60)));
    json_client(&server.url)
        .enqueue(SendEmail { to: "a@b".into() })
        .budget(
            BudgetBindingInput::new("emails")
                .cost(2)
                .create_with(policy),
        )
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{
            "key": "emails",
            "cost": 2,
            "create_with": {
                "allocation": 100,
                "strategy": { "type": "time_based", "duration_ms": 60_000 },
            },
        }])
    );
}

// A key that isn't known until runtime is the reason the binding's key
// is a `Cow` rather than a `&'static str`.
#[tokio::test]
async fn a_binding_accepts_a_key_computed_at_runtime() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    let tenant = format!("emails:{}", "acme");
    json_client(&server.url)
        .enqueue(SendEmail { to: "a@b".into() })
        .budget(tenant)
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{ "key": "emails:acme" }])
    );
}

#[tokio::test]
async fn the_type_default_applies_when_the_call_site_is_silent() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(ChargeCard {
            invoice_id: "inv-1".into(),
        })
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([
            { "key": "stripe", "cost": 2 },
            {
                "key": "notifications",
                "create_with": {
                    "allocation": 3,
                    "strategy": { "type": "while_in_flight" },
                },
            },
        ])
    );
}

// Every other per-type default is replaced rather than merged when the
// call site sets one, and budgets follow that rule.
#[tokio::test]
async fn a_call_site_budget_replaces_the_type_default() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(ChargeCard {
            invoice_id: "inv-1".into(),
        })
        .budget("urgent")
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{ "key": "urgent" }])
    );
}

#[tokio::test]
async fn budgets_sets_the_whole_list_at_once() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(ChargeCard {
            invoice_id: "inv-1".into(),
        })
        .budget("dropped")
        .budgets(["a", "b"])
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["budgets"],
        json!([{ "key": "a" }, { "key": "b" }])
    );
}

#[tokio::test]
async fn clear_budgets_enqueues_a_budgeted_type_unthrottled() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .enqueue(ChargeCard {
            invoice_id: "inv-1".into(),
        })
        .clear_budgets()
        .await
        .unwrap();

    assert_eq!(body_of(&server).await.get("budgets"), None);
}

#[tokio::test]
async fn a_job_reports_what_it_is_bound_to() {
    let server = MockServer::start().await;
    let mut response = job_response();
    response["budgets"] = json!([
        { "key": "emails", "cost": 2 },
        { "key": "stripe", "cost": 1 },
    ]);
    server.set_response_json(200, response).await;

    let job = json_client(&server.url).get_job("job-1").await.unwrap();

    assert_eq!(
        job.budgets,
        vec![
            BudgetBinding {
                key: "emails".into(),
                cost: 2
            },
            BudgetBinding {
                key: "stripe".into(),
                cost: 1
            },
        ]
    );
}

// A pre-0.7.0 server sends no `budgets` field at all.
#[tokio::test]
async fn a_job_from_an_older_server_reports_no_budgets() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    let job = json_client(&server.url).get_job("job-1").await.unwrap();

    assert!(job.budgets.is_empty());
}

// A cron entry's job is an enqueue input, so budgets ride along.
#[tokio::test]
async fn a_cron_entry_carries_its_jobs_budgets() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            json!({ "name": "billing", "paused": false, "entries": [] }),
        )
        .await;

    let client = json_client(&server.url);
    client
        .replace_cron("billing")
        .entry(CronEntry::new(
            "charge",
            "0 * * * *",
            client.enqueue(ChargeCard {
                invoice_id: "inv-1".into(),
            }),
        ))
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await["entries"][0]["job"]["budgets"],
        json!([
            { "key": "stripe", "cost": 2 },
            {
                "key": "notifications",
                "create_with": {
                    "allocation": 3,
                    "strategy": { "type": "while_in_flight" },
                },
            },
        ])
    );
}

// --- Rebinding an enqueued job ----------------------------------------------

#[tokio::test]
async fn bind_budget_puts_the_key_in_the_path_not_the_body() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .bind_budget("job-1", BudgetBindingInput::new("emails").cost(2))
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/jobs/job-1/budgets/emails");
    // The key is in the path; repeating it here would be a second,
    // contradictable source of truth.
    assert_eq!(body_of(&server).await, json!({ "cost": 2 }));
}

#[tokio::test]
async fn bind_budget_can_create_the_budget_atomically() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    let policy = BudgetPolicy::new(3, BudgetStrategy::WhileInFlight);
    json_client(&server.url)
        .bind_budget(
            "job-1",
            BudgetBindingInput::new("stripe").create_with(policy),
        )
        .await
        .unwrap();

    assert_eq!(
        body_of(&server).await,
        json!({
            "create_with": {
                "allocation": 3,
                "strategy": { "type": "while_in_flight" },
            },
        })
    );
}

#[tokio::test]
async fn binding_a_job_twice_is_a_conflict() {
    let server = MockServer::start().await;
    server
        .set_response_json(409, json!({ "error": "job already draws on 'emails'" }))
        .await;

    let err = json_client(&server.url)
        .bind_budget("job-1", "emails")
        .await
        .unwrap_err();

    assert!(err.is_conflict());
}

#[tokio::test]
async fn rebind_budget_replaces_the_binding_whole() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .rebind_budget("job-1", BudgetBindingInput::new("emails").cost(5))
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/jobs/job-1/budgets/emails");
    assert_eq!(body_of(&server).await, json!({ "cost": 5 }));
}

#[tokio::test]
async fn set_budget_cost_patches_only_the_cost() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .set_budget_cost("job-1", "emails", 7)
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/jobs/job-1/budgets/emails");
    assert_eq!(body_of(&server).await, json!({ "cost": 7 }));
}

#[tokio::test]
async fn unbind_budget_targets_one_binding() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .unbind_budget("job-1", "emails")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/jobs/job-1/budgets/emails");
    assert!(req.body.is_empty());
}

#[tokio::test]
async fn unbind_all_budgets_targets_the_collection() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .unbind_all_budgets("job-1")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/jobs/job-1/budgets");
}

// Here the key *is* carried per binding — there is no path segment to
// take it from, so dropping it would silently bind nothing.
#[tokio::test]
async fn replace_budgets_sends_each_key_in_the_body() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .replace_budgets(
            "job-1",
            [
                BudgetBindingInput::new("emails").cost(2),
                BudgetBindingInput::new("stripe"),
            ],
        )
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/jobs/job-1/budgets");
    assert_eq!(
        body_of(&server).await,
        json!({ "budgets": [{ "key": "emails", "cost": 2 }, { "key": "stripe" }] })
    );
}

#[tokio::test]
async fn replacing_with_nothing_sends_an_empty_list() {
    let server = MockServer::start().await;
    server.set_response_json(200, job_response()).await;

    json_client(&server.url)
        .replace_budgets("job-1", Vec::<BudgetBindingInput>::new())
        .await
        .unwrap();

    assert_eq!(body_of(&server).await, json!({ "budgets": [] }));
}

#[tokio::test]
async fn rebinding_an_in_flight_job_is_refused() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            422,
            json!({ "error": "job 'job-1' is InFlight — only queued jobs may have their budgets changed" }),
        )
        .await;

    let err = json_client(&server.url)
        .unbind_budget("job-1", "emails")
        .await
        .unwrap_err();

    assert!(err.is_invalid_request());
}

// --- Bulk rebinding ---------------------------------------------------------

fn change_json() -> serde_json::Value {
    json!({ "changed": 12, "blocked": ["01K9", "01KA"] })
}

#[tokio::test]
async fn bulk_bind_reports_what_changed_and_what_was_in_flight() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    let change = json_client(&server.url)
        .bind_all_jobs_budget(BudgetBindingInput::new("stripe").cost(2))
        .queue(["billing"])
        .await
        .unwrap();

    assert_eq!(change.changed, 12);
    assert_eq!(change.blocked, ["01K9", "01KA"]);

    let req = server.last_request().await;
    assert_eq!(req.method, "POST");
    assert!(req.path.starts_with("/jobs/budgets/stripe?"));
    assert!(req.path.contains("queue=billing"));
    assert_eq!(body_of(&server).await, json!({ "cost": 2 }));
}

#[tokio::test]
async fn bulk_rebind_uses_put() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    json_client(&server.url)
        .rebind_all_jobs_budget("stripe")
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PUT");
    assert_eq!(req.path, "/jobs/budgets/stripe");
}

#[tokio::test]
async fn bulk_set_cost_patches_the_named_budget() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    json_client(&server.url)
        .set_all_jobs_budget_cost("stripe", 4)
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "PATCH");
    assert_eq!(req.path, "/jobs/budgets/stripe");
    assert_eq!(body_of(&server).await, json!({ "cost": 4 }));
}

// The sequence that empties a budget so it can be deleted.
#[tokio::test]
async fn bulk_unbind_selects_by_the_budget_being_removed() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    json_client(&server.url)
        .unbind_all_jobs_budget("emails")
        .budgets_key(["emails"])
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert!(req.path.starts_with("/jobs/budgets/emails?"));
    assert!(req.path.contains("budgets.key=emails"));
}

#[tokio::test]
async fn bulk_clear_names_no_budget() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    json_client(&server.url)
        .clear_all_jobs_budgets()
        .queue(["emails"])
        .await
        .unwrap();

    let req = server.last_request().await;
    assert_eq!(req.method, "DELETE");
    assert!(req.path.starts_with("/jobs/budgets?"));
    assert!(req.path.contains("queue=emails"));
}

// An empty filter must not become a change-everything request.
#[tokio::test]
async fn an_empty_filter_changes_nothing_without_a_request() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    let change = json_client(&server.url)
        .clear_all_jobs_budgets()
        .queue(Vec::<String>::new())
        .await
        .unwrap();

    assert_eq!(change.changed, 0);
    assert!(change.blocked.is_empty());
    assert!(server.requests().await.is_empty());
}

#[tokio::test]
async fn budgets_key_filters_a_listing() {
    let server = MockServer::start().await;
    server
        .set_response_json(
            200,
            json!({
                "jobs": [],
                "pages": { "self": "/jobs", "next": null, "prev": null },
            }),
        )
        .await;

    json_client(&server.url)
        .list_jobs()
        .budgets_key(["emails", "stripe"])
        .await
        .unwrap();

    let req = server.last_request().await;
    assert!(req.path.starts_with("/jobs?"));
    assert!(req.path.contains("budgets.key=emails%2Cstripe"));
}

#[tokio::test]
async fn budgets_key_filters_a_count() {
    let server = MockServer::start().await;
    server.set_response_json(200, json!({ "count": 3 })).await;

    let count = json_client(&server.url)
        .count_jobs()
        .budgets_key(["emails"])
        .await
        .unwrap();

    assert_eq!(count, 3);
    assert!(server
        .last_request()
        .await
        .path
        .contains("budgets.key=emails"));
}

// An empty `budgets_key` is the likeliest empty filter here — it is
// what you get from `budgets_key(keys_to_drain)` when that list turns
// out to be empty. Treated as "no filter" it would unbind every job on
// the server.
#[tokio::test]
async fn an_empty_budgets_key_filter_changes_nothing() {
    let server = MockServer::start().await;
    server.set_response_json(200, change_json()).await;

    let change = json_client(&server.url)
        .clear_all_jobs_budgets()
        .budgets_key(Vec::<String>::new())
        .await
        .unwrap();

    assert_eq!(change.changed, 0);
    assert!(server.requests().await.is_empty());
}

#[tokio::test]
async fn an_empty_budgets_key_filter_deletes_nothing() {
    let server = MockServer::start().await;
    server
        .set_response_json(200, json!({ "deleted": 99 }))
        .await;

    let deleted = json_client(&server.url)
        .delete_all_jobs()
        .budgets_key(Vec::<String>::new())
        .await
        .unwrap();

    assert_eq!(deleted, 0);
    assert!(server.requests().await.is_empty());
}
