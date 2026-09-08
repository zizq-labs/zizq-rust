# Concurrency & Rate Limiting

> [!NOTE]
> This feature requires a Zizq [pro license](https://zizq.io/pricing) on the
> server.

Applications enqueue jobs to move expensive work off the request path, but some
of that work may put pressure on systems that cannot absorb it. An image
service may cap you at 10,000 requests an hour. A surge of push notifications
may dominate the queue and starve everything else of workers. Both are solved
by a feature Zizq calls **budgets**.

A budget is a named pool of _tokens_ managed under a _strategy_. Jobs bind to
one or more budgets, each with a _cost_ that defaults to `1`, and a job must
debit that cost from every budget it is bound to before it can be dispatched.

Crucially this happens on the server, before a worker ever sees the job.
Workers stay naive: they receive jobs and run them, with no waiting, no
sleeping, and no re-queueing something that arrived too early. A job at the
front of the queue that cannot yet debit its cost is _parked_ — it stays in the
queue and is dispatched the moment its budgets allow. Everything else keeps
flowing past it.

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BudgetPolicy, BudgetStrategy, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "charge_card", queue = "billing")]
> # struct ChargeCard { invoice_id: String }
> # async fn run(client: &Client, invoice_id: String) -> Result<(), zizq::ZizqError> {
> // At most 3 of these run at once. Typically done once at startup.
> client
>     .create_budget("stripe", BudgetPolicy::new(3, BudgetStrategy::WhileInFlight))
>     .await?;
>
> client
>     .enqueue(ChargeCard { invoice_id })
>     .budget("stripe")
>     .await?;
> # Ok(()) }
> ```

That is the whole integration. The handler that eventually runs the job knows
nothing about the limit.

> [!NOTE]
> Budgets are a shared resource, and the server caps how many distinct ones can
> exist — `8192` by default, configurable with `--max-budgets`
> (`$ZIZQ_MAX_BUDGETS`) when launching `zizq serve`. That is far more than most
> applications need. A future release will add sub-buckets for dynamically
> allocated scenarios.

## Strategies

Two strategies exist. Both take an `allocation`, the number of tokens in the
pool. A job may bind to several budgets freely mixing both, in which case _all_
of them must be satisfied before it is dispatched.

### `WhileInFlight`

Pure concurrency control: at most `N` of these jobs run at once.

> Rust:
>
> ```rust
> # use zizq::{BudgetPolicy, BudgetStrategy, Client};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .create_budget(
>         "image-service",
>         BudgetPolicy::new(20, BudgetStrategy::WhileInFlight),
>     )
>     .await?;
> # Ok(()) }
> ```

Tokens are debited when the job is dispatched and released when it stops
running — on success or on failure. There is no clock involved. With an
allocation of `20` you get 20 concurrent jobs at the default cost, or 10 at a
cost of `2`, or any mix that fits:

- `20 × cost=1`
- `10 × cost=2`
- `(5 × cost=2) + (10 × cost=1)`
- `(3 × cost=5) + (2 × cost=2)`

### `TimeBased`

A rate limit: at most `N` jobs _dispatched_ over a period, set by `duration`.

> Rust:
>
> ```rust
> # use std::time::Duration;
> # use zizq::{BudgetPolicy, BudgetStrategy, Client};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .create_budget(
>         "image-service",
>         BudgetPolicy::new(
>             10_000,
>             BudgetStrategy::time_based(Duration::from_secs(60 * 60)),
>         ),
>     )
>     .await?;
> # Ok(()) }
> ```

`BudgetStrategy::time_based` is shorthand for the common case of no burst
ceiling; the full struct variant is used below.

Unlike `WhileInFlight`, tokens are _not_ returned when a job finishes. They
return on the cadence the duration sets. So a `TimeBased` budget governs how
often work **starts**, and says nothing about how much runs at once — jobs
slower than the duration will overlap, by design.

The server implements this lazily. It does not scan for work that has become
affordable; it knows when the next token is due and sleeps until then, or until
something else wakes it.

#### Implementation note

`TimeBased` is a _continuous_ (drip) rate limiter — a
[leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) — rather than one
that buckets tokens into fixed windows. With 100 tokens over 5 minutes and an
empty pool, you have 20 tokens after a minute, 80 after four, and all 100 after
five. Work spreads out evenly instead of arriving in a spike at each window
boundary and then stalling until the next one.

A full pool is a different matter. 100 tokens available means 100 jobs can go
at once, after which the pace settles to roughly one every three seconds. This
is usually desirable — it absorbs short-lived spikes — but not always, so this
strategy also provides the `burst` field.

### `burst`

`burst` caps how full the pool may get at any moment.

> Rust:
>
> ```rust
> # use std::time::Duration;
> # use zizq::{BudgetPolicy, BudgetStrategy, Client};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .create_budget(
>         "image-service",
>         BudgetPolicy::new(
>             10_000,
>             BudgetStrategy::TimeBased {
>                 duration: Duration::from_secs(60 * 60),
>                 burst: Some(500),
>             },
>         ),
>     )
>     .await?;
> # Ok(()) }
> ```

At most 500 jobs go at once, then 10,000/hour at a steady pace. A `burst` of
`1` removes the spike entirely and paces dispatches evenly at all times.

A burst _above_ the allocation is meaningful too: `20_000` on a 10,000/hour
budget permits a deliberate spike beyond the rate limit, but only if the budget
went unused long enough to accrue it.

The opening burst only happens when the pool is genuinely full — either nothing
has been dispatched for a whole duration, or the budget is newly created (or
the server was restarted).

> [!NOTE]
> Every job's cost must fit inside the budget's capacity — the burst where one
> is set, and the allocation otherwise — or the job could never be dispatched.
> The server refuses to accept a binding that cannot fit, and refuses a change
> to a budget that would strand a job already bound to it. With a `burst` set
> it is the _smaller_ number that decides, so a cost well within the allocation
> may still be refused.

## Binding jobs to budgets

Bindings are attached per enqueue, via `budget`:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BudgetBindingInput, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email", queue = "emails")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(SendEmail { to: "user@example.com".into() })
>     .budget(BudgetBindingInput::new("emails").cost(2))
>     .await?;
> # Ok(()) }
> ```

A bare `&str` or `String` converts into a binding at the default cost, so
`.budget("emails")` is the short form. Call `budget` repeatedly to bind
several, or `budgets` to set the whole list at once.

With no budgets a job is unthrottled and dispatches as soon as it reaches the
front of the queue. With several, it must satisfy all of them. A job bound to a
`WhileInFlight` limit of 10 and a `TimeBased` limit of 1000/hour honours both:
never more than 10 at once, never more than 1000 an hour.

Use `cost` to make jobs weigh differently against the same pool. A bulk send
costing `10` against an allocation of `100` leaves room for 90 more single
sends.

### Per-type defaults

A job type that always draws on the same budgets can declare as such once, with
the derive macro, `#[zizq(budget(...))]`. Repeat the attribute to bind multiple
budgets:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(
>     name = "charge_card",
>     queue = "billing",
>     budget(key = "stripe", cost = 2),
>     budget(key = "audit"),
> )]
> struct ChargeCard {
>     invoice_id: String,
> }
> ```

This just generates `JobKind::BUDGETS`, which a manual `impl` can set directly.
Setting any budget at the call site *replaces* the list rather than adding to
it, the same as every other per-type default. To enqueue one job unthrottled,
use `clear_budgets`:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "charge_card", budget(key = "stripe"))]
> # struct ChargeCard { invoice_id: String }
> # async fn run(client: &Client, invoice_id: String) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(ChargeCard { invoice_id })
>     .clear_budgets()
>     .await?;
> # Ok(()) }
> ```

### Automatically creating a budget on enqueue

A budget normally exists before anything binds to it. `create_with` lets a
single enqueue operation create both the budget and the job atomically:

> Rust:
>
> ```rust
> # use std::time::Duration;
> # use serde::{Deserialize, Serialize};
> # use zizq::{BudgetBindingInput, BudgetPolicy, BudgetStrategy, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "send_email", queue = "emails")]
> # struct SendEmail { to: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(SendEmail { to: "user@example.com".into() })
>     .budget(
>         BudgetBindingInput::new("emails")
>             .cost(2)
>             .create_with(BudgetPolicy::new(
>                 100,
>                 BudgetStrategy::time_based(Duration::from_secs(60)),
>             )),
>     )
>     .await?;
> # Ok(()) }
> ```

If the budget already exists the policy is **ignored** and the stored one stays
authoritative, so an enqueue cannot accidentally clobber an existing budget.

The same thing works as a per-type default, because every constructor on
`BudgetBindingInput` is `const`. An application declaring its budgets this way
needs no startup call at all:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(
>     name = "charge_card",
>     budget(
>         key = "stripe",
>         cost = 2,
>         create_with(allocation = 3, while_in_flight)
>     ),
> )]
> struct ChargeCard {
>     invoice_id: String,
> }
> ```

The strategy inside `create_with` is either the bare `while_in_flight` or
`time_based(duration_ms = ..., burst = ...)`. Raw milliseconds appear here
matching `backoff(base_ms = ...)` and `retention(dead_ms = ...)` because a
derive attribute has no reasonable way of expressing one. Const arithmetic
works here too: `duration_ms = 60 * 60 * 1000`.

Cron entries carry budgets the same way, since a cron entry's job is an enqueue
builder.

## Managing budgets

> Rust:
>
> ```rust
> # use zizq::{BudgetPatch, BudgetPolicy, Client};
> # async fn run(client: &Client, policy: BudgetPolicy) -> Result<(), zizq::ZizqError> {
> client.list_budgets().await?;
> client.get_budget("emails").await?;
> client.create_budget("emails", policy.clone()).await?;
> client.update_budget("emails", BudgetPatch::new().burst(5)).await?;
> client.put_budget("emails", policy).await?;
> client.delete_budget("emails").await?;
> # Ok(()) }
> ```

`create_budget` refuses to create a duplicate of an existing key with a `409`
and leaves the stored policy alone. That is deliberate: it means every instance
of an application can declare its budgets on boot without coordinating, and
those that lose the race treat the conflict as success.

> Rust:
>
> ```rust
> # use zizq::{BudgetPolicy, Client};
> # async fn run(client: &Client, policy: BudgetPolicy) -> Result<(), Box<dyn std::error::Error>> {
> if let Err(e) = client.create_budget("emails", policy).await {
>     if !e.is_conflict() {
>         return Err(e.into());
>     }
> }
> # Ok(()) }
> ```

`put_budget` overwrites instead. A replace changes the policy, not the budget's
identity, so `created_at` survives it.

`update_budget` takes a `BudgetPatch` — a recursive merge patch, so one field
within the strategy can be changed without repeating the others. `clear_burst`
is the one meaningful "clear": it resets the ceiling back to the allocation. A
field no setter touched is left unchanged.

> Rust:
>
> ```rust
> # use std::time::Duration;
> # use zizq::{BudgetPatch, BudgetStrategy, Client};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> // Cap the spike without restating the rate.
> client.update_budget("emails", BudgetPatch::new().burst(500)).await?;
>
> // Let it spike again, up to the full allocation.
> client.update_budget("emails", BudgetPatch::new().clear_burst()).await?;
>
> // Change the policy wholesale.
> client
>     .update_budget(
>         "emails",
>         BudgetPatch::new()
>             .allocation(20_000)
>             .strategy(BudgetStrategy::time_based(Duration::from_secs(60 * 60))),
>     )
>     .await?;
> # Ok(()) }
> ```

`strategy` states every field the new strategy implies, so the result is
exactly what you asked for rather than a merge with what was there — a
`TimeBased` with no burst clears the stored one.

## Modifying budget bindings

Bindings are mutable even after jobs are enqueued. This is useful when tuning a
budget or responding to an incident that requires making changes to budgets,
such as splitting one shared budget in two, or removing a budget from a job
that is delayed and needs to run immediately.

> Rust:
>
> ```rust
> # use zizq::{BudgetBindingInput, Client};
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> client.bind_budget(id, BudgetBindingInput::new("emails").cost(2)).await?;
> client.rebind_budget(id, "emails").await?;
> client.set_budget_cost(id, "emails", 5).await?;
> client.unbind_budget(id, "emails").await?;
> client.unbind_all_budgets(id).await?;
> client.replace_budgets(id, [BudgetBindingInput::new("emails").cost(2)]).await?;
> # Ok(()) }
> ```

Each returns the updated `Job`. `bind_budget` fails with a `409` if the job
already draws on that budget; `rebind_budget` replaces the binding whole.

The same operations run over a selection, returning a `BudgetChange` rather
than a job:

> Rust:
>
> ```rust
> # use zizq::{BudgetBindingInput, Client};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let change = client
>     .bind_all_jobs_budget(BudgetBindingInput::new("stripe").cost(2))
>     .queue(["emails"])
>     .await?;
>
> println!("{} changed, {} in flight", change.changed, change.blocked.len());
> # Ok(()) }
> ```

Alongside `bind_all_jobs_budget` are `rebind_all_jobs_budget`,
`set_all_jobs_budget_cost`, `unbind_all_jobs_budget` (one budget) and
`clear_all_jobs_budgets` (every budget). All five accept the same filters as
`list_jobs`.

> [!WARNING]
> With no filters set, these act on *every job on the server* — the `all_jobs`
> in the name is literal. A filter explicitly set to an empty collection
> instead matches nothing, and the request is short-circuited without
> contacting the server.

> [!IMPORTANT]
> Only queued (`Scheduled`, `Ready`) jobs can be rebound. An in-flight job has
> already debited its tokens, and jobs in a terminal status are always
> immutable. The bulk forms report the ones they could not change rather than
> skipping them silently:
>
> ```text
> BudgetChange { changed: 12, blocked: ["01K9...", "01KA..."] }
> ```
>
> `blocked` is always in-flight jobs, so it is essentially a retry list — they
> eventually drain on their own, and the same call afterwards picks them up.

## Finding what is bound to a budget

A budget cannot be deleted while anything remains bound to it — the attempt
fails with a `409` naming how many jobs are in the way. The `budgets_key`
filter selects exactly what those are, and works anywhere jobs are filtered:

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client.count_jobs().budgets_key(["emails"]).await?;
>
> client.unbind_all_jobs_budget("emails").budgets_key(["emails"]).await?;
> client.delete_budget("emails").await?;
> # Ok(()) }
> ```

`Job::budgets` reports what a job is bound to.

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> for binding in &client.get_job(id).await?.budgets {
>     println!("{} costs {}", binding.key, binding.cost);
> }
> # Ok(()) }
> ```
