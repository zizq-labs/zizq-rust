# Batched Jobs

> [!NOTE]
> This feature requires a Zizq [pro license](https://zizq.io/pricing) on the
> server.

Batched jobs let you accumulate many enqueue calls into a single pending job.
This is useful when your application enqueues one work unit at a time, but the
downstream service you're calling accepts (or prefers) batches — for example,
push notification providers that accept up to 1000 device tokens per request,
or bulk email APIs that accept many recipients per call.

Rather than each enqueue creating its own job, subsequent enqueues that share
a batch key are _folded_ into the pending job's payload. When a worker
eventually claims the job, it sees a single job with a combined payload and
makes a single downstream call.

Once claimed, a batched job is just a normal job — the worker sees one merged
payload and acks once.

## The `BatchConfig` type

Batching is enabled per-enqueue by attaching a `BatchConfig` to the enqueue.
In the Zizq API, `batch` is an object with three fields, mirrored by the
`BatchConfig` struct in Rust:

<table>
  <thead>
    <tr>
      <th>Field</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>key</code></td>
      <td>
        Identifies the batch. Only one unsealed batch exists per key at a time.
      </td>
    </tr>
    <tr>
      <td><code>when</code></td>
      <td>
        A <code>jq</code> predicate that decides whether to fold the incoming
        payload into the existing job. A truthy result means fold/merge instead
        of enqueueing a new job; a falsy result means seal the existing batch
        and enqueue a new job to start a new batch from this enqueue.
      </td>
    </tr>
    <tr>
      <td><code>fold</code></td>
      <td>
        A <code>jq</code> expression that produces the merged payload. Both
        expressions run with <code>$existing</code> bound to the current
        pending payload and <code>$new</code> bound to the incoming payload.
      </td>
    </tr>
  </tbody>
</table>

You _can_ construct a `BatchConfig` by hand, but for the common case the Rust
client ships a fluent builder that produces the right shape from a target
path and a limit.

## Configuring batches with `BatchConfig::at`

The simplest form targets the whole payload, which is appropriate when the
payload itself is the array being accumulated.

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "audit.events", queue = "audit")]
> # struct AuditEvents(Vec<AuditEvent>);
> # #[derive(Serialize, Deserialize)]
> # struct AuditEvent { actor: String, action: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(AuditEvents(vec![AuditEvent {
>         actor: "user_1234".into(),
>         action: "login".into(),
>     }]))
>     .batch(BatchConfig::at(".", 1000).keyed_by("audit"))
>     .await?;
> # Ok(()) }
> ```

The `1000` is the maximum combined length of the accumulated payload before
the batch is sealed and a new one starts.

When the payload is a struct with a specific field to accumulate, pass the
`jq` path as the first argument:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "push.notifications", queue = "push")]
> # struct PushNotifications { device_ids: Vec<String>, platform: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(PushNotifications {
>         device_ids: vec!["abc123".into()],
>         platform: "apple".into(),
>     })
>     .batch(BatchConfig::at(".device_ids", 100).keyed_by("push:apple"))
>     .await?;
> # Ok(()) }
> ```

Path syntax is jq-compatible: `.`, `.foo`, `.foo.bar`, `.foo[0]`, `.[0]`, and
`.["key.with.dots"]` for keys that themselves contain literal dots. Invalid
paths surface as `422 Unprocessable Entity` on the first enqueue (see
[Enqueue-time validation](#enqueue-time-validation) below).

> [!IMPORTANT]
> Paths (both the batch `path` and any `key(only|except)` paths in
> the derive form below) address the **serialized** field names, not
> the Rust field identifiers. If your struct uses `#[serde(rename =
> "...")]` or `#[serde(rename_all = "...")]` to change the wire form,
> the paths must match the wire form too — payloads are hashed and
> folded *after* serialisation.
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[serde(rename_all = "camelCase")]
> #[zizq(name = "push", batch(path = ".deviceIds", limit = 100))]
> struct Push {
>     device_ids: Vec<String>,   // serialises as "deviceIds"
>     platform: String,
> }
> ```
>
> Using `.device_ids` here would silently match nothing on the server,
> which for the batch path would break folding entirely (the `when`
> and `fold` expressions would never find the list they're supposed
> to accumulate into) and for `key(only|except)` would collapse the
> hashed subset to `{}` — every enqueue getting the same key.

### Options

`BatchConfig::at` returns a `BatchConfigAt` builder with two modifier
methods before you call `.keyed_by`:

<table>
    <thead>
        <tr>
            <th>Method</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code>.dedup()</code></td>
            <td>
                Append <code>| unique</code> to the fold expression so
                duplicate entries within the batch collapse. Note:
                <code>unique</code> in jq sorts as a side effect, so this
                subsumes <code>.sorted()</code> when both are set.
            </td>
        </tr>
        <tr>
            <td><code>.sorted()</code></td>
            <td>
                Append <code>| sort</code> to the fold expression so entries
                are sorted within the batch. Ignored when <code>.dedup()</code>
                is also set.
            </td>
        </tr>
    </tbody>
</table>

> Rust:
>
> ```rust
> # use zizq::BatchConfig;
> BatchConfig::at(".device_ids", 100)
>     .dedup()
>     .keyed_by("push:apple")
> # ;
> ```

### Batch semantics

For a job with `BatchConfig::at(".device_ids", 100).keyed_by("push:apple")`,
the client emits:

- `when` = `(($existing | .device_ids) + ($new | .device_ids)) | length <= 100`
- `fold` = `$existing | .device_ids += ($new | .device_ids)`

The `when` predicate returns true when the combined length would still fit
within the limit — in which case the new payload is folded in. When it returns
false, the existing batch is _sealed_ (no more folds against it) and a fresh
pending job is created with just the new payload.

This is _all-or-nothing_: an incoming payload of 20 elements against an
existing batch of 90 does not partially fill the batch to 100 and overflow the
remaining 10 into a new one — it seals the existing batch at 90 and starts a
new one with all 20. Applications that need strict per-batch caps should size
their enqueues to avoid overshoot.

## Batch keys

Enqueues that share a batch key fold into the same pending job. Unlike some
clients that hash the payload to derive a key automatically, the Rust client
requires you to supply the key explicitly — either as a plain string or by
deriving one from your payload.

The idiomatic way to derive a key from payload data is
[`UniqueKey::tagged_hash_of`](./unique-jobs.md#building-a-key) — it produces
a stable, tagged SHA-256 of a serialisable value. `UniqueKey` exposes the
computed string on its `.key` field:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind, UniqueKey};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "push.notifications")]
> # struct PushNotifications { device_ids: Vec<String>, platform: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let payload = PushNotifications {
>     device_ids: vec!["abc".into()],
>     platform: "apple".into(),
> };
>
> // Hash the non-batch field (platform) to derive a stable batch key.
> let key = UniqueKey::tagged_hash_of("push.notifications", &payload.platform).key;
>
> client
>     .enqueue(payload)
>     .batch(BatchConfig::at(".device_ids", 100).keyed_by(key))
>     .await?;
> # Ok(()) }
> ```

Two enqueues that produce the same key fold together; ones that produce
different keys go to separate batches.

## Attaching a `BatchConfig` to a job

There are two ways to attach a config to a job.

**Per job type via `#[zizq(batch(...))]`** — the derive generates a
`fn batch(&self)` from the attribute:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(
>     name = "push.notifications",
>     queue = "push",
>     batch(path = ".device_ids", limit = 100),
> )]
> struct PushNotifications {
>     device_ids: Vec<String>,
>     platform: String,
> }
> ```

`path` and `limit` are required. The default batch key is a hash of the
payload with the batch path excluded — so two enqueues that differ only in
`.device_ids` collide (and fold), and enqueues that differ in `platform`
end up in separate batches.

Add `dedup` or `sorted` (mutually exclusive) to switch the fold expression
from append to `| unique` or `| sort`:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "push", batch(path = ".device_ids", limit = 100, dedup))]
> struct Push { device_ids: Vec<String> }
> ```

The `key(...)` inner attribute controls what contributes to the batch key.
`only` and `except` narrow the hashed subset via jq-compatible paths (same
grammar as [unique-jobs](./unique-jobs.md#selecting-a-subset)); `prefix =
false` drops the type-name tag. `except` is **additive** to the batch
`path` — the batch data is always excluded from the key regardless.

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(
>     name = "push",
>     batch(
>         path = ".device_ids",
>         limit = 100,
>         key(only = [".platform"]),
>     ),
> )]
> struct Push {
>     device_ids: Vec<String>,
>     platform: String,
>     tenant_id: u64,     // ignored — not in `only`
> }
> ```

All paths (the batch `path` and any `key(only|except)` paths) are validated
at compile time — malformed jq syntax surfaces as a compile error with a
caret on the offending string literal.

**Per enqueue via `EnqueueBuilder::batch`** — supply a config for a single
call, overriding whatever the `JobKind` would derive:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "push.notifications")]
> # struct PushNotifications { device_ids: Vec<String>, platform: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(PushNotifications {
>         device_ids: vec!["abc".into()],
>         platform: "apple".into(),
>     })
>     .batch(BatchConfig::at(".device_ids", 50).keyed_by("push:apple:hot-path"))
>     .await?;
> # Ok(()) }
> ```

## Fully custom `BatchConfig`

Nothing about `BatchConfig::at` is magic or required. `BatchConfig` is a
plain struct, and you can construct it end-to-end when the fluent builder
doesn't fit. Both `when` and `fold` are `jq` expressions; both run with
`$existing` bound to the current job's payload and `$new` bound to the
incoming payload.

Here's an audit-log example that batches events by tenant, caps at 50 events
per batch, and includes a "did we already see this event id?" dedup step
within the fold that goes beyond what `.dedup()` would do:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "audit.events", queue = "audit")]
> # struct AuditBatch { tenant_id: String, events: Vec<AuditEvent> }
> # #[derive(Serialize, Deserialize)]
> # struct AuditEvent { id: String, actor: String, action: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let payload = AuditBatch {
>     tenant_id: "acme".into(),
>     events: vec![AuditEvent {
>         id: "evt_1".into(),
>         actor: "u1".into(),
>         action: "login".into(),
>     }],
> };
>
> let key = format!("audit:{}", payload.tenant_id);
>
> client
>     .enqueue(payload)
>     .batch(BatchConfig {
>         key,
>         when: "($existing.events + $new.events | length) <= 50".into(),
>         fold: r#"
>             $existing
>             | .events = (
>                 .events
>                 + ($new.events | map(select(. as $n | ($existing.events | any(.id == $n.id)) | not)))
>               )
>         "#.into(),
>     })
>     .await?;
> # Ok(()) }
> ```

Bring your own jq — the server evaluates it against the payloads.

## Manual `batch` impl

The derive's `#[zizq(batch(...))]` covers the common cases: a fixed jq
path + limit, standard fold modes, key derivation from payload fields.
Reach for a hand-written `fn batch(&self)` when the key needs computation
the attribute grammar can't express — reading runtime state, bucketing by
time windows, or combining values that don't map cleanly to `only`/`except`.

The signature mirrors what the derive generates:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> use zizq::{BatchConfig, JobKind, UniqueKey};
>
> # #[derive(Serialize, Deserialize)]
> struct PushNotifications {
>     device_ids: Vec<String>,
>     platform: String,
> }
>
> impl JobKind for PushNotifications {
>     const NAME: &'static str = "push.notifications";
>     const QUEUE: &'static str = "push";
>
>     fn batch(&self) -> Option<BatchConfig> {
>         // Hash whichever fields identify the batch. The derive would
>         // do this via `key(only = [".platform"])`; the manual form
>         // lets you reach into `&self` however you need to.
>         let key = UniqueKey::tagged_hash_of(Self::NAME, &self.platform).key;
>         Some(BatchConfig::at(".device_ids", 100).keyed_by(key))
>     }
> }
> ```

Mix and match: a job type with `#[zizq(batch(...))]` and one with a manual
`fn batch(&self)` coexist in the same worker without issue.

## The returned `Job`

Awaiting an enqueue returns a `Job` whose `folded` flag tells you whether
this enqueue was folded into an existing pending job or created a new one:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{BatchConfig, Client, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "push.notifications", queue = "push")]
> # struct PushNotifications { device_ids: Vec<String>, platform: String }
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> // First enqueue creates the batch.
> let first = client
>     .enqueue(PushNotifications {
>         device_ids: vec!["abc".into()],
>         platform: "apple".into(),
>     })
>     .batch(BatchConfig::at(".device_ids", 100).keyed_by("push:apple"))
>     .await?;
> assert_eq!(first.folded, Some(false));
>
> // Second enqueue with the same key folds into the existing job.
> let second = client
>     .enqueue(PushNotifications {
>         device_ids: vec!["def".into()],
>         platform: "apple".into(),
>     })
>     .batch(BatchConfig::at(".device_ids", 100).keyed_by("push:apple"))
>     .await?;
> assert_eq!(second.folded, Some(true));
> assert_eq!(second.id, first.id); // same job
> # Ok(()) }
> ```

The same is true for
[`Client::enqueue_bulk`](./enqueuing-jobs.md#bulk-enqueue) — items within
the same bulk call that share a batch key fold together.

## Inspecting a batched job

The stored `when` / `fold` / `key` on the pending job are readable through
the normal job-read APIs. Because the [first enqueue's config wins for the
life of the batch](#first-enqueue-config-wins), this is the source of truth
for what the server is actually evaluating on subsequent folds — useful when
a fold isn't behaving as expected.

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> let job = client.get_job(id).await?;
> if let Some(cfg) = job.batch {
>     println!("key:  {}", cfg.key);
>     println!("when: {}", cfg.when);
>     println!("fold: {}", cfg.fold);
> }
> # Ok(()) }
> ```

## First-enqueue config wins {#first-enqueue-config-wins}

The `when` and `fold` expressions stored on the first pending job are used
for every subsequent fold against it. Follow-up enqueues that would produce
different expressions (perhaps because you changed the batch config in code
and redeployed) don't take effect until the current batch is sealed and a
new one starts.

If you need new expressions to take immediate effect, changing the batch key
— for example by version-suffixing it — forces a fresh batch.

## Incompatibility with `unique_key`

`unique_key` and `batch` are mutually exclusive on the same enqueue. The
server rejects the combination with `400 Bad Request`. If your job needs
both uniqueness and batching semantics, model them separately (e.g. use
`unique_key` on a "schedule" job that then enqueues a batched "process" job).

## Interaction with scheduling

Scheduled batched enqueues (`ready_at` in the future) opt out of the fold
path entirely. They persist as normal scheduled jobs with a `batch` config
stored on them for observability, but no fold happens across a `ready_at`
boundary in either direction. Folding is strictly `ready` → `ready`.

Rationale: fold preserves the existing job's `ready_at` and discards the
incoming job's `ready_at` — either promoting a scheduled job to immediate or
delaying an immediate one into the future, both surprising. Disallowing the
crossing is easier to explain than either.

For "delay some work then batch it" workflows, schedule a job that _enqueues_
the batched job at fire time.

## Enqueue-time validation

Both `when` and `fold` are compiled and dry-run against the incoming
payload on every batched enqueue. Bad expressions (syntax errors, undefined
variables, or shape errors that only manifest against actual data) surface
as `422 Unprocessable Entity` up front rather than at first fold.

The dry-run catches common mistakes but can't detect logic errors that only
appear on data your first enqueue didn't include, so test your expressions
in development with realistic payloads.
