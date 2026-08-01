# Unique Jobs

A unique key prevents duplicate jobs. When a job carries a `UniqueKey`, the
server rejects a new enqueue whose key matches an existing job within the
key's scope — returning the existing job instead of creating a second one.

> [!NOTE]
> Unique jobs require a [Pro license](https://zizq.io/pricing) on the server.

This solves problems like "only one `rebuild_search_index` job should be
queued at a time", or "don't email the same user twice for the same event".

## Deriving a unique key

The `#[zizq(unique(...))]` attribute on `#[derive(JobKind)]` generates a
`unique_key` implementation that hashes the payload (or a subset of it) and
attaches it to every enqueue automatically.

The bare form hashes the entire payload, tagged with the job type name:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "rebuild_index", unique)]
> struct RebuildIndex;
> ```

For a job type with no fields, "hash the whole payload" is a stable per-type
key — perfect for the "only one queued at a time" case.

### Selecting a subset

`only` and `except` narrow the hashed subset via jq-compatible paths:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> // Two jobs collide when they share `user_id` and `campaign_id`,
> // regardless of what `body` contains.
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "send_email", unique(only = [".user_id", ".campaign_id"]))]
> struct SendEmail {
>     user_id: u64,
>     campaign_id: u64,
>     body: String,
> }
> ```

`except` is the inverse — hash everything but the listed paths. `only` and
`except` are mutually exclusive; setting both is a compile error.

Paths are validated at compile time. An unknown or malformed path (e.g.
`user_id` without the leading `.`) surfaces as a compile error with a caret
on the offending string literal.

> [!IMPORTANT]
> Paths address the **serialized** field names, not the Rust field
> identifiers. If your struct uses `#[serde(rename = "...")]` or
> `#[serde(rename_all = "...")]` to change the wire form, the paths must
> match the wire form too. This is because the payload is hashed *after*
> serialisation — the derive doesn't rewrite paths to match Rust names.

For example, a struct that serialises to camelCase must use camelCase
paths:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[serde(rename_all = "camelCase")]
> #[zizq(name = "send_email", unique(only = [".userId", ".campaignId"]))]
> struct SendEmail {
>     user_id: u64,       // serialises as "userId"
>     campaign_id: u64,   // serialises as "campaignId"
>     body: String,
> }
> ```

Using `[".user_id"]` here would silently match nothing (jq's behaviour
for missing keys), which would collapse the picked subset to `{}` and
give every enqueue the same key — probably not what you want. If you're
using serde renames, keep the two forms in sync.

### Scope

The `scope` field selects the lifecycle window during which duplicates are
rejected:

<table>
    <thead>
        <tr><th>Scope</th><th>A duplicate is rejected while an existing job is…</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>"queued"</code></td>
            <td>waiting to run (ready or scheduled) — the default.</td>
        </tr>
        <tr>
            <td><code>"active"</code></td>
            <td>queued <em>or</em> currently being processed.</td>
        </tr>
        <tr>
            <td><code>"exists"</code></td>
            <td>
                present in any state at all, including completed or dead
                (until reaped by retention).
            </td>
        </tr>
    </tbody>
</table>

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "reindex", unique(scope = "active"))]
> struct Reindex;
> ```

### Dropping the type-name prefix

By default the hash is prefixed with the job type name, so two different job
types can't collide on identical payload data. Set `prefix = false` to opt
out — useful when you want two job types to share a uniqueness namespace
(for example, an insert-or-update pattern where either job type dedups
against the other):

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::JobKind;
> #[derive(Serialize, Deserialize, JobKind)]
> #[zizq(name = "sync_user", unique(only = [".user_id"], prefix = false))]
> struct SyncUser { user_id: u64 }
> ```

## Per-enqueue override

`EnqueueBuilder::unique_key` supplies a key for a single call, overriding
whatever the `JobKind` would produce:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind, UniqueKey};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "rebuild_index")]
> # struct RebuildIndex;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client
>     .enqueue(RebuildIndex)
>     .unique_key(UniqueKey::raw("rebuild_index"))
>     .await?;
> # Ok(()) }
> ```

Chain `.scope(...)` on a `UniqueKey` to change the scope:

> Rust:
>
> ```rust
> use zizq::{UniqueKey, UniqueScope};
> 
> let key = UniqueKey::raw("rebuild_index").scope(UniqueScope::Active);
> ```

## Detecting a duplicate

When an enqueue collides with an existing unique job, the call still succeeds
— it returns the *existing* job, with its `duplicate` field set to
`Some(true)`:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, JobKind, UniqueKey};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "rebuild_index")]
> # struct RebuildIndex;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let job = client
>     .enqueue(RebuildIndex)
>     .unique_key(UniqueKey::raw("rebuild_index"))
>     .await?;
> 
> if job.duplicate == Some(true) {
>     println!("a rebuild was already queued — job {}", job.id);
> }
> # Ok(()) }
> ```

## Manual `unique_key` impl

The derive covers hashing-based keys — the 90% case. Reach for a hand-written
`fn unique_key(&self)` when the key needs computation the attribute grammar
can't express: reaching into `&self` in non-obvious ways, combining runtime
state, or producing a raw string key without hashing.

`UniqueKey::tagged_hash_of` is the same helper the derive uses under the
hood; feed it whatever identifies the job — the whole payload (`self`), a
single field (`&self.field`), or a tuple of fields:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> use zizq::{JobKind, UniqueKey};
> 
> # #[derive(Serialize, Deserialize)]
> struct SendWelcomeEmail {
>     user_id: u64,
>     campaign: String,
> }
> 
> impl JobKind for SendWelcomeEmail {
>     const NAME: &'static str = "send_welcome_email";
> 
>     fn unique_key(&self) -> Option<UniqueKey> {
>         // Two elements — a tuple serialises to a JSON array, so
>         // order matters and must stay stable.
>         Some(UniqueKey::tagged_hash_of(
>             Self::NAME,
>             (&self.user_id, &self.campaign),
>         ))
>     }
> }
> ```

`UniqueKey::hash_of` is the same without the tag prefix (equivalent to
`unique(prefix = false)` on the derive). `UniqueKey::raw(key)` wraps a
literal string verbatim — no hashing or transformation applied.

Chain `.scope(UniqueScope::Active | Exists)` on the returned `UniqueKey` to
match the derive's `scope = "active" | "exists"`.
