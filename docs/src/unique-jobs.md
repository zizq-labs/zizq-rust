# Unique Jobs

A unique key prevents duplicate jobs. When a job carries a `UniqueKey`, the
server rejects a new enqueue whose key matches an existing job within the
key's scope — returning the existing job instead of creating a second one.

> [!NOTE]
> Unique jobs require a [Pro license](https://zizq.io/pricing) on the server.

This solves problems like "only one `rebuild_search_index` job should be
queued at a time", or "don't email the same user twice for the same event".

## Building a key

The recommended way to build a key from data is to **hash** it.
`UniqueKey::tagged_hash_of` serialises a value to canonical JSON (object keys
sorted, so the result is independent of field ordering) and hashes it with
SHA-256. The tag — pass the job type name — is a readable prefix that keeps
two job types from colliding on identical data. `UniqueKey::hash_of` is the
same without the tag.

Pass whatever identifies the job: the whole payload (`self`), a single field
(`&self.field`), or a tuple of fields for a subset:

```rust
# use zizq::UniqueKey;
// Whole payload.
# let _ =
UniqueKey::tagged_hash_of("send_email", ("alice@example.com", "Welcome!"))
# ;
```

> [!NOTE]
> A tuple of fields serialises to a JSON array, so the order of its elements
> matters — keep it stable.

If you already have a key string, wrap it verbatim with `UniqueKey::raw` — no
hashing or transformation is applied.

## Supplying a key

There are two ways to attach a key to a job.

**Per job type** — override `JobKind::unique_key` to derive a key from the
payload. Every enqueue of that type then carries it automatically:

```rust
# use serde::{Deserialize, Serialize};
use zizq::{JobKind, UniqueKey};

# #[derive(Serialize, Deserialize)]
struct SendWelcomeEmail {
    user_id: u64,
}

impl JobKind for SendWelcomeEmail {
    const NAME: &'static str = "send_welcome_email";

    fn unique_key(&self) -> Option<UniqueKey> {
        Some(UniqueKey::tagged_hash_of(Self::NAME, &self.user_id))
    }
}
```

**Per enqueue** — supply a key for a single call with
`EnqueueBuilder::unique_key`. This overrides whatever the `JobKind` would
derive:

```rust
# use serde::{Deserialize, Serialize};
# use zizq::{Client, JobKind, UniqueKey};
# #[derive(Serialize, Deserialize)]
# struct RebuildIndex;
# impl JobKind for RebuildIndex { const NAME: &'static str = "rebuild_index"; }
# async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
client
    .enqueue(RebuildIndex)
    .unique_key(UniqueKey::raw("rebuild_index"))
    .await?;
# Ok(()) }
```

## Scope

A `UniqueKey` has a *scope* — the lifecycle window during which it blocks
duplicates. Set it with `UniqueScope`:

<table>
    <thead>
        <tr><th>Scope</th><th>A duplicate is rejected while an existing job is…</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>Queued</code></td>
            <td>waiting to run (ready or scheduled) — the default.</td>
        </tr>
        <tr>
            <td><code>Active</code></td>
            <td>queued <em>or</em> currently being processed.</td>
        </tr>
        <tr>
            <td><code>Exists</code></td>
            <td>
                present in any state at all, including completed or dead
                (until reaped by retention).
            </td>
        </tr>
    </tbody>
</table>

`UniqueKey::raw(key)` uses the default `Queued` scope. To choose a different
scope, chain `.scope(...)`:

```rust
use zizq::{UniqueKey, UniqueScope};

let key = UniqueKey::raw("rebuild_index").scope(UniqueScope::Active);
```

## Detecting a duplicate

When an enqueue collides with an existing unique job, the call still succeeds —
it returns the *existing* job, with its `duplicate` field set to
`Some(true)`:

```rust
# use serde::{Deserialize, Serialize};
# use zizq::{Client, JobKind, UniqueKey};
# #[derive(Serialize, Deserialize)]
# struct RebuildIndex;
# impl JobKind for RebuildIndex { const NAME: &'static str = "rebuild_index"; }
# async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
let job = client
    .enqueue(RebuildIndex)
    .unique_key(UniqueKey::raw("rebuild_index"))
    .await?;

if job.duplicate == Some(true) {
    println!("a rebuild was already queued — job {}", job.id);
}
# Ok(()) }
```
