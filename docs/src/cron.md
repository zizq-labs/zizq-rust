# Cron Scheduling

Cron lets the server enqueue jobs on a recurring schedule. A **cron group** is
a named set of **entries**; each entry pairs a cron expression with a job
template that the server enqueues on every tick.

> [!NOTE]
> Cron requires a [Pro license](https://zizq.io/pricing) on the server. Cron
> calls against a server without one return a `ZizqError::Response` with
> `status: 403`.

## Defining entries

A `CronEntry` is built from a job — the same `enqueue(...)` builder you would
use to enqueue that job directly:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, CronEntry, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "cleanup")]
> # struct Cleanup { older_than_days: u32 }
> # fn build(client: &Client) -> CronEntry {
> CronEntry::new(
>     "nightly-cleanup",                              // entry name
>     "0 0 * * *",                                    // cron expression
>     client.enqueue(Cleanup { older_than_days: 30 }), // the job to enqueue
> )
> .timezone("Australia/Melbourne")  // optional — overrides the group's
> # }
> ```

> [!WARNING]
> A cron entry's schedule comes from its expression, not the job. The server
> rejects a job template that sets a ready-at time — do not call `.delay()`,
> `.ready_at()`, or `.run_at()` on the enqueue builder you pass to
> `CronEntry::new`.

## Replacing a group

`Client::replace_cron` atomically installs a group's entire entry set. Chain
`.entry(...)` per entry, then `.await`:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, CronEntry, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "cleanup")]
> # struct Cleanup;
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "digest")]
> # struct Digest;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let group = client
>     .replace_cron("maintenance")
>     .entry(CronEntry::new("cleanup", "0 0 * * *", client.enqueue(Cleanup)))
>     .entry(CronEntry::new("digest", "0 6 * * *", client.enqueue(Digest)))
>     .await?;
> 
> println!("{} has {} entries", group.name, group.entries.len());
> # Ok(()) }
> ```

`replace_cron` is a *replace*: entries not included are removed, and the group
is created if it does not yet exist. Awaiting it returns the resulting
`CronGroup`.

## Timezones

Most schedules run in one timezone throughout, which is what the group's
`.timezone(...)` is for. It applies to every entry that does not set its own:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, CronEntry, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "cleanup")]
> # struct Cleanup;
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "digest")]
> # struct Digest;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let group = client
>     .replace_cron("maintenance")
>     .timezone("Australia/Melbourne")
>     .entry(CronEntry::new("cleanup", "0 0 * * *", client.enqueue(Cleanup)))
>     // This one runs at 6am UTC, whatever the group says.
>     .entry(
>         CronEntry::new("digest", "0 6 * * *", client.enqueue(Digest))
>             .timezone("UTC"),
>     )
>     .await?;
> 
> assert_eq!(group.timezone.as_deref(), Some("Australia/Melbourne"));
> # Ok(()) }
> ```

With neither set, expressions are evaluated in the server's local timezone.

The group's timezone is stored on the server as the group's own rather than
copied onto each entry, so `CronGroup::timezone` still reports it when the
schedule is read back with `get_cron`. Because `replace_cron` replaces the
group in full, omitting `.timezone(...)` clears whatever the group had.

> [!NOTE]
> A group-level timezone requires Zizq 0.7.0 or newer on the server. Against an
> older server it is ignored, and entries relying on it fall back to the
> server's local timezone.

## Single-entry operations

Each entry can also be managed individually without replacing the whole group:

> Rust:
>
> ```rust
> # use serde::{Deserialize, Serialize};
> # use zizq::{Client, CronEntry, JobKind};
> # #[derive(Serialize, Deserialize, JobKind)]
> # #[zizq(name = "cleanup")]
> # struct Cleanup;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> // Add or replace one entry.
> client
>     .add_cron_entry("maintenance", CronEntry::new("hourly", "0 * * * *", client.enqueue(Cleanup)))
>     .await?;
> client
>     .put_cron_entry("maintenance", CronEntry::new("hourly", "*/30 * * * *", client.enqueue(Cleanup)))
>     .await?;
> 
> // Fetch or delete one entry.
> let entry = client.get_cron_entry("maintenance", "hourly").await?;
> client.delete_cron_entry("maintenance", "hourly").await?;
> # Ok(()) }
> ```

## Pausing & resuming

A paused entry does not enqueue; a paused *group* suspends all of its entries.

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client.pause_cron("maintenance").await?;             // whole group
> client.resume_cron("maintenance").await?;
> 
> client.pause_cron_entry("maintenance", "cleanup").await?;   // single entry
> client.resume_cron_entry("maintenance", "cleanup").await?;
> # Ok(()) }
> ```

## Reading cron data

`list_crons` returns the group names; `get_cron` returns a `CronGroup` with
its `CronEntryRecord` entries:

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> for name in client.list_crons().await? {
>     let group = client.get_cron(&name).await?;
>     for entry in &group.entries {
>         println!(
>             "{}/{}: {} (next: {:?})",
>             group.name, entry.name, entry.expression, entry.next_enqueue_at,
>         );
>     }
> }
> # Ok(()) }
> ```

A `CronEntryRecord` carries the schedule, its paused state, runtime fields like
`next_enqueue_at` / `last_enqueue_at`, and the stored job as a `JobTemplate`.
A whole group is removed with `Client::delete_cron`.
