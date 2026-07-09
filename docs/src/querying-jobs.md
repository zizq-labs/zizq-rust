# Querying & Managing Jobs

Beyond enqueuing and processing, the client can inspect and manage jobs
already on the server — list and count them, fetch one by id, update or
delete them, and read a job's failure history.

## Fetching a single job

> Rust:
>
> ```rust
> # use zizq::{Client, ZizqError};
> # async fn run(client: &Client, id: &str) -> Result<(), Box<dyn std::error::Error>> {
> match client.get_job(id).await {
>     Ok(job) => println!("{} is {:?} on {}", job.id, job.status, job.queue),
>     Err(ZizqError::Response { status: 404, .. }) => println!("not found"),
>     Err(e) => return Err(e.into()),
> }
> # Ok(()) }
> ```

## Listing jobs

`Client::list_jobs` returns a `ListJobsBuilder`. Chain filters, then `.await`
for a single `JobPage`:

> Rust:
>
> ```rust
> # use zizq::{Client, JobStatus, Order};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let page = client
>     .list_jobs()
>     .status([JobStatus::Ready, JobStatus::Scheduled])
>     .queue(["emails"])
>     .order(Order::Desc)
>     .limit(100)
>     .await?;
> 
> for job in &page.jobs {
>     println!("{}", job.id);
> }
> # Ok(()) }
> ```

Filters (`status`, `queue`, `job_type`, `id`, a `filter` jq expression, and
the `priority` / `ready_at` / `attempts` range filters described
[below](#filtering-by-range)) combine with AND. The same filter set is
shared by `count_jobs`, `delete_all_jobs`, and `patch_all_jobs`.

### Streaming every match

Rather than following `page.pages.next` by hand, call `.stream()` to get a
`Stream` that fetches pages lazily and yields every matching job:

> Rust:
>
> ```rust
> use futures_util::TryStreamExt;
> # use zizq::{Client, JobStatus};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let mut jobs = client.list_jobs().status([JobStatus::Dead]).limit(2000).stream();
> 
> while let Some(job) = jobs.try_next().await? {
>     println!("dead: {}", job.id);
> }
> # Ok(()) }
> ```

## Counting jobs

> Rust:
>
> ```rust
> # use zizq::{Client, JobStatus};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let ready = client.count_jobs().status([JobStatus::Ready]).await?;
> println!("{ready} jobs ready");
> # Ok(()) }
> ```

## Filtering by payload with jq

The `filter` method takes a server-side jq expression evaluated against each
job's payload. Writing jq by hand is error-prone, so the client provides
helpers that build the common expressions from any serializable value:

> Rust:
>
> ```rust
> # use zizq::{Client, jq_contains, jq_eq};
> # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
> // Subset match — jobs whose payload contains these fields.
> let page = client
>     .list_jobs()
>     .filter(jq_contains(&serde_json::json!({ "region": "eu" }))?)
>     .await?;
> 
> // Exact match — payload equals this value.
> let _ = client
>     .list_jobs()
>     .filter(jq_eq(&serde_json::json!({ "region": "eu", "tier": "pro" }))?)
>     .await?;
> # let _ = page;
> # Ok(()) }
> ```

`jq_eq`, `jq_contains`, and `jq_array_prefix_eq` each return a `String` you
pass straight to `.filter(...)`.

## Filtering by range

The `priority`, `ready_at`, and `attempts` setters all share a calling
convention: pass an exact value (`50`, `OffsetDateTime`, …) for an exact
match, or one of the **inclusive** Rust range syntaxes for a range:

| Argument | Meaning                          |
| -------- | -------------------------------- |
| `n`      | Exactly `n`.                     |
| `a..=b`  | Between `a` and `b`, inclusive.  |
| `a..`    | At least `a`.                    |
| `..=b`   | At most `b`.                     |
| `..`     | Unbounded — every value matches. |

The half-open `a..b` form is **deliberately not accepted** — the server
only supports inclusive bounds, and `0..100` would silently shift the upper
bound by one. There is no `From<Range<T>>` impl for `RangeFilter<T>`, so
the compiler rejects it at the call site. Write `0..=99` if that is what
you meant.

> Rust:
>
> ```rust
> # use zizq::{Client, JobStatus};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> // Jobs that have failed at least once.
> let flaky = client
>     .list_jobs()
>     .attempts(1u32..)
>     .await?;
> 
> // High-priority jobs across a single queue.
> let urgent = client
>     .count_jobs()
>     .queue(["emails"])
>     .priority(..=100u16)
>     .await?;
> # let _ = (flaky, urgent);
> # Ok(()) }
> ```

`ready_at` accepts `time::OffsetDateTime` bounds — the same type as
`EnqueueBuilder::ready_at`. The client converts each bound to milliseconds
since the Unix epoch on the wire:

> Rust:
>
> ```rust
> # use zizq::{Client, JobStatus};
> # use time::{Duration, OffsetDateTime};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let now = OffsetDateTime::now_utc();
> 
> // Scheduled jobs that are eligible to run by now.
> let due = client
>     .count_jobs()
>     .status([JobStatus::Scheduled])
>     .ready_at(..=now)
>     .await?;
> 
> // Anything ready within the next 24 hours.
> let upcoming = client
>     .count_jobs()
>     .ready_at(now..=(now + Duration::hours(24)))
>     .await?;
> # let _ = (due, upcoming);
> # Ok(()) }
> ```

Each range filter applies independently to its own field, and composes with
the existing `status` / `queue` / `job_type` / `id` / `filter` filters via
AND.

## Updating jobs

`Client::patch_job` updates a single job's mutable fields. Build the change
set with `JobPatch`:

> Rust:
>
> ```rust
> # use zizq::{Client, JobPatch};
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> let job = client
>     .patch_job(id, JobPatch::new().priority(10).retry_limit(5))
>     .await?;
> # let _ = job;
> # Ok(()) }
> ```

A `JobPatch` distinguishes three things per field: set a value, *clear* it to
the server default (`clear_*` methods), or leave it unchanged. `patch_all_jobs`
applies one `JobPatch` to every job matching a filter:

> Rust:
>
> ```rust
> # use zizq::{Client, JobPatch, JobStatus};
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> let moved = client
>     .patch_all_jobs()
>     .status([JobStatus::Scheduled])
>     .queue(["old-queue"])
>     .patch(JobPatch::new().queue("new-queue"))
>     .await?;
> println!("moved {moved} jobs");
> # Ok(()) }
> ```

## Deleting jobs

`delete_job` removes one job; `delete_all_jobs` removes every job matching a
filter — and, **with no filter, every job on the server**:

> Rust:
>
> ```rust
> # use zizq::{Client, JobStatus};
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> client.delete_job(id).await?;
> 
> let removed = client.delete_all_jobs().status([JobStatus::Dead]).await?;
> println!("removed {removed} dead jobs");
> # Ok(()) }
> ```

> [!CAUTION]
> `delete_all_jobs` with no filter deletes everything. A filter set to an
> explicitly empty set (e.g. `.status([])`) instead deletes nothing.

## Error history

Each failed attempt of a job is recorded. `list_errors` pages (or streams)
a job's failure history; `get_error` fetches one attempt:

> Rust:
>
> ```rust
> use futures_util::TryStreamExt;
> # use zizq::Client;
> # async fn run(client: &Client, id: &str) -> Result<(), zizq::ZizqError> {
> let mut errors = client.list_errors(id).stream();
> while let Some(err) = errors.try_next().await? {
>     println!("attempt {}: {}", err.attempt, err.message);
> }
> 
> let first = client.get_error(id, 1).await?;
> # let _ = first;
> # Ok(()) }
> ```

## Server introspection

> Rust:
>
> ```rust
> # use zizq::Client;
> # async fn run(client: &Client) -> Result<(), zizq::ZizqError> {
> client.health().await?;                           // is the server reachable?
> let version = client.server_version().await?;     // the server's version
> let queues = client.list_queues().await?;         // queues currently in use
> # let _ = (version, queues);
> # Ok(()) }
> ```
