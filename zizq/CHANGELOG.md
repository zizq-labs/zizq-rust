# Changelog

## 0.7.0 (Unreleased)

### Added

- **A group-level cron timezone.** `ReplaceCronBuilder::timezone`
  sets an IANA timezone on the whole schedule, applied to every entry
  that does not set its own with `CronEntry::timezone`:

      let group = client
          .replace_cron("maintenance")
          .timezone("Australia/Melbourne")
          .entry(CronEntry::new("cleanup", "0 0 * * *", client.enqueue(Cleanup)))
          .entry(
              CronEntry::new("digest", "0 6 * * *", client.enqueue(Digest))
                  .timezone("UTC"),
          )
          .await?;

  With neither set, expressions are evaluated in the server's local
  timezone, as before.

  It is stored on the server as the group's timezone rather than
  copied onto each entry, so the new `CronGroup::timezone` field
  still reports it after a `get_cron`. Because `replace_cron`
  replaces the group in full, omitting `.timezone(...)` clears
  whatever the group had.

  Needs Zizq 0.7.0 or newer on the server. Against an older server
  the field is ignored, and `CronGroup::timezone` is always `None`.


## 0.6.0

### Added

- **`#[derive(JobKind)]`** — a proc-macro that generates the
  `JobKind` impl from declarative `#[zizq(...)]` attributes on the
  payload struct, replacing the manual `impl JobKind for MyJob { ... }`
  block for the common cases. Lives in a companion `zizq-derive`
  crate and is re-exported from `zizq` behind the `derive` feature
  (enabled by default; opt out with `default-features = false` if
  you want to skip the proc-macro compile-time cost).

  Every associated const has an attribute — `name`, `queue`,
  `priority`, `retry_limit`, `backoff(...)`, `retention(...)` —
  plus attributes for `unique(...)` (generating `fn unique_key`) and
  `batch(...)` (generating `fn batch`). Numeric fields accept
  const-evaluable expressions, so
  `retention(dead_ms = 7 * 24 * 60 * 60 * 1000)` works. Path
  arguments (jq-compatible dotted paths) are validated at derive
  expansion time — malformed paths surface as compile errors with a
  caret on the offending literal, not runtime panics.

      use zizq::JobKind;

      #[derive(serde::Serialize, serde::Deserialize, JobKind)]
      #[zizq(
          name = "send_email",
          queue = "emails",
          priority = 100,
          retry_limit = 3,
          unique(only = [".user_id"], scope = "active"),
      )]
      struct SendEmail {
          user_id: u64,
          body: String,
      }

  Manual `impl JobKind` still works for cases that don't fit the
  attribute grammar — dynamic keys, computed defaults, or generic
  jobs. The derive is the recommended path for the 90% case.

- **Batched jobs** — a new server-side folding mechanism, gated behind
  a Zizq Pro license on the server. Successive enqueues that share a
  batch key are folded into a single pending job via configurable jq
  `when` / `fold` expressions, so the worker eventually claims one job
  with a combined payload instead of many separate jobs. Useful for
  push notifications, bulk email, audit-log ingestion, or any
  downstream API that prefers batches.

  Configure per-job via a new `JobKind::batch` trait method or
  per-enqueue via `EnqueueBuilder::batch`:

      use zizq::{BatchConfig, JobKind, UniqueKey};

      struct PushNotifications {
          device_ids: Vec<String>,
          platform: String,
      }

      impl JobKind for PushNotifications {
          const NAME: &'static str = "push.notifications";

          fn batch(&self) -> Option<BatchConfig> {
              let key = UniqueKey::tagged_hash_of(Self::NAME, &self.platform).key;
              Some(BatchConfig::at(".device_ids", 100).keyed_by(key))
          }
      }

  `BatchConfig::at(path, limit).keyed_by(key)` is a fluent builder for
  the common "cap a jq path at N entries" case, with `.dedup()` and
  `.sorted()` modifiers that switch the generated `fold` to `| unique`
  or `| sort` respectively. `BatchConfig` can also be constructed
  directly for full control over `when` / `fold`.

  Responses to enqueue calls now include a `folded: Option<bool>`
  flag on `Job`, mirroring the existing `duplicate` flag. Fetching
  a job also returns the stored `batch: Option<BatchConfig>` for
  visibility into what the server will evaluate on subsequent folds.

  Full docs: [Batched Jobs](https://zizq.io/docs/clients/rust/batched-jobs.html).


## 0.5.0

### Added

- **Three new range filters** on `ListJobsBuilder`, `CountJobsBuilder`,
  `DeleteJobsBuilder`, and `PatchJobsBuilder`: `priority`, `ready_at`,
  and `attempts`. Each setter accepts an exact value or one of the
  inclusive Rust range syntaxes:

      client.list_jobs()
          .priority(50u16)              // exact match
          .ready_at(now..=tomorrow)     // bounded
          .attempts(1u32..)             // 1 or more
          .await?;

  The exclusive half-open form `a..b` is **deliberately not accepted** —
  there is no `From<Range<T>>` impl for `RangeFilter<T>`, so the
  compiler rejects it at the call site. The server only supports
  inclusive bounds, and accepting `a..b` would silently shift the
  upper bound by one. Use `a..=b - 1` explicitly if that is what you
  meant.

- **`RangeFilter<T>`** — new public enum in the prelude. Most callers
  never name it directly because each setter takes
  `impl Into<RangeFilter<T>>`, but it is exported for users who want
  to construct one programmatically or store one in a struct field.
  `From` impls cover bare values (`u16`, `u32`, `u64`,
  `time::OffsetDateTime`) and the inclusive range types
  (`RangeInclusive`, `RangeFrom`, `RangeToInclusive`, `RangeFull`).

- `ready_at` bounds accept `time::OffsetDateTime` — consistent with
  `EnqueueBuilder::ready_at`. The client converts each bound to
  milliseconds since the Unix epoch on the wire.

### Requires

- Zizq server **0.5.0** or later. Older servers will reject requests
  that include any of the new query parameters with `400 Bad Request`.

## 0.4.0

### Added

- **`Client::delete_all_crons`** — `DELETE /crons`. Wipes every cron
  group on the server in a single call and returns the number of
  groups removed. Pro-only.
- **`Client::reset`** — `POST /reset`. Wipes every cron group and
  every job in one request. Primarily intended as a setup/teardown
  step for test suites that want a known-empty server between
  scenarios. The integration suite's `fresh()` helper now uses this
  instead of `delete_all_jobs`. Also available as
  `Client::erase_all_data`.

### Requires

- Zizq server **0.4.0** or later for the new endpoints.

## 0.3.3

### Added

- **`ClientBuilder::stream_idle_timeout`** — separate per-read timeout
  for the long-lived `/jobs/take` stream consumed by `Worker`. Defaults
  to 30 seconds; reset by each frame received, so the server's heartbeats
  keep it alive while only genuinely dead connections (NAT rebind,
  firewall conntrack expiry, etc.) trigger a reconnect. Normal API traffic
  continues to use `read_timeout` (also 30s default), so it can be
  tightened independently without affecting the take stream.

### Changed

- The HTTP/1.1 take-stream pool no longer reuses `read_timeout`; its
  per-read timeout is now `stream_idle_timeout`. Defaults are unchanged
  (both 30s), so behaviour is preserved unless explicitly tuned.


## 0.3.2

### Fixed

- **Zero-sized payloads (`struct Foo;`) now encode correctly over
  MessagePack.** Previously, rmp-serde's default representation of a
  unit struct was an empty fixarray, which the server stored as
  `Value::Array([])` and which then failed to round-trip back into
  the unit struct on the worker side (`expected unit struct, got
  sequence`). The encoded form also broke cross-language interop —
  Ruby/Node consumers expected `null`/`{}` for "no payload" jobs.
  The client now wraps ZST payloads in a thin `Serializer` shim that
  re-emits `serialize_unit_struct` as `serialize_unit`, producing
  `nil`/`null` on the wire. Non-ZST payloads (the common case) take
  the existing fast path with zero wrapper overhead.


## 0.3.1

### Added

- **`Router::with_state` and the `State<S>` extractor.** Routers can now
  thread shared state (database pool, API clients, config) through every
  handler — handlers built on a stateful router take `State<S>` as their
  first argument, axum-style. Stateless handlers (`Fn(T)`) remain valid
  on both stateless and stateful routers, so existing code keeps
  compiling. Sub-state projection (FromRef-style) is deferred — for now,
  combine slices into one struct and destructure inside each handler.

### Changed

- **Handler error bound relaxed.** Handlers passed to `Router::route` and
  the `JobHandler` blanket impl no longer require
  `E: Error + Send + Sync + 'static`. The bound is now
  `E: Into<Box<dyn Error + Send + Sync + 'static>>`, so
  `Box<dyn Error + Send + Sync>` and `anyhow::Error` work directly without
  an intermediate wrapper struct. Existing handlers with typed errors keep
  the same captured `type_name` in `HandlerError` — backwards compatible
  for any handler that compiles today.


## 0.3.0

- Initial release
- Async client using `tokio` and `reqwest`
- `JobKind` trait for per-type defaults
- Long-running `Worker` with bounded concurrency
- `Router` for mapping `JobKind` to handlers
- Bulk acknowledgment batching
- Job introspection and management
- Unique jobs
- Cron scheduling
- TLS and mutual TLS support (features `rustls-tls`, `native-tls`)
