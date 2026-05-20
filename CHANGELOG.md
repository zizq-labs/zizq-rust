# Changelog

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
