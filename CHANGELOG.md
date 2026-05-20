# Changelog

## 0.3.1

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
