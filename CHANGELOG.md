# Changelog

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
