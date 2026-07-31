# zizq-derive

Proc-macro companion for the [`zizq`](https://crates.io/crates/zizq)
crate — hosts `#[derive(JobKind)]` and any future zizq derives.

**You should not depend on this crate directly.** It is re-exported
from the main `zizq` crate behind its `derive` feature (enabled by
default). Add `zizq` to your `Cargo.toml` and the derive comes along:

```toml
[dependencies]
zizq = "0.6"
```

See the [`zizq` docs](https://docs.rs/zizq) for usage. This crate
exists as a separate publish target because Rust proc-macros must
live in their own `crate-type = "proc-macro"` library.
