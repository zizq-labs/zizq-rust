// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! End-to-end sanity check for `#[derive(JobKind)]`.
//!
//! The whole file compiles only when the `derive` feature is on —
//! CI's `--no-default-features` runs then skip it. Later commits
//! add coverage for each `#[zizq(...)]` attribute as it's parsed
//! by the derive.

#![cfg(feature = "derive")]

use serde::{Deserialize, Serialize};
use zizq::JobKind;

#[test]
fn derive_provides_default_name_from_struct_ident() {
    #[derive(Serialize, Deserialize, JobKind)]
    struct SendEmail {
        _to: String,
    }

    // Skeleton derive uses the struct's identifier as NAME. When
    // attribute parsing lands, `#[zizq(name = "...")]` will
    // override this.
    assert_eq!(SendEmail::NAME, "SendEmail");
    // Trait defaults come through the derived impl unchanged.
    assert_eq!(SendEmail::QUEUE, "default");
    assert_eq!(SendEmail::PRIORITY, None);
    assert_eq!(SendEmail::RETRY_LIMIT, None);
}
