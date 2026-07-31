// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! End-to-end tests for `#[derive(JobKind)]`.
//!
//! The whole file compiles only when the `derive` feature is on —
//! CI's `--no-default-features` runs skip it. Each test exercises
//! one or more `#[zizq(...)]` attributes through the observable
//! behaviour of the generated impl.

#![cfg(feature = "derive")]

use serde::{Deserialize, Serialize};
use zizq::JobKind;

#[test]
fn defaults_come_from_the_trait_when_no_attrs_are_set() {
    #[derive(Serialize, Deserialize, JobKind)]
    struct SendEmail {
        _to: String,
    }

    // NAME falls back to the struct's identifier.
    assert_eq!(SendEmail::NAME, "SendEmail");
    // Everything else comes through unchanged from the trait's own
    // default associated consts.
    assert_eq!(SendEmail::QUEUE, "default");
    assert_eq!(SendEmail::PRIORITY, None);
    assert_eq!(SendEmail::RETRY_LIMIT, None);
}

#[test]
fn name_attribute_overrides_the_struct_ident() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(name = "send.email")]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::NAME, "send.email");
}

#[test]
fn queue_attribute_sets_the_queue_const() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(queue = "emails")]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::QUEUE, "emails");
}

#[test]
fn priority_attribute_sets_the_priority_const() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(priority = 100)]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::PRIORITY, Some(100));
}

#[test]
fn retry_limit_attribute_sets_the_retry_limit_const() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(retry_limit = 5)]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::RETRY_LIMIT, Some(5));
}

#[test]
fn all_four_attributes_compose_in_a_single_annotation() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(name = "send.email", queue = "emails", priority = 100, retry_limit = 3)]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::NAME, "send.email");
    assert_eq!(SendEmail::QUEUE, "emails");
    assert_eq!(SendEmail::PRIORITY, Some(100));
    assert_eq!(SendEmail::RETRY_LIMIT, Some(3));
}

#[test]
fn stacked_zizq_attributes_are_merged() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(name = "send.email")]
    #[zizq(queue = "emails")]
    #[zizq(priority = 50)]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(SendEmail::NAME, "send.email");
    assert_eq!(SendEmail::QUEUE, "emails");
    assert_eq!(SendEmail::PRIORITY, Some(50));
}
