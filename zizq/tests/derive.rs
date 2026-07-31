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
use zizq::{BackoffConfig, JobKind, RetentionConfig, UniqueKey, UniqueScope};

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
fn backoff_attribute_emits_full_config() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(backoff(base_ms = 1000, exponent = 2.0, jitter_ms = 500))]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(
        SendEmail::BACKOFF,
        Some(BackoffConfig {
            base_ms: 1000,
            exponent: 2.0,
            jitter_ms: 500,
        }),
    );
}

#[test]
fn numeric_fields_accept_const_evaluable_expressions() {
    // Priority is fixed; retention/backoff use arithmetic for readability.
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(
        priority = 50 + 50,
        retry_limit = 5 * 2,
        backoff(base_ms = 5 * 200, exponent = 1.5 + 0.5, jitter_ms = 100 * 5),
        retention(completed_ms = 60 * 1000, dead_ms = 7 * 24 * 60 * 60 * 1000),
    )]
    struct WithMath {
        _x: u32,
    }
    assert_eq!(WithMath::PRIORITY, Some(100));
    assert_eq!(WithMath::RETRY_LIMIT, Some(10));
    assert_eq!(
        WithMath::BACKOFF,
        Some(BackoffConfig {
            base_ms: 1000,
            exponent: 2.0,
            jitter_ms: 500,
        }),
    );
    assert_eq!(
        WithMath::RETENTION,
        Some(RetentionConfig {
            completed_ms: Some(60_000),
            dead_ms: Some(604_800_000), // one week
        }),
    );
}

#[test]
fn retention_with_both_fields_emits_full_config() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(retention(completed_ms = 60_000, dead_ms = 86_400_000))]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(
        SendEmail::RETENTION,
        Some(RetentionConfig {
            completed_ms: Some(60_000),
            dead_ms: Some(86_400_000),
        }),
    );
}

#[test]
fn retention_with_only_completed_ms_leaves_dead_ms_none() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(retention(completed_ms = 60_000))]
    struct SendEmail {
        _to: String,
    }
    assert_eq!(
        SendEmail::RETENTION,
        Some(RetentionConfig {
            completed_ms: Some(60_000),
            dead_ms: None,
        }),
    );
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

// --- unique ---

#[test]
fn bare_unique_hashes_whole_payload_with_type_tag() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique)]
    struct SendEmail {
        to: String,
        subject: String,
    }
    let job = SendEmail {
        to: "alice@example.com".into(),
        subject: "hi".into(),
    };
    let derived = job.unique_key().expect("unique_key returned Some");
    // Compare against a hand-rolled tagged_hash_of on the whole payload.
    let expected = UniqueKey::tagged_hash_of(SendEmail::NAME, &job);
    assert_eq!(derived.key, expected.key);
    // No scope was set — the trait's default (`Queued`) applies via `None`.
    assert!(derived.scope.is_none());
}

#[test]
fn unique_with_scope_carries_scope_through() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique(scope = "active"))]
    struct SendEmail {
        to: String,
    }
    let key = SendEmail { to: "a@b".into() }.unique_key().unwrap();
    assert_eq!(key.scope, Some(UniqueScope::Active));
}

#[test]
fn unique_only_hashes_a_subset_of_fields() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique(only = [".user_id"]))]
    struct SendEmail {
        user_id: u64,
        body: String,
    }
    // Two jobs with the same `user_id` but different `body` should
    // produce the same key.
    let a = SendEmail {
        user_id: 42,
        body: "hello".into(),
    };
    let b = SendEmail {
        user_id: 42,
        body: "goodbye".into(),
    };
    assert_eq!(a.unique_key().unwrap().key, b.unique_key().unwrap().key);
    // A different `user_id` gives a different key.
    let c = SendEmail {
        user_id: 99,
        body: "hello".into(),
    };
    assert_ne!(a.unique_key().unwrap().key, c.unique_key().unwrap().key);
}

#[test]
fn unique_except_hashes_everything_but_named_fields() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique(except = [".body"]))]
    struct SendEmail {
        user_id: u64,
        body: String,
    }
    // Two jobs differing only in `body` (which is excluded) collide.
    let a = SendEmail {
        user_id: 42,
        body: "x".into(),
    };
    let b = SendEmail {
        user_id: 42,
        body: "y".into(),
    };
    assert_eq!(a.unique_key().unwrap().key, b.unique_key().unwrap().key);
}

#[test]
fn unique_prefix_false_drops_the_type_name_tag() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique(prefix = false))]
    struct SendEmail {
        to: String,
    }
    let job = SendEmail { to: "a@b".into() };
    let derived = job.unique_key().unwrap();
    // With prefix off, the emitted call is UniqueKey::hash_of(&self)
    // — verify by comparing against a hand-rolled call.
    let expected = UniqueKey::hash_of(&job);
    assert_eq!(derived.key, expected.key);
    // And critically: it's NOT the tagged form.
    let tagged = UniqueKey::tagged_hash_of(SendEmail::NAME, &job);
    assert_ne!(derived.key, tagged.key);
}

#[test]
fn unique_composes_only_scope_and_prefix() {
    #[derive(Serialize, Deserialize, JobKind)]
    #[zizq(unique(only = [".platform"], scope = "exists", prefix = false))]
    struct Push {
        device_ids: Vec<String>,
        platform: String,
    }
    let job = Push {
        device_ids: vec!["a".into(), "b".into()],
        platform: "apple".into(),
    };
    let key = job.unique_key().unwrap();
    assert_eq!(key.scope, Some(UniqueScope::Exists));
    // Hash must equal `hash_of(payload_only(&job, [".platform"]))`
    // which — because only the platform is in the picked subset —
    // is the hash of `{"platform": "apple"}`.
    let expected = UniqueKey::hash_of(&serde_json::json!({ "platform": "apple" }));
    assert_eq!(key.key, expected.key);
}
