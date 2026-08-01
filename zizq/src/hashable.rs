// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Payload-to-hashable transformations used by the derive-generated
//! `unique_key` and `batch` methods.
//!
//! Neither function hashes anything itself — each returns a
//! [`serde_json::Value`] that the caller then hands to
//! [`UniqueKey::tagged_hash_of`] or [`UniqueKey::hash_of`] to produce
//! the final key. Keeping the two concerns separate lets the derive
//! decide whether to prefix the type name (the `prefix` attribute)
//! without this module needing to know.
//!
//! Not part of the public API — used internally by the payload-hash
//! path the derive emits.
//!
//! [`UniqueKey::tagged_hash_of`]: crate::UniqueKey::tagged_hash_of
//! [`UniqueKey::hash_of`]: crate::UniqueKey::hash_of

use serde::Serialize;
use serde_json::Value;

use crate::jq_path::{self, PathStep};

/// Serialise `payload` and return a copy with each of `paths`
/// removed.
///
/// Empty `paths` returns the payload as-is. A path of `.` in the
/// list removes the whole payload, replacing it with
/// [`Value::Null`] (`.remove` handles that case for us). Paths that
/// don't exist on the payload are silently skipped, matching jq's
/// behaviour for missing keys.
///
/// The two-pass model (serialise, then walk-and-remove) means path
/// syntax uses serialised field names, so `#[serde(rename = "x")]`
/// and container-level `rename_all` are honoured automatically —
/// the payload has already been rendered by the time we look at it.
pub fn payload_except(payload: &impl Serialize, paths: &[Vec<PathStep>]) -> Value {
    let mut value =
        serde_json::to_value(payload).expect("payload should serialize to serde_json::Value");
    for path in paths {
        jq_path::remove(&mut value, path);
    }
    value
}

/// Serialise `payload` and return a new [`Value`] containing only
/// the sub-values at the given paths, preserving the nesting each
/// path implies.
///
/// - Empty `paths` returns an empty object (`{}`) — nothing was
///   picked, so nothing gets hashed.
/// - A `.` in the list short-circuits to the full payload.
/// - Paths that don't exist on the payload are silently skipped.
///
/// The serialise-first model gives free `#[serde(rename = ...)]`
/// interop for the same reason [`payload_except`] does.
pub fn payload_only(payload: &impl Serialize, paths: &[Vec<PathStep>]) -> Value {
    let value =
        serde_json::to_value(payload).expect("payload should serialize to serde_json::Value");
    jq_path::pick(&value, paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unique_key::UniqueKey;
    use serde::Serialize;
    use serde_json::json;

    #[derive(Serialize)]
    struct Push {
        device_ids: Vec<String>,
        platform: String,
        tenant_id: u64,
    }

    // Local helpers for readable inline path construction. The
    // runtime parser lives in `zizq-derive` and only runs at derive
    // expansion, so tests here build `PathStep` values directly.

    fn field(name: &str) -> PathStep {
        PathStep::Field(name.into())
    }

    fn paths<const N: usize>(paths: [Vec<PathStep>; N]) -> Vec<Vec<PathStep>> {
        paths.into_iter().collect()
    }

    // --- payload_except ---

    #[test]
    fn except_with_empty_paths_returns_full_payload() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_except(&p, &[]),
            json!({
                "device_ids": ["a"],
                "platform": "apple",
                "tenant_id": 42,
            }),
        );
    }

    #[test]
    fn except_removes_a_top_level_field() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_except(&p, &paths([vec![field("device_ids")]])),
            json!({ "platform": "apple", "tenant_id": 42 }),
        );
    }

    #[test]
    fn except_removes_multiple_fields() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_except(
                &p,
                &paths([vec![field("device_ids")], vec![field("tenant_id")]])
            ),
            json!({ "platform": "apple" }),
        );
    }

    #[test]
    fn except_root_path_yields_null() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        // Root path (empty step list) — the "whole payload" exclusion.
        assert_eq!(payload_except(&p, &paths([Vec::new()])), Value::Null);
    }

    #[test]
    fn except_skips_missing_paths_silently() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_except(
                &p,
                &paths([vec![field("not_there")], vec![field("device_ids")]])
            ),
            json!({ "platform": "apple", "tenant_id": 42 }),
        );
    }

    // --- payload_only ---

    #[test]
    fn only_with_empty_paths_returns_empty_object() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(payload_only(&p, &[]), json!({}));
    }

    #[test]
    fn only_picks_a_top_level_field() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_only(&p, &paths([vec![field("platform")]])),
            json!({ "platform": "apple" }),
        );
    }

    #[test]
    fn only_picks_multiple_fields() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_only(
                &p,
                &paths([vec![field("platform")], vec![field("tenant_id")]])
            ),
            json!({ "platform": "apple", "tenant_id": 42 }),
        );
    }

    #[test]
    fn only_root_path_short_circuits_to_full_payload() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        // Root path (empty step list) — the "whole payload" pick.
        assert_eq!(
            payload_only(&p, &paths([Vec::new()])),
            json!({
                "device_ids": ["a"],
                "platform": "apple",
                "tenant_id": 42,
            }),
        );
    }

    #[test]
    fn only_preserves_nested_structure() {
        #[derive(Serialize)]
        struct Nested {
            user: User,
            note: String,
        }
        #[derive(Serialize)]
        struct User {
            id: u64,
            name: String,
        }
        let n = Nested {
            user: User {
                id: 42,
                name: "alice".into(),
            },
            note: "irrelevant".into(),
        };
        assert_eq!(
            payload_only(&n, &paths([vec![field("user"), field("id")]])),
            json!({ "user": { "id": 42 } }),
        );
    }

    #[test]
    fn only_skips_missing_paths_silently() {
        let p = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        assert_eq!(
            payload_only(
                &p,
                &paths([vec![field("not_there")], vec![field("platform")]])
            ),
            json!({ "platform": "apple" }),
        );
    }

    // --- Round-trip with UniqueKey ---
    //
    // The whole point of these helpers is to feed the result to
    // UniqueKey::hash_of / tagged_hash_of. Check that (a) the pipe
    // works end-to-end and (b) equivalent-except transformations
    // produce equivalent keys.

    #[test]
    fn hashes_via_unique_key_are_stable_for_equivalent_inputs() {
        let a = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        let b = Push {
            device_ids: vec!["b".into(), "c".into()], // different batch data
            platform: "apple".into(),
            tenant_id: 42,
        };
        // Excluding the batch path yields the same hashable → same key
        // — this is the property the batch-key derivation relies on.
        let key_a = UniqueKey::hash_of(payload_except(&a, &paths([vec![field("device_ids")]]))).key;
        let key_b = UniqueKey::hash_of(payload_except(&b, &paths([vec![field("device_ids")]]))).key;
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn hashes_via_unique_key_differ_when_key_fields_differ() {
        let apple = Push {
            device_ids: vec!["a".into()],
            platform: "apple".into(),
            tenant_id: 42,
        };
        let android = Push {
            device_ids: vec!["a".into()],
            platform: "android".into(),
            tenant_id: 42,
        };
        let key_apple =
            UniqueKey::hash_of(payload_except(&apple, &paths([vec![field("device_ids")]]))).key;
        let key_android = UniqueKey::hash_of(payload_except(
            &android,
            &paths([vec![field("device_ids")]]),
        ))
        .key;
        assert_ne!(key_apple, key_android);
    }
}
