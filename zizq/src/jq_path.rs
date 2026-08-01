// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Walker over [`serde_json::Value`] addressed by a pre-parsed
//! sequence of [`PathStep`]s (fields + indices).
//!
//! The path grammar is jq-compatible (`.foo`, `.foo.bar`, `.foo[0]`,
//! `.[0]`, `.["dotted.key"]`), but this crate no longer contains a
//! parser — every path a user writes is parsed at derive expansion
//! time inside `zizq-derive`, which then emits the `PathStep`
//! sequence directly. This module supplies only the runtime
//! traversal primitives.
//!
//! Not part of the public API — used internally by the payload-hash
//! helpers that back the derive-generated `unique_key` / `batch`
//! methods.

use serde_json::{Map, Value};

/// A single step in a resolved path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathStep {
    /// An object key.
    Field(String),

    /// An array index.
    Index(usize),
}

/// Walk `value` following `path` and return a reference to the
/// sub-value, or `None` if any step misses (missing field,
/// out-of-range index, or a type mismatch along the way).
pub fn walk<'a>(value: &'a Value, path: &[PathStep]) -> Option<&'a Value> {
    let mut cur = value;
    for step in path {
        cur = match (step, cur) {
            (PathStep::Field(name), Value::Object(map)) => map.get(name)?,
            (PathStep::Index(idx), Value::Array(arr)) => arr.get(*idx)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Remove the sub-value at `path` from `value` in place.
///
/// An empty path (`.`) is treated as "the whole payload is
/// excluded" — the value is replaced with `Value::Null` so the
/// caller still produces a stable digest. Paths that don't fully
/// match are silent no-ops (mirroring jq's behaviour for missing
/// keys).
pub fn remove(value: &mut Value, path: &[PathStep]) {
    if path.is_empty() {
        *value = Value::Null;
        return;
    }

    // Walk to the parent of the last step. Bail out silently if any
    // hop doesn't fit the value's shape.
    let mut cur = value;
    for step in &path[..path.len() - 1] {
        cur = match (step, cur) {
            (PathStep::Field(name), Value::Object(map)) => match map.get_mut(name) {
                Some(next) => next,
                None => return,
            },
            (PathStep::Index(idx), Value::Array(arr)) => match arr.get_mut(*idx) {
                Some(next) => next,
                None => return,
            },
            _ => return,
        };
    }

    // Delete the final step.
    match (path.last().unwrap(), cur) {
        (PathStep::Field(name), Value::Object(map)) => {
            map.remove(name);
        }
        (PathStep::Index(idx), Value::Array(arr)) if *idx < arr.len() => {
            arr.remove(*idx);
        }
        _ => {}
    }
}

/// Return a new [`Value`] containing only the sub-values from `source`
/// at the given paths, preserving the nesting each path implies.
///
/// - An empty path (`.`) short-circuits to `source.clone()` — the
///   whole payload is picked.
/// - Paths that don't exist in `source` are silently skipped.
/// - When no path matches, the result is an empty object (`{}`), so
///   the caller always gets a well-defined value to hash.
pub fn pick(source: &Value, paths: &[Vec<PathStep>]) -> Value {
    let mut target = Value::Null;
    for steps in paths {
        if steps.is_empty() {
            return source.clone();
        }
        if let Some(v) = walk(source, steps) {
            set_path(&mut target, steps, v.clone());
        }
    }
    if target.is_null() {
        Value::Object(Map::new())
    } else {
        target
    }
}

/// Write `value` into `target` at `path`, creating intermediate
/// objects/arrays as needed. The container chosen at each level
/// depends on whether the *next* step is a field or an index.
fn set_path(target: &mut Value, path: &[PathStep], value: Value) {
    debug_assert!(!path.is_empty(), "set_path called with an empty path");

    // Initialize the root container from the shape of the first step,
    // but only if the caller handed us a null target.
    if target.is_null() {
        *target = container_for(&path[0]);
    }

    // Walk to the parent of the final step, creating intermediates.
    let mut cur = target;
    for i in 0..path.len() - 1 {
        let child_init = container_for(&path[i + 1]);
        cur = match (&path[i], cur) {
            (PathStep::Field(name), Value::Object(map)) => {
                map.entry(name.clone()).or_insert(child_init)
            }
            (PathStep::Index(idx), Value::Array(arr)) => {
                while arr.len() <= *idx {
                    arr.push(Value::Null);
                }
                if arr[*idx].is_null() {
                    arr[*idx] = child_init;
                }
                &mut arr[*idx]
            }
            _ => return, // shape mismatch — silently abandon
        };
    }

    // Set the leaf.
    match (path.last().unwrap(), cur) {
        (PathStep::Field(name), Value::Object(map)) => {
            map.insert(name.clone(), value);
        }
        (PathStep::Index(idx), Value::Array(arr)) => {
            while arr.len() <= *idx {
                arr.push(Value::Null);
            }
            arr[*idx] = value;
        }
        _ => {}
    }
}

fn container_for(step: &PathStep) -> Value {
    match step {
        PathStep::Field(_) => Value::Object(Map::new()),
        PathStep::Index(_) => Value::Array(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Tiny helpers for readable inline path construction. The runtime
    // parser lives in `zizq-derive`; tests here operate on already-
    // parsed `PathStep` values, so we spell them out directly.

    fn field(name: &str) -> PathStep {
        PathStep::Field(name.into())
    }

    fn index(i: usize) -> PathStep {
        PathStep::Index(i)
    }

    // --- Walk ---

    #[test]
    fn walks_to_present_field() {
        let v = json!({ "a": { "b": 42 } });
        assert_eq!(walk(&v, &[field("a"), field("b")]), Some(&json!(42)));
    }

    #[test]
    fn walks_to_present_index() {
        let v = json!([10, 20, 30]);
        assert_eq!(walk(&v, &[index(1)]), Some(&json!(20)));
    }

    #[test]
    fn walk_returns_none_on_missing_field() {
        let v = json!({ "a": 1 });
        assert_eq!(walk(&v, &[field("missing")]), None);
    }

    #[test]
    fn walk_returns_none_on_out_of_range_index() {
        let v = json!([10, 20]);
        assert_eq!(walk(&v, &[index(9)]), None);
    }

    #[test]
    fn walk_returns_none_on_type_mismatch() {
        // `.a` is a number — can't `.b` into it.
        let v = json!({ "a": 1 });
        assert_eq!(walk(&v, &[field("a"), field("b")]), None);
    }

    #[test]
    fn walk_returns_full_value_on_empty_path() {
        let v = json!({ "a": 1 });
        assert_eq!(walk(&v, &[]), Some(&v));
    }

    #[test]
    fn walk_distinguishes_missing_from_null() {
        let v = json!({ "a": null });
        assert_eq!(walk(&v, &[field("a")]), Some(&Value::Null));
        assert_eq!(walk(&v, &[field("b")]), None);
    }

    // --- Remove ---

    #[test]
    fn remove_deletes_a_field() {
        let mut v = json!({ "a": 1, "b": 2 });
        remove(&mut v, &[field("a")]);
        assert_eq!(v, json!({ "b": 2 }));
    }

    #[test]
    fn remove_deletes_a_nested_field() {
        let mut v = json!({ "a": { "b": 1, "c": 2 } });
        remove(&mut v, &[field("a"), field("b")]);
        assert_eq!(v, json!({ "a": { "c": 2 } }));
    }

    #[test]
    fn remove_deletes_an_array_element() {
        let mut v = json!([10, 20, 30]);
        remove(&mut v, &[index(1)]);
        assert_eq!(v, json!([10, 30]));
    }

    #[test]
    fn remove_is_a_noop_on_missing_path() {
        let mut v = json!({ "a": 1 });
        remove(&mut v, &[field("b")]);
        assert_eq!(v, json!({ "a": 1 }));
    }

    #[test]
    fn remove_is_a_noop_on_shape_mismatch() {
        // `.a` is a scalar — `.a.b` doesn't fit.
        let mut v = json!({ "a": 1 });
        remove(&mut v, &[field("a"), field("b")]);
        assert_eq!(v, json!({ "a": 1 }));
    }

    #[test]
    fn remove_root_replaces_value_with_null() {
        let mut v = json!({ "a": 1 });
        remove(&mut v, &[]);
        assert_eq!(v, Value::Null);
    }

    // --- Pick ---

    #[test]
    fn pick_selects_top_level_fields() {
        let src = json!({ "a": 1, "b": 2, "c": 3 });
        let paths = vec![vec![field("a")], vec![field("b")]];
        assert_eq!(pick(&src, &paths), json!({ "a": 1, "b": 2 }));
    }

    #[test]
    fn pick_preserves_nesting() {
        let src = json!({ "user": { "id": 42, "name": "x" } });
        let paths = vec![vec![field("user"), field("id")]];
        assert_eq!(pick(&src, &paths), json!({ "user": { "id": 42 } }));
    }

    #[test]
    fn pick_merges_multiple_paths_into_shared_parent() {
        let src = json!({ "user": { "id": 42, "name": "x", "email": "a@b" } });
        let paths = vec![
            vec![field("user"), field("id")],
            vec![field("user"), field("name")],
        ];
        assert_eq!(
            pick(&src, &paths),
            json!({ "user": { "id": 42, "name": "x" } }),
        );
    }

    #[test]
    fn pick_root_short_circuits_to_full_source() {
        let src = json!({ "a": 1, "b": 2 });
        assert_eq!(pick(&src, &[Vec::new()]), src);
    }

    #[test]
    fn pick_skips_missing_paths_silently() {
        let src = json!({ "a": 1 });
        let paths = vec![vec![field("a")], vec![field("missing")]];
        assert_eq!(pick(&src, &paths), json!({ "a": 1 }));
    }

    #[test]
    fn pick_with_no_matches_returns_empty_object() {
        let src = json!({ "a": 1 });
        let paths = vec![vec![field("x")], vec![field("y")]];
        assert_eq!(pick(&src, &paths), json!({}));
    }

    #[test]
    fn pick_handles_array_indices() {
        let src = json!({ "arr": [10, 20, 30] });
        let paths = vec![vec![field("arr"), index(0)], vec![field("arr"), index(2)]];
        // Indices preserved by position in the reconstructed array.
        assert_eq!(pick(&src, &paths), json!({ "arr": [10, null, 30] }));
    }
}
