// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Minimal jq-compatible dotted-path parser and walker over
//! [`serde_json::Value`].
//!
//! Supports the subset needed to address fields for batched-job
//! configuration and payload hashing:
//!
//! - `.`               — the whole value (no steps)
//! - `.foo`            — object key
//! - `.foo.bar`        — nested keys
//! - `.foo[0]`         — array index
//! - `.[0]`            — root array index
//! - `.["dotted.key"]` — quoted key (escape hatch for keys with dots
//!   or other special characters)
//!
//! Not part of the public API — used internally by the payload-hash
//! helpers that back the derive-generated `unique_key` / `batch`
//! methods.

use std::fmt;

use serde_json::{Map, Value};

/// A single step in a parsed path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathStep {
    /// An object key.
    Field(String),

    /// An array index.
    Index(usize),
}

/// Error returned by [`parse`] when the input isn't a valid jq path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathParseError {
    /// Human-readable description of the failure.
    pub message: String,

    /// Byte offset into the input where the failure was detected.
    pub position: usize,
}

impl fmt::Display for PathParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (at position {})", self.message, self.position)
    }
}

impl std::error::Error for PathParseError {}

/// Parse a jq-compatible dotted path.
///
/// The empty-path input `"."` returns `Ok(vec![])`; every other form
/// yields one [`PathStep`] per addressable segment.
pub(crate) fn parse(input: &str) -> Result<Vec<PathStep>, PathParseError> {
    if input == "." {
        return Ok(Vec::new());
    }
    if input.is_empty() {
        return Err(PathParseError {
            message: "path is empty (use `.` for the root)".into(),
            position: 0,
        });
    }
    let bytes = input.as_bytes();
    if bytes[0] != b'.' {
        return Err(PathParseError {
            message: format!("path must start with `.`, got {:?}", input),
            position: 0,
        });
    }

    let mut steps = Vec::new();
    let mut i = 1;
    let n = bytes.len();

    while i < n {
        match bytes[i] {
            b'[' => {
                i += 1;
                if i < n && bytes[i] == b'"' {
                    // Quoted key — consume until the closing quote,
                    // honoring `\` as an escape for a single following
                    // character (so `\"` and `\\` work).
                    i += 1;
                    let mut name = String::new();
                    while i < n && bytes[i] != b'"' {
                        if bytes[i] == b'\\' && i + 1 < n {
                            name.push(bytes[i + 1] as char);
                            i += 2;
                        } else {
                            // Multi-byte-safe: index via char boundary.
                            let ch = input[i..].chars().next().unwrap();
                            name.push(ch);
                            i += ch.len_utf8();
                        }
                    }
                    if i >= n || bytes[i] != b'"' {
                        return Err(PathParseError {
                            message: "unterminated quoted key".into(),
                            position: i,
                        });
                    }
                    i += 1; // consume closing `"`
                    if i >= n || bytes[i] != b']' {
                        return Err(PathParseError {
                            message: "expected `]` after quoted key".into(),
                            position: i,
                        });
                    }
                    i += 1;
                    steps.push(PathStep::Field(name));
                } else {
                    // Numeric index.
                    let start = i;
                    while i < n && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i == start {
                        return Err(PathParseError {
                            message: "expected digit or quoted key after `[`".into(),
                            position: i,
                        });
                    }
                    if i >= n || bytes[i] != b']' {
                        return Err(PathParseError {
                            message: "expected `]` after array index".into(),
                            position: i,
                        });
                    }
                    let digits = &input[start..i];
                    let index: usize = digits.parse().map_err(|_| PathParseError {
                        message: format!("invalid array index `{digits}`"),
                        position: start,
                    })?;
                    i += 1;
                    steps.push(PathStep::Index(index));
                }
            }
            b'.' => {
                // Follow-on dot before a name or bracket.
                i += 1;
            }
            b if is_name_start(b) => {
                let start = i;
                i += 1;
                while i < n && is_name_cont(bytes[i]) {
                    i += 1;
                }
                steps.push(PathStep::Field(input[start..i].to_string()));
            }
            _ => {
                let ch = input[i..].chars().next().unwrap();
                return Err(PathParseError {
                    message: format!("unexpected character `{ch}`"),
                    position: i,
                });
            }
        }
    }

    Ok(steps)
}

fn is_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_name_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Walk `value` following `path` and return a reference to the
/// sub-value, or `None` if any step misses (missing field,
/// out-of-range index, or a type mismatch along the way).
pub(crate) fn walk<'a>(value: &'a Value, path: &[PathStep]) -> Option<&'a Value> {
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
pub(crate) fn remove(value: &mut Value, path: &[PathStep]) {
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
        (PathStep::Index(idx), Value::Array(arr)) => {
            if *idx < arr.len() {
                arr.remove(*idx);
            }
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
pub(crate) fn pick(source: &Value, paths: &[Vec<PathStep>]) -> Value {
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

    // --- Parser ---

    #[test]
    fn parses_root() {
        assert_eq!(parse(".").unwrap(), Vec::<PathStep>::new());
    }

    #[test]
    fn parses_single_field() {
        assert_eq!(parse(".foo").unwrap(), vec![PathStep::Field("foo".into())]);
    }

    #[test]
    fn parses_nested_fields() {
        assert_eq!(
            parse(".foo.bar.baz").unwrap(),
            vec![
                PathStep::Field("foo".into()),
                PathStep::Field("bar".into()),
                PathStep::Field("baz".into()),
            ],
        );
    }

    #[test]
    fn parses_bracket_index() {
        assert_eq!(
            parse(".foo[0]").unwrap(),
            vec![PathStep::Field("foo".into()), PathStep::Index(0)],
        );
    }

    #[test]
    fn parses_root_index() {
        assert_eq!(parse(".[7]").unwrap(), vec![PathStep::Index(7)]);
    }

    #[test]
    fn parses_multi_digit_index() {
        assert_eq!(parse(".[123]").unwrap(), vec![PathStep::Index(123)]);
    }

    #[test]
    fn parses_chained_indices() {
        assert_eq!(
            parse(".[0][1]").unwrap(),
            vec![PathStep::Index(0), PathStep::Index(1)],
        );
    }

    #[test]
    fn parses_quoted_key_with_dots() {
        assert_eq!(
            parse(r#".["dotted.key"]"#).unwrap(),
            vec![PathStep::Field("dotted.key".into())],
        );
    }

    #[test]
    fn parses_quoted_key_with_escapes() {
        assert_eq!(
            parse(r#".["a\"b"]"#).unwrap(),
            vec![PathStep::Field("a\"b".into())],
        );
    }

    #[test]
    fn parses_field_with_underscore_and_digits() {
        assert_eq!(
            parse(".foo_bar123").unwrap(),
            vec![PathStep::Field("foo_bar123".into())],
        );
    }

    #[test]
    fn rejects_empty_input() {
        let err = parse("").unwrap_err();
        assert!(err.message.contains("empty"), "{err}");
        assert_eq!(err.position, 0);
    }

    #[test]
    fn rejects_input_without_leading_dot() {
        let err = parse("foo").unwrap_err();
        assert!(err.message.contains("start with"), "{err}");
        assert_eq!(err.position, 0);
    }

    #[test]
    fn rejects_field_starting_with_digit() {
        let err = parse(".9foo").unwrap_err();
        assert!(err.message.contains("unexpected"), "{err}");
    }

    #[test]
    fn rejects_unterminated_quoted_key() {
        let err = parse(r#".["missing"#).unwrap_err();
        assert!(err.message.contains("unterminated"), "{err}");
    }

    #[test]
    fn rejects_bracket_without_index_or_key() {
        let err = parse(".[]").unwrap_err();
        assert!(err.message.contains("expected digit"), "{err}");
    }

    #[test]
    fn rejects_missing_closing_bracket() {
        let err = parse(".[0").unwrap_err();
        assert!(err.message.contains("expected `]`"), "{err}");
    }

    // --- Walk ---

    #[test]
    fn walks_to_present_field() {
        let v = json!({ "a": { "b": 42 } });
        let p = parse(".a.b").unwrap();
        assert_eq!(walk(&v, &p), Some(&json!(42)));
    }

    #[test]
    fn walks_to_present_index() {
        let v = json!([10, 20, 30]);
        let p = parse(".[1]").unwrap();
        assert_eq!(walk(&v, &p), Some(&json!(20)));
    }

    #[test]
    fn walk_returns_none_on_missing_field() {
        let v = json!({ "a": 1 });
        let p = parse(".missing").unwrap();
        assert_eq!(walk(&v, &p), None);
    }

    #[test]
    fn walk_returns_none_on_out_of_range_index() {
        let v = json!([10, 20]);
        let p = parse(".[9]").unwrap();
        assert_eq!(walk(&v, &p), None);
    }

    #[test]
    fn walk_returns_none_on_type_mismatch() {
        let v = json!({ "a": 1 });
        let p = parse(".a.b").unwrap(); // .a is a number, can't .b it
        assert_eq!(walk(&v, &p), None);
    }

    #[test]
    fn walk_returns_full_value_on_empty_path() {
        let v = json!({ "a": 1 });
        assert_eq!(walk(&v, &[]), Some(&v));
    }

    #[test]
    fn walk_distinguishes_missing_from_null() {
        let v = json!({ "a": null });
        let p = parse(".a").unwrap();
        assert_eq!(walk(&v, &p), Some(&Value::Null));
        let p_missing = parse(".b").unwrap();
        assert_eq!(walk(&v, &p_missing), None);
    }

    // --- Remove ---

    #[test]
    fn remove_deletes_a_field() {
        let mut v = json!({ "a": 1, "b": 2 });
        remove(&mut v, &parse(".a").unwrap());
        assert_eq!(v, json!({ "b": 2 }));
    }

    #[test]
    fn remove_deletes_a_nested_field() {
        let mut v = json!({ "a": { "b": 1, "c": 2 } });
        remove(&mut v, &parse(".a.b").unwrap());
        assert_eq!(v, json!({ "a": { "c": 2 } }));
    }

    #[test]
    fn remove_deletes_an_array_element() {
        let mut v = json!([10, 20, 30]);
        remove(&mut v, &parse(".[1]").unwrap());
        assert_eq!(v, json!([10, 30]));
    }

    #[test]
    fn remove_is_a_noop_on_missing_path() {
        let mut v = json!({ "a": 1 });
        remove(&mut v, &parse(".b").unwrap());
        assert_eq!(v, json!({ "a": 1 }));
    }

    #[test]
    fn remove_is_a_noop_on_shape_mismatch() {
        let mut v = json!({ "a": 1 });
        remove(&mut v, &parse(".a.b").unwrap()); // .a is a scalar
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
        let paths = vec![parse(".a").unwrap(), parse(".b").unwrap()];
        assert_eq!(pick(&src, &paths), json!({ "a": 1, "b": 2 }));
    }

    #[test]
    fn pick_preserves_nesting() {
        let src = json!({ "user": { "id": 42, "name": "x" } });
        let paths = vec![parse(".user.id").unwrap()];
        assert_eq!(pick(&src, &paths), json!({ "user": { "id": 42 } }));
    }

    #[test]
    fn pick_merges_multiple_paths_into_shared_parent() {
        let src = json!({ "user": { "id": 42, "name": "x", "email": "a@b" } });
        let paths = vec![parse(".user.id").unwrap(), parse(".user.name").unwrap()];
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
        let paths = vec![parse(".a").unwrap(), parse(".missing").unwrap()];
        assert_eq!(pick(&src, &paths), json!({ "a": 1 }));
    }

    #[test]
    fn pick_with_no_matches_returns_empty_object() {
        let src = json!({ "a": 1 });
        let paths = vec![parse(".x").unwrap(), parse(".y").unwrap()];
        assert_eq!(pick(&src, &paths), json!({}));
    }

    #[test]
    fn pick_handles_array_indices() {
        let src = json!({ "arr": [10, 20, 30] });
        let paths = vec![parse(".arr[0]").unwrap(), parse(".arr[2]").unwrap()];
        // Indices preserved by position in the reconstructed array.
        assert_eq!(pick(&src, &paths), json!({ "arr": [10, null, 30] }));
    }
}
