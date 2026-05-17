// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Helpers for generating server-side jq filter expressions from
//! typed values.
//!
//! The listing endpoints take a jq expression via `.filter(...)`
//! ([`ListJobsBuilder`], [`CountJobsBuilder`], and the bulk
//! delete / patch builders), evaluated against each job's payload.
//! Writing jq by hand is easy to get wrong; these helpers build the
//! two common payload-matching expressions from any [`Serialize`]
//! value instead:
//!
//! - [`jq_eq`] — the payload must **equal** the value exactly. Pass
//!   the full payload struct.
//! - [`jq_contains`] — the payload must **contain** the value (a
//!   subset match). Pass a partial set of fields, e.g. an inline
//!   [`serde_json::json!`] object — there is no need to declare a
//!   separate "partial" struct.
//! - [`jq_array_prefix_eq`] — the payload array's leading elements
//!   must equal the value. Pass an ordered sequence — a `Vec`, a
//!   tuple (for heterogeneous elements), or a `json!` array.
//!
//! Values become JSON literals, and JSON serialisation handles all
//! escaping, so string fields with quotes or other special
//! characters stay valid jq.
//!
//! [`ListJobsBuilder`]: crate::ListJobsBuilder
//! [`CountJobsBuilder`]: crate::CountJobsBuilder

use serde::Serialize;

use crate::error::ZizqError;

/// Build a jq expression matching jobs whose payload **equals**
/// `value` exactly.
///
/// Every field is compared, so pass the job's complete payload —
/// typically the real payload struct, which keeps the match fully
/// type-checked.
///
/// # Examples
///
/// ```
/// use serde::Serialize;
/// use zizq::jq_eq;
///
/// #[derive(Serialize)]
/// struct SendEmail {
///     to: String,
/// }
///
/// let expr = jq_eq(&SendEmail { to: "x@y.z".into() }).unwrap();
/// assert_eq!(expr, r#". == {"to":"x@y.z"}"#);
/// ```
///
/// # Errors
///
/// Returns [`ZizqError::Encode`] if `value` cannot be serialised to
/// JSON.
pub fn jq_eq<T: Serialize>(value: &T) -> Result<String, ZizqError> {
    Ok(format!(". == {}", to_json(value)?))
}

/// Build a jq expression matching jobs whose payload **contains**
/// `value` — a subset match.
///
/// Jobs match as long as their payload includes the given fields
/// with the given values, whatever else they carry. Pass a partial
/// object — an inline [`serde_json::json!`] is usually simplest:
///
/// ```
/// # use zizq::jq_contains;
/// let expr = jq_contains(&serde_json::json!({ "to": "x@y.z" })).unwrap();
/// assert_eq!(expr, r#". | contains({"to":"x@y.z"})"#);
/// ```
///
/// Note: jq's `contains` is recursive and, for *array* values,
/// order-independent — `[1, 2]` is "contained" by `[2, 1]`. For the
/// usual object-subset match this is exactly the wanted behaviour,
/// but matching an array field positionally needs a hand-written
/// expression.
///
/// # Errors
///
/// Returns [`ZizqError::Encode`] if `value` cannot be serialised to
/// JSON.
pub fn jq_contains<T: Serialize>(value: &T) -> Result<String, ZizqError> {
    Ok(format!(". | contains({})", to_json(value)?))
}

/// Build a jq expression matching jobs whose payload array **begins
/// with** `value` — an exact match on the leading elements.
///
/// `value` must serialise to a JSON array; pass an ordered sequence
/// — a `Vec`, a tuple (handy for heterogeneous elements), a `[T; N]`,
/// or a `json!` array. The expression compares the payload's first
/// `N` elements (`N` being the array's length) for exact equality,
/// so the jobs it matches must have a payload that is itself a JSON
/// array.
///
/// # Examples
///
/// ```
/// use zizq::jq_array_prefix_eq;
///
/// // A tuple gives a heterogeneous, positionally-typed prefix.
/// let expr = jq_array_prefix_eq(&(42, "example")).unwrap();
/// assert_eq!(expr, r#".[0:2] == [42,"example"]"#);
/// ```
///
/// # Errors
///
/// Returns [`ZizqError::Encode`] if `value` cannot be serialised, or
/// if it does not serialise to a JSON array.
pub fn jq_array_prefix_eq<T: Serialize>(value: &T) -> Result<String, ZizqError> {
    let json = serde_json::to_value(value).map_err(|e| ZizqError::Encode(e.to_string()))?;
    let Some(items) = json.as_array() else {
        return Err(ZizqError::Encode(
            "jq_array_prefix_eq expects a value that serialises to a JSON array".into(),
        ));
    };
    let len = items.len();
    Ok(format!(".[0:{len}] == {json}"))
}

/// Serialise `value` to a compact JSON string, mapping any failure
/// to [`ZizqError::Encode`].
fn to_json<T: Serialize>(value: &T) -> Result<String, ZizqError> {
    serde_json::to_string(value).map_err(|e| ZizqError::Encode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Payload {
        to: String,
        priority: u32,
    }

    #[test]
    fn eq_emits_full_equality() {
        let expr = jq_eq(&Payload {
            to: "a@b.c".into(),
            priority: 5,
        })
        .unwrap();
        assert_eq!(expr, r#". == {"to":"a@b.c","priority":5}"#);
    }

    #[test]
    fn contains_emits_a_subset_match() {
        let expr = jq_contains(&serde_json::json!({ "to": "a@b.c" })).unwrap();
        assert_eq!(expr, r#". | contains({"to":"a@b.c"})"#);
    }

    #[test]
    fn string_values_are_json_escaped() {
        // Quotes in a value must stay valid jq — JSON serialisation
        // does the escaping, so the helper needs no escaping of its
        // own (and there is no injection vector).
        let expr = jq_contains(&serde_json::json!({ "note": "she said \"hi\"" })).unwrap();
        assert_eq!(expr, r#". | contains({"note":"she said \"hi\""})"#);
    }

    #[test]
    fn array_prefix_eq_from_a_vec() {
        let expr = jq_array_prefix_eq(&vec![1, 2, 3]).unwrap();
        assert_eq!(expr, ".[0:3] == [1,2,3]");
    }

    #[test]
    fn array_prefix_eq_from_a_heterogeneous_tuple() {
        let expr = jq_array_prefix_eq(&(42, "example")).unwrap();
        assert_eq!(expr, r#".[0:2] == [42,"example"]"#);
    }

    #[test]
    fn array_prefix_eq_rejects_a_non_array() {
        // An object doesn't serialise to a JSON array — caught, not
        // turned into a broken expression.
        let err = jq_array_prefix_eq(&serde_json::json!({ "a": 1 })).unwrap_err();
        assert!(matches!(err, ZizqError::Encode(_)));
    }
}
