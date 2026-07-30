// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Uniqueness keys used to deduplicate jobs at enqueue-time.
//!
//! Unique jobs requires a [Pro license](https://zizq.io/pricing) on
//! the server.
//!
//! A [`UniqueKey`] is an arbitrary string the server uses to reject
//! duplicate enqueues while the matching [`UniqueScope`] applies. Keys
//! can be supplied explicitly at the enqueue call site or derived from
//! the payload via [`JobKind::unique_key`].
//!
//! [`JobKind::unique_key`]: crate::JobKind::unique_key

use std::fmt::Write;

use serde::{Deserialize, Serialize};
use sha2::{digest::Update, Digest, Sha256};

/// A uniqueness key sent with an enqueue request.
///
/// Constructed with [`UniqueKey::raw`] when you already have a string,
/// or via the [`From<&str>`] / [`From<String>`] conversions when
/// passing a literal into [`EnqueueBuilder::unique_key`].
///
/// [`EnqueueBuilder::unique_key`]: crate::EnqueueBuilder::unique_key
///
/// # Examples
///
/// ```
/// use zizq::{UniqueKey, UniqueScope};
///
/// let key = UniqueKey::raw("user:42").scope(UniqueScope::Queued);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueKey {
    /// Raw string key.
    pub key: String,

    /// Lifecycle scope for which the uniqueness is enforced.
    pub scope: Option<UniqueScope>,
}

impl UniqueKey {
    /// Wrap a pre-built key string. No hashing or transformation is
    /// applied — the string is sent verbatim to the server.
    pub fn raw(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            scope: None,
        }
    }

    /// Attach a [`UniqueScope`] to this key. Returns `self` for
    /// chaining; with no scope set, the server applies the default
    /// [`UniqueScope::Queued`].
    pub fn scope(mut self, scope: UniqueScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Build a key by stably hashing a JSON-serialisable value.
    ///
    /// The value is serialised to canonical JSON — object keys sorted,
    /// so the result is independent of field/map ordering — and
    /// hashed with SHA-256. The key is therefore deterministic across
    /// processes and runs.
    ///
    /// Pass whatever identifies the job: the whole payload (`self`), a
    /// single field (`&self.field`), or a tuple of fields for a
    /// subset (`(&self.a, &self.b)`). A tuple serialises to a JSON
    /// array, so the order of its elements is significant.
    ///
    /// Prefer [`UniqueKey::tagged_hash_of`] when deriving keys from a
    /// payload, so distinct job types can't collide.
    ///
    /// # Panics
    ///
    /// Panics if `value` cannot be serialised to JSON. Any
    /// `#[derive(Serialize)]` payload of ordinary fields always can;
    /// this only fires for a pathological hand-written `Serialize`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zizq::UniqueKey;
    ///
    /// let key = UniqueKey::hash_of(("user", 42));
    /// assert_eq!(key.key.len(), 64); // SHA-256, lowercase hex
    /// ```
    pub fn hash_of(value: impl Serialize) -> Self {
        Self::raw(hash_hex(&value))
    }

    /// Build a key by hashing a value (see [`UniqueKey::hash_of`]),
    /// prefixed with `tag`.
    ///
    /// The tag is a readable prefix on the resulting key — the key
    /// looks like `"send_email:<hash>"`. Use the job type name
    /// (`Self::NAME`) as the tag so two job types that hash identical
    /// data don't deduplicate against one another.
    ///
    /// # Panics
    ///
    /// See [`UniqueKey::hash_of`].
    ///
    /// # Examples
    ///
    /// ```
    /// use zizq::UniqueKey;
    ///
    /// let key = UniqueKey::tagged_hash_of("send_email", "alice@example.com");
    /// assert!(key.key.starts_with("send_email:"));
    /// ```
    pub fn tagged_hash_of(tag: &str, value: impl Serialize) -> Self {
        Self::raw(format!("{tag}:{}", hash_hex(&value)))
    }
}

/// Serialise `value` to a canonical byte form and return the
/// lowercase-hex SHA-256 digest.
fn hash_hex(value: &impl Serialize) -> String {
    let json = serde_json::to_value(value)
        .expect("value passed to UniqueKey hashing must be JSON-serialisable");
    let mut hasher = Sha256::new();
    write_canonical(&json, &mut hasher);

    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // `write!` appends into the pre-sized `String` — one
        // allocation total, rather than a throwaway `String` per byte.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Feed `value` to `hasher` as canonical JSON: object keys are sorted,
/// so the encoding is independent of field/map insertion order.
///
/// Tokens are streamed straight into the hasher via [`Update`] — there
/// is no intermediate document buffer. The sort is done here rather
/// than relying on `serde_json`'s `Map` backing (a `BTreeMap`), which
/// is current behaviour, not a stable API contract. Scalar tokens are
/// rendered by `serde_json`, whose number/string formatting is stable.
fn write_canonical<H: Update>(value: &serde_json::Value, hasher: &mut H) {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            hasher.update(b"{");
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    hasher.update(b",");
                }
                // A JSON string token — `serde_json` handles escaping.
                let token = serde_json::to_string(key).expect("a string serialises");
                hasher.update(token.as_bytes());
                hasher.update(b":");
                write_canonical(&map[key], hasher);
            }
            hasher.update(b"}");
        }
        serde_json::Value::Array(items) => {
            // Arrays keep their order — it is semantically meaningful.
            hasher.update(b"[");
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    hasher.update(b",");
                }
                write_canonical(item, hasher);
            }
            hasher.update(b"]");
        }
        // Scalars (`null`, bool, number, string): `serde_json`'s
        // rendering is deterministic and stable.
        scalar => {
            let token = serde_json::to_string(scalar).expect("a scalar serialises");
            hasher.update(token.as_bytes());
        }
    }
}

impl From<&str> for UniqueKey {
    fn from(s: &str) -> Self {
        Self::raw(s)
    }
}

impl From<String> for UniqueKey {
    fn from(s: String) -> Self {
        Self::raw(s)
    }
}

/// The lifecycle window during which a [`UniqueKey`] prevents
/// duplicate enqueues.
///
/// The scope is taken from any successfully enqueued job. Subsequent
/// enqueues that conflict are de-duplicated (the server returns the
/// existing job with the `duplicate` flag set to `true`).
///
/// Serialised in the API as snake_case: `queued`, `active`, `exists`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UniqueScope {
    /// Prevent duplicates while this job is in the [`Scheduled`] or
    /// [`Ready`] states. As soon as the job is dequeued, a subsequent
    /// enqueue is permitted.
    ///
    /// [`Scheduled`]: crate::JobStatus::Scheduled
    /// [`Ready`]: crate::JobStatus::Ready
    Queued,

    /// Prevent duplicate enqueues while this job is in the [`Scheduled`],
    /// [`Ready`] or [`InFlight`] states. As soon as the job completes or
    /// exceeds its retry limit, a subsequent enqueue is permitted.
    ///
    /// [`Scheduled`]: crate::JobStatus::Scheduled
    /// [`Ready`]: crate::JobStatus::Ready
    /// [`InFlight`]: crate::JobStatus::InFlight
    Active,

    /// Prevent duplicate enqueues for as long as this job exists on
    /// the server (i.e. until it is reaped, according to the retention
    /// policy).
    Exists,

    /// A scope this client version doesn't recognise — e.g. a newer
    /// server introduced a scope the client predates.
    ///
    /// The catch-all keeps an unknown scope from failing the whole
    /// [`Job`] decode, so older clients keep working against newer
    /// servers. Treat it as opaque — don't round-trip it back as a
    /// uniqueness scope on a new enqueue.
    ///
    /// [`Job`]: crate::Job
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_constructor_stores_key_without_scope() {
        let k = UniqueKey::raw("abc");
        assert_eq!(k.key, "abc");
        assert_eq!(k.scope, None);
    }

    #[test]
    fn from_str_uses_raw() {
        let k: UniqueKey = "abc".into();
        assert_eq!(k.key, "abc");
    }

    #[test]
    fn scope_attaches() {
        let k = UniqueKey::raw("abc").scope(UniqueScope::Exists);
        assert_eq!(k.scope, Some(UniqueScope::Exists));
    }

    #[test]
    fn known_scope_deserialises() {
        let s: UniqueScope = serde_json::from_str("\"active\"").unwrap();
        assert_eq!(s, UniqueScope::Active);
    }

    #[test]
    fn unknown_scope_falls_back_to_unknown() {
        // A scope this client version doesn't know must not fail the
        // decode — it lands on the `Unknown` catch-all.
        let s: UniqueScope = serde_json::from_str("\"some_future_scope\"").unwrap();
        assert_eq!(s, UniqueScope::Unknown);
    }

    #[test]
    fn hash_of_is_deterministic_and_64_hex_chars() {
        let a = UniqueKey::hash_of(serde_json::json!({ "to": "x@y.z" }));
        let b = UniqueKey::hash_of(serde_json::json!({ "to": "x@y.z" }));
        assert_eq!(a.key, b.key);
        assert_eq!(a.key.len(), 64);
        assert!(a.key.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_of_differs_for_different_values() {
        let a = UniqueKey::hash_of(serde_json::json!({ "to": "a@x.z" }));
        let b = UniqueKey::hash_of(serde_json::json!({ "to": "b@x.z" }));
        assert_ne!(a.key, b.key);
    }

    #[test]
    fn tagged_hash_of_prefixes_the_tag() {
        let k = UniqueKey::tagged_hash_of("send_email", serde_json::json!({ "to": "x@y.z" }));
        assert!(k.key.starts_with("send_email:"));
        assert_eq!(k.key.len(), "send_email:".len() + 64);

        // The tag participates — a different tag gives a different key.
        let other = UniqueKey::tagged_hash_of("send_sms", serde_json::json!({ "to": "x@y.z" }));
        assert_ne!(k.key, other.key);
    }

    /// A trivial [`Update`] sink that just gathers the bytes, so a
    /// test can inspect the canonical encoding `write_canonical`
    /// streams out.
    #[derive(Default)]
    struct Collected(Vec<u8>);

    impl Update for Collected {
        fn update(&mut self, data: &[u8]) {
            self.0.extend_from_slice(data);
        }
    }

    #[test]
    fn write_canonical_sorts_object_keys_and_keeps_array_order() {
        // Object keys are emitted sorted; array element order is
        // preserved (it is semantically meaningful).
        let mut sink = Collected::default();
        write_canonical(
            &serde_json::json!({ "b": [3, 1, 2], "a": { "d": 4, "c": 3 } }),
            &mut sink,
        );
        assert_eq!(
            std::str::from_utf8(&sink.0).unwrap(),
            r#"{"a":{"c":3,"d":4},"b":[3,1,2]}"#,
        );
    }

    #[test]
    fn hash_is_independent_of_map_key_order() {
        // `write_canonical` sorts object keys itself, so the hash is
        // independent of insertion order regardless of how
        // `serde_json` stores object entries.
        let mut first = serde_json::Map::new();
        first.insert("x".into(), serde_json::json!(1));
        first.insert("y".into(), serde_json::json!(2));
        let mut second = serde_json::Map::new();
        second.insert("y".into(), serde_json::json!(2));
        second.insert("x".into(), serde_json::json!(1));
        assert_eq!(
            UniqueKey::hash_of(serde_json::Value::Object(first)).key,
            UniqueKey::hash_of(serde_json::Value::Object(second)).key,
        );
    }

    #[test]
    fn hash_of_accepts_a_field_subset_tuple() {
        // A tuple of fields is `Serialize` — the idiomatic way to key
        // on a subset of a payload.
        let to = "x@y.z".to_string();
        let subject = "Hello".to_string();
        let k = UniqueKey::hash_of((&to, &subject));
        assert_eq!(k.key.len(), 64);

        // A tuple serialises to a JSON array — element order matters.
        let swapped = UniqueKey::hash_of((&subject, &to));
        assert_ne!(k.key, swapped.key);
    }
}
