// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Job enqueueing — the [`EnqueueBuilder`] returned by [`Client::enqueue`].
//!
//! The builder takes per-job overrides as chained method calls and is
//! finalised by `await`ing it (via [`IntoFuture`]). Defaults come from
//! the payload's [`JobKind`] impl; anything set on the builder overrides
//! the trait defaults. The transport itself — URL, headers, encoding,
//! dispatch — lives on [`Client::enqueue_raw`]; this module is just
//! resolution and hand-off.
//!
//! [`Client::enqueue`]: crate::Client::enqueue
//! [`Client::enqueue_raw`]: crate::Client::enqueue_raw

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use serde::{Serialize, Serializer};
use time::OffsetDateTime;

use crate::client::{Client, EnqueueRequest};
use crate::error::ZizqError;
use crate::job::JobKind;
use crate::resources::{BackoffConfig, Job, RetentionConfig};
use crate::timestamp::to_ms_epoch;
use crate::unique_key::UniqueKey;

/// Builder for an enqueue request.
///
/// Produced by [`Client::enqueue`]. Chain builder methods to override
/// any of the per-type defaults supplied by [`JobKind`], then `.await`
/// the builder to send the request and receive the resulting [`Job`].
///
/// [`Client::enqueue`]: crate::Client::enqueue
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use serde::{Deserialize, Serialize};
/// use zizq::{Client, JobKind};
///
/// #[derive(Serialize, Deserialize)]
/// struct SendEmail { to: String }
///
/// impl JobKind for SendEmail {
///     const NAME: &'static str = "send_email";
/// }
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::builder().url("http://127.0.0.1:7890").build()?;
///
/// let job = client
///     .enqueue(SendEmail { to: "alice@example.com".into() })
///     .queue("high")
///     .priority(100)
///     .delay(Duration::from_secs(60))
///     .await?;
///
/// println!("scheduled {} on {}", job.id, job.queue);
/// # Ok(()) }
/// ```
pub struct EnqueueBuilder<'a, T> {
    /// The client to which the enqueue request is ultimately sent.
    client: &'a Client,

    /// The data in the payload (a `JobKind`).
    payload: T,

    /// Queue this job belongs to.
    queue: Option<String>,

    /// Priority within the queue. Lower values run sooner.
    priority: Option<u16>,

    /// Optional time at which the job becomes ready, as Unix ms.
    ///
    /// Future-dated values cause the enqueued job to be in the `Scheduled`
    /// status.
    ready_at_ms: Option<u64>,

    /// Maximum attempts permitted before the job is considered dead on failure.
    retry_limit: Option<u32>,

    /// Per-job backoff configuration.
    backoff: Option<BackoffConfig>,

    /// Per-job retention configuration.
    retention: Option<RetentionConfig>,

    /// Optional unique identifier and scope for this job.
    unique_key: Option<UniqueKey>,
}

impl<'a, T: JobKind> EnqueueBuilder<'a, T> {
    /// Override the queue this job is placed on. Overrides [`JobKind::QUEUE`].
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Override the priority. Lower values run sooner. Valid range is
    /// 0 to 65535. Overrides [`JobKind::PRIORITY`].
    pub fn priority(mut self, priority: u16) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Delay the job by the given duration relative to the moment this
    /// method is called. Mutually exclusive with [`Self::ready_at`] /
    /// [`Self::run_at`] — the last one set wins.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.ready_at_ms = Some(to_ms_epoch(OffsetDateTime::now_utc() + delay));
        self
    }

    /// Schedule the job to become ready at an absolute point in time.
    /// Mutually exclusive with [`Self::delay`] — the last one set wins.
    pub fn ready_at(mut self, when: OffsetDateTime) -> Self {
        self.ready_at_ms = Some(to_ms_epoch(when));
        self
    }

    /// Alias for [`Self::ready_at`], provided for naming preference.
    pub fn run_at(self, when: OffsetDateTime) -> Self {
        self.ready_at(when)
    }

    /// Override the retry budget. Overrides [`JobKind::RETRY_LIMIT`].
    pub fn retry_limit(mut self, n: u32) -> Self {
        self.retry_limit = Some(n);
        self
    }

    /// Override the per-job backoff configuration. Overrides
    /// [`JobKind::BACKOFF`].
    pub fn backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = Some(backoff);
        self
    }

    /// Override the per-job retention configuration. Overrides
    /// [`JobKind::RETENTION`].
    pub fn retention(mut self, retention: RetentionConfig) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Attach a uniqueness key. Overrides any key the payload would derive
    /// via [`JobKind::unique_key`]. Accepts a [`UniqueKey`] directly or
    /// anything convertible to one (e.g. `&str`, `String`).
    pub fn unique_key(mut self, key: impl Into<UniqueKey>) -> Self {
        self.unique_key = Some(key.into());
        self
    }

    /// Initialize a new `EnqueueBuilder` for the given `Client` and `JobKind`.
    pub(crate) fn new(client: &'a Client, payload: T) -> Self {
        Self {
            client,
            payload,
            queue: None,
            priority: None,
            ready_at_ms: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
        }
    }

    /// Resolve `JobKind` defaults, serialise the payload, and produce
    /// the owned, non-generic [`EnqueueRequest`]. Shared by the
    /// single-enqueue [`IntoFuture`] impl and the
    /// [`BulkEnqueueBuilder`]-side `.add` / `.push` methods, which both
    /// need the resolved request without sending it.
    ///
    /// [`BulkEnqueueBuilder`]: crate::BulkEnqueueBuilder
    pub(crate) fn into_request(self) -> Result<EnqueueRequest, ZizqError> {
        let queue = self.queue.unwrap_or_else(|| T::QUEUE.to_string());
        let unique_key = self.unique_key.or_else(|| self.payload.unique_key());
        let (unique_key_str, unique_scope) = match unique_key {
            Some(uk) => (Some(uk.key), uk.scope),
            None => (None, None),
        };
        // Box the payload as a type-erased serializer — at body encode
        // time it walks through serde once, directly, without a
        // `serde_json::Value` intermediate allocation. `T: JobKind`
        // already requires `Send + 'static`, which is enough to
        // satisfy the dyn-trait bound here.
        //
        // Zero-sized payload types (`struct Foo;`) go through a thin
        // serializer wrapper that re-emits `serialize_unit_struct`
        // calls as plain `serialize_unit` — see [`ZstPayload`] for the
        // why. Non-ZST payloads (the common case) take the fast path
        // with no wrapper overhead.
        let payload: Box<dyn erased_serde::Serialize + Send> = if std::mem::size_of::<T>() == 0 {
            Box::new(ZstPayload(self.payload))
        } else {
            Box::new(self.payload)
        };
        Ok(EnqueueRequest {
            job_type: T::NAME,
            queue,
            payload,
            priority: self.priority.or(T::PRIORITY),
            ready_at: self.ready_at_ms,
            retry_limit: self.retry_limit.or(T::RETRY_LIMIT),
            backoff: self.backoff.or(T::BACKOFF),
            retention: self.retention.or(T::RETENTION),
            unique_key: unique_key_str,
            unique_while: unique_scope,
        })
    }
}

impl<'a, T: JobKind> IntoFuture for EnqueueBuilder<'a, T> {
    /// The final outcome of the Future.
    type Output = Result<Job, ZizqError>;

    /// The type of the Future itself.
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    /// Called to send the request when .await is invoked at the end of the
    /// builder chain.
    fn into_future(self) -> Self::IntoFuture {
        let client = self.client;
        Box::pin(async move {
            let req = self.into_request()?;
            client.enqueue_raw(req).await
        })
    }
}

// --- ZST payload encoding ---------------------------------------------------
//
// Why this exists: `rmp_serde::to_vec_named` encodes a unit struct
// (`struct Foo;`) as MessagePack `fixarray 0` rather than `nil`. When
// the server reads that into a `serde_json::Value` payload it becomes
// `Value::Array([])`, which then fails to deserialize back into the
// unit struct on the worker side via `serde_json::from_value`
// (`expected unit struct, got sequence`). This also breaks cross-
// language interop — a Ruby/Node consumer sees `[]` where they
// expect `null`/`{}`.
//
// The fix is purely client-side: when the user's payload type is a
// ZST (`std::mem::size_of::<T>() == 0`), wrap it in `ZstPayload<T>`
// before type-erasing. The wrapper hands the user's `T` a tiny
// shim `Serializer` that re-emits `serialize_unit_struct` calls as
// plain `serialize_unit()` — producing nil/null on the wire, which
// every format and language round-trips cleanly. Every other
// `Serializer` method delegates straight through.
//
// Non-ZST payloads bypass the wrapper entirely.

/// Newtype around a ZST payload that re-encodes its
/// `serialize_unit_struct` call as plain `serialize_unit` to dodge
/// the rmp-serde empty-fixarray quirk.
struct ZstPayload<T>(T);

impl<T: Serialize> Serialize for ZstPayload<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(ZstFixupSerializer(serializer))
    }
}

/// Serializer shim that re-routes `serialize_unit_struct` to
/// `serialize_unit` and delegates everything else verbatim.
struct ZstFixupSerializer<S>(S);

impl<S: Serializer> Serializer for ZstFixupSerializer<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = S::SerializeSeq;
    type SerializeTuple = S::SerializeTuple;
    type SerializeTupleStruct = S::SerializeTupleStruct;
    type SerializeTupleVariant = S::SerializeTupleVariant;
    type SerializeMap = S::SerializeMap;
    type SerializeStruct = S::SerializeStruct;
    type SerializeStructVariant = S::SerializeStructVariant;

    // The one method we intercept: emit a plain unit instead of a
    // named unit struct (which rmp-serde mishandles).
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }

    // Everything else: straight delegation.
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bool(v)
    }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i8(v)
    }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i16(v)
    }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i32(v)
    }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i64(v)
    }
    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i128(v)
    }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u8(v)
    }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u16(v)
    }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u32(v)
    }
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u64(v)
    }
    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u128(v)
    }
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_f32(v)
    }
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_f64(v)
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_char(v)
    }
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_str(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bytes(v)
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }
    fn serialize_some<V: ?Sized + Serialize>(self, value: &V) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(value)
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        idx: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, idx, variant)
    }
    fn serialize_newtype_struct<V: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &V,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, value)
    }
    fn serialize_newtype_variant<V: ?Sized + Serialize>(
        self,
        name: &'static str,
        idx: u32,
        variant: &'static str,
        value: &V,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_variant(name, idx, variant, value)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(len)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(len)
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0.serialize_tuple_struct(name, len)
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        idx: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0.serialize_tuple_variant(name, idx, variant, len)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(len)
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, len)
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        idx: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0.serialize_struct_variant(name, idx, variant, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{decode_body, encode_body};
    use crate::format::Format;
    use crate::unique_key::UniqueScope;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct SendEmail {
        to: String,
    }

    impl JobKind for SendEmail {
        const NAME: &'static str = "send_email";
        const QUEUE: &'static str = "emails";
        const PRIORITY: Option<u16> = Some(50);
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Bare {
        x: i32,
    }
    impl JobKind for Bare {
        const NAME: &'static str = "bare";
    }

    #[test]
    fn json_body_uses_trait_defaults() {
        let body = EnqueueRequest {
            job_type: SendEmail::NAME,
            queue: SendEmail::QUEUE.to_string(),
            payload: Box::new(serde_json::json!({ "to": "a@b" })),
            priority: SendEmail::PRIORITY,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        };

        let json: serde_json::Value =
            serde_json::from_slice(&encode_body(&body, Format::Json).unwrap()).unwrap();

        assert_eq!(json["type"], "send_email");
        assert_eq!(json["queue"], "emails");
        assert_eq!(json["priority"], 50);
        assert_eq!(json["payload"]["to"], "a@b");
        assert!(json.get("ready_at").is_none());
        assert!(json.get("unique_key").is_none());
    }

    #[test]
    fn json_body_skips_none_fields() {
        let body = EnqueueRequest {
            job_type: Bare::NAME,
            queue: Bare::QUEUE.to_string(),
            payload: Box::new(serde_json::json!({ "x": 7 })),
            priority: None,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        };

        let json: serde_json::Value =
            serde_json::from_slice(&encode_body(&body, Format::Json).unwrap()).unwrap();

        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 3); // type, queue, payload
        assert!(obj.contains_key("type"));
        assert!(obj.contains_key("queue"));
        assert!(obj.contains_key("payload"));
    }

    #[test]
    fn json_body_includes_backoff_and_retention_when_set() {
        let body = EnqueueRequest {
            job_type: SendEmail::NAME,
            queue: SendEmail::QUEUE.to_string(),
            payload: Box::new(serde_json::json!({ "to": "a@b" })),
            priority: None,
            ready_at: None,
            retry_limit: None,
            backoff: Some(BackoffConfig {
                base_ms: 1_000,
                exponent: 2.0,
                jitter_ms: 500,
            }),
            retention: Some(RetentionConfig {
                completed_ms: Some(60_000),
                dead_ms: None,
            }),
            unique_key: None,
            unique_while: None,
        };

        let json: serde_json::Value =
            serde_json::from_slice(&encode_body(&body, Format::Json).unwrap()).unwrap();

        assert_eq!(json["backoff"]["base_ms"], 1_000);
        assert_eq!(json["backoff"]["exponent"], 2.0);
        assert_eq!(json["backoff"]["jitter_ms"], 500);
        assert_eq!(json["retention"]["completed_ms"], 60_000);
        assert!(json["retention"].get("dead_ms").is_none());
    }

    #[test]
    fn msgpack_round_trips_a_job_response() {
        let job = serde_json::json!({
            "id": "abc",
            "type": "send_email",
            "queue": "emails",
            "status": "ready",
            "priority": 50,
            "ready_at": 0,
            "attempts": 0,
            "payload": { "to": "a@b" },
            "unique_key": "user:42",
            "unique_while": "exists"
        });

        let mut buf = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut buf)
            .with_struct_map()
            .with_human_readable();
        job.serialize(&mut ser).unwrap();

        let parsed: Job = decode_body(&buf, Format::MessagePack).unwrap();
        assert_eq!(parsed.id, "abc");
        assert_eq!(parsed.job_type, "send_email");
        assert_eq!(parsed.queue, "emails");
        assert_eq!(parsed.priority, 50);
        let uk = parsed.unique_key.expect("unique_key folded");
        assert_eq!(uk.key, "user:42");
        assert_eq!(uk.scope, Some(UniqueScope::Exists));
        assert_eq!(parsed.payload.as_ref().unwrap()["to"], "a@b");
        assert_eq!(parsed.duplicate, None);
    }

    #[test]
    fn zst_payload_encodes_as_null_not_empty_sequence() {
        // Reproduces the bug: a unit struct serialised via rmp-serde
        // would naturally become a `fixarray 0` (empty sequence) on
        // the wire, which the server stores as `Value::Array([])` and
        // then fails to round-trip back into the unit struct via
        // `serde_json::from_value`. The `ZstPayload` wrapper rewrites
        // the call to `serialize_unit`, producing `nil`/`null`.
        #[derive(Debug, Serialize, Deserialize)]
        struct Marker;
        impl JobKind for Marker {
            const NAME: &'static str = "marker";
        }

        let body = EnqueueRequest {
            job_type: Marker::NAME,
            queue: "default".into(),
            payload: Box::new(ZstPayload(Marker)),
            priority: None,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        };

        // MessagePack: server-side path. Decode as Value to mirror
        // what the server does when stashing the payload.
        let mp = encode_body(&body, Format::MessagePack).unwrap();
        let as_value: serde_json::Value = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(
            as_value["payload"],
            serde_json::Value::Null,
            "expected nil payload on the wire; got {:?}",
            as_value["payload"],
        );

        // And the round-trip back into the unit struct via
        // `serde_json::from_value` — the path the router takes — now
        // succeeds.
        let payload = as_value["payload"].clone();
        let _: Marker = serde_json::from_value(payload).unwrap();
    }

    #[test]
    fn typed_payload_with_fields_is_unchanged_by_the_zst_wrapper() {
        // Sanity check: ordinary payloads (the common case) don't
        // change wire shape — the wrapper only kicks in for ZSTs.
        let body = EnqueueRequest {
            job_type: SendEmail::NAME,
            queue: SendEmail::QUEUE.to_string(),
            payload: Box::new(SendEmail { to: "a@b".into() }),
            priority: None,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: None,
            unique_while: None,
        };
        let json: serde_json::Value =
            serde_json::from_slice(&encode_body(&body, Format::Json).unwrap()).unwrap();
        assert_eq!(json["payload"]["to"], "a@b");
    }

    #[test]
    fn unique_key_with_scope_emits_unique_while() {
        let body = EnqueueRequest {
            job_type: SendEmail::NAME,
            queue: SendEmail::QUEUE.to_string(),
            payload: Box::new(serde_json::json!({ "to": "a@b" })),
            priority: None,
            ready_at: None,
            retry_limit: None,
            backoff: None,
            retention: None,
            unique_key: Some("user:42".to_string()),
            unique_while: Some(UniqueScope::Exists),
        };

        let json: serde_json::Value =
            serde_json::from_slice(&encode_body(&body, Format::Json).unwrap()).unwrap();

        assert_eq!(json["unique_key"], "user:42");
        assert_eq!(json["unique_while"], "exists");
    }
}
