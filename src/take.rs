// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Streaming job acquisition — the [`TakeBuilder`] returned by
//! [`Client::take`] and the [`TakeStream`] that yields decoded jobs.
//!
//! The builder collects filters (`queues`, `prefetch`) and is awaited
//! to open the connection. The returned [`TakeStream`] implements
//! [`Stream`] and yields one [`Job`] at a time until the server
//! disconnects (clean = `None`, broken = `Err` then `None`) or the
//! caller drops the stream to cancel.
//!
//! Two serialization formats are supported, picked from the response
//! `Content-Type`:
//!
//! - **NDJSON** (`application/json`): newline-delimited JSON objects.
//!   Empty / whitespace-only lines are heartbeats and are filtered out.
//! - **Length-prefixed MessagePack** (`application/msgpack`): each
//!   record is `[u32 big-endian length][MessagePack payload]`. A
//!   zero-length frame is a heartbeat.
//!
//! [`Client::take`]: crate::Client::take

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;

use crate::client::Client;
use crate::error::ZizqError;
use crate::format::Format;
use crate::resources::Job;

/// Type alias for the boxed byte stream returned by reqwest.
type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>;

/// Builder for a streaming take request.
///
/// Produced by [`Client::take`]. Chain filters and `.await` to open
/// the streaming connection — the await resolves to a [`TakeStream`]
/// once the response status has been validated.
///
/// [`Client::take`]: crate::Client::take
///
/// # Examples
///
/// ```no_run
/// use futures_util::TryStreamExt;
/// # use zizq::Client;
/// # async fn run(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
/// let mut stream = client
///     .take()
///     .queues(["emails", "urgent"])
///     .prefetch(16)
///     .await?;
///
/// while let Some(job) = stream.try_next().await? {
///     // ... process ...
/// }
/// # Ok(()) }
/// ```
pub struct TakeBuilder<'a> {
    /// The client to which the take request is ultimately sent.
    client: &'a Client,

    /// Queues to stream from. Empty means all queues.
    queues: Vec<String>,

    /// Maximum number of in-flight, unacknowledged jobs the server
    /// will send. `None` is the server’s default of 1 (job must be ack'd
    /// before another job is sent).
    prefetch: Option<u32>,
}

impl<'a> TakeBuilder<'a> {
    /// Restrict the stream to the given queues. If unset (or an
    /// empty list), the server streams from all queues. Accepts any
    /// iterable of string-like values.
    pub fn queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Set the prefetch limit — the maximum number of in-flight,
    /// unacknowledged jobs the server will send before pausing. `None`
    /// (the default) is the server’s default limit of 1, meaning each
    /// job must be acknowledged before the next is sent.
    pub fn prefetch(mut self, n: u32) -> Self {
        self.prefetch = Some(n);
        self
    }

    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            queues: Vec::new(),
            prefetch: None,
        }
    }
}

impl<'a> std::future::IntoFuture for TakeBuilder<'a> {
    /// The final outcome — a [`TakeStream`] once the connection is
    /// established and the response status has been validated.
    type Output = Result<TakeStream, ZizqError>;

    /// The type of the Future itself.
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    /// Open the streaming connection when `.await` is invoked at the
    /// end of the builder chain.
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.client.take_raw(self.queues, self.prefetch).await })
    }
}

/// A stream of [`Job`]s pulled from the server via `/jobs/take`.
///
/// Produced by awaiting a [`TakeBuilder`]. Iterate via `.next().await`
/// (importing [`futures_util::StreamExt`] or similar). Drop the stream
/// to cancel and close the underlying connection. Heartbeats from the
/// server are filtered out transparently.
///
/// # Termination
///
/// - **Clean end-of-body** (server closed the stream cleanly): yields
///   `None`. No error.
/// - **Network failure / mid-frame disconnect**: yields
///   `Err(ZizqError::Transport(_))` then `None`.
/// - **Buffered partial frame at clean end-of-body**: yields
///   `Err(ZizqError::Decode(_))` then `None` — the server's framing
///   contract was broken.
///
/// Be aware that dropping the stream before in-flight jobs have
/// been acknowledged will result in those in-flight jobs being
/// returned to the `Ready` status by the server. Coordinate shutdown
/// processes accordingly, so the stream is dropped *after* pending
/// ack's have been sent.
///
/// [`futures_util::StreamExt`]: https://docs.rs/futures-util/latest/futures_util/stream/trait.StreamExt.html
pub struct TakeStream {
    /// The underlying chunked-bytes stream from the HTTP response.
    inner: ByteStream,

    /// Buffered bytes awaiting decode, plus the format-specific
    /// decode strategy.
    decoder: StreamDecoder,

    /// Terminal flag — once set, subsequent polls return `None`
    /// without touching the inner stream or buffer.
    finished: bool,
}

impl TakeStream {
    pub(crate) fn new(inner: ByteStream, format: Format) -> Self {
        Self {
            inner,
            decoder: StreamDecoder::for_format(format),
            finished: false,
        }
    }
}

impl std::fmt::Debug for TakeStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeStream")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl Stream for TakeStream {
    type Item = Result<Job, ZizqError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.finished {
                return Poll::Ready(None);
            }

            // First, try to extract one record from the buffer.
            match this.decoder.next() {
                DecodeResult::Job(job) => return Poll::Ready(Some(Ok(job))),
                DecodeResult::Error(e) => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
                DecodeResult::NeedMore => {}
            }

            // Buffer doesn't have a complete record. Poll the inner
            // stream for more bytes.
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.decoder.feed(chunk);
                    // Loop to try decoding again.
                }
                Poll::Ready(Some(Err(e))) => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(ZizqError::Transport(e))));
                }
                Poll::Ready(None) => {
                    // Clean end-of-body. Surface any trailing partial
                    // frame as a decode error; otherwise terminate.
                    this.finished = true;
                    return Poll::Ready(this.decoder.finalize().map(Err));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// --- Framing decoders ------------------------------------------------------

// `Job` is the largest variant by a wide margin, so clippy suggests
// boxing it. We deliberately don't — DecodeResult is constructed and
// matched within a single function call (never stored), so the cost
// of the larger enum is one ~400-byte stack memcpy per record, which
// is cheaper than a heap alloc+dealloc per record on the hot path.
#[allow(clippy::large_enum_variant)]
enum DecodeResult {
    /// A complete record was decoded.
    Job(Job),
    /// A complete frame was found but failed to deserialise.
    Error(ZizqError),
    /// Buffer didn't contain a complete frame — feed more bytes.
    NeedMore,
}

/// Buffered framing decoder. Stateful: bytes go in via `feed`, jobs
/// come out via `next`.
enum StreamDecoder {
    Ndjson { buffer: BytesMut },
    Msgpack { buffer: BytesMut },
}

impl StreamDecoder {
    fn for_format(format: Format) -> Self {
        match format {
            Format::Json => Self::Ndjson {
                buffer: BytesMut::new(),
            },
            Format::MessagePack => Self::Msgpack {
                buffer: BytesMut::new(),
            },
        }
    }

    fn feed(&mut self, chunk: Bytes) {
        match self {
            Self::Ndjson { buffer } | Self::Msgpack { buffer } => buffer.extend_from_slice(&chunk),
        }
    }

    fn next(&mut self) -> DecodeResult {
        match self {
            Self::Ndjson { buffer } => decode_ndjson(buffer),
            Self::Msgpack { buffer } => decode_msgpack(buffer),
        }
    }

    /// Called once the inner stream has ended. Returns an error if
    /// the buffer contains a non-empty trailing partial frame. Always
    /// clears the buffer so subsequent polls don't retry.
    fn finalize(&mut self) -> Option<ZizqError> {
        let buffer = match self {
            Self::Ndjson { buffer } | Self::Msgpack { buffer } => buffer,
        };
        if buffer.is_empty() {
            return None;
        }
        buffer.clear();
        Some(ZizqError::Decode(
            "incomplete frame at end of stream".to_string(),
        ))
    }
}

fn decode_ndjson(buffer: &mut BytesMut) -> DecodeResult {
    loop {
        let Some(nl) = buffer.iter().position(|&b| b == b'\n') else {
            return DecodeResult::NeedMore;
        };

        // Take the line including its trailing newline; the slice we
        // parse strips the newline (and the CR for CRLF endings).
        let line_bytes = buffer.split_to(nl + 1);
        let mut line: &[u8] = &line_bytes[..nl];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }

        // Heartbeat: empty or whitespace-only line.
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        return match serde_json::from_slice::<Job>(line) {
            Ok(job) => DecodeResult::Job(job),
            Err(e) => DecodeResult::Error(ZizqError::Decode(e.to_string())),
        };
    }
}

fn decode_msgpack(buffer: &mut BytesMut) -> DecodeResult {
    loop {
        if buffer.len() < 4 {
            return DecodeResult::NeedMore;
        }

        let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if len == 0 {
            // Heartbeat: consume the 4 header bytes and try again.
            buffer.advance(4);
            continue;
        }

        if buffer.len() < 4 + len {
            return DecodeResult::NeedMore;
        }

        let _header = buffer.split_to(4);
        let payload = buffer.split_to(len);

        return match rmp_serde::from_slice::<Job>(&payload) {
            Ok(job) => DecodeResult::Job(job),
            Err(e) => DecodeResult::Error(ZizqError::Decode(e.to_string())),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;

    fn fake_job_json(id: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "id": id,
            "type": "send_email",
            "queue": "emails",
            "status": "ready",
            "priority": 50,
            "ready_at": 0,
            "attempts": 0
        }))
        .unwrap()
    }

    fn fake_job_msgpack(id: &str) -> Vec<u8> {
        let value = json!({
            "id": id,
            "type": "send_email",
            "queue": "emails",
            "status": "ready",
            "priority": 50,
            "ready_at": 0,
            "attempts": 0
        });
        let mut buf = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut buf)
            .with_struct_map()
            .with_human_readable();
        value.serialize(&mut ser).unwrap();
        buf
    }

    fn msgpack_frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn msgpack_heartbeat() -> [u8; 4] {
        [0, 0, 0, 0]
    }

    fn assert_job(result: DecodeResult, expect_id: &str) {
        match result {
            DecodeResult::Job(job) => assert_eq!(job.id, expect_id),
            DecodeResult::Error(e) => panic!("expected job, got Error({e:?})"),
            DecodeResult::NeedMore => panic!("expected job, got NeedMore"),
        }
    }

    // --- NDJSON ---------------------------------------------------------

    #[test]
    fn ndjson_decodes_a_single_job() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        let mut bytes = fake_job_json("job-1");
        bytes.push(b'\n');
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
        assert!(dec.finalize().is_none());
    }

    #[test]
    fn ndjson_decodes_multiple_jobs_in_one_chunk() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&fake_job_json("job-1"));
        bytes.push(b'\n');
        bytes.extend_from_slice(&fake_job_json("job-2"));
        bytes.push(b'\n');
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
        assert_job(dec.next(), "job-2");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn ndjson_skips_empty_lines_as_heartbeats() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        let mut bytes = Vec::new();
        bytes.push(b'\n'); // heartbeat
        bytes.push(b'\n'); // heartbeat
        bytes.extend_from_slice(&fake_job_json("job-1"));
        bytes.push(b'\n');
        bytes.push(b'\n'); // heartbeat
        bytes.extend_from_slice(&fake_job_json("job-2"));
        bytes.push(b'\n');
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
        assert_job(dec.next(), "job-2");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn ndjson_handles_partial_line_across_chunks() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        let bytes = fake_job_json("job-1");
        let (first, second) = bytes.split_at(10);

        dec.feed(Bytes::copy_from_slice(first));
        assert!(matches!(dec.next(), DecodeResult::NeedMore));

        let mut rest = second.to_vec();
        rest.push(b'\n');
        dec.feed(Bytes::from(rest));
        assert_job(dec.next(), "job-1");
    }

    #[test]
    fn ndjson_handles_crlf_line_endings() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        let mut bytes = fake_job_json("job-1");
        bytes.push(b'\r');
        bytes.push(b'\n');
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
    }

    #[test]
    fn ndjson_finalize_reports_trailing_partial_line() {
        let mut dec = StreamDecoder::for_format(Format::Json);
        // A partial JSON object with no trailing newline — represents a
        // truncated stream.
        dec.feed(Bytes::from_static(b"{\"id\":\"a"));
        assert!(matches!(dec.next(), DecodeResult::NeedMore));

        let err = dec.finalize().expect("trailing bytes");
        assert!(matches!(err, ZizqError::Decode(_)));
    }

    // --- MessagePack ----------------------------------------------------

    #[test]
    fn msgpack_decodes_a_single_job() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        dec.feed(Bytes::from(msgpack_frame(&fake_job_msgpack("job-1"))));

        assert_job(dec.next(), "job-1");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
        assert!(dec.finalize().is_none());
    }

    #[test]
    fn msgpack_decodes_multiple_jobs_in_one_chunk() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&msgpack_frame(&fake_job_msgpack("job-1")));
        bytes.extend_from_slice(&msgpack_frame(&fake_job_msgpack("job-2")));
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
        assert_job(dec.next(), "job-2");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn msgpack_skips_zero_length_frames_as_heartbeats() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&msgpack_heartbeat());
        bytes.extend_from_slice(&msgpack_heartbeat());
        bytes.extend_from_slice(&msgpack_frame(&fake_job_msgpack("job-1")));
        bytes.extend_from_slice(&msgpack_heartbeat());
        bytes.extend_from_slice(&msgpack_frame(&fake_job_msgpack("job-2")));
        dec.feed(Bytes::from(bytes));

        assert_job(dec.next(), "job-1");
        assert_job(dec.next(), "job-2");
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn msgpack_needs_more_for_incomplete_length_prefix() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        dec.feed(Bytes::from_static(&[0, 0])); // only 2 of 4 header bytes
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn msgpack_needs_more_for_incomplete_frame_body() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        let job = fake_job_msgpack("job-1");
        let frame = msgpack_frame(&job);
        // Send header + half the body.
        let (first, _) = frame.split_at(4 + job.len() / 2);
        dec.feed(Bytes::copy_from_slice(first));
        assert!(matches!(dec.next(), DecodeResult::NeedMore));
    }

    #[test]
    fn msgpack_decodes_after_partial_chunks_arrive() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        let frame = msgpack_frame(&fake_job_msgpack("job-1"));
        let (first, second) = frame.split_at(6);

        dec.feed(Bytes::copy_from_slice(first));
        assert!(matches!(dec.next(), DecodeResult::NeedMore));

        dec.feed(Bytes::copy_from_slice(second));
        assert_job(dec.next(), "job-1");
    }

    #[test]
    fn msgpack_finalize_reports_trailing_partial_frame() {
        let mut dec = StreamDecoder::for_format(Format::MessagePack);
        dec.feed(Bytes::from_static(&[0, 0, 0, 5, 1, 2])); // header says 5 bytes, only 2 sent
        assert!(matches!(dec.next(), DecodeResult::NeedMore));

        let err = dec.finalize().expect("trailing bytes");
        assert!(matches!(err, ZizqError::Decode(_)));
    }
}
