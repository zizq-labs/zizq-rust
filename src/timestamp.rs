// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Conversion helpers between [`OffsetDateTime`] and the Zizq API's
//! millisecond timestamps.
//!
//! The API uses Unix milliseconds (signed 64-bit) throughout —
//! `ready_at`, `dequeued_at`, `failed_at`, `completed_at`, `purge_at`,
//! and the cron entry timestamps. The `time` crate doesn't expose a
//! direct millis accessor (only seconds and nanoseconds), so we route
//! via nanoseconds.

use time::OffsetDateTime;

/// Convert an [`OffsetDateTime`] to a Unix millisecond timestamp.
pub(crate) fn to_ms_epoch(t: OffsetDateTime) -> i64 {
    let nanos = t.unix_timestamp_nanos();
    (nanos / 1_000_000) as i64
}
