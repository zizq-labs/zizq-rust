// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Range expression used by the `priority`, `ready_at`, and `attempts`
//! filter fields on the list / count / bulk delete / bulk patch
//! endpoints.
//!
//! The serialized format is `lo..hi` with inclusive bounds on both sides;
//! either side may be omitted for an unbounded end. To keep callers
//! from accidentally sending the wrong semantics, this type only
//! accepts the inclusive Rust range syntaxes — `a..=b`, `a..`, `..=b`,
//! and `..`. The standard half-open `a..b` form is **not** convertible
//! to a `RangeFilter`; the compiler will reject it. Callers who really
//! want `a..b - 1` semantics should write the inclusive form
//! explicitly.

use std::ops::{RangeFrom, RangeFull, RangeInclusive, RangeToInclusive};

/// A range expression for filtering jobs by a numeric field.
///
/// Construct via the [`From`] impls — pass a bare value for an exact
/// match, or one of the inclusive Rust range syntaxes for a range:
///
/// | Input          | Variant                | Wire format |
/// | -------------- | ---------------------- | ----------- |
/// | `n`            | [`Self::Exact`]        | `"n"`       |
/// | `a..=b`        | [`Self::Bounded`]      | `"a..b"`    |
/// | `a..`          | [`Self::AtLeast`]      | `"a.."`     |
/// | `..=b`         | [`Self::AtMost`]       | `"..b"`     |
/// | `..`           | [`Self::Unbounded`]    | `".."`      |
///
/// `RangeFilter` is rarely named explicitly in user code: each filter
/// setter takes `impl Into<RangeFilter<T>>`, so call sites read as
/// `.priority(0..=100)` rather than spelling out the variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeFilter<T> {
    /// Match a single exact value.
    Exact(T),
    /// Match values inside an inclusive range (both ends).
    Bounded(T, T),
    /// Match values greater than or equal to the bound.
    AtLeast(T),
    /// Match values less than or equal to the bound.
    AtMost(T),
    /// Match every value (no lower or upper bound).
    Unbounded,
}

impl<T> RangeFilter<T> {
    /// Render the range to the server's query format using `to_str` to
    /// stringify each bound.
    pub(crate) fn encode<F>(&self, to_str: F) -> String
    where
        F: Fn(&T) -> String,
    {
        match self {
            Self::Exact(v) => to_str(v),
            Self::Bounded(a, b) => format!("{}..{}", to_str(a), to_str(b)),
            Self::AtLeast(a) => format!("{}..", to_str(a)),
            Self::AtMost(b) => format!("..{}", to_str(b)),
            Self::Unbounded => "..".to_string(),
        }
    }
}

impl<T> From<RangeInclusive<T>> for RangeFilter<T> {
    fn from(r: RangeInclusive<T>) -> Self {
        let (start, end) = r.into_inner();
        Self::Bounded(start, end)
    }
}

impl<T> From<RangeFrom<T>> for RangeFilter<T> {
    fn from(r: RangeFrom<T>) -> Self {
        Self::AtLeast(r.start)
    }
}

impl<T> From<RangeToInclusive<T>> for RangeFilter<T> {
    fn from(r: RangeToInclusive<T>) -> Self {
        Self::AtMost(r.end)
    }
}

impl<T> From<RangeFull> for RangeFilter<T> {
    fn from(_: RangeFull) -> Self {
        Self::Unbounded
    }
}

// Single-value conversions for the concrete types used by each
// filterable field. A blanket `impl<T> From<T> for RangeFilter<T>`
// would conflict with the per-shape From impls above when `T` is
// instantiated to one of the range types (`From<RangeFull>` for
// `T = RangeFull`, etc.), so we list the concrete types instead.
impl From<u16> for RangeFilter<u16> {
    fn from(v: u16) -> Self {
        Self::Exact(v)
    }
}

impl From<u32> for RangeFilter<u32> {
    fn from(v: u32) -> Self {
        Self::Exact(v)
    }
}

impl From<u64> for RangeFilter<u64> {
    fn from(v: u64) -> Self {
        Self::Exact(v)
    }
}

impl From<::time::OffsetDateTime> for RangeFilter<::time::OffsetDateTime> {
    fn from(v: ::time::OffsetDateTime) -> Self {
        Self::Exact(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_u16(r: RangeFilter<u16>) -> String {
        r.encode(|v| v.to_string())
    }

    #[test]
    fn from_single_value() {
        assert_eq!(encode_u16(RangeFilter::from(50u16)), "50");
    }

    #[test]
    fn from_inclusive_range() {
        assert_eq!(encode_u16(RangeFilter::from(0u16..=100)), "0..100");
    }

    #[test]
    fn from_range_from() {
        assert_eq!(encode_u16(RangeFilter::from(50u16..)), "50..");
    }

    #[test]
    fn from_range_to_inclusive() {
        assert_eq!(encode_u16(RangeFilter::from(..=100u16)), "..100");
    }

    #[test]
    fn from_range_full() {
        assert_eq!(encode_u16(RangeFilter::from(..)), "..");
    }

    #[test]
    fn encode_with_custom_stringifier() {
        // Stand-in for the OffsetDateTime → ms conversion used by
        // ready_at: tweak the stringifier per call site, the rest of
        // the encoding logic stays the same.
        let r: RangeFilter<u64> = RangeFilter::from(1_500u64..=2_500);
        assert_eq!(r.encode(|v| (v * 2).to_string()), "3000..5000");
    }
}
