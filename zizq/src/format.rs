// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! API serialization format selection for client/server traffic.
//!
//! The Zizq server speaks both JSON and MessagePack on every endpoint.
//! Choose [`Format::MessagePack`] (default) for compactness and performance,
//! or [`Format::Json`] for a human-readable payload.

/// Serialization format used for both request bodies and response bodies.
///
/// Set on the [`Client`] via [`ClientBuilder::format`]. The chosen format
/// determines both the `Content-Type` header sent on requests and the `Accept`
/// header used to negotiate the response shape.
///
/// Both serialization formats are compatibile with one another and can be
/// freely mixed between API consumers.
///
/// [`Client`]: crate::Client
/// [`ClientBuilder::format`]: crate::ClientBuilder::format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Standard JSON. Human-readable and easy to inspect with `curl`.
    Json,

    /// MessagePack (default). Compact binary encoding that round-trips the
    /// same logical shape as the JSON form.
    #[default]
    MessagePack,
}

impl Format {
    /// Content type sent on request/response endpoints — `application/json`
    /// or `application/msgpack`.
    pub(crate) fn content_type(self) -> &'static str {
        match self {
            Format::Json => "application/json",
            Format::MessagePack => "application/msgpack",
        }
    }

    /// Content type sent on the streaming `/jobs/take` endpoint. The
    /// streaming format is framed differently from the request/response
    /// body (NDJSON / length-prefixed MessagePack), so it has its own
    /// content type.
    pub(crate) fn stream_content_type(self) -> &'static str {
        match self {
            Format::Json => "application/x-ndjson",
            Format::MessagePack => "application/vnd.zizq.msgpack-stream",
        }
    }

    /// Parse a `Content-Type` header value into a [`Format`], ignoring
    /// any media-type parameters (e.g. `application/json; charset=utf-8`).
    /// Recognises both the request/response and streaming content types.
    /// Returns `None` when the type isn't one we know — callers fall
    /// back to the configured format in that case.
    pub(crate) fn from_content_type(s: &str) -> Option<Self> {
        let mime = s.split(';').next()?.trim();
        match mime {
            "application/json" | "application/x-ndjson" => Some(Format::Json),
            "application/msgpack" | "application/vnd.zizq.msgpack-stream" => {
                Some(Format::MessagePack)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_content_type() {
        assert_eq!(
            Format::from_content_type("application/json"),
            Some(Format::Json)
        );
    }

    #[test]
    fn parses_msgpack_content_type() {
        assert_eq!(
            Format::from_content_type("application/msgpack"),
            Some(Format::MessagePack),
        );
    }

    #[test]
    fn parses_ndjson_stream_content_type_as_json() {
        assert_eq!(
            Format::from_content_type("application/x-ndjson"),
            Some(Format::Json),
        );
    }

    #[test]
    fn parses_msgpack_stream_content_type_as_msgpack() {
        assert_eq!(
            Format::from_content_type("application/vnd.zizq.msgpack-stream"),
            Some(Format::MessagePack),
        );
    }

    #[test]
    fn strips_media_type_parameters() {
        assert_eq!(
            Format::from_content_type("application/json; charset=utf-8"),
            Some(Format::Json),
        );
        assert_eq!(
            Format::from_content_type("application/json;charset=utf-8"),
            Some(Format::Json),
        );
        assert_eq!(
            Format::from_content_type("application/json ; charset=utf-8"),
            Some(Format::Json),
        );
    }

    #[test]
    fn tolerates_surrounding_whitespace() {
        assert_eq!(
            Format::from_content_type("  application/msgpack  "),
            Some(Format::MessagePack),
        );
    }

    #[test]
    fn unknown_type_returns_none() {
        assert_eq!(Format::from_content_type("text/plain"), None);
        assert_eq!(Format::from_content_type("application/xml"), None);
    }

    #[test]
    fn empty_string_returns_none() {
        assert_eq!(Format::from_content_type(""), None);
    }

    #[test]
    fn case_sensitive() {
        // Matching is strict-case for now; the server we control sends
        // lowercase, so this is fine. Flip to case-insensitive if we
        // ever sit behind a proxy that reformats Content-Type.
        assert_eq!(Format::from_content_type("Application/JSON"), None);
    }
}
