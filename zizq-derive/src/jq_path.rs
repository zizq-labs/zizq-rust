// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Compile-time jq path parser used by `#[derive(JobKind)]`.
//!
//! This is the **only** jq path parser in the workspace — every
//! user-supplied path is parsed here at derive expansion time and
//! the resulting [`PathStep`] sequence is emitted directly into the
//! generated code, so no parsing happens at runtime. The main
//! `zizq` crate supplies only the walker/remover/picker that operate
//! on already-parsed paths.
//!
//! Malformed paths surface as compile errors with a caret on the
//! offending string literal.

use std::fmt;

/// A single step in a parsed path. Structurally mirrors
/// `zizq::__internal::PathStep` — the emitter translates each
/// variant into the equivalent runtime constructor call.
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
            message: format!("path must start with `.`, got {input:?}"),
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
                    i += 1;
                    let mut name = String::new();
                    while i < n && bytes[i] != b'"' {
                        if bytes[i] == b'\\' && i + 1 < n {
                            name.push(bytes[i + 1] as char);
                            i += 2;
                        } else {
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
                    i += 1;
                    if i >= n || bytes[i] != b']' {
                        return Err(PathParseError {
                            message: "expected `]` after quoted key".into(),
                            position: i,
                        });
                    }
                    i += 1;
                    steps.push(PathStep::Field(name));
                } else {
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

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke tests over the grammar. The full test surface for the
    // parser is exercised end-to-end via `zizq/tests/derive.rs`
    // (which triggers compile-error paths through the derive) and
    // via the walker tests in `zizq/src/jq_path.rs` (which prove
    // that the emitted `PathStep` sequence is what runtime code
    // expects).

    #[test]
    fn parses_root() {
        assert_eq!(parse(".").unwrap(), Vec::<PathStep>::new());
    }

    #[test]
    fn parses_nested_fields() {
        assert_eq!(
            parse(".foo.bar").unwrap(),
            vec![PathStep::Field("foo".into()), PathStep::Field("bar".into()),],
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
    fn parses_quoted_key_with_dots() {
        assert_eq!(
            parse(r#".["dotted.key"]"#).unwrap(),
            vec![PathStep::Field("dotted.key".into())],
        );
    }

    #[test]
    fn rejects_missing_leading_dot() {
        assert!(parse("foo").is_err());
    }

    #[test]
    fn rejects_field_starting_with_digit() {
        assert!(parse(".9foo").is_err());
    }

    #[test]
    fn rejects_unterminated_quoted_key() {
        assert!(parse(r#".["missing"#).is_err());
    }
}
