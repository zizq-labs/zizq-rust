// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Parser for `#[zizq(...)]` container attributes.
//!
//! Collects every `#[zizq(...)]` attribute on the derived item into
//! a single [`ZizqAttrs`] struct. Each field mirrors one of the
//! trait's associated constants and stays `None` when the user hasn't
//! set it — that way the emitter can decide whether to emit an
//! override or let the trait's default apply.

use syn::{DeriveInput, Expr, ExprArray, ExprLit, Lit, LitBool, LitStr};

/// Container-level `#[zizq(...)]` attributes recognised on a
/// `#[derive(JobKind)]` struct.
///
/// `Debug` is only derived under `cfg(test)` — `syn::Expr` /
/// `syn::LitStr` require syn's `extra-traits` feature for it, which
/// this crate only enables in dev-dependencies to avoid paying the
/// cost in the shipped proc-macro.
///
/// Numeric fields are stored as [`Expr`] rather than a literal type
/// so users can write const-evaluable arithmetic (e.g.
/// `dead_ms = 7 * 24 * 60 * 60 * 1000`). The trade-off: range
/// validation happens at Rust's const-eval time on the generated
/// code, not at derive expansion, so an overflow error points at
/// the trait's const declaration rather than the user's literal.
#[derive(Default)]
#[cfg_attr(test, derive(Debug))]
pub(crate) struct ZizqAttrs {
    /// `#[zizq(name = "...")]` — overrides the API job type name.
    /// When absent, the emitter defaults to the struct's identifier.
    pub name: Option<LitStr>,

    /// `#[zizq(queue = "...")]` — overrides [`JobKind::QUEUE`].
    pub queue: Option<LitStr>,

    /// `#[zizq(priority = <expr>)]` — overrides [`JobKind::PRIORITY`].
    /// Any const-evaluable expression that fits in `u16`.
    pub priority: Option<Expr>,

    /// `#[zizq(retry_limit = <expr>)]` — overrides
    /// [`JobKind::RETRY_LIMIT`]. Any const-evaluable expression that
    /// fits in `u32`.
    pub retry_limit: Option<Expr>,

    /// `#[zizq(backoff(base_ms = ..., exponent = ..., jitter_ms = ...))]`
    /// — overrides [`JobKind::BACKOFF`]. All three inner fields are
    /// required.
    pub backoff: Option<BackoffAttr>,

    /// `#[zizq(retention(completed_ms = ..., dead_ms = ...))]` —
    /// overrides [`JobKind::RETENTION`]. Each inner field is optional
    /// individually but at least one must be present, otherwise the
    /// override is meaningless.
    pub retention: Option<RetentionAttr>,

    /// `#[zizq(unique)]` / `#[zizq(unique(only = [...], except = [...],
    /// scope = "...", prefix = false))]` — emits a
    /// [`JobKind::unique_key`] implementation.
    pub unique: Option<UniqueAttr>,
}

/// Parsed contents of `#[zizq(unique(...))]`. All fields are
/// optional; a bare `#[zizq(unique)]` produces `UniqueAttr::default()`
/// which hashes the entire payload with the type name as tag prefix.
#[cfg_attr(test, derive(Debug))]
#[derive(Default)]
pub(crate) struct UniqueAttr {
    /// Path-set narrowing the payload before hashing. `Only` and
    /// `Except` are mutually exclusive.
    pub selection: Option<UniqueSelection>,

    /// `scope = "queued" | "active" | "exists"`. When `None`, the
    /// server's default (`Queued`) applies.
    pub scope: Option<UniqueScopeAttr>,

    /// `prefix = true | false`. When `None`, treated as `true` — the
    /// type name is used as the hash's tag prefix
    /// ([`UniqueKey::tagged_hash_of`]). `false` switches to the
    /// unprefixed form ([`UniqueKey::hash_of`]).
    ///
    /// [`UniqueKey::tagged_hash_of`]: zizq::UniqueKey::tagged_hash_of
    /// [`UniqueKey::hash_of`]: zizq::UniqueKey::hash_of
    pub prefix: Option<LitBool>,
}

/// Payload subsetting for [`UniqueAttr::selection`]. Only one variant
/// applies per attribute — the parser rejects the combination.
#[cfg_attr(test, derive(Debug))]
pub(crate) enum UniqueSelection {
    /// `only = [".foo", ".bar"]` — hash only these sub-paths.
    Only(Vec<LitStr>),

    /// `except = [".foo", ".bar"]` — hash the payload minus these
    /// sub-paths.
    Except(Vec<LitStr>),
}

/// One of the three [`UniqueScope`] variants selectable by string in
/// `#[zizq(unique(scope = "..."))]`.
///
/// [`UniqueScope`]: zizq::UniqueScope
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) enum UniqueScopeAttr {
    Queued,
    Active,
    Exists,
}

/// Parsed contents of `#[zizq(backoff(...))]`. Every field is populated
/// by the time this struct is stored on [`ZizqAttrs`] — the parser
/// rejects the whole attribute if any is missing.
#[cfg_attr(test, derive(Debug))]
pub(crate) struct BackoffAttr {
    pub base_ms: Expr,
    pub exponent: Expr,
    pub jitter_ms: Expr,
}

/// Parsed contents of `#[zizq(retention(...))]`. Both fields are
/// `Option` because the outer attribute is a partial override — the
/// user can set only `completed_ms`, only `dead_ms`, or both, and the
/// unspecified field falls through to the server's default.
#[cfg_attr(test, derive(Debug))]
pub(crate) struct RetentionAttr {
    pub completed_ms: Option<Expr>,
    pub dead_ms: Option<Expr>,
}

impl ZizqAttrs {
    /// Walk every `#[zizq(...)]` attribute on `input` and collect the
    /// recognised keys. Returns a span-attached [`syn::Error`] for
    /// unknown keys, duplicate keys, or out-of-range integer values.
    ///
    /// Multiple `#[zizq(...)]` attributes on the same item are
    /// merged into one [`ZizqAttrs`], mirroring how serde treats
    /// stacked `#[serde(...)]`s.
    pub(crate) fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut attrs = ZizqAttrs::default();

        for attr in &input.attrs {
            if !attr.path().is_ident("zizq") {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                let ident = meta.path.require_ident()?;
                match ident.to_string().as_str() {
                    "name" => {
                        if attrs.name.is_some() {
                            return Err(meta.error("duplicate `name` attribute"));
                        }
                        attrs.name = Some(meta.value()?.parse::<LitStr>()?);
                    }
                    "queue" => {
                        if attrs.queue.is_some() {
                            return Err(meta.error("duplicate `queue` attribute"));
                        }
                        attrs.queue = Some(meta.value()?.parse::<LitStr>()?);
                    }
                    "priority" => {
                        if attrs.priority.is_some() {
                            return Err(meta.error("duplicate `priority` attribute"));
                        }
                        attrs.priority = Some(meta.value()?.parse::<Expr>()?);
                    }
                    "retry_limit" => {
                        if attrs.retry_limit.is_some() {
                            return Err(meta.error("duplicate `retry_limit` attribute"));
                        }
                        attrs.retry_limit = Some(meta.value()?.parse::<Expr>()?);
                    }
                    "backoff" => {
                        if attrs.backoff.is_some() {
                            return Err(meta.error("duplicate `backoff` attribute"));
                        }
                        attrs.backoff = Some(parse_backoff(&meta)?);
                    }
                    "retention" => {
                        if attrs.retention.is_some() {
                            return Err(meta.error("duplicate `retention` attribute"));
                        }
                        attrs.retention = Some(parse_retention(&meta)?);
                    }
                    "unique" => {
                        if attrs.unique.is_some() {
                            return Err(meta.error("duplicate `unique` attribute"));
                        }
                        // `unique` may be bare (Meta::Path) or take a
                        // parenthesised body (Meta::List). Peek for the
                        // opening paren to decide which shape to parse.
                        if meta.input.peek(syn::token::Paren) {
                            attrs.unique = Some(parse_unique(&meta)?);
                        } else {
                            attrs.unique = Some(UniqueAttr::default());
                        }
                    }
                    other => {
                        return Err(meta.error(format!("unknown `zizq` attribute `{other}`")));
                    }
                }
                Ok(())
            })?;
        }

        Ok(attrs)
    }
}

/// Parse the `(base_ms = ..., exponent = ..., jitter_ms = ...)` body
/// of a `#[zizq(backoff(...))]` attribute. All three fields are
/// required — a missing field is a compile error with a span on the
/// outer `backoff` keyword (the natural place for the user to add it).
fn parse_backoff(meta: &syn::meta::ParseNestedMeta) -> syn::Result<BackoffAttr> {
    let mut base_ms: Option<Expr> = None;
    let mut exponent: Option<Expr> = None;
    let mut jitter_ms: Option<Expr> = None;

    meta.parse_nested_meta(|inner| {
        let ident = inner.path.require_ident()?;
        match ident.to_string().as_str() {
            "base_ms" => {
                if base_ms.is_some() {
                    return Err(inner.error("duplicate `base_ms` field"));
                }
                base_ms = Some(inner.value()?.parse::<Expr>()?);
            }
            "exponent" => {
                if exponent.is_some() {
                    return Err(inner.error("duplicate `exponent` field"));
                }
                exponent = Some(inner.value()?.parse::<Expr>()?);
            }
            "jitter_ms" => {
                if jitter_ms.is_some() {
                    return Err(inner.error("duplicate `jitter_ms` field"));
                }
                jitter_ms = Some(inner.value()?.parse::<Expr>()?);
            }
            other => {
                return Err(inner.error(format!("unknown `backoff` field `{other}`")));
            }
        }
        Ok(())
    })?;

    match (base_ms, exponent, jitter_ms) {
        (Some(base_ms), Some(exponent), Some(jitter_ms)) => Ok(BackoffAttr {
            base_ms,
            exponent,
            jitter_ms,
        }),
        _ => {
            Err(meta.error("`backoff(...)` requires all of `base_ms`, `exponent`, and `jitter_ms`"))
        }
    }
}

/// Parse the `(completed_ms = ..., dead_ms = ...)` body of a
/// `#[zizq(retention(...))]` attribute. Both fields are optional but
/// the attribute as a whole must set at least one — an empty
/// `retention()` is a compile error since it's a no-op.
fn parse_retention(meta: &syn::meta::ParseNestedMeta) -> syn::Result<RetentionAttr> {
    let mut completed_ms: Option<Expr> = None;
    let mut dead_ms: Option<Expr> = None;

    meta.parse_nested_meta(|inner| {
        let ident = inner.path.require_ident()?;
        match ident.to_string().as_str() {
            "completed_ms" => {
                if completed_ms.is_some() {
                    return Err(inner.error("duplicate `completed_ms` field"));
                }
                completed_ms = Some(inner.value()?.parse::<Expr>()?);
            }
            "dead_ms" => {
                if dead_ms.is_some() {
                    return Err(inner.error("duplicate `dead_ms` field"));
                }
                dead_ms = Some(inner.value()?.parse::<Expr>()?);
            }
            other => {
                return Err(inner.error(format!("unknown `retention` field `{other}`")));
            }
        }
        Ok(())
    })?;

    if completed_ms.is_none() && dead_ms.is_none() {
        return Err(
            meta.error("`retention(...)` requires at least one of `completed_ms` or `dead_ms`")
        );
    }

    Ok(RetentionAttr {
        completed_ms,
        dead_ms,
    })
}

/// Parse the `(only = [...], except = [...], scope = "...", prefix = ...)`
/// body of a `#[zizq(unique(...))]` attribute.
///
/// `only` and `except` are mutually exclusive — setting both is a
/// compile error. `scope` accepts one of the three string variants
/// mapped to [`UniqueScopeAttr`]. `prefix` is a bare bool literal.
fn parse_unique(meta: &syn::meta::ParseNestedMeta) -> syn::Result<UniqueAttr> {
    let mut only: Option<Vec<LitStr>> = None;
    let mut except: Option<Vec<LitStr>> = None;
    let mut scope: Option<UniqueScopeAttr> = None;
    let mut prefix: Option<LitBool> = None;

    meta.parse_nested_meta(|inner| {
        let ident = inner.path.require_ident()?;
        match ident.to_string().as_str() {
            "only" => {
                if only.is_some() {
                    return Err(inner.error("duplicate `only` field"));
                }
                only = Some(parse_str_array(&inner)?);
            }
            "except" => {
                if except.is_some() {
                    return Err(inner.error("duplicate `except` field"));
                }
                except = Some(parse_str_array(&inner)?);
            }
            "scope" => {
                if scope.is_some() {
                    return Err(inner.error("duplicate `scope` field"));
                }
                let lit: LitStr = inner.value()?.parse()?;
                scope = Some(match lit.value().as_str() {
                    "queued" => UniqueScopeAttr::Queued,
                    "active" => UniqueScopeAttr::Active,
                    "exists" => UniqueScopeAttr::Exists,
                    other => {
                        return Err(syn::Error::new_spanned(
                            &lit,
                            format!(
                                "unknown scope `{other}` — expected `queued`, `active`, or `exists`",
                            ),
                        ));
                    }
                });
            }
            "prefix" => {
                if prefix.is_some() {
                    return Err(inner.error("duplicate `prefix` field"));
                }
                prefix = Some(inner.value()?.parse::<LitBool>()?);
            }
            other => {
                return Err(inner.error(format!("unknown `unique` field `{other}`")));
            }
        }
        Ok(())
    })?;

    let selection = match (only, except) {
        (Some(_), Some(_)) => {
            return Err(meta.error("`only` and `except` are mutually exclusive on `unique(...)`"));
        }
        (Some(paths), None) => Some(UniqueSelection::Only(paths)),
        (None, Some(paths)) => Some(UniqueSelection::Except(paths)),
        (None, None) => None,
    };

    Ok(UniqueAttr {
        selection,
        scope,
        prefix,
    })
}

/// Parse a bracketed array of string literals (`["a", "b", ...]`)
/// out of `inner.value()`. Each element must be a plain `LitStr` —
/// bare identifiers, integers, or other expressions error with a
/// span on the offending element.
fn parse_str_array(inner: &syn::meta::ParseNestedMeta) -> syn::Result<Vec<LitStr>> {
    let arr: ExprArray = inner.value()?.parse()?;
    arr.elems
        .into_iter()
        .map(|elem| match elem {
            Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) => Ok(s),
            other => Err(syn::Error::new_spanned(
                &other,
                "expected a string literal (e.g. `\".foo\"`)",
            )),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_str(src: &str) -> syn::Result<ZizqAttrs> {
        let input: DeriveInput = syn::parse_str(src)?;
        ZizqAttrs::parse(&input)
    }

    #[test]
    fn no_zizq_attrs_yields_all_none() {
        let attrs = parse_str("struct Foo;").unwrap();
        assert!(attrs.name.is_none());
        assert!(attrs.queue.is_none());
        assert!(attrs.priority.is_none());
        assert!(attrs.retry_limit.is_none());
    }

    #[test]
    fn parses_name() {
        let attrs = parse_str(r#"#[zizq(name = "foo")] struct Foo;"#).unwrap();
        assert_eq!(attrs.name.unwrap().value(), "foo");
    }

    #[test]
    fn parses_queue() {
        let attrs = parse_str(r#"#[zizq(queue = "emails")] struct Foo;"#).unwrap();
        assert_eq!(attrs.queue.unwrap().value(), "emails");
    }

    /// Helper — round-trip an [`Expr`] through the token stream so we
    /// can compare the parsed contents by shape rather than value.
    /// Integration tests in `zizq/tests/derive.rs` verify actual
    /// runtime const values.
    fn expr_tokens(expr: &Expr) -> String {
        use quote::ToTokens;
        expr.to_token_stream().to_string()
    }

    #[test]
    fn parses_priority() {
        let attrs = parse_str(r#"#[zizq(priority = 100)] struct Foo;"#).unwrap();
        assert_eq!(expr_tokens(&attrs.priority.unwrap()), "100");
    }

    #[test]
    fn parses_retry_limit() {
        let attrs = parse_str(r#"#[zizq(retry_limit = 5)] struct Foo;"#).unwrap();
        assert_eq!(expr_tokens(&attrs.retry_limit.unwrap()), "5");
    }

    #[test]
    fn parses_priority_from_arithmetic_expression() {
        // Numeric fields accept any const-evaluable expression, not
        // just literals. Overflow is caught at Rust's const-eval time
        // on the generated code.
        let attrs = parse_str(r#"#[zizq(priority = 50 + 50)] struct Foo;"#).unwrap();
        assert_eq!(expr_tokens(&attrs.priority.unwrap()), "50 + 50");
    }

    #[test]
    fn parses_retry_limit_from_arithmetic_expression() {
        let attrs = parse_str(r#"#[zizq(retry_limit = 3 * 2)] struct Foo;"#).unwrap();
        assert_eq!(expr_tokens(&attrs.retry_limit.unwrap()), "3 * 2");
    }

    #[test]
    fn parses_all_in_one_attribute() {
        let attrs = parse_str(
            r#"#[zizq(name = "n", queue = "q", priority = 10, retry_limit = 3)] struct Foo;"#,
        )
        .unwrap();
        assert_eq!(attrs.name.unwrap().value(), "n");
        assert_eq!(attrs.queue.unwrap().value(), "q");
        assert_eq!(expr_tokens(&attrs.priority.unwrap()), "10");
        assert_eq!(expr_tokens(&attrs.retry_limit.unwrap()), "3");
    }

    #[test]
    fn stacked_zizq_attributes_merge() {
        let attrs = parse_str(
            r#"
            #[zizq(name = "n")]
            #[zizq(queue = "q")]
            struct Foo;
            "#,
        )
        .unwrap();
        assert_eq!(attrs.name.unwrap().value(), "n");
        assert_eq!(attrs.queue.unwrap().value(), "q");
    }

    #[test]
    fn priority_at_upper_bound_parses() {
        // Range validation is deferred to Rust's const-eval on the
        // generated code, so the parser just captures the expression.
        let attrs = parse_str(r#"#[zizq(priority = 65535)] struct Foo;"#).unwrap();
        assert_eq!(expr_tokens(&attrs.priority.unwrap()), "65535");
    }

    // Out-of-range integers like `priority = 65536` are no longer
    // caught at derive time — they surface as a Rust const-eval error
    // on the generated `Some(65536_u16)` (or similar). See
    // `zizq/tests/derive_compile_fail_demo/` (added later) for the
    // expected error UX.

    #[test]
    fn unknown_attribute_errors_with_helpful_message() {
        let err = parse_str(r#"#[zizq(nam = "typo")] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown"), "unexpected error: {msg}");
        assert!(msg.contains("`nam`"), "unexpected error: {msg}");
    }

    #[test]
    fn duplicate_key_within_one_attr_errors() {
        let err = parse_str(r#"#[zizq(name = "a", name = "b")] struct Foo;"#).unwrap_err();
        assert!(
            err.to_string().contains("duplicate"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn duplicate_key_across_attrs_errors() {
        let err = parse_str(
            r#"
            #[zizq(name = "a")]
            #[zizq(name = "b")]
            struct Foo;
            "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn non_zizq_attributes_are_ignored() {
        // serde attrs (and any other) must pass through untouched.
        let attrs = parse_str(
            r#"
            #[serde(rename_all = "camelCase")]
            #[zizq(name = "foo")]
            struct Foo;
            "#,
        )
        .unwrap();
        assert_eq!(attrs.name.unwrap().value(), "foo");
    }

    // --- backoff ---

    #[test]
    fn parses_full_backoff() {
        let attrs = parse_str(
            r#"#[zizq(backoff(base_ms = 1000, exponent = 2.0, jitter_ms = 500))] struct Foo;"#,
        )
        .unwrap();
        let b = attrs.backoff.unwrap();
        assert_eq!(expr_tokens(&b.base_ms), "1000");
        assert_eq!(expr_tokens(&b.exponent), "2.0");
        assert_eq!(expr_tokens(&b.jitter_ms), "500");
    }

    #[test]
    fn parses_backoff_with_arithmetic_expressions() {
        let attrs = parse_str(
            r#"#[zizq(backoff(base_ms = 5 * 200, exponent = 1.5 + 0.5, jitter_ms = 100 * 5))] struct Foo;"#,
        )
        .unwrap();
        let b = attrs.backoff.unwrap();
        assert_eq!(expr_tokens(&b.base_ms), "5 * 200");
        assert_eq!(expr_tokens(&b.exponent), "1.5 + 0.5");
        assert_eq!(expr_tokens(&b.jitter_ms), "100 * 5");
    }

    #[test]
    fn backoff_missing_field_errors() {
        let err = parse_str(r#"#[zizq(backoff(base_ms = 1000, exponent = 2.0))] struct Foo;"#)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("requires all of"), "unexpected error: {msg}");
        assert!(msg.contains("jitter_ms"), "unexpected error: {msg}");
    }

    #[test]
    fn backoff_unknown_field_errors() {
        let err = parse_str(
            r#"#[zizq(backoff(base_ms = 1000, exponent = 2.0, jitter_ms = 500, foo = 1))] struct Foo;"#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown `backoff` field"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("`foo`"), "unexpected error: {msg}");
    }

    #[test]
    fn backoff_duplicate_field_errors() {
        let err = parse_str(
            r#"#[zizq(backoff(base_ms = 1000, base_ms = 2000, exponent = 2.0, jitter_ms = 500))] struct Foo;"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate `base_ms`"),
            "unexpected error: {err}",
        );
    }

    // The `exponent` field now accepts any expression, so `exponent = 2`
    // parses at derive time. The generated `Some(BackoffConfig { exponent: 2, ... })`
    // will fail at Rust's type-check ("expected `f32`, found integer")
    // rather than at derive expansion — a small UX regression from the
    // switch to `Expr`, offset by supporting arithmetic like
    // `exponent = 1.5 + 0.5`.

    #[test]
    fn duplicate_backoff_attribute_errors() {
        let err = parse_str(
            r#"
            #[zizq(backoff(base_ms = 1000, exponent = 2.0, jitter_ms = 500))]
            #[zizq(backoff(base_ms = 2000, exponent = 3.0, jitter_ms = 100))]
            struct Foo;
            "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate `backoff`"),
            "unexpected error: {err}",
        );
    }

    // --- retention ---

    #[test]
    fn parses_retention_with_both_fields() {
        let attrs = parse_str(
            r#"#[zizq(retention(completed_ms = 60000, dead_ms = 86400000))] struct Foo;"#,
        )
        .unwrap();
        let r = attrs.retention.unwrap();
        assert_eq!(expr_tokens(&r.completed_ms.unwrap()), "60000");
        assert_eq!(expr_tokens(&r.dead_ms.unwrap()), "86400000");
    }

    #[test]
    fn parses_retention_with_arithmetic_expressions() {
        // The main win of moving to `Expr`: readable duration math.
        let attrs =
            parse_str(r#"#[zizq(retention(dead_ms = 7 * 24 * 60 * 60 * 1000))] struct Foo;"#)
                .unwrap();
        let r = attrs.retention.unwrap();
        assert!(r.completed_ms.is_none());
        assert_eq!(expr_tokens(&r.dead_ms.unwrap()), "7 * 24 * 60 * 60 * 1000",);
    }

    #[test]
    fn parses_retention_with_only_completed_ms() {
        let attrs = parse_str(r#"#[zizq(retention(completed_ms = 60000))] struct Foo;"#).unwrap();
        let r = attrs.retention.unwrap();
        assert_eq!(expr_tokens(&r.completed_ms.unwrap()), "60000");
        assert!(r.dead_ms.is_none());
    }

    #[test]
    fn parses_retention_with_only_dead_ms() {
        let attrs = parse_str(r#"#[zizq(retention(dead_ms = 86400000))] struct Foo;"#).unwrap();
        let r = attrs.retention.unwrap();
        assert!(r.completed_ms.is_none());
        assert_eq!(expr_tokens(&r.dead_ms.unwrap()), "86400000");
    }

    #[test]
    fn empty_retention_errors() {
        // Two error paths reach the user: either syn rejects the empty
        // parens up front ("unexpected end of input, expected nested
        // attribute") or our own "requires at least one" check fires
        // once the inner parse loop completes with nothing populated.
        // Both point at the empty parens, so either is acceptable UX.
        let err = parse_str(r#"#[zizq(retention())] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("requires at least one") || msg.contains("expected nested attribute"),
            "unexpected error: {msg}",
        );
    }

    #[test]
    fn retention_unknown_field_errors() {
        let err = parse_str(r#"#[zizq(retention(completed_ms = 60000, unknown = 1))] struct Foo;"#)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown `retention` field"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn retention_duplicate_field_errors() {
        let err =
            parse_str(r#"#[zizq(retention(completed_ms = 1, completed_ms = 2))] struct Foo;"#)
                .unwrap_err();
        assert!(
            err.to_string().contains("duplicate `completed_ms`"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn duplicate_retention_attribute_errors() {
        let err = parse_str(
            r#"
            #[zizq(retention(completed_ms = 1))]
            #[zizq(retention(dead_ms = 2))]
            struct Foo;
            "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate `retention`"),
            "unexpected error: {err}",
        );
    }

    // --- unique ---

    #[test]
    fn bare_unique_parses_with_defaults() {
        let attrs = parse_str(r#"#[zizq(unique)] struct Foo;"#).unwrap();
        let u = attrs.unique.unwrap();
        assert!(u.selection.is_none());
        assert!(u.scope.is_none());
        assert!(u.prefix.is_none());
    }

    #[test]
    fn parses_unique_only() {
        let attrs =
            parse_str(r#"#[zizq(unique(only = [".user_id", ".campaign_id"]))] struct Foo;"#)
                .unwrap();
        let u = attrs.unique.unwrap();
        let paths = match u.selection {
            Some(UniqueSelection::Only(p)) => p,
            other => panic!("expected Only, got {other:?}"),
        };
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].value(), ".user_id");
        assert_eq!(paths[1].value(), ".campaign_id");
    }

    #[test]
    fn parses_unique_except() {
        let attrs = parse_str(r#"#[zizq(unique(except = [".body"]))] struct Foo;"#).unwrap();
        let u = attrs.unique.unwrap();
        let paths = match u.selection {
            Some(UniqueSelection::Except(p)) => p,
            other => panic!("expected Except, got {other:?}"),
        };
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].value(), ".body");
    }

    #[test]
    fn only_and_except_together_error() {
        let err = parse_str(r#"#[zizq(unique(only = [".a"], except = [".b"]))] struct Foo;"#)
            .unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn parses_unique_scope() {
        let attrs = parse_str(r#"#[zizq(unique(scope = "active"))] struct Foo;"#).unwrap();
        let u = attrs.unique.unwrap();
        assert_eq!(u.scope, Some(UniqueScopeAttr::Active));
    }

    #[test]
    fn parses_unique_scope_queued_and_exists() {
        let q = parse_str(r#"#[zizq(unique(scope = "queued"))] struct Foo;"#).unwrap();
        assert_eq!(q.unique.unwrap().scope, Some(UniqueScopeAttr::Queued));
        let e = parse_str(r#"#[zizq(unique(scope = "exists"))] struct Foo;"#).unwrap();
        assert_eq!(e.unique.unwrap().scope, Some(UniqueScopeAttr::Exists));
    }

    #[test]
    fn unknown_scope_errors() {
        let err = parse_str(r#"#[zizq(unique(scope = "bogus"))] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown scope"), "unexpected error: {msg}");
        assert!(msg.contains("`bogus`"), "unexpected error: {msg}");
    }

    #[test]
    fn parses_unique_prefix_false() {
        let attrs = parse_str(r#"#[zizq(unique(prefix = false))] struct Foo;"#).unwrap();
        let u = attrs.unique.unwrap();
        assert_eq!(u.prefix.unwrap().value, false);
    }

    #[test]
    fn parses_unique_all_options_combined() {
        let attrs = parse_str(
            r#"#[zizq(unique(only = [".x", ".y"], scope = "active", prefix = false))] struct Foo;"#,
        )
        .unwrap();
        let u = attrs.unique.unwrap();
        assert!(matches!(u.selection, Some(UniqueSelection::Only(_))));
        assert_eq!(u.scope, Some(UniqueScopeAttr::Active));
        assert_eq!(u.prefix.unwrap().value, false);
    }

    #[test]
    fn unknown_unique_field_errors() {
        let err = parse_str(r#"#[zizq(unique(bogus = true))] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown `unique` field"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn non_string_element_in_only_errors() {
        let err = parse_str(r#"#[zizq(unique(only = [42]))] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("string literal"), "unexpected error: {msg}");
    }

    #[test]
    fn duplicate_unique_attribute_errors() {
        let err = parse_str(
            r#"
            #[zizq(unique)]
            #[zizq(unique(scope = "active"))]
            struct Foo;
            "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate `unique`"),
            "unexpected error: {err}",
        );
    }
}
