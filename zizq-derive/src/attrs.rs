// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Parser for `#[zizq(...)]` container attributes.
//!
//! Collects every `#[zizq(...)]` attribute on the derived item into
//! a single [`ZizqAttrs`] struct. Each field mirrors one of the
//! trait's associated constants and stays `None` when the user hasn't
//! set it — that way the emitter can decide whether to emit an
//! override or let the trait's default apply.

use syn::{DeriveInput, LitInt, LitStr};

/// Container-level `#[zizq(...)]` attributes recognised on a
/// `#[derive(JobKind)]` struct.
///
/// `Debug` is only derived under `cfg(test)` — `syn::LitStr` /
/// `syn::LitInt` require syn's `extra-traits` feature for it, which
/// this crate only enables in dev-dependencies to avoid paying the
/// cost in the shipped proc-macro.
#[derive(Default)]
#[cfg_attr(test, derive(Debug))]
pub(crate) struct ZizqAttrs {
    /// `#[zizq(name = "...")]` — overrides the API job type name.
    /// When absent, the emitter defaults to the struct's identifier.
    pub name: Option<LitStr>,

    /// `#[zizq(queue = "...")]` — overrides [`JobKind::QUEUE`].
    pub queue: Option<LitStr>,

    /// `#[zizq(priority = N)]` — overrides [`JobKind::PRIORITY`].
    /// Validated to fit in `u16` at parse time.
    pub priority: Option<LitInt>,

    /// `#[zizq(retry_limit = N)]` — overrides [`JobKind::RETRY_LIMIT`].
    /// Validated to fit in `u32` at parse time.
    pub retry_limit: Option<LitInt>,
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
                        let lit: LitInt = meta.value()?.parse()?;
                        // Validate range up front so the error span
                        // points at the user's literal, not at the
                        // derive-generated code.
                        lit.base10_parse::<u16>()?;
                        attrs.priority = Some(lit);
                    }
                    "retry_limit" => {
                        if attrs.retry_limit.is_some() {
                            return Err(meta.error("duplicate `retry_limit` attribute"));
                        }
                        let lit: LitInt = meta.value()?.parse()?;
                        lit.base10_parse::<u32>()?;
                        attrs.retry_limit = Some(lit);
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

    #[test]
    fn parses_priority() {
        let attrs = parse_str(r#"#[zizq(priority = 100)] struct Foo;"#).unwrap();
        assert_eq!(attrs.priority.unwrap().base10_parse::<u16>().unwrap(), 100);
    }

    #[test]
    fn parses_retry_limit() {
        let attrs = parse_str(r#"#[zizq(retry_limit = 5)] struct Foo;"#).unwrap();
        assert_eq!(attrs.retry_limit.unwrap().base10_parse::<u32>().unwrap(), 5,);
    }

    #[test]
    fn parses_all_in_one_attribute() {
        let attrs = parse_str(
            r#"#[zizq(name = "n", queue = "q", priority = 10, retry_limit = 3)] struct Foo;"#,
        )
        .unwrap();
        assert_eq!(attrs.name.unwrap().value(), "n");
        assert_eq!(attrs.queue.unwrap().value(), "q");
        assert_eq!(attrs.priority.unwrap().base10_parse::<u16>().unwrap(), 10);
        assert_eq!(attrs.retry_limit.unwrap().base10_parse::<u32>().unwrap(), 3,);
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
    fn priority_at_upper_bound_is_accepted() {
        let attrs = parse_str(r#"#[zizq(priority = 65535)] struct Foo;"#).unwrap();
        assert_eq!(
            attrs.priority.unwrap().base10_parse::<u16>().unwrap(),
            65535,
        );
    }

    #[test]
    fn priority_out_of_range_errors() {
        let err = parse_str(r#"#[zizq(priority = 65536)] struct Foo;"#).unwrap_err();
        let msg = err.to_string();
        // syn's base10_parse produces "number too large to fit in target type"
        assert!(
            msg.contains("number too large") || msg.contains("out of range"),
            "unexpected error: {msg}",
        );
    }

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
}
