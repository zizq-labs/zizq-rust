// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Proc-macro companion for the [`zizq`] crate.
//!
//! This crate exists only to house `#[derive(JobKind)]` (and any
//! future zizq derives). It has no user-facing API of its own — it is
//! re-exported from `zizq` when its `derive` feature is enabled.
//!
//! Do not depend on this crate directly.
//!
//! [`zizq`]: https://docs.rs/zizq

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

use crate::attrs::{UniqueAttr, UniqueScopeAttr, UniqueSelection};

mod attrs;
mod jq_path;

/// Derive an implementation of `zizq::JobKind` for the annotated
/// struct.
///
/// Supports the following container attributes:
///
/// - `#[zizq(name = "...")]` — overrides the API job type name.
///   Defaults to the struct's identifier when absent.
/// - `#[zizq(queue = "...")]` — overrides `JobKind::QUEUE`.
/// - `#[zizq(priority = N)]` — overrides `JobKind::PRIORITY`
///   (`u16`, range 0-65535).
/// - `#[zizq(retry_limit = N)]` — overrides `JobKind::RETRY_LIMIT`
///   (`u32`).
/// - `#[zizq(backoff(base_ms = ..., exponent = ..., jitter_ms = ...))]`
///   — overrides `JobKind::BACKOFF`. All three fields are required.
/// - `#[zizq(retention(completed_ms = ..., dead_ms = ...))]` —
///   overrides `JobKind::RETENTION`. At least one inner field is
///   required; the unspecified one falls through to the server default.
/// - `#[zizq(unique)]` — hash the whole payload as the uniqueness
///   key, with the type name as tag prefix. May take a
///   `unique(only = [...], except = [...], scope = "...", prefix = ...)`
///   body: `only`/`except` narrow the hashed subset (mutually
///   exclusive), `scope` selects one of `"queued"` / `"active"` /
///   `"exists"`, `prefix = false` drops the type-name tag.
///
/// The next commit adds `batch`.
#[proc_macro_derive(JobKind, attributes(zizq))]
pub fn derive_job_kind(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_job_kind_impl(&input) {
        Ok(tokens) => tokens.into(),
        // Route parse errors back through the compiler as span-attached
        // diagnostics instead of panicking. `to_compile_error()` produces
        // a token stream containing `compile_error!(...)` invocations
        // that rustc renders with the original spans intact.
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_job_kind_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let attrs = attrs::ZizqAttrs::parse(input)?;

    let ty = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // NAME defaults to the struct's identifier when the user hasn't
    // provided an explicit `#[zizq(name = "...")]`. Any override wins.
    let name_expr = match &attrs.name {
        Some(lit) => quote! { #lit },
        None => {
            let default = ty.to_string();
            quote! { #default }
        }
    };

    // The remaining constants only get emitted when the user set them
    // — otherwise the trait's own default applies, unchanged.
    let queue_const = attrs.queue.as_ref().map(|lit| {
        quote! { const QUEUE: &'static str = #lit; }
    });
    let priority_const = attrs.priority.as_ref().map(|lit| {
        quote! { const PRIORITY: ::core::option::Option<u16> = ::core::option::Option::Some(#lit); }
    });
    let retry_limit_const = attrs.retry_limit.as_ref().map(|lit| {
        quote! { const RETRY_LIMIT: ::core::option::Option<u32> = ::core::option::Option::Some(#lit); }
    });
    let backoff_const = attrs.backoff.as_ref().map(|b| {
        let base_ms = &b.base_ms;
        let exponent = &b.exponent;
        let jitter_ms = &b.jitter_ms;
        quote! {
            const BACKOFF: ::core::option::Option<::zizq::BackoffConfig> =
                ::core::option::Option::Some(::zizq::BackoffConfig {
                    base_ms: #base_ms,
                    exponent: #exponent,
                    jitter_ms: #jitter_ms,
                });
        }
    });
    let unique_key_fn = attrs.unique.as_ref().map(emit_unique_key).transpose()?;
    let retention_const = attrs.retention.as_ref().map(|r| {
        // Each inner field maps to `Some(lit)` or `None`, so the
        // emitted struct literal is always a `RetentionConfig` with
        // both fields explicit.
        let completed_ms = match &r.completed_ms {
            Some(lit) => quote! { ::core::option::Option::Some(#lit) },
            None => quote! { ::core::option::Option::None },
        };
        let dead_ms = match &r.dead_ms {
            Some(lit) => quote! { ::core::option::Option::Some(#lit) },
            None => quote! { ::core::option::Option::None },
        };
        quote! {
            const RETENTION: ::core::option::Option<::zizq::RetentionConfig> =
                ::core::option::Option::Some(::zizq::RetentionConfig {
                    completed_ms: #completed_ms,
                    dead_ms: #dead_ms,
                });
        }
    });

    Ok(quote! {
        impl #impl_generics ::zizq::JobKind for #ty #ty_generics #where_clause {
            const NAME: &'static str = #name_expr;
            #queue_const
            #priority_const
            #retry_limit_const
            #backoff_const
            #retention_const
            #unique_key_fn
        }
    })
}

/// Emit the `fn unique_key(&self) -> Option<UniqueKey>` body from a
/// parsed [`UniqueAttr`].
///
/// The flow branches on two axes:
///
/// 1. **Selection** — bare / `only` / `except`. Bare hashes `&self`
///    directly; `only`/`except` transform through `payload_only` /
///    `payload_except` and hash the resulting subset.
/// 2. **Prefix** — the emitted call is either
///    `UniqueKey::tagged_hash_of(Self::NAME, ...)` (default) or
///    `UniqueKey::hash_of(...)` when `prefix = false`.
///
/// The scope, if set, chains a `.scope(UniqueScope::...)` on the
/// resulting [`UniqueKey`].
///
/// For `only`/`except`, every path string is parsed at derive
/// expansion by [`jq_path::parse`] and emitted as a pre-built
/// `Vec<PathStep>` constructor. Invalid paths become
/// span-attached compile errors, and the emitted code contains no
/// runtime parse call — so there's no "panic on first
/// unique_key() call, six hours after startup" failure mode.
fn emit_unique_key(unique: &UniqueAttr) -> syn::Result<TokenStream2> {
    let prefix_on = unique.prefix.as_ref().map_or(true, |b| b.value);

    // Build the "hashable" expression — either `self` directly (bare)
    // or the result of subsetting via a runtime helper.
    let hashable: TokenStream2 = match &unique.selection {
        None => quote! { self },
        Some(UniqueSelection::Only(paths)) => {
            let paths_static = emit_paths_static(paths)?;
            quote! {
                {
                    #paths_static
                    &::zizq::__internal::payload_only(self, &PATHS)
                }
            }
        }
        Some(UniqueSelection::Except(paths)) => {
            let paths_static = emit_paths_static(paths)?;
            quote! {
                {
                    #paths_static
                    &::zizq::__internal::payload_except(self, &PATHS)
                }
            }
        }
    };

    // Build the hash call — with or without the type-name tag prefix.
    let hash_call = if prefix_on {
        quote! { ::zizq::UniqueKey::tagged_hash_of(<Self as ::zizq::JobKind>::NAME, #hashable) }
    } else {
        quote! { ::zizq::UniqueKey::hash_of(#hashable) }
    };

    // Optionally chain a scope.
    let scoped = match &unique.scope {
        Some(scope) => {
            let variant = match scope {
                UniqueScopeAttr::Queued => quote! { Queued },
                UniqueScopeAttr::Active => quote! { Active },
                UniqueScopeAttr::Exists => quote! { Exists },
            };
            quote! { #hash_call.scope(::zizq::UniqueScope::#variant) }
        }
        None => hash_call,
    };

    Ok(quote! {
        fn unique_key(&self) -> ::core::option::Option<::zizq::UniqueKey> {
            ::core::option::Option::Some(#scoped)
        }
    })
}

/// Emit a `static PATHS: LazyLock<Vec<Vec<PathStep>>>` initializer
/// whose closure returns each path as a pre-built `Vec<PathStep>`.
/// The paths are parsed at derive expansion — malformed inputs
/// return a `syn::Error` with a span on the offending string
/// literal, surfaced as a compile error via `to_compile_error()`.
///
/// The `LazyLock` is still used because heap allocation for the
/// outer `Vec<Vec<PathStep>>` happens on first access rather than on
/// every call. Since the derive owns every path string, the
/// initializer body is closure code that produces the vec directly —
/// no runtime parsing.
fn emit_paths_static(paths: &[syn::LitStr]) -> syn::Result<TokenStream2> {
    let vec_ctors = paths
        .iter()
        .map(|lit| {
            let parsed = jq_path::parse(&lit.value()).map_err(|e| {
                syn::Error::new_spanned(lit, format!("invalid jq path {:?}: {e}", lit.value()))
            })?;
            Ok(emit_path_steps_vec(&parsed))
        })
        .collect::<syn::Result<Vec<TokenStream2>>>()?;

    Ok(quote! {
        static PATHS: ::std::sync::LazyLock<::std::vec::Vec<::std::vec::Vec<::zizq::__internal::PathStep>>> =
            ::std::sync::LazyLock::new(|| ::std::vec![#(#vec_ctors),*]);
    })
}

/// Emit a `vec![PathStep::Field(...), PathStep::Index(...), ...]`
/// literal for one parsed path.
fn emit_path_steps_vec(steps: &[jq_path::PathStep]) -> TokenStream2 {
    let step_ctors = steps.iter().map(|step| match step {
        jq_path::PathStep::Field(name) => quote! {
            ::zizq::__internal::PathStep::Field(::std::string::String::from(#name))
        },
        jq_path::PathStep::Index(idx) => quote! {
            ::zizq::__internal::PathStep::Index(#idx)
        },
    });
    quote! { ::std::vec![#(#step_ctors),*] }
}
