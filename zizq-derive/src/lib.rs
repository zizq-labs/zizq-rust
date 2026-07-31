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
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

mod attrs;

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
///
/// Subsequent commits will add `backoff`, `retention`, `unique`,
/// and `batch`.
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

    Ok(quote! {
        impl #impl_generics ::zizq::JobKind for #ty #ty_generics #where_clause {
            const NAME: &'static str = #name_expr;
            #queue_const
            #priority_const
            #retry_limit_const
        }
    })
}
