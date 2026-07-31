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

/// Derive an implementation of `zizq::JobKind` for the annotated
/// struct.
///
/// This skeleton commit emits only the trait's required associated
/// constant, defaulted to the struct's identifier. Attribute parsing
/// for `name`, `queue`, `priority`, `retry_limit`, `backoff`,
/// `retention`, `unique`, and `batch` arrives in subsequent commits.
#[proc_macro_derive(JobKind, attributes(zizq))]
pub fn derive_job_kind(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let name_str = name.to_string();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ::zizq::JobKind for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
        }
    };

    TokenStream::from(expanded)
}
