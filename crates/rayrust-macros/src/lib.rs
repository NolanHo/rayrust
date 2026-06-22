//! Proc macros for `rayrust`.
//!
//! ## `#[ray::remote]`
//!
//! Marks a function as a Ray remote task. Generates:
//! - A registration function name for the task
//! - A `.remote(args...)` method that submits the task to the Ray cluster
//!
//! ## Example
//! ```ignore
//! #[ray::remote]
//! fn add(a: i32, b: i32) -> i32 { a + b }
//!
//! // Usage:
//! let obj_ref = add__remote(1, 2);
//! let result: i32 = ray::get(obj_ref);
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, ReturnType, Signature};

#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // Original function name: `add`
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();

    // Generated remote caller name: `add_remote`
    let remote_fn_name = format_ident!("{}_remote", fn_name);

    let sig = &input_fn.sig;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // Extract argument types and names
    let arg_names: Vec<_> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                let pat = &pat_type.pat;
                Some(quote! { #pat })
            } else {
                None
            }
        })
        .collect();

    let _arg_types: Vec<_> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                let ty = &pat_type.ty;
                Some(quote! { #ty })
            } else {
                None
            }
        })
        .collect();

    // The return type (extract inner type from `-> T`)
    let return_type: proc_macro2::TokenStream = match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Generate the original function + the remote wrapper
    let expanded = quote! {
        // Keep the original function unchanged
        #input_fn

        // Remote wrapper: serializes args, calls ray::task_call, returns ObjectRef
        pub fn #remote_fn_name(#inputs) -> ::rayrust::ObjectRef<#return_type> {
            // Serialize each argument with msgpack
            let args_data: Vec<Vec<u8>> = vec![
                #(
                    ::rayrust::serialize(&#arg_names)
                        .expect(concat!("failed to serialize arg of ", #fn_name_str))
                ),*
            ];

            // Build the args slice for FFI
            let args_ref: Vec<&[u8]> = args_data.iter().map(|v| v.as_slice()).collect();

            // Call the Ray runtime
            ::rayrust::task_call(#fn_name_str, &args_ref)
                .expect(concat!("ray task call failed: ", #fn_name_str))
                .cast()
        }
    };

    expanded.into()
}

/// Helper to extract function name from a Signature (used internally).
#[allow(dead_code)]
fn _fn_name(sig: &Signature) -> &syn::Ident {
    &sig.ident
}
