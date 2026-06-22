//! Proc macros for `rayrust`.
//!
//! ## `#[rayrust::remote]`
//!
//! Marks a function as a Ray remote task. Generates:
//! - A C-compatible callback that deserializes args, calls the function, serializes result
//! - A `register()` function that registers the callback with Ray's FunctionManager
//! - A `{name}_remote()` sync caller
//! - A `{name}_remote_async()` async caller (tokio)
//! - A `#[ctor]` auto-registration that runs when the .so is loaded by the Ray worker

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, ReturnType};

#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();

    let remote_fn_name = format_ident!("{}_remote", fn_name);
    let remote_async_fn_name = format_ident!("{}_remote_async", fn_name);
    let register_fn_name = format_ident!("{}_register", fn_name);
    let callback_fn_name = format_ident!("__rayrust_callback_{}", fn_name);
    let ctor_name = format_ident!("__RAYRUST_CTOR_{}", fn_name);

    let sig = &input_fn.sig;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // Extract argument names and types
    let arg_data: Vec<(syn::Pat, syn::Type)> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                Some((*pat_type.pat.clone(), *pat_type.ty.clone()))
            } else {
                None
            }
        })
        .collect();

    let arg_names: Vec<_> = arg_data.iter().map(|(pat, _)| quote! { #pat }).collect();

    // Build (name, type, index) triples for deserialization
    let arg_deserialize: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, (pat, ty))| {
            quote! {
                let #pat: #ty = ::rayrust::deserialize(&deserialized[#i])
                    .expect(concat!("failed to deserialize arg ", #i, " of ", #fn_name_str));
            }
        })
        .collect();

    // The return type
    let return_type: proc_macro2::TokenStream = match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    let expanded = quote! {
        // Keep the original function unchanged
        #input_fn

        /// C-compatible callback invoked by Ray's FunctionManager.
        #[no_mangle]
        pub extern "C" fn #callback_fn_name(
            args: *const ::rayrust::sys::RayBytes,
            arg_count: usize,
        ) -> ::rayrust::sys::RayBytes {
            let args_slice = if arg_count == 0 {
                &[][..]
            } else {
                unsafe { ::std::slice::from_raw_parts(args, arg_count) }
            };

            let mut deserialized: Vec<Vec<u8>> = Vec::with_capacity(arg_count);
            for i in 0..arg_count {
                let raw = unsafe {
                    ::std::slice::from_raw_parts(
                        args_slice[i].data as *const u8,
                        args_slice[i].len,
                    )
                };
                deserialized.push(raw.to_vec());
            }

            let result = {
                #( #arg_deserialize )*
                #fn_name(#(#arg_names),*)
            };

            let result_bytes = ::rayrust::serialize(&result)
                .expect(concat!("failed to serialize result of ", #fn_name_str));

            let layout = ::std::alloc::Layout::array::<u8>(result_bytes.len())
                .expect("layout alloc failed");
            let ptr = unsafe { ::std::alloc::alloc(layout) };
            if !ptr.is_null() {
                unsafe {
                    ::std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), ptr, result_bytes.len());
                }
            }

            ::rayrust::sys::RayBytes {
                data: ptr as *const ::std::os::raw::c_char,
                len: result_bytes.len(),
            }
        }

        /// Register this function with Ray's FunctionManager.
        pub fn #register_fn_name() {
            let name_c = ::rayrust::sys::to_cstring(#fn_name_str);
            unsafe {
                ::rayrust::sys::ray_register_function(name_c.as_ptr(), #callback_fn_name);
            }
        }

        /// Auto-registration via #[ctor].
        #[::rayrust::ctor::ctor]
        fn #ctor_name() {
            #register_fn_name();
        }

        /// Submit this function as a remote task (sync).
        pub fn #remote_fn_name(#inputs) -> ::rayrust::ObjectRef<#return_type> {
            let args_data: Vec<Vec<u8>> = vec![
                #(
                    ::rayrust::serialize(&#arg_names)
                        .expect(concat!("failed to serialize arg of ", #fn_name_str))
                ),*
            ];

            let args_ref: Vec<&[u8]> = args_data.iter().map(|v| v.as_slice()).collect();

            ::rayrust::task_call(#fn_name_str, &args_ref)
                .expect(concat!("ray task call failed: ", #fn_name_str))
                .cast()
        }

        /// Submit this function as a remote task (async).
        ///
        /// Serializes args and submits the task on a blocking thread pool,
        /// then returns an ObjectRef that can be awaited with `.get_async()`.
        pub async fn #remote_async_fn_name(#inputs) -> ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError> {
            let args_data: Vec<Vec<u8>> = vec![
                #(
                    ::rayrust::serialize(&#arg_names)
                        .expect(concat!("failed to serialize arg of ", #fn_name_str))
                ),*
            ];

            let obj_ref = ::rayrust::task_call_async(#fn_name_str, args_data).await?;
            Ok(obj_ref.cast())
        }
    };

    expanded.into()
}
