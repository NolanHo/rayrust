//! Proc macros for `rayrust`.
//!
//! ## `#[rayrust::remote]`
//!
//! Marks a function as a Ray remote task. Generates:
//! - A C-compatible callback that deserializes args, calls the function, serializes result
//! - A `register()` function that registers the callback with Ray's FunctionManager
//! - A `{name}_remote()` caller that submits the task to the cluster

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, ReturnType};

#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();

    let remote_fn_name = format_ident!("{}_remote", fn_name);
    let register_fn_name = format_ident!("{}_register", fn_name);
    let callback_fn_name = format_ident!("__rayrust_callback_{}", fn_name);

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
    let _arg_types: Vec<_> = arg_data.iter().map(|(_, ty)| quote! { #ty }).collect();

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

    let _num_args = arg_names.len();

    // The return type
    let return_type: proc_macro2::TokenStream = match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    let expanded = quote! {
        // Keep the original function unchanged
        #input_fn

        /// C-compatible callback invoked by Ray's FunctionManager.
        /// Deserializes msgpack args, calls the function, serializes result.
        #[no_mangle]
        pub extern "C" fn #callback_fn_name(
            args: *const ::rayrust::sys::RayBytes,
            arg_count: usize,
        ) -> ::rayrust::sys::RayBytes {
            // Safety: args is valid for arg_count elements, called from C++.
            let args_slice = if arg_count == 0 {
                &[][..]
            } else {
                unsafe { ::std::slice::from_raw_parts(args, arg_count) }
            };

            // Copy raw msgpack bytes for each argument
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

            // Deserialize each arg to its type and call the function
            let result = {
                #( #arg_deserialize )*
                #fn_name(#(#arg_names),*)
            };

            // Serialize result to msgpack
            let result_bytes = ::rayrust::serialize(&result)
                .expect(concat!("failed to serialize result of ", #fn_name_str));

            // Allocate via malloc so C++ can free it with free()
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
        /// Call this before `ray::init` or before the first task call.
        pub fn #register_fn_name() {
            let name_c = ::rayrust::sys::to_cstring(#fn_name_str);
            unsafe {
                ::rayrust::sys::ray_register_function(name_c.as_ptr(), #callback_fn_name);
            }
        }

        /// Submit this function as a remote task.
        /// Returns an ObjectRef for the result.
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
