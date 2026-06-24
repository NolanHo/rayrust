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
//!
//! Supports both sync `fn` and `async fn`. For `async fn`, the callback
//! uses the persistent global tokio runtime to execute the future.
//!
//! ## `#[rayrust::actor]`
//!
//! Marks an `impl` block as a Ray actor. Generates:
//! - A factory callback that calls `new()` and returns a raw pointer
//! - Member function callbacks for each method (excluding `new`)
//! - `#[ctor]` auto-registration of all callbacks
//! - `{Type}_actor_create()` function to create instances
//! - `{Type}_actor_call()` / `{Type}_actor_call_async()` to call methods

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, FnArg, ImplItem, ItemFn, ItemImpl, ReturnType, Type, Visibility,
};

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

    // Detect if the function is async
    let is_async = sig.asyncness.is_some();

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

    // The return type (strip `async` — the actual return type is the inner type of the Future)
    let return_type: proc_macro2::TokenStream = match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // The call expression: sync vs async
    let call_expr = if is_async {
        quote! {
            // Use the persistent global tokio runtime (created once, reused).
            // This avoids allocating a new runtime per call.
            ::rayrust::block_on_async(#fn_name(#(#arg_names),*))
        }
    } else {
        quote! {
            #fn_name(#(#arg_names),*)
        }
    };

    // For async fn, we need to keep the original function as async, but also
    // generate a sync wrapper for the callback. The caller functions (_remote, _remote_async)
    // stay the same — they just submit the task, the execution happens in the callback.

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
                #call_expr
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

            ::rayrust::task_call(#fn_name_str, &args_ref, &[])
                .expect(concat!("ray task call failed: ", #fn_name_str))
                .cast()
        }

        /// Submit this function as a remote task (async).
        pub async fn #remote_async_fn_name(#inputs) -> ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError> {
            let args_data: Vec<Vec<u8>> = vec![
                #(
                    ::rayrust::serialize(&#arg_names)
                        .expect(concat!("failed to serialize arg of ", #fn_name_str))
                ),*
            ];

            let obj_ref = ::rayrust::task_call_async(#fn_name_str, args_data, Vec::new()).await?;
            Ok(obj_ref.cast())
        }
    };

    expanded.into()
}

// ─── Actor macro ────────────────────────────────────────────────

/// Helper to generate a unique callback function name for an actor method.
fn callback_name(type_name: &str, method_name: &str) -> String {
    format!("__rayrust_member_{}_{}", type_name.to_lowercase(), method_name)
}

/// Helper to generate the factory callback name.
fn factory_name(type_name: &str) -> String {
    format!("__rayrust_actor_factory_{}", type_name.to_lowercase())
}

#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    // Extract the type name from the impl block
    let type_name = match *input.self_ty {
        Type::Path(ref p) => {
            p.path.segments.last()
                .map(|s| s.ident.to_string())
                .expect("actor: could not determine type name")
        }
        _ => panic!("#[rayrust::actor] requires a named type"),
    };

    let type_name_lower = type_name.to_lowercase();
    let type_ident = format_ident!("{}", type_name);
    let factory_str = factory_name(&type_name);
    let factory_ident = format_ident!("{}", factory_str);
    let register_ident = format_ident!("__register_{}_actor", type_name_lower);
    let ctor_ident = format_ident!("__RAYRUST_CTOR_ACTOR_{}", type_name_upper(&type_name));

    // Find `new` constructor and collect methods
    let mut methods: Vec<(syn::Ident, Vec<(syn::Pat, syn::Type)>, Type)> = Vec::new();
    let mut new_args: Vec<(syn::Pat, syn::Type)> = Vec::new();
    let mut has_new = false;

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            let method_name = method.sig.ident.to_string();
            if method_name == "new" {
                has_new = true;
                // Extract args (skip self)
                for arg in method.sig.inputs.iter() {
                    if let FnArg::Typed(pat_type) = arg {
                        new_args.push((*pat_type.pat.clone(), *pat_type.ty.clone()));
                    }
                }
            } else {
                // All non-new methods become actor methods (regardless of visibility)
                let m_args: Vec<(syn::Pat, syn::Type)> = method.sig.inputs.iter()
                    .filter_map(|arg| {
                        if let FnArg::Typed(pat_type) = arg {
                            Some((*pat_type.pat.clone(), *pat_type.ty.clone()))
                        } else {
                            None // skip `self`
                        }
                    })
                    .collect();

                let ret_type = match &method.sig.output {
                    ReturnType::Default => Type::Verbatim(quote! { () }),
                    ReturnType::Type(_, ty) => (**ty).clone(),
                };

                methods.push((method.sig.ident.clone(), m_args, ret_type));
            }
        }
    }

    if !has_new {
        panic!("#[rayrust::actor] requires a `new` constructor method");
    }

    // Generate factory callback
    let new_arg_names: Vec<_> = new_args.iter().map(|(p, _)| quote! { #p }).collect();
    let new_arg_deserialize: Vec<_> = new_args.iter()
        .enumerate()
        .map(|(i, (pat, ty))| {
            quote! {
                let #pat: #ty = ::rayrust::deserialize(&deserialized[#i])
                    .expect("failed to deserialize constructor arg");
            }
        })
        .collect();

    // Generate member callbacks
    let member_callbacks: Vec<_> = methods.iter()
        .map(|(method_ident, m_args, ret_type)| {
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            let reg_name = format!("{}::{}", factory_str, method_ident);
            let m_arg_names: Vec<_> = m_args.iter().map(|(p, _)| quote! { #p }).collect();
            let m_arg_deserialize: Vec<_> = m_args.iter()
                .enumerate()
                .map(|(i, (pat, ty))| {
                    quote! {
                        let #pat: #ty = ::rayrust::deserialize(&deserialized[#i])
                            .expect("failed to deserialize method arg");
                    }
                })
                .collect();

            // Check if method takes &mut self or &self
            let self_kind = input.items.iter()
                .find_map(|item| {
                    if let ImplItem::Fn(m) = item {
                        if m.sig.ident == *method_ident {
                            let kind = match m.sig.receiver() {
                                Some(syn::Receiver { reference: Some(_), mutability: Some(_), .. }) => "mut",
                                Some(syn::Receiver { reference: Some(_), mutability: None, .. }) => "ref",
                                Some(syn::Receiver { reference: None, .. }) => "owned",
                                None => "static",
                            };
                            return Some(kind);
                        }
                    }
                    None
                }).unwrap_or("ref");

            let call_expr = match self_kind {
                "mut" => quote! { unsafe { (actor_ptr as *mut #type_ident).as_mut().unwrap().#method_ident(#(#m_arg_names),*) } },
                "ref" => quote! { unsafe { (actor_ptr as *const #type_ident).as_ref().unwrap().#method_ident(#(#m_arg_names),*) } },
                _ => quote! { #type_ident::#method_ident(#(#m_arg_names),*) },
            };

            quote! {
                #[no_mangle]
                pub extern "C" fn #cb_ident(
                    actor_ptr: u64,
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
                        #( #m_arg_deserialize )*
                        #call_expr
                    };

                    let result_bytes = ::rayrust::serialize(&result)
                        .expect("failed to serialize method result");

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
            }
        })
        .collect();

    // Generate registration
    let reg_names: Vec<_> = methods.iter()
        .map(|(method_ident, _, _)| {
            let name = format!("{}::{}", factory_str, method_ident);
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            quote! {
                let name_c = ::rayrust::sys::to_cstring(#name);
                unsafe {
                    ::rayrust::sys::ray_register_member_function(name_c.as_ptr(), #cb_ident);
                }
            }
        })
        .collect();

    // Generate caller functions
    let create_fn = format_ident!("{}_actor_create", type_name_lower);
    let create_inputs: Vec<_> = new_args.iter().map(|(p, t)| quote! { #p: #t }).collect();
    let create_arg_names: Vec<_> = new_args.iter().map(|(p, _)| quote! { #p }).collect();

    let method_callers: Vec<_> = methods.iter()
        .map(|(method_ident, m_args, ret_type)| {
            let caller_name = format_ident!("{}_{}", type_name_lower, method_ident);
            let m_inputs: Vec<_> = m_args.iter().map(|(p, t)| quote! { #p: #t }).collect();
            let m_arg_names: Vec<_> = m_args.iter().map(|(p, _)| quote! { #p }).collect();
            let method_full_name = format!("{}::{}", factory_str, method_ident);

            quote! {
                pub async fn #caller_name(
                    handle: &::rayrust::ActorHandle,
                    #(#m_inputs),*
                ) -> ::std::result::Result<::rayrust::ObjectRef<#ret_type>, ::rayrust::RayError> {
                    let args_data: Vec<Vec<u8>> = vec![
                        #( ::rayrust::serialize(&#m_arg_names)
                            .expect("failed to serialize method arg") ),*
                    ];
                    let func_name = #method_full_name.to_string();
                    let obj_ref = ::rayrust::actor_call_async(handle.id(), &func_name, args_data).await?;
                    Ok(obj_ref.cast())
                }
            }
        })
        .collect();

    let expanded = quote! {
        // Keep the original impl block unchanged
        #input

        // Factory callback
        #[no_mangle]
        pub extern "C" fn #factory_ident(
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

            let instance = {
                #( #new_arg_deserialize )*
                #type_ident::new(#(#new_arg_names),*)
            };

            let boxed = ::std::boxed::Box::new(instance);
            let ptr_val = ::std::boxed::Box::into_raw(boxed) as u64;

            let result_bytes = ::rayrust::serialize(&ptr_val)
                .expect("failed to serialize actor ptr");

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

        // Member callbacks
        #( #member_callbacks )*

        // Registration
        pub fn #register_ident() {
            // Register factory
            let name_c = ::rayrust::sys::to_cstring(#factory_str);
            unsafe {
                ::rayrust::sys::ray_register_function(name_c.as_ptr(), #factory_ident);
            }
            // Register member functions
            #( #reg_names )*
        }

        // Auto-registration via #[ctor]
        #[::rayrust::ctor::ctor]
        fn #ctor_ident() {
            #register_ident();
        }

        // Caller: create actor
        pub fn #create_fn(#(#create_inputs),*) -> ::std::result::Result<::rayrust::ActorHandle, ::rayrust::RayError> {
            let args_data: Vec<Vec<u8>> = vec![
                #( ::rayrust::serialize(&#create_arg_names)
                    .expect("failed to serialize constructor arg") ),*
            ];
            let args_ref: Vec<&[u8]> = args_data.iter().map(|v| v.as_slice()).collect();
            ::rayrust::actor_create(#factory_str, &args_ref, &[])
        }

        // Caller: call methods
        #( #method_callers )*
    };

    expanded.into()
}

fn type_name_upper(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_lowercase() { c.to_ascii_uppercase() } else { c }).collect()
}
