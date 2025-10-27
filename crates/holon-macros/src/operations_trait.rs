use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, Meta, Pat, Type};

pub fn operations_trait_impl(attr: &str, trait_def: ItemTrait) -> TokenStream {
    // Parse provider_name from attribute: #[operations_trait(provider_name = "todoist")]
    let provider_name = parse_provider_name_str(attr);

    let trait_name = &trait_def.ident;
    let operations_fn_name = format_ident!("{}", to_snake_case(&trait_name.to_string()));
    let operations_module_name =
        format_ident!("__operations_{}", to_snake_case(&trait_name.to_string()));

    // Check if trait has generic type parameters
    let has_generics = !trait_def.generics.params.is_empty();

    // Extract where clause constraints for the entity type parameter
    // Look for constraints on the generic parameter (usually T or E)
    // We need to map T -> E in the constraints
    let entity_constraints: Vec<_> = trait_def
        .generics
        .where_clause
        .as_ref()
        .map(|where_clause| {
            where_clause
                .predicates
                .iter()
                .filter_map(|pred| {
                    // Look for type bounds like `T: BlockEntity + Send + Sync`
                    if let syn::WherePredicate::Type(pred_type) = pred {
                        // Replace the type parameter name (T) with E in the predicate
                        // This is a simplified approach - we assume the first generic param is the entity type
                        let mut new_pred = pred_type.clone();
                        // Replace T with E in the type path
                        if let syn::Type::Path(type_path) = &mut new_pred.bounded_ty {
                            if let Some(segment) = type_path.path.segments.first_mut() {
                                if segment.ident == "T" {
                                    segment.ident = syn::Ident::new("E", segment.ident.span());
                                }
                            }
                        }
                        Some(quote! { #new_pred })
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    // Detect crate path for Result type and Value types (needed for dispatch function generation)
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
    let is_internal = pkg_name == "holon" || pkg_name == "holon-core";
    let crate_path = if is_internal {
        quote! { crate }
    } else {
        quote! { holon }
    };

    // Determine the Operation type path - Operation is now in holon-api
    // All crates should use holon_api::Operation
    let operation_type_path = quote! { holon_api::Operation };

    // OperationResult is re-exported from holon::core::datasource for external crates
    // For holon-core itself, use crate::OperationResult
    // For holon crate, use crate::core::datasource::OperationResult
    // For external crates, use holon::core::datasource::OperationResult
    let operation_result_path = if pkg_name == "holon-core" {
        quote! { crate::OperationResult }
    } else if pkg_name == "holon" {
        quote! { crate::core::datasource::OperationResult }
    } else {
        quote! { holon::core::datasource::OperationResult }
    };

    // UndoAction is still needed for extracting undo from OperationResult
    let undo_action_path = if pkg_name == "holon-core" {
        quote! { crate::UndoAction }
    } else if pkg_name == "holon" {
        quote! { crate::core::datasource::UndoAction }
    } else {
        quote! { holon::core::datasource::UndoAction }
    };

    // Extract all async fn methods (skip associated types, consts, etc.)
    let methods: Vec<_> = trait_def
        .items
        .iter()
        .filter_map(|item| {
            // In syn 2.0, methods are TraitItem::Fn
            if let syn::TraitItem::Fn(method) = item {
                // Check if method is async (has asyncness)
                if method.sig.asyncness.is_some() {
                    Some(method)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Collect enum_from annotations for resolver function generation
    // Each entry is (operation_name, method_to_call, param_name)
    let enum_from_annotations: Vec<(String, String, String)> = methods
        .iter()
        .filter_map(|method| {
            let method_name = method.sig.ident.to_string();
            extract_enum_from(&method.attrs).map(|ef| (method_name, ef.method_name, ef.param_name))
        })
        .collect();

    // Generate OperationDescriptor function for each method
    let operation_fns: Vec<_> = methods
        .iter()
        .map(|method| {
            let method_name = &method.sig.ident;
            let fn_name = format_ident!("{}_OP", method_name.to_string().to_uppercase());

            // Extract doc comments for description
            let description = extract_doc_comments(&method.attrs);

            // Extract parameters (skip &self, only include required params)
            let params: Vec<_> = method
                .sig
                .inputs
                .iter()
                .skip(1) // Skip &self
                .filter_map(|arg| match arg {
                    FnArg::Typed(pat_type) => {
                        let param_name = extract_param_name(&pat_type.pat);
                        let (type_str, required) = infer_type(&pat_type.ty);

                        // Skip optional parameters (Option<T> types)
                        if !required {
                            return None;
                        }

                        let param_name_lit = param_name.clone();
                        let type_str_lit = type_str.clone();

                        // Parse type hint with entity ID detection
                        let type_hint_expr =
                            parse_param_type_hint(&param_name, &pat_type.attrs, &type_str_lit);

                        Some(quote! {
                            holon_api::OperationParam {
                                name: #param_name_lit.to_string(),
                                type_hint: #type_hint_expr,
                                description: String::new(), // TODO: Extract from doc comments
                            }
                        })
                    }
                    _ => None,
                })
                .collect();

            // Use stringify! for name and description (compile-time strings)
            let name_lit = method_name.to_string();
            let display_name = to_display_name(&name_lit);
            let desc_lit = if description.is_empty() {
                format!("Execute {}", display_name)
            } else {
                description.clone()
            };

            // Extract and generate precondition if present
            let precondition_field =
                if let Some(precondition_tokens) = extract_require_precondition(&method.attrs) {
                    let precondition_closure =
                        generate_precondition_closure(method, &precondition_tokens, &crate_path);
                    quote! {
                        precondition: Some(#precondition_closure),
                    }
                } else {
                    quote! {
                        precondition: None,
                    }
                };

            // Extract affected fields from #[operation(affects = [...])] attribute
            let affected_fields = extract_affected_fields(&method.attrs);
            let affected_fields_expr = if affected_fields.is_empty() {
                quote! { vec![] }
            } else {
                let fields: Vec<_> = affected_fields
                    .iter()
                    .map(|s| quote! { #s.to_string() })
                    .collect();
                quote! { vec![#(#fields),*] }
            };

            // Extract param_mappings from #[triggered_by(...)] attributes
            let param_mappings = extract_param_mappings(&method.attrs);
            let param_mappings_expr = if param_mappings.is_empty() {
                quote! { vec![] }
            } else {
                let mapping_exprs: Vec<_> = param_mappings
                    .iter()
                    .map(|m| {
                        let from = &m.availability_of;
                        let provides: Vec<_> = m
                            .providing
                            .iter()
                            .map(|s| quote! { #s.to_string() })
                            .collect();
                        quote! {
                            holon_api::ParamMapping {
                                from: #from.to_string(),
                                provides: vec![#(#provides),*],
                                defaults: std::collections::HashMap::new(),
                            }
                        }
                    })
                    .collect();
                quote! { vec![#(#mapping_exprs),*] }
            };

            // Construct entity_name: if provider_name is set, use "{provider_name}.{operation_name}", otherwise use passed entity_name
            let entity_name_expr = if let Some(ref provider) = provider_name {
                let provider_lit = provider.clone();
                let operation_name_lit = name_lit.clone();
                quote! {
                    format!("{}.{}", #provider_lit, #operation_name_lit)
                }
            } else {
                quote! {
                    entity_name.to_string()
                }
            };

            quote! {
                /// Generate operation descriptor for this method
                ///
                /// Parameters:
                /// - entity_name: Entity identifier (e.g., "todoist_tasks", "logseq_blocks")
                ///   Note: If provider_name is set in macro, entity_name will be "{provider_name}.{operation_name}"
                /// - entity_short_name: Short name for entity-typed params (e.g., "task", "project")
                /// - table: Database table name (e.g., "todoist_tasks", "logseq_blocks")
                /// - id_column: Primary key column name (default: "id")
                pub fn #fn_name(
                    entity_name: &str,
                    entity_short_name: &str,
                    table: &str,
                    id_column: &str
                ) -> holon_api::OperationDescriptor {
                    holon_api::OperationDescriptor {
                        entity_name: #entity_name_expr,
                        entity_short_name: entity_short_name.to_string(),
                        id_column: id_column.to_string(),
                        name: #name_lit.to_string(),
                        display_name: #display_name.to_string(),
                        description: #desc_lit.to_string(),
                        required_params: vec![
                            #(#params),*
                        ],
                        affected_fields: #affected_fields_expr,
                        param_mappings: #param_mappings_expr,
                        #precondition_field
                    }
                }
            }
        })
        .collect();

    // Generate operation constructor functions (*_op) for each method
    let operation_constructor_fns: Vec<_> = methods
        .iter()
        .map(|method| {
            let method_name = &method.sig.ident;
            let op_fn_name = format_ident!("{}_op", method_name);
            let name_lit = method_name.to_string();
            let display_name = to_display_name(&name_lit);
            let _description = extract_doc_comments(&method.attrs);

            // Extract all parameters (including Option<T>) for the constructor function
            let mut param_defs = Vec::new();
            let mut param_conversions = Vec::new();

            for arg in method.sig.inputs.iter().skip(1) {  // Skip &self
                if let FnArg::Typed(pat_type) = arg {
                    let param_name = extract_param_name(&pat_type.pat);
                    let param_name_ident = match &*pat_type.pat {
                        Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                        _ => syn::Ident::new(&param_name, proc_macro2::Span::call_site()),
                    };
                    let (type_str, is_required) = infer_type(&pat_type.ty);
                    let type_str_cleaned = type_str.replace(" ", "");

                    // Generate parameter definition
                    // Use quote! to properly format the type
                    let param_ty = &pat_type.ty;
                    param_defs.push(quote! {
                        #param_name_ident: #param_ty
                    });

                    // Generate conversion to Value for StorageEntity
                    let param_name_lit = param_name.clone();
                    let conversion = if type_str_cleaned == "String" || type_str_cleaned == "&str" {
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::String(#param_name_ident.to_string()))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(|v| holon_api::Value::String(v.to_string())).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    } else if type_str_cleaned == "bool" {
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::Boolean(#param_name_ident))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(holon_api::Value::Boolean).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    } else if type_str_cleaned.starts_with("i64") {
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::Integer(#param_name_ident))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(holon_api::Value::Integer).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    } else if type_str_cleaned.starts_with("i32") {
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::Integer(#param_name_ident as i64))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(|v| holon_api::Value::Integer(v as i64)).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    } else if type_str_cleaned == "HashMap" {
                        // For HashMap<String, Value>, use directly
                        quote! {
                            (#param_name_lit.to_string(), holon_api::Value::Object(#param_name_ident))
                        }
                    } else if type_str_cleaned.contains("DateTime") {
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::from_datetime(#param_name_ident))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(|v| holon_api::Value::from_datetime(v)).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    } else {
                        // Fallback: try to convert via Value::from
                        if is_required {
                            quote! {
                                (#param_name_lit.to_string(), holon_api::Value::from(#param_name_ident))
                            }
                        } else {
                            quote! {
                                (#param_name_lit.to_string(), #param_name_ident.map(|v| holon_api::Value::from(v)).unwrap_or(holon_api::Value::Null))
                            }
                        }
                    };

                    param_conversions.push(conversion);
                }
            }

            // Construct entity_name expression (same logic as operation descriptor)
            let entity_name_expr = if let Some(ref provider) = provider_name {
                let provider_lit = provider.clone();
                let operation_name_lit = name_lit.clone();
                quote! {
                    format!("{}.{}", #provider_lit, #operation_name_lit)
                }
            } else {
                quote! {
                    entity_name.to_string()
                }
            };

            quote! {
                /// Construct an Operation for this method
                ///
                /// # Parameters
                /// - entity_name: Entity identifier (e.g., "todoist-task", "logseq-block")
                /// - All method parameters (same as the trait method, minus &self)
                ///
                /// # Returns
                /// An Operation struct ready to be executed via OperationProvider
                pub fn #op_fn_name(
                    entity_name: &str,
                    #(#param_defs),*
                ) -> #operation_type_path {
                    #operation_type_path::from_params(
                        #entity_name_expr,
                        #name_lit,
                        #display_name,
                        vec![
                            #(#param_conversions),*
                        ]
                    )
                }
            }
        })
        .collect();

    // Generate dispatch function code for each method
    let dispatch_cases: Vec<_> = methods.iter()
        .map(|method| {
            let method_name = &method.sig.ident;
            let method_name_str = method_name.to_string();

            // Extract parameters and generate extraction code, building both lists together
            let mut param_extractions_code = Vec::new();
            let mut param_names_for_call = Vec::new();

            for arg in method.sig.inputs.iter().skip(1) {  // Skip &self
                if let FnArg::Typed(pat_type) = arg {
                    let param_name_ident = match &*pat_type.pat {
                        Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                        _ => {
                            // Fallback: try to extract from string
                            let name_str = extract_param_name(&pat_type.pat);
                            syn::Ident::new(&name_str, proc_macro2::Span::call_site())
                        }
                    };
                    let param_name_str = param_name_ident.to_string();
                    let (type_str, is_required) = infer_type(&pat_type.ty);
                    let is_optional = !is_required;  // Convert required flag to optional flag
                    let type_str_cleaned = type_str.replace(" ", "");

                    // Check if original type was a reference (for &str handling)
                    // Check the actual type structure, not stringified version
                    let is_ref_type = matches!(&*pat_type.ty, syn::Type::Reference(_));

                    // For Option<&str>, check if inner type is a reference
                    let is_option_ref_str = if is_optional {
                        if let syn::Type::Path(type_path) = &*pat_type.ty {
                            if let Some(segment) = type_path.path.segments.last() {
                                if segment.ident == "Option" {
                                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                                        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                                            matches!(inner_ty, syn::Type::Reference(_))
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // Generate extraction code based on type
                    let extraction = if type_str_cleaned == "String" || type_str_cleaned == "&str" {
                        if is_optional {
                            quote! {
                                let #param_name_ident: Option<String> = params.get(#param_name_str)
                                    .and_then(|v| v.as_string().map(|s| s.to_string()));
                            }
                        } else {
                            quote! {
                                let #param_name_ident: String = params.get(#param_name_str)
                                    .and_then(|v| v.as_string().map(|s| s.to_string()))
                                    .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected String)", #param_name_str))?;
                            }
                        }
                    } else if type_str_cleaned == "bool" {
                        if is_optional {
                            quote! {
                                let #param_name_ident: Option<bool> = params.get(#param_name_str)
                                    .and_then(|v| v.as_bool());
                            }
                        } else {
                            quote! {
                                let #param_name_ident: bool = params.get(#param_name_str)
                                    .and_then(|v| v.as_bool())
                                    .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected bool)", #param_name_str))?;
                            }
                        }
                    } else if type_str_cleaned.starts_with("i64") {
                        if is_optional {
                            quote! {
                                let #param_name_ident: Option<i64> = params.get(#param_name_str)
                                    .and_then(|v| v.as_i64());
                            }
                        } else {
                            quote! {
                                let #param_name_ident: i64 = params.get(#param_name_str)
                                    .and_then(|v| v.as_i64())
                                    .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected i64)", #param_name_str))?;
                            }
                        }
                    } else if type_str_cleaned.starts_with("i32") {
                        if is_optional {
                            quote! {
                                let #param_name_ident: Option<i32> = params.get(#param_name_str)
                                    .and_then(|v| v.as_i64().map(|i| i as i32));
                            }
                        } else {
                            quote! {
                                let #param_name_ident: i32 = params.get(#param_name_str)
                                    .and_then(|v| v.as_i64().map(|i| i as i32))
                                    .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected i32)", #param_name_str))?;
                            }
                        }
                    } else if type_str_cleaned == "HashMap" {
                        // For HashMap<String, Value>, extract the whole StorageEntity
                        // Check original type to confirm it's HashMap<String, Value>
                        let original_type_str = quote! { #pat_type.ty }.to_string();
                        let original_type_contains_value = original_type_str.contains("Value");
                        if original_type_contains_value {
                            quote! {
                                let #param_name_ident: std::collections::HashMap<String, holon_api::Value> = params.clone();
                            }
                        } else {
                            quote! {
                                let #param_name_ident: holon_api::Value = params.get(#param_name_str)
                                    .cloned()
                                    .ok_or_else(|| format!("Missing parameter '{}' (expected Value)", #param_name_str))?;
                            }
                        }
                    } else if is_optional && type_str_cleaned.contains("DateTime") {
                        quote! {
                            let #param_name_ident: Option<chrono::DateTime<chrono::Utc>> = params.get(#param_name_str)
                                .and_then(|v| v.as_datetime());
                        }
                    } else if type_str_cleaned == "Value" {
                        // For Value type, clone directly
                        if is_optional {
                            quote! {
                                let #param_name_ident: Option<holon_api::Value> = params.get(#param_name_str).cloned();
                            }
                        } else {
                            quote! {
                                let #param_name_ident: holon_api::Value = params.get(#param_name_str)
                                    .cloned()
                                    .ok_or_else(|| format!("Missing parameter '{}' (expected Value)", #param_name_str))?;
                            }
                        }
                    } else {
                        // For other types, try to clone Value and let the trait method handle conversion
                        quote! {
                            let #param_name_ident: holon_api::Value = params.get(#param_name_str)
                                .cloned()
                                .ok_or_else(|| format!("Missing parameter '{}' (expected Value)", #param_name_str))?;
                        }
                    };

                    param_extractions_code.push(extraction);

                    // If parameter type is &str, we need to borrow the String
                    // Also handle Option<&str> specially
                    if (is_ref_type && type_str_cleaned == "String") || is_option_ref_str {
                        if is_optional {
                            // For Option<&str>, extract as Option<String> and borrow
                            param_names_for_call.push(quote! { #param_name_ident.as_ref().map(|s| s.as_str()) });
                        } else {
                            param_names_for_call.push(quote! { &*#param_name_ident });
                        }
                    } else {
                        param_names_for_call.push(quote! { #param_name_ident });
                    }
                }
            }

            // Check return type - handle different return types
            // Use syn to inspect the return type structure instead of string conversion
            let return_handling = match &method.sig.output {
                syn::ReturnType::Type(_, ty) => {
                    // Check if it's Result<T>
                    if let syn::Type::Path(type_path) = &**ty {
                        if let Some(segment) = type_path.path.segments.last() {
                            if segment.ident == "Result" {
                                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                                        // Check inner type
                                        match inner_ty {
                                            syn::Type::Path(inner_path) if inner_path.path.is_ident("String") => {
                                                // Result<String> -> Result<OperationResult>
                                                // This shouldn't happen anymore, but handle it
                                                quote! {
                                                    target.#method_name(#(#param_names_for_call),*).await.map(|_| #operation_result_path::irreversible(Vec::new()))
                                                }
                                            }
                                            syn::Type::Tuple(tuple) if tuple.elems.len() == 2 => {
                                                // Result<(String, OperationResult)> -> Result<OperationResult> (extract the OperationResult)
                                                quote! {
                                                    target.#method_name(#(#param_names_for_call),*).await.map(|(_, result)| result)
                                                }
                                            }
                                            syn::Type::Path(inner_path) => {
                                                // Check if it's OperationResult, UndoAction (for backward compat), or Option<Operation>
                                                if let Some(seg) = inner_path.path.segments.last() {
                                                    if seg.ident == "OperationResult" {
                                                        // Result<OperationResult> -> Result<OperationResult> (pass through)
                                                        quote! {
                                                            target.#method_name(#(#param_names_for_call),*).await
                                                        }
                                                    } else if seg.ident == "UndoAction" {
                                                        // Result<UndoAction> -> Result<OperationResult> (convert via From)
                                                        quote! {
                                                            target.#method_name(#(#param_names_for_call),*).await.map(|undo| #operation_result_path::from(undo))
                                                        }
                                                    } else if seg.ident == "Option" {
                                                        // Result<Option<Operation>> -> Result<OperationResult> (convert via Into then From)
                                                        quote! {
                                                            target.#method_name(#(#param_names_for_call),*).await.map(|opt| #operation_result_path::from(#undo_action_path::from(opt)))
                                                        }
                                                    } else {
                                                        // Other Result<T> -> pass through as Irreversible
                                                        quote! {
                                                            target.#method_name(#(#param_names_for_call),*).await.map(|_| #operation_result_path::irreversible(Vec::new()))
                                                        }
                                                    }
                                                } else {
                                                    quote! {
                                                        target.#method_name(#(#param_names_for_call),*).await.map(|_| #operation_result_path::irreversible(Vec::new()))
                                                    }
                                                }
                                            }
                                            _ => {
                                                quote! {
                                                    target.#method_name(#(#param_names_for_call),*).await
                                                }
                                            }
                                        }
                                    } else {
                                        quote! {
                                            target.#method_name(#(#param_names_for_call),*).await
                                        }
                                    }
                                } else {
                                    quote! {
                                        target.#method_name(#(#param_names_for_call),*).await
                                    }
                                }
                            } else {
                                quote! {
                                    target.#method_name(#(#param_names_for_call),*).await
                                }
                            }
                        } else {
                            quote! {
                                target.#method_name(#(#param_names_for_call),*).await
                            }
                        }
                    } else {
                        quote! {
                            target.#method_name(#(#param_names_for_call),*).await
                        }
                    }
                }
                syn::ReturnType::Default => {
                    quote! {
                        target.#method_name(#(#param_names_for_call),*).await
                    }
                }
            };

            quote! {
                #method_name_str => {
                    #(#param_extractions_code)*
                    #return_handling
                }
            }
        })
        .collect();

    // Generate function calls for the operations array
    let operation_calls: Vec<_> = methods
        .iter()
        .map(|method| {
            let method_name = &method.sig.ident;
            let fn_name = format_ident!("{}_OP", method_name.to_string().to_uppercase());
            quote! { #fn_name(entity_name, entity_short_name, table, id_column) }
        })
        .collect();

    // Generate the dispatch function differently based on whether trait has generics
    let dispatch_fn = if has_generics {
        quote! {
            pub async fn dispatch_operation<DS, E>(
                target: &DS,
                op_name: &str,
                params: &StorageEntity
            ) -> Result<#operation_result_path>
            where
                DS: #trait_name<E> + Send + Sync,
                E: Send + Sync + 'static,
                #(#entity_constraints),*
            {
                match op_name {
                    #(#dispatch_cases),*
                    _ => Err(#crate_path::core::datasource::UnknownOperationError::new(
                        stringify!(#trait_name),
                        op_name,
                    ).into())
                }
            }
        }
    } else {
        quote! {
            pub async fn dispatch_operation<DS>(
                target: &DS,
                op_name: &str,
                params: &StorageEntity
            ) -> Result<#operation_result_path>
            where
                DS: #trait_name + Send + Sync,
            {
                match op_name {
                    #(#dispatch_cases),*
                    _ => Err(#crate_path::core::datasource::UnknownOperationError::new(
                        stringify!(#trait_name),
                        op_name,
                    ).into())
                }
            }
        }
    };

    // Generate resolver function that resolves enum_from annotations
    let resolver_fn_name = format_ident!("{}_with_resolver", operations_fn_name);

    // Generate resolver statements for each enum_from annotation
    let resolver_statements: Vec<_> = enum_from_annotations
        .iter()
        .map(|(op_name, method_name, param_name)| {
            let method_ident = syn::Ident::new(method_name, proc_macro2::Span::call_site());
            quote! {
                // Resolve enum values for #op_name.#param_name from #method_name()
                if let Some(op) = ops.iter_mut().find(|o| o.name == #op_name) {
                    if let Some(param) = op.required_params.iter_mut().find(|p| p.name == #param_name) {
                        // Convert CompletionStateInfo to Value
                        // Note: CompletionStateInfo is from holon::core::datasource, accessible via the trait
                        let values: Vec<holon_api::Value> = ds.#method_ident()
                            .into_iter()
                            .map(|info| {
                                // Serialize CompletionStateInfo to serde_json::Value, then convert to holon_api::Value
                                let json_value = serde_json::to_value(&info)
                                    .unwrap_or_else(|_| serde_json::Value::Null);
                                holon_api::Value::from_json_value(json_value)
                            })
                            .collect();
                        param.type_hint = holon_api::TypeHint::OneOf {
                            values,
                        };
                    }
                }
            }
        })
        .collect();

    // Generate resolver function with trait bounds matching the dispatch function
    let resolver_fn = if has_generics {
        quote! {
            /// All operations for this trait with resolved enum values
            ///
            /// This function resolves #[enum_from] annotations by calling the
            /// specified methods on the datasource to get valid enum values.
            ///
            /// Parameters:
            /// - ds: Datasource implementing this trait
            /// - entity_name: Entity identifier (e.g., "todoist_tasks", "logseq_blocks")
            /// - entity_short_name: Short name for entity-typed params (e.g., "task", "project")
            /// - table: Database table name (e.g., "todoist_tasks", "logseq_blocks")
            /// - id_column: Primary key column name (default: "id")
            pub fn #resolver_fn_name<DS, E>(
                ds: &DS,
                entity_name: &str,
                entity_short_name: &str,
                table: &str,
                id_column: &str
            ) -> Vec<holon_api::OperationDescriptor>
            where
                DS: #trait_name<E> + Send + Sync,
                #(#entity_constraints),*
            {
                let mut ops = #operations_fn_name(entity_name, entity_short_name, table, id_column);
                #(#resolver_statements)*
                ops
            }
        }
    } else {
        quote! {
            /// All operations for this trait with resolved enum values
            ///
            /// This function resolves #[enum_from] annotations by calling the
            /// specified methods on the datasource to get valid enum values.
            ///
            /// Parameters:
            /// - ds: Datasource implementing this trait
            /// - entity_name: Entity identifier (e.g., "todoist_tasks", "logseq_blocks")
            /// - entity_short_name: Short name for entity-typed params (e.g., "task", "project")
            /// - table: Database table name (e.g., "todoist_tasks", "logseq_blocks")
            /// - id_column: Primary key column name (default: "id")
            pub fn #resolver_fn_name<DS>(
                ds: &DS,
                entity_name: &str,
                entity_short_name: &str,
                table: &str,
                id_column: &str
            ) -> Vec<holon_api::OperationDescriptor>
            where
                DS: #trait_name + Send + Sync,
            {
                let mut ops = #operations_fn_name(entity_name, entity_short_name, table, id_column);
                #(#resolver_statements)*
                ops
            }
        }
    };

    let expanded = quote! {
        // Original trait (unchanged)
        #trait_def

        // Generated operations module
        #[doc(hidden)]
        pub mod #operations_module_name {
            use super::*;
            use holon_api::StorageEntity;
            use holon_api::Value;
            use #crate_path::core::datasource::Result;
            // Operation is now in holon-api, use holon_api::Operation
            use #operation_type_path as Operation;

            #(#operation_fns)*

            // Operation constructor functions (*_op)
            #(#operation_constructor_fns)*

            /// All operations for this trait
            ///
            /// Parameters:
            /// - entity_name: Entity identifier (e.g., "todoist_tasks", "logseq_blocks")
            /// - entity_short_name: Short name for entity-typed params (e.g., "task", "project")
            /// - table: Database table name (e.g., "todoist_tasks", "logseq_blocks")
            /// - id_column: Primary key column name (default: "id")
            pub fn #operations_fn_name(
                entity_name: &str,
                entity_short_name: &str,
                table: &str,
                id_column: &str
            ) -> Vec<holon_api::OperationDescriptor> {
                vec![
                    #(#operation_calls),*
                ]
            }

            #resolver_fn

            /// Dispatch operation to appropriate trait method
            ///
            /// Extracts parameters from StorageEntity and calls the appropriate trait method.
            /// Returns an error if the operation name is not recognized or parameters are invalid.
            ///
            /// Note: For generic traits, the entity type `E` must satisfy all constraints required by the trait.
            /// For example, `BlockOperations<E>` requires `E: BlockEntity`.
            #dispatch_fn
        }
    };

    expanded
}

/// Parse provider_name from macro attribute string: #[operations_trait(provider_name = "todoist")]
fn parse_provider_name_str(attr_str: &str) -> Option<String> {
    if attr_str.is_empty() {
        return None;
    }

    // Look for provider_name = "value" pattern
    if let Some(start) = attr_str.find("provider_name") {
        if let Some(equals) = attr_str[start..].find('=') {
            let value_start = attr_str[start + equals + 1..].find('"')? + start + equals + 1;
            let value_end = attr_str[value_start + 1..].find('"')? + value_start + 1;
            return Some(attr_str[value_start + 1..value_end].to_string());
        }
    }
    None
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_lowercase().next().unwrap_or(c));
    }
    result
}

fn to_display_name(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' {
            result.push(' ');
            capitalize_next = true;
        } else if c.is_uppercase() && !result.is_empty() {
            result.push(' ');
            result.push(c);
            capitalize_next = false;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Extract doc comments from attributes
pub fn extract_doc_comments(attrs: &[syn::Attribute]) -> String {
    let mut docs = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("doc") {
            match &attr.meta {
                Meta::NameValue(meta) => {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &meta.value
                    {
                        let doc = s.value();
                        let cleaned = doc.trim();
                        if !cleaned.is_empty() {
                            docs.push(cleaned.to_string());
                        }
                    }
                }
                Meta::List(meta_list) => {
                    let tokens = &meta_list.tokens;
                    let token_str = quote! { #tokens }.to_string();
                    let cleaned = token_str
                        .strip_prefix('"')
                        .and_then(|s| s.strip_suffix('"'))
                        .unwrap_or(&token_str)
                        .trim();
                    if !cleaned.is_empty() {
                        docs.push(cleaned.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    docs.join(" ")
}

/// Extract require precondition tokens from attributes
fn extract_require_precondition(attrs: &[syn::Attribute]) -> Option<proc_macro2::TokenStream> {
    let mut preconditions = Vec::new();

    for attr in attrs {
        let is_require = attr.path().is_ident("require")
            || (attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "holon_macros"
                && attr.path().segments[1].ident == "require");

        if is_require {
            if let Meta::List(meta_list) = &attr.meta {
                preconditions.push(meta_list.tokens.clone());
            }
        }
    }

    if preconditions.is_empty() {
        None
    } else if preconditions.len() == 1 {
        Some(preconditions[0].clone())
    } else {
        let mut combined = preconditions[0].clone();
        for prec in preconditions.iter().skip(1) {
            combined = quote! { (#combined) && (#prec) };
        }
        Some(combined)
    }
}

use crate::attr_parser::{self, ParsedEnumFrom, ParsedParamMapping};

fn extract_param_mappings(attrs: &[syn::Attribute]) -> Vec<ParsedParamMapping> {
    attr_parser::extract_param_mappings(attrs)
}

fn extract_enum_from(attrs: &[syn::Attribute]) -> Option<ParsedEnumFrom> {
    attr_parser::extract_enum_from(attrs)
}

/// Generate precondition closure code for a method
fn generate_precondition_closure(
    method: &syn::TraitItemFn,
    precondition_tokens: &proc_macro2::TokenStream,
    _crate_path: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut param_declarations = Vec::new();

    for arg in method.sig.inputs.iter().skip(1) {
        if let FnArg::Typed(pat_type) = arg {
            let param_name_ident = match &*pat_type.pat {
                Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                _ => {
                    let name_str = extract_param_name(&pat_type.pat);
                    syn::Ident::new(&name_str, proc_macro2::Span::call_site())
                }
            };
            let param_name_str = param_name_ident.to_string();
            let (type_str, is_required) = infer_type(&pat_type.ty);
            let is_optional = !is_required;
            let type_str_cleaned = type_str.replace(" ", "");

            let is_ref_type = matches!(&*pat_type.ty, syn::Type::Reference(_));

            let type_conversion = if type_str_cleaned == "String" || type_str_cleaned == "&str" {
                if is_optional {
                    quote! {
                        let #param_name_ident: Option<String> = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_string().map(|s| s.to_string()))
                            });
                    }
                } else {
                    quote! {
                        let #param_name_ident: String = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_string().map(|s| s.to_string()))
                            })
                            .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected String)", #param_name_str))?;
                    }
                }
            } else if type_str_cleaned == "bool" {
                if is_optional {
                    quote! {
                        let #param_name_ident: Option<bool> = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_bool())
                            });
                    }
                } else {
                    quote! {
                        let #param_name_ident: bool = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_bool())
                            })
                            .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected bool)", #param_name_str))?;
                    }
                }
            } else if type_str_cleaned.starts_with("i64") {
                if is_optional {
                    quote! {
                        let #param_name_ident: Option<i64> = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_i64())
                            });
                    }
                } else {
                    quote! {
                        let #param_name_ident: i64 = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_i64())
                            })
                            .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected i64)", #param_name_str))?;
                    }
                }
            } else if type_str_cleaned.starts_with("i32") {
                if is_optional {
                    quote! {
                        let #param_name_ident: Option<i32> = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_i64().map(|i| i as i32))
                            });
                    }
                } else {
                    quote! {
                        let #param_name_ident: i32 = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>()
                                    .and_then(|v| v.as_i64().map(|i| i as i32))
                            })
                            .ok_or_else(|| format!("Missing or invalid parameter '{}' (expected i32)", #param_name_str))?;
                    }
                }
            } else if is_optional && type_str_cleaned.contains("DateTime") {
                quote! {
                    let #param_name_ident: Option<chrono::DateTime<chrono::Utc>> = params.get(#param_name_str)
                        .and_then(|any_val| {
                            any_val.downcast_ref::<holon_api::Value>()
                                .and_then(|v| v.as_datetime())
                        });
                }
            } else {
                if is_optional {
                    quote! {
                        let #param_name_ident: Option<holon_api::Value> = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>().cloned()
                            });
                    }
                } else {
                    quote! {
                        let #param_name_ident: holon_api::Value = params.get(#param_name_str)
                            .and_then(|any_val| {
                                any_val.downcast_ref::<holon_api::Value>().cloned()
                            })
                            .ok_or_else(|| format!("Missing parameter '{}' (expected Value)", #param_name_str))?;
                    }
                }
            };

            param_declarations.push(type_conversion);

            if is_ref_type && (type_str_cleaned == "String" || type_str_cleaned == "&str") {
                // String handling for references
            }
        }
    }

    quote! {
        {
            use std::sync::Arc;
            use std::any::Any;
            use std::collections::HashMap;

            Arc::new(Box::new(move |params: &HashMap<String, Box<dyn Any + Send + Sync>>| -> std::result::Result<bool, String> {
                #(#param_declarations)*
                Ok(#precondition_tokens)
            }) as Box<holon_api::PreconditionChecker>)
        }
    }
}

/// Extract parameter name from pattern
pub fn extract_param_name(pat: &Pat) -> String {
    match pat {
        Pat::Ident(pat_ident) => pat_ident.ident.to_string(),
        Pat::Wild(_) => "_".to_string(),
        _ => quote! { #pat }.to_string(),
    }
}

/// Infer type string and required flag from Rust type
pub fn infer_type(ty: &Type) -> (String, bool) {
    // Handle reference types: &str, &T
    if let Type::Reference(ref_type) = ty {
        return infer_type(&ref_type.elem);
    }

    // Handle Option<T> via AST path matching
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        let (inner_type, _) = infer_type(inner_ty);
                        return (inner_type, false);
                    }
                }
            }
        }
    }

    let type_name = extract_type_name(ty);
    (infer_type_string(&type_name), true)
}

/// Extract the leaf type name from a syn::Type using AST matching.
fn extract_type_name(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident.to_string()
            } else {
                quote! { #ty }.to_string().replace(" ", "")
            }
        }
        Type::Reference(ref_type) => extract_type_name(&ref_type.elem),
        _ => quote! { #ty }.to_string().replace(" ", ""),
    }
}

/// Infer type string from cleaned type name
fn infer_type_string(type_str: &str) -> String {
    let without_lifetime = type_str.split('<').next().unwrap_or(type_str);

    match without_lifetime {
        "str" => "String".to_string(),
        "String" => "String".to_string(),
        "i64" => "i64".to_string(),
        "i32" => "i32".to_string(),
        "u64" => "u64".to_string(),
        "u32" => "u32".to_string(),
        "usize" => "usize".to_string(),
        "bool" => "bool".to_string(),
        "f64" => "f64".to_string(),
        "f32" => "f32".to_string(),
        s if s.contains("HashMap") => "HashMap".to_string(),
        s if s.contains("Vec") => "Vec".to_string(),
        s if s.contains("DateTime") => "DateTime".to_string(),
        s if s.contains("Value") => "Value".to_string(),
        _ => type_str.to_string(),
    }
}

/// Parse parameter type hint with entity ID detection
fn parse_param_type_hint(
    param_name: &str,
    attrs: &[syn::Attribute],
    rust_type_str: &str,
) -> proc_macro2::TokenStream {
    let mut entity_ref_override: Option<String> = None;
    let mut not_entity = false;

    for attr in attrs {
        if attr.path().is_ident("entity_ref") {
            if let Meta::List(meta_list) = &attr.meta {
                let tokens = &meta_list.tokens;
                let token_str = quote! { #tokens }.to_string();
                if let Some(stripped) = token_str
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                {
                    entity_ref_override = Some(stripped.to_string());
                }
            }
        }

        if attr.path().is_ident("not_entity") {
            not_entity = true;
        }
    }

    if let Some(entity_name) = entity_ref_override {
        quote! {
            holon_api::TypeHint::EntityId {
                entity_name: #entity_name.to_string(),
            }
        }
    } else if not_entity {
        infer_type_hint_from_rust_type(rust_type_str)
    } else if let Some(entity_name) = param_name.strip_suffix("_id").filter(|s| !s.is_empty()) {
        let entity_name_lit = entity_name.to_string();
        quote! {
            holon_api::TypeHint::EntityId {
                entity_name: #entity_name_lit.to_string(),
            }
        }
    } else {
        infer_type_hint_from_rust_type(rust_type_str)
    }
}

/// Infer TypeHint from Rust type string
fn infer_type_hint_from_rust_type(rust_type_str: &str) -> proc_macro2::TokenStream {
    match rust_type_str {
        "String" | "&str" | "str" => {
            quote! { holon_api::TypeHint::String }
        }
        "bool" => {
            quote! { holon_api::TypeHint::Bool }
        }
        "i64" | "i32" | "u64" | "u32" | "usize" | "integer" => {
            quote! { holon_api::TypeHint::Number }
        }
        s if s.contains("DateTime") => {
            quote! { holon_api::TypeHint::String }
        }
        _ => {
            quote! { holon_api::TypeHint::String }
        }
    }
}

fn extract_affected_fields(attrs: &[syn::Attribute]) -> Vec<String> {
    attr_parser::extract_affected_fields(attrs)
}
