use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, FnArg, ItemFn, ItemTrait, Meta, Pat, Type, parse_macro_input,
};

pub(crate) mod attr_parser;
mod operations_trait;

#[proc_macro_derive(
    Entity,
    attributes(entity, primary_key, indexed, reference, lens, jsonb)
)]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let entity_attr = extract_entity_attribute(&input.attrs);
    let entity_name = &entity_attr.name;
    let short_name_expr = match &entity_attr.short_name {
        Some(sn) => quote! { Some(#sn) },
        None => quote! { None },
    };

    // Entity types come from holon_api, or from 'crate' when used within holon-api itself
    let api_path = match &entity_attr.api_crate {
        Some(crate_name) if crate_name == "crate" => quote! { crate },
        Some(crate_name) => {
            let ident = syn::Ident::new(crate_name, proc_macro2::Span::call_site());
            quote! { #ident }
        }
        None => quote! { holon_api },
    };

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("Entity can only be derived for structs with named fields"),
        },
        _ => panic!("Entity can only be derived for structs"),
    };

    let mut primary_key_field = None;
    let mut field_schemas = Vec::new();
    let lens_definitions: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut to_entity_fields = Vec::new();
    let mut from_entity_fields = Vec::new();
    let mut schema_fields = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        let field_type = &field.ty;

        let is_primary_key = field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("primary_key"));

        let is_indexed = field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("indexed"));

        let is_jsonb = field.attrs.iter().any(|attr| attr.path().is_ident("jsonb"));

        let _skip_lens = field.attrs.iter().any(|attr| {
            if attr.path().is_ident("lens")
                && let Meta::List(meta_list) = &attr.meta
            {
                let tokens_str = meta_list.tokens.to_string();
                return tokens_str == "skip";
            }
            false
        });

        let skip_serialization = field.attrs.iter().any(|attr| {
            if attr.path().is_ident("serde")
                && let Meta::List(meta_list) = &attr.meta
            {
                let tokens_str = meta_list.tokens.to_string();
                return tokens_str.contains("skip");
            }
            false
        });

        let reference_entity = field
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("reference"))
            .and_then(|attr| {
                if let Meta::List(meta_list) = &attr.meta {
                    let tokens = &meta_list.tokens;
                    Some(quote! { #tokens }.to_string())
                } else {
                    None
                }
            });

        if is_primary_key {
            primary_key_field = Some(field_name_str.clone());
        }

        let field_type_enum = if let Some(ref_entity) = reference_entity {
            quote! { #api_path::FieldType::Reference(#ref_entity.to_string()) }
        } else {
            type_to_field_type(field_type, &api_path)
        };

        let is_required = !is_option_type(field_type);

        field_schemas.push(quote! {
            #api_path::EntityFieldSchema {
                name: #field_name_str.to_string(),
                field_type: #field_type_enum,
                required: #is_required,
                indexed: #is_indexed,
            }
        });

        // Lenses are currently disabled
        let _ = &lens_definitions; // suppress unused warning

        if !skip_serialization {
            let sql_type = rust_type_to_sql_type(field_type);
            let nullable = is_option_type(field_type);

            let mut field_schema_builder = quote! {
                #api_path::FieldSchema::new(#field_name_str, #sql_type)
            };

            if is_primary_key {
                field_schema_builder = quote! { #field_schema_builder.primary_key() };
            }

            if is_indexed {
                field_schema_builder = quote! { #field_schema_builder.indexed() };
            }

            if nullable {
                field_schema_builder = quote! { #field_schema_builder.nullable() };
            }

            if is_jsonb {
                field_schema_builder = quote! { #field_schema_builder.jsonb() };
            }

            schema_fields.push(field_schema_builder);
        }

        if !skip_serialization {
            to_entity_fields.push(quote! {
                entity.set(#field_name_str, self.#field_name.clone())
            });

            let from_entity_conversion = if is_option_type(field_type) {
                quote! {
                    #field_name: entity.get(#field_name_str).and_then(|v| v.clone().try_into().ok())
                }
            } else {
                quote! {
                    #field_name: entity.get(#field_name_str)
                        .and_then(|v| v.clone().try_into().ok())
                        .ok_or_else(|| format!("Missing or invalid field: {}", #field_name_str))?
                }
            };

            from_entity_fields.push(from_entity_conversion);
        } else {
            let default_value = if is_option_type(field_type) {
                quote! { #field_name: None }
            } else if is_vec_type(field_type) {
                quote! { #field_name: Vec::new() }
            } else {
                quote! { #field_name: Default::default() }
            };
            from_entity_fields.push(default_value);
        }
    }

    let primary_key = primary_key_field.unwrap_or_else(|| "id".to_string());

    let expanded = quote! {
        impl #name {
            pub fn entity_schema() -> #api_path::EntitySchema {
                #api_path::EntitySchema {
                    name: #entity_name.to_string(),
                    primary_key: #primary_key.to_string(),
                    fields: vec![
                        #(#field_schemas),*
                    ],
                }
            }

            /// Returns the short name for this entity type (e.g., "task" for "todoist_tasks")
            /// Used for generating entity-typed parameters like "task_id"
            /// flutter_rust_bridge:ignore
            pub fn short_name() -> Option<&'static str> {
                #short_name_expr
            }
        }

        #(#lens_definitions)*

        impl #api_path::HasSchema for #name {
            fn schema() -> #api_path::Schema {
                #api_path::Schema::new(
                    #entity_name,
                    vec![
                        #(#schema_fields),*
                    ]
                )
            }

            fn to_entity(&self) -> #api_path::DynamicEntity {
                let mut entity = #api_path::DynamicEntity::new(#entity_name);
                #(#to_entity_fields;)*
                entity
            }

            fn from_entity(entity: #api_path::DynamicEntity) -> #api_path::entity::Result<Self> {
                Ok(Self {
                    #(#from_entity_fields),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

use attr_parser::EntityAttribute;

fn extract_entity_attribute(attrs: &[syn::Attribute]) -> EntityAttribute {
    attr_parser::extract_entity_attribute(attrs)
}

#[allow(dead_code)]
fn extract_entity_name(attrs: &[syn::Attribute]) -> String {
    extract_entity_attribute(attrs).name
}

fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Option";
    }
    false
}

fn is_vec_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Vec";
    }
    false
}

fn type_to_field_type(
    ty: &syn::Type,
    api_path: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let type_str = quote! { #ty }.to_string();

    let inner_type = if type_str.starts_with("Option <") {
        type_str
            .trim_start_matches("Option <")
            .trim_end_matches('>')
            .trim()
    } else {
        type_str.as_str()
    };

    match inner_type {
        "String" => quote! { #api_path::FieldType::String },
        "i64" | "i32" | "u64" | "u32" | "usize" => {
            quote! { #api_path::FieldType::Integer }
        }
        "bool" => quote! { #api_path::FieldType::Boolean },
        t if t.contains("DateTime") => quote! { #api_path::FieldType::DateTime },
        _ => quote! { #api_path::FieldType::Json },
    }
}

fn rust_type_to_sql_type(ty: &syn::Type) -> String {
    let type_str = quote! { #ty }.to_string();

    let inner_type = if type_str.starts_with("Option <") {
        type_str
            .trim_start_matches("Option <")
            .trim_end_matches('>')
            .trim()
    } else {
        type_str.as_str()
    };

    match inner_type {
        "String" => "TEXT".to_string(),
        "i64" | "i32" | "u64" | "u32" | "usize" => "INTEGER".to_string(),
        "bool" => "INTEGER".to_string(),
        "f64" | "f32" => "REAL".to_string(),
        t if t.contains("DateTime") => "TEXT".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Generate operation descriptors for all async methods in a trait
///
/// This macro generates:
/// - One function `fn OPERATION_NAME_OP() -> OperationDescriptor` per async method
/// - One function `fn TRAIT_NAME_operations() -> Vec<OperationDescriptor>` returning all operations
/// - A module `__operations_trait_name` (snake_case) containing all operations
///
/// Usage:
/// ```rust
/// #[operations_trait]
/// #[async_trait]
/// pub trait CrudOperations<T>: Send + Sync {
///     /// Set single field
///     async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()>;
/// }
/// ```
///
/// The generated operations can be accessed via:
/// ```rust
/// use crate::core::datasource::mutable_data_source_operations;
/// let ops = mutable_data_source_operations();
/// ```
#[proc_macro_attribute]
pub fn operations_trait(attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_def = parse_macro_input!(item as ItemTrait);
    let attr_str = attr.to_string();

    // Delegate to the module implementation which has full support for:
    // - enum_from annotations
    // - OperationResult return type
    // - _with_resolver function generation
    TokenStream::from(operations_trait::operations_trait_impl(
        &attr_str, trait_def,
    ))
}

/// Extract doc comments from attributes
fn extract_doc_comments(attrs: &[syn::Attribute]) -> String {
    let mut docs = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("doc") {
            // Handle both NameValue (/// doc) and List (/// doc) formats
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
                    // Try to parse as a string literal
                    let tokens = &meta_list.tokens;
                    let token_str = quote! { #tokens }.to_string();
                    // Remove quotes if present
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

fn extract_affected_fields(attrs: &[syn::Attribute]) -> Vec<String> {
    attr_parser::extract_affected_fields(attrs)
}

/// Extract parameter name from pattern
fn extract_param_name(pat: &Pat) -> String {
    match pat {
        Pat::Ident(pat_ident) => pat_ident.ident.to_string(),
        Pat::Wild(_) => "_".to_string(),
        _ => {
            // Fallback: try to stringify the pattern
            quote! { #pat }.to_string()
        }
    }
}

/// Infer type string and required flag from Rust type
fn infer_type(ty: &Type) -> (String, bool) {
    let type_str = quote! { #ty }.to_string();
    let cleaned = type_str.replace(" ", "");

    // Check if it's an Option type
    if cleaned.starts_with("Option<") {
        let inner = cleaned
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix(">"))
            .unwrap_or(&cleaned);
        let inner_type = infer_type_string(inner);
        return (inner_type, false);
    }

    // Check for reference types (strip & but don't affect required flag)
    let inner = if cleaned.starts_with("&") {
        cleaned.strip_prefix("&").unwrap_or(&cleaned)
    } else {
        cleaned.as_str()
    };

    let type_str = infer_type_string(inner);
    (type_str, true)
}

/// Infer type string from cleaned type name
fn infer_type_string(type_str: &str) -> String {
    // Remove lifetime parameters
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

/// Pass-through attribute for #[affects(...)] - allows Rust to accept the attribute
/// The actual parsing is done by extract_affected_fields() in the operations_trait macro.
#[proc_macro_attribute]
pub fn affects(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Pass through unchanged - this just allows Rust to accept the attribute
    item
}

/// Pass-through attribute for #[triggered_by(...)] - allows Rust to accept the attribute
/// The actual parsing is done by extract_param_mappings() in the operations_trait macro.
///
/// This attribute declares that an operation is triggered by the availability of a
/// contextual parameter. When that parameter is present (e.g., from a gesture like
/// drag-drop), only operations that declare they're triggered by it will be considered.
///
/// Usage:
/// ```rust
/// // Transform case: tree_position provides parent_id and after_block_id
/// #[triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
/// async fn move_block(&self, id: &str, parent_id: &str, after_block_id: Option<&str>) -> Result<()>
///
/// // Identity case: completed triggers and provides itself
/// #[triggered_by(availability_of = "completed")]
/// async fn set_completion(&self, id: &str, completed: bool) -> Result<()>
/// ```
#[proc_macro_attribute]
pub fn triggered_by(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Pass through unchanged - this just allows Rust to accept the attribute
    item
}

/// Pass-through attribute for #[enum_from(...)] - allows Rust to accept the attribute
/// The actual parsing is done by the operations_trait macro.
///
/// This attribute declares that a parameter's valid values come from calling
/// another method on the datasource (e.g., completion_states_with_progress).
///
/// Usage:
/// ```rust
/// #[enum_from(method = "completion_states_with_progress", param = "task_state")]
/// async fn set_state(&self, id: &str, task_state: String) -> Result<OperationResult>
/// ```
#[proc_macro_attribute]
pub fn enum_from(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Pass through unchanged - this just allows Rust to accept the attribute
    item
}

/// Generate an OperationDescriptor for a standalone async function
///
/// This macro generates a const `OPERATION_NAME_OP: OperationDescriptor` for a single function.
/// Useful for operations that aren't part of a trait.
///
/// Usage:
/// ```rust
/// #[operation]
/// /// Delete a block by ID
/// async fn delete_block(id: &str) -> Result<()> {
///     // Implementation
/// }
/// ```
///
/// The generated descriptor can be accessed via:
/// ```rust
/// use crate::operations::DELETE_BLOCK_OP;
/// let op = DELETE_BLOCK_OP();
/// ```
#[proc_macro_attribute]
pub fn operation(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let fn_item = parse_macro_input!(item as ItemFn);

    // Detect crate path (same logic as Entity macro)
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
    let is_internal = pkg_name == "holon" || pkg_name == "holon-core";
    let crate_path = if is_internal {
        quote! { crate }
    } else {
        quote! { holon }
    };

    let fn_name = &fn_item.sig.ident;
    let const_name = format_ident!("{}_OP", fn_name.to_string().to_uppercase());

    // Extract doc comments for description
    let description = extract_doc_comments(&fn_item.attrs);

    // Extract parameters (skip &self if present)
    let params: Vec<_> = fn_item
        .sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Receiver(_) => None, // Skip &self
            FnArg::Typed(pat_type) => {
                let param_name = extract_param_name(&pat_type.pat);
                let (type_str, required) = infer_type(&pat_type.ty);
                let param_name_lit = param_name.clone();
                let type_str_lit = type_str.clone();
                Some(quote! {
                    #crate_path::core::datasource::ParamDescriptor {
                        name: #param_name_lit.to_string(),
                        param_type: #type_str_lit.to_string(),
                        required: #required,
                        default: None,
                    }
                })
            }
        })
        .collect();

    let name_lit = fn_name.to_string();
    let desc_lit = if description.is_empty() {
        String::new()
    } else {
        description.clone()
    };

    // Extract affected fields from #[operation(affects = [...])] attribute
    let affected_fields = extract_affected_fields(&fn_item.attrs);
    let affected_fields_expr = if affected_fields.is_empty() {
        quote! { vec![] }
    } else {
        let fields: Vec<_> = affected_fields
            .iter()
            .map(|s| quote! { #s.to_string() })
            .collect();
        quote! { vec![#(#fields),*] }
    };

    let expanded = quote! {
        // Original function (unchanged)
        #fn_item

        // Generated operation descriptor
        pub fn #const_name() -> #crate_path::core::datasource::OperationDescriptor {
            #crate_path::core::datasource::OperationDescriptor {
                name: #name_lit.to_string(),
                description: #desc_lit.to_string(),
                params: vec![
                    #(#params),*
                ],
                affected_fields: #affected_fields_expr,
            }
        }
    };

    TokenStream::from(expanded)
}

/// No-op proc macro for #[require(...)] attribute
/// This allows the attribute to be recognized by Rust's parser
/// The actual processing is done by the operations_trait macro
#[proc_macro_attribute]
pub fn require(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Just return the item unchanged - the operations_trait macro will process the require attributes
    // This is a no-op macro that just passes through the item
    // We clone the token stream to ensure proper span preservation for rust-analyzer
    item
}
