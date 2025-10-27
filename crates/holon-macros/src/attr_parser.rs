//! Shared attribute parsing helpers using syn AST instead of string manipulation.

use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitStr, Token, bracketed};

/// A single key = "value" pair in an attribute, e.g. `name = "blocks"`
struct KeyValue {
    key: Ident,
    value: AttrValue,
}

/// Value side of a key = value pair
enum AttrValue {
    Str(String),
    List(Vec<String>),
}

impl Parse for KeyValue {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;

        let value = if input.peek(syn::token::Bracket) {
            // Parse array: ["a", "b"]
            let content;
            bracketed!(content in input);
            let items =
                syn::punctuated::Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
            AttrValue::List(items.iter().map(|lit| lit.value()).collect())
        } else {
            // Parse string: "value"
            let lit: LitStr = input.parse()?;
            AttrValue::Str(lit.value())
        };

        Ok(KeyValue { key, value })
    }
}

/// Parsed set of key-value pairs from an attribute's token stream.
struct AttrArgs {
    pairs: Vec<KeyValue>,
}

impl Parse for AttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut pairs = Vec::new();
        while !input.is_empty() {
            pairs.push(input.parse()?);
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(AttrArgs { pairs })
    }
}

impl AttrArgs {
    fn get_str(&self, key: &str) -> Option<String> {
        self.pairs.iter().find_map(|kv| {
            if kv.key == key {
                match &kv.value {
                    AttrValue::Str(s) => Some(s.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.pairs.iter().find_map(|kv| {
            if kv.key == key {
                match &kv.value {
                    AttrValue::List(v) => Some(v.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
    }
}

/// Parse key-value pairs from a Meta::List attribute.
///
/// Returns None if the tokens cannot be parsed as key-value pairs.
fn parse_meta_list_args(meta_list: &syn::MetaList) -> Option<AttrArgs> {
    syn::parse2::<AttrArgs>(meta_list.tokens.clone()).ok()
}

// ============================================================================
// Public API: typed extractors for specific attributes
// ============================================================================

/// Parsed #[entity(name = "...", short_name = "...", api_crate = "...")]
pub struct EntityAttribute {
    pub name: String,
    pub short_name: Option<String>,
    pub api_crate: Option<String>,
}

/// Extract entity attribute from #[entity(name = "...", ...)]
pub fn extract_entity_attribute(attrs: &[syn::Attribute]) -> EntityAttribute {
    for attr in attrs {
        if attr.path().is_ident("entity") {
            if let syn::Meta::List(meta_list) = &attr.meta {
                if let Some(args) = parse_meta_list_args(meta_list) {
                    if let Some(name) = args.get_str("name") {
                        return EntityAttribute {
                            name,
                            short_name: args.get_str("short_name"),
                            api_crate: args.get_str("api_crate"),
                        };
                    }
                }
            }
        }
    }
    panic!("Entity derive macro requires #[entity(name = \"...\")]");
}

/// Parsed #[triggered_by(availability_of = "...", providing = ["...", "..."])]
pub struct ParsedParamMapping {
    pub availability_of: String,
    pub providing: Vec<String>,
}

/// Extract triggered_by annotations from method attributes.
pub fn extract_param_mappings(attrs: &[syn::Attribute]) -> Vec<ParsedParamMapping> {
    let mut mappings = Vec::new();

    for attr in attrs {
        let is_triggered_by_attr = attr.path().is_ident("triggered_by")
            || (attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "holon_macros"
                && attr.path().segments[1].ident == "triggered_by");

        if is_triggered_by_attr {
            if let syn::Meta::List(meta_list) = &attr.meta {
                if let Some(args) = parse_meta_list_args(meta_list) {
                    if let Some(availability_of) = args.get_str("availability_of") {
                        let providing = args
                            .get_list("providing")
                            .unwrap_or_else(|| vec![availability_of.clone()]);
                        mappings.push(ParsedParamMapping {
                            availability_of,
                            providing,
                        });
                    }
                }
            }
        }
    }

    mappings
}

/// Parsed #[enum_from(method = "...", param = "...")]
pub struct ParsedEnumFrom {
    pub method_name: String,
    pub param_name: String,
}

/// Extract enum_from annotation from method attributes.
pub fn extract_enum_from(attrs: &[syn::Attribute]) -> Option<ParsedEnumFrom> {
    for attr in attrs {
        let is_enum_from_attr = attr.path().is_ident("enum_from")
            || (attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "holon_macros"
                && attr.path().segments[1].ident == "enum_from");

        if is_enum_from_attr {
            if let syn::Meta::List(meta_list) = &attr.meta {
                if let Some(args) = parse_meta_list_args(meta_list) {
                    if let (Some(method_name), Some(param_name)) =
                        (args.get_str("method"), args.get_str("param"))
                    {
                        return Some(ParsedEnumFrom {
                            method_name,
                            param_name,
                        });
                    }
                }
            }
        }
    }
    None
}

/// Extract affected fields from #[affects("field1", "field2")] or #[operation(affects = [...])]
pub fn extract_affected_fields(attrs: &[syn::Attribute]) -> Vec<String> {
    for attr in attrs {
        // Check for standalone #[affects("field1", "field2")]
        let is_affects_attr = attr.path().is_ident("affects")
            || (attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "holon_macros"
                && attr.path().segments[1].ident == "affects");

        if is_affects_attr {
            if let syn::Meta::List(meta_list) = &attr.meta {
                // Parse as comma-separated string literals
                if let Ok(items) = meta_list.parse_args_with(
                    syn::punctuated::Punctuated::<LitStr, Token![,]>::parse_terminated,
                ) {
                    return items.iter().map(|lit| lit.value()).collect();
                }
            }
        }

        // Also check for #[operation(affects = [...])]
        let is_operation_attr = attr.path().is_ident("operation")
            || (attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "holon_macros"
                && attr.path().segments[1].ident == "operation");

        if is_operation_attr {
            if let syn::Meta::List(meta_list) = &attr.meta {
                if let Some(args) = parse_meta_list_args(meta_list) {
                    if let Some(fields) = args.get_list("affects") {
                        return fields;
                    }
                }
            }
        }
    }
    Vec::new()
}
