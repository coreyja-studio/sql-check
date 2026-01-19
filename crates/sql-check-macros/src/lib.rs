//! Procedural macros for sql-check.
//!
//! Provides the `query!` macro for compile-time SQL validation.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use sql_check::{validate_query, Schema};
use std::path::PathBuf;
use syn::{parse_macro_input, LitStr};

/// Get the schema file path from environment or default.
fn get_schema_path() -> PathBuf {
    if let Ok(path) = std::env::var("SQL_CHECK_SCHEMA") {
        PathBuf::from(path)
    } else if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        PathBuf::from(manifest_dir).join("schema.sql")
    } else {
        PathBuf::from("schema.sql")
    }
}

/// Load and parse the schema file.
fn load_schema() -> Result<Schema, String> {
    let path = get_schema_path();

    if !path.exists() {
        return Err(format!(
            "Schema file not found: {}. Set SQL_CHECK_SCHEMA env var or create schema.sql in your crate root.",
            path.display()
        ));
    }

    Schema::from_file(&path).map_err(|e| format!("Failed to parse schema: {}", e))
}

/// The `query!` macro validates SQL at compile time and generates typed code.
///
/// # Example
///
/// ```ignore
/// let users = query!("SELECT id, name FROM users WHERE active = $1", active)
///     .fetch_all(&pool)
///     .await?;
/// ```
///
/// This will:
/// 1. Validate that `users` table exists
/// 2. Validate that `id`, `name`, and `active` columns exist
/// 3. Generate a struct with the correct types for the result
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input_str = parse_macro_input!(input as LitStr);
    let sql = input_str.value();

    // Load schema
    let schema = match load_schema() {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(input_str, e)
                .to_compile_error()
                .into();
        }
    };

    // Validate query
    let result = match validate_query(&schema, &sql) {
        Ok(r) => r,
        Err(e) => {
            return syn::Error::new_spanned(input_str, format!("SQL validation error: {}", e))
                .to_compile_error()
                .into();
        }
    };

    // Generate the output
    let generated = generate_query_code(&sql, &result);

    generated.into()
}

/// Generate the code for a validated query.
fn generate_query_code(sql: &str, result: &sql_check::validate::QueryResult) -> TokenStream2 {
    // Generate field definitions for the result struct
    let fields: Vec<TokenStream2> = result
        .columns
        .iter()
        .map(|col| {
            let name = format_ident!("{}", sanitize_field_name(&col.name));
            let ty = rust_type_to_tokens(&col.rust_type);
            quote! { pub #name: #ty }
        })
        .collect();

    // Generate FromRow implementation fields
    let from_row_fields: Vec<TokenStream2> = result
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let name = format_ident!("{}", sanitize_field_name(&col.name));
            let idx_lit = syn::Index::from(idx);
            quote! { #name: row.get(#idx_lit) }
        })
        .collect();

    let struct_name = format_ident!("QueryResult");

    quote! {
        {
            #[derive(Debug)]
            struct #struct_name {
                #(#fields),*
            }

            impl<'r> ::tokio_postgres::types::FromSql<'r> for #struct_name {
                fn from_sql(
                    _ty: &::tokio_postgres::types::Type,
                    _raw: &'r [u8],
                ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                    unimplemented!("FromSql for row struct")
                }

                fn accepts(_ty: &::tokio_postgres::types::Type) -> bool {
                    false
                }
            }

            ::sql_check::Query::<#struct_name>::new(
                #sql,
                |row: &::tokio_postgres::Row| -> #struct_name {
                    #struct_name {
                        #(#from_row_fields),*
                    }
                }
            )
        }
    }
}

/// Sanitize a column name to be a valid Rust identifier.
fn sanitize_field_name(name: &str) -> String {
    let name = name.replace(|c: char| !c.is_alphanumeric() && c != '_', "_");

    // Handle reserved keywords
    match name.as_str() {
        "type" => "r#type".to_string(),
        "match" => "r#match".to_string(),
        "ref" => "r#ref".to_string(),
        "self" => "r#self".to_string(),
        _ => {
            // If starts with digit, prefix with underscore
            if name.chars().next().map(|c| c.is_numeric()).unwrap_or(false) {
                format!("_{}", name)
            } else {
                name
            }
        }
    }
}

/// Convert our RustType to proc_macro2 tokens.
fn rust_type_to_tokens(ty: &sql_check::RustType) -> TokenStream2 {
    use sql_check::RustType;

    match ty {
        RustType::I16 => quote! { i16 },
        RustType::I32 => quote! { i32 },
        RustType::I64 => quote! { i64 },
        RustType::F32 => quote! { f32 },
        RustType::F64 => quote! { f64 },
        RustType::Decimal => quote! { rust_decimal::Decimal },
        RustType::String => quote! { String },
        RustType::VecU8 => quote! { Vec<u8> },
        RustType::Bool => quote! { bool },
        RustType::DateTime => quote! { chrono::DateTime<chrono::Utc> },
        RustType::Date => quote! { chrono::NaiveDate },
        RustType::Time => quote! { chrono::NaiveTime },
        RustType::Duration => quote! { chrono::Duration },
        RustType::Uuid => quote! { uuid::Uuid },
        RustType::JsonValue => quote! { serde_json::Value },
        RustType::IpAddr => quote! { std::net::IpAddr },
        RustType::Vec(inner) => {
            let inner_tokens = rust_type_to_tokens(inner);
            quote! { Vec<#inner_tokens> }
        }
        RustType::Option(inner) => {
            let inner_tokens = rust_type_to_tokens(inner);
            quote! { Option<#inner_tokens> }
        }
        RustType::Custom(name) => {
            let ident = format_ident!("{}", name);
            quote! { #ident }
        }
    }
}
