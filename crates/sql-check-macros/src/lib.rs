//! Procedural macros for sql-check.
//!
//! Provides the `query!` macro for compile-time SQL validation.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use sql_check::{validate_query, Schema};
use std::path::PathBuf;
use syn::{parse::Parse, parse::ParseStream, parse_macro_input, Expr, LitStr, Token};

/// Input for the query! macro: SQL string followed by optional parameters.
struct QueryInput {
    sql: LitStr,
    params: Vec<Expr>,
}

impl Parse for QueryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let sql: LitStr = input.parse()?;
        let mut params = Vec::new();

        // Parse optional parameters after comma
        while input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;
            // Handle trailing comma
            if input.is_empty() {
                break;
            }
            let param: Expr = input.parse()?;
            params.push(param);
        }

        Ok(QueryInput { sql, params })
    }
}

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
/// // Without parameters
/// let users = query!("SELECT id, name FROM users")
///     .fetch_all(&client)
///     .await?;
///
/// // With parameters
/// let user = query!("SELECT id, name FROM users WHERE id = $1", user_id)
///     .fetch_one(&client)
///     .await?;
///
/// // With the runtime feature, returns Vec<QueryResult> where QueryResult has typed fields
/// for user in users {
///     println!("{}: {}", user.id, user.name);
/// }
/// ```
///
/// This will:
/// 1. Validate that `users` table exists
/// 2. Validate that `id` and `name` columns exist
/// 3. Generate a struct with the correct types for the result
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let query_input = parse_macro_input!(input as QueryInput);
    let sql = query_input.sql.value();
    let params = query_input.params;

    // Load schema
    let schema = match load_schema() {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(query_input.sql, e)
                .to_compile_error()
                .into();
        }
    };

    // Validate query
    let result = match validate_query(&schema, &sql) {
        Ok(r) => r,
        Err(e) => {
            return syn::Error::new_spanned(
                query_input.sql,
                format!("SQL validation error: {}", e),
            )
            .to_compile_error()
            .into();
        }
    };

    // Count parameter placeholders in SQL
    let param_count = count_placeholders(&sql);
    if params.len() != param_count {
        return syn::Error::new_spanned(
            query_input.sql,
            format!(
                "Expected {} parameter(s) but got {}",
                param_count,
                params.len()
            ),
        )
        .to_compile_error()
        .into();
    }

    // Generate the output
    let generated = generate_query_code(&sql, &result, &params);

    generated.into()
}

/// Count the number of $N placeholders in SQL.
fn count_placeholders(sql: &str) -> usize {
    let mut max_placeholder = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let mut num_str = String::new();
            while let Some(&digit) = chars.peek() {
                if digit.is_ascii_digit() {
                    num_str.push(digit);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(num) = num_str.parse::<usize>() {
                max_placeholder = max_placeholder.max(num);
            }
        }
    }

    max_placeholder
}

/// Generate the code for a validated query.
fn generate_query_code(
    sql: &str,
    result: &sql_check::validate::QueryResult,
    params: &[Expr],
) -> TokenStream2 {
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

    // Generate the row mapping code - get each column by index
    let field_mappings: Vec<TokenStream2> = result
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let name = format_ident!("{}", sanitize_field_name(&col.name));
            quote! { #name: row.get(#idx) }
        })
        .collect();

    // Use a unique struct name to avoid conflicts
    let struct_name = format_ident!("SqlCheckQueryResult");

    // Generate code based on whether we have parameters
    if params.is_empty() {
        quote! {
            {
                #[derive(Debug, Clone)]
                pub struct #struct_name {
                    #(#fields),*
                }

                ::sql_check::Query::<#struct_name>::new(
                    #sql,
                    |row: &::tokio_postgres::Row| -> #struct_name {
                        #struct_name {
                            #(#field_mappings),*
                        }
                    }
                )
            }
        }
    } else {
        // With parameters, we need to create the params vec
        quote! {
            {
                #[derive(Debug, Clone)]
                pub struct #struct_name {
                    #(#fields),*
                }

                ::sql_check::QueryWithParams::<#struct_name>::new(
                    #sql,
                    |row: &::tokio_postgres::Row| -> #struct_name {
                        #struct_name {
                            #(#field_mappings),*
                        }
                    },
                    vec![#(&#params as &(dyn ::tokio_postgres::types::ToSql + Sync)),*]
                )
            }
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
        "mod" => "r#mod".to_string(),
        "fn" => "r#fn".to_string(),
        "let" => "r#let".to_string(),
        "use" => "r#use".to_string(),
        "pub" => "r#pub".to_string(),
        "struct" => "r#struct".to_string(),
        "enum" => "r#enum".to_string(),
        "trait" => "r#trait".to_string(),
        "impl" => "r#impl".to_string(),
        "const" => "r#const".to_string(),
        "static" => "r#static".to_string(),
        "mut" => "r#mut".to_string(),
        "as" => "r#as".to_string(),
        "break" => "r#break".to_string(),
        "continue" => "r#continue".to_string(),
        "return" => "r#return".to_string(),
        "if" => "r#if".to_string(),
        "else" => "r#else".to_string(),
        "loop" => "r#loop".to_string(),
        "while" => "r#while".to_string(),
        "for" => "r#for".to_string(),
        "in" => "r#in".to_string(),
        "where" => "r#where".to_string(),
        "async" => "r#async".to_string(),
        "await" => "r#await".to_string(),
        "move" => "r#move".to_string(),
        "dyn" => "r#dyn".to_string(),
        "super" => "r#super".to_string(),
        "crate" => "r#crate".to_string(),
        "extern" => "r#extern".to_string(),
        "unsafe" => "r#unsafe".to_string(),
        _ => {
            // If starts with digit, prefix with underscore
            if name.chars().next().map(|c| c.is_numeric()).unwrap_or(false) {
                format!("_{}", name)
            } else if name.is_empty() {
                "_unnamed".to_string()
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
