//! Query validation against a schema.

use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::types::RustType;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, JoinOperator, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

/// Result of validating a query - contains the inferred column types.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<QueryColumn>,
}

/// A column in the query result.
#[derive(Debug)]
pub struct QueryColumn {
    pub name: String,
    pub rust_type: RustType,
}

/// Validate a query against a schema and return the inferred types.
pub fn validate_query(schema: &Schema, sql: &str) -> Result<QueryResult> {
    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| Error::QueryParse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(Error::InvalidQuery(
            "Expected exactly one statement".to_string(),
        ));
    }

    match &statements[0] {
        Statement::Query(query) => validate_select(schema, query),
        Statement::Insert(insert) => validate_insert(schema, insert),
        Statement::Update(update) => validate_update(schema, &update.table, update.returning.as_deref()),
        Statement::Delete(delete) => validate_delete(schema, delete),
        _ => Err(Error::InvalidQuery(
            "Only SELECT, INSERT, UPDATE, and DELETE are supported".to_string(),
        )),
    }
}

/// Context for resolving column references.
#[derive(Debug, Default)]
struct ResolveContext {
    /// Map from alias/table name -> table name in schema
    table_aliases: HashMap<String, String>,
    /// Tables that are LEFT JOINed (columns from these are nullable)
    left_joined_tables: Vec<String>,
}

impl ResolveContext {
    fn is_nullable_table(&self, table: &str) -> bool {
        self.left_joined_tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(table))
    }
}

/// Validate a SELECT query.
fn validate_select(schema: &Schema, query: &Query) -> Result<QueryResult> {
    match query.body.as_ref() {
        SetExpr::Select(select) => validate_select_body(schema, select),
        _ => Err(Error::InvalidQuery(
            "Only simple SELECT queries are supported".to_string(),
        )),
    }
}

/// Validate the SELECT body.
fn validate_select_body(schema: &Schema, select: &Select) -> Result<QueryResult> {
    let mut ctx = ResolveContext::default();

    // First, resolve table references in FROM clause
    for table_with_joins in &select.from {
        resolve_table_refs(schema, table_with_joins, &mut ctx)?;
    }

    // Then validate and infer types for each selected item
    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (name, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                columns.push(QueryColumn { name, rust_type });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (_, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                columns.push(QueryColumn {
                    name: alias.value.clone(),
                    rust_type,
                });
            }
            SelectItem::Wildcard(_) => {
                // For *, we need to add all columns from all tables
                for (alias, table_name) in &ctx.table_aliases {
                    let table = schema
                        .get_table(table_name)
                        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

                    for col in &table.columns {
                        let mut rust_type = col.data_type.to_rust_type();
                        if col.nullable || ctx.is_nullable_table(alias) {
                            rust_type = rust_type.nullable();
                        }
                        columns.push(QueryColumn {
                            name: col.name.clone(),
                            rust_type,
                        });
                    }
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                // For table.*, add all columns from that table
                use sqlparser::ast::SelectItemQualifiedWildcardKind;
                let table_alias = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(obj_name) => obj_name
                        .0
                        .first()
                        .and_then(|part| part.as_ident())
                        .map(|i| i.value.clone())
                        .ok_or_else(|| {
                            Error::InvalidQuery("Empty qualified wildcard".to_string())
                        })?,
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        return Err(Error::InvalidQuery(
                            "Expression wildcards not supported".to_string(),
                        ));
                    }
                };

                let table_name = ctx
                    .table_aliases
                    .get(&table_alias.to_lowercase())
                    .ok_or_else(|| Error::UnknownTable(table_alias.clone()))?;

                let table = schema
                    .get_table(table_name)
                    .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

                for col in &table.columns {
                    let mut rust_type = col.data_type.to_rust_type();
                    if col.nullable || ctx.is_nullable_table(&table_alias) {
                        rust_type = rust_type.nullable();
                    }
                    columns.push(QueryColumn {
                        name: col.name.clone(),
                        rust_type,
                    });
                }
            }
        }
    }

    Ok(QueryResult { columns })
}

/// Resolve table references from FROM clause.
fn resolve_table_refs(
    schema: &Schema,
    twj: &TableWithJoins,
    ctx: &mut ResolveContext,
) -> Result<()> {
    // Process the main table
    resolve_table_factor(schema, &twj.relation, ctx, false)?;

    // Process JOINs
    for join in &twj.joins {
        let is_left_join = matches!(
            join.join_operator,
            JoinOperator::LeftOuter(_) | JoinOperator::LeftSemi(_) | JoinOperator::LeftAnti(_)
        );
        resolve_table_factor(schema, &join.relation, ctx, is_left_join)?;
    }

    Ok(())
}

/// Resolve a single table factor.
fn resolve_table_factor(
    schema: &Schema,
    factor: &TableFactor,
    ctx: &mut ResolveContext,
    is_left_joined: bool,
) -> Result<()> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|i| i.value.clone())
                .ok_or_else(|| Error::InvalidQuery("Empty table name".to_string()))?;

            // Verify table exists
            if !schema.has_table(&table_name) {
                return Err(Error::UnknownTable(table_name));
            }

            // Use alias if provided, otherwise use table name
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| table_name.clone());

            ctx.table_aliases
                .insert(alias_name.to_lowercase(), table_name.clone());

            if is_left_joined {
                ctx.left_joined_tables.push(alias_name.to_lowercase());
            }
        }
        TableFactor::Derived { alias: Some(a), .. } => {
            // Subquery - for now, just track the alias
            // We can't easily resolve subquery columns, so mark as custom
            ctx.table_aliases
                .insert(a.name.value.to_lowercase(), "_subquery".to_string());
        }
        TableFactor::Derived { alias: None, .. } => {
            // Subquery without alias - nothing to track
        }
        _ => {
            // Other table factors (UNNEST, etc.) - skip for now
        }
    }

    Ok(())
}

/// Infer the type of an expression.
fn infer_expr_type(
    schema: &Schema,
    ctx: &ResolveContext,
    expr: &Expr,
) -> Result<(String, RustType)> {
    match expr {
        Expr::Identifier(ident) => {
            // Unqualified column reference - need to find which table it's from
            let col_name = &ident.value;
            let (table_alias, col) = find_column_in_tables(schema, ctx, col_name)?;

            let mut rust_type = col.data_type.to_rust_type();
            if col.nullable || ctx.is_nullable_table(&table_alias) {
                rust_type = rust_type.nullable();
            }

            Ok((col_name.clone(), rust_type))
        }
        Expr::CompoundIdentifier(idents) => {
            // Qualified column reference: table.column
            if idents.len() != 2 {
                return Err(Error::InvalidQuery(format!(
                    "Expected table.column, got {} parts",
                    idents.len()
                )));
            }

            let table_alias = &idents[0].value;
            let col_name = &idents[1].value;

            let table_name = ctx
                .table_aliases
                .get(&table_alias.to_lowercase())
                .ok_or_else(|| Error::UnknownTable(table_alias.clone()))?;

            let table = schema
                .get_table(table_name)
                .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

            let col = table
                .get_column(col_name)
                .ok_or_else(|| Error::UnknownColumn {
                    table: table_name.clone(),
                    column: col_name.clone(),
                })?;

            let mut rust_type = col.data_type.to_rust_type();
            if col.nullable || ctx.is_nullable_table(table_alias) {
                rust_type = rust_type.nullable();
            }

            Ok((col_name.clone(), rust_type))
        }
        Expr::Function(func) => {
            // Handle aggregate functions
            let func_name = func
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|i| i.value.to_lowercase())
                .unwrap_or_default();

            let rust_type = match func_name.as_str() {
                "count" => RustType::I64,
                "sum" => {
                    // SUM returns numeric for integers, or the argument type
                    // For simplicity, always return Decimal (nullable for non-count aggregates)
                    RustType::Option(Box::new(RustType::Decimal))
                }
                "avg" => RustType::Option(Box::new(RustType::Decimal)),
                "min" | "max" => {
                    // Return type matches argument type, but nullable
                    if let Some(inner_type) = get_first_arg_type(schema, ctx, &func.args)? {
                        // Strip existing Option if present, then wrap in Option
                        let inner = match inner_type {
                            RustType::Option(t) => *t,
                            t => t,
                        };
                        RustType::Option(Box::new(inner))
                    } else {
                        RustType::Option(Box::new(RustType::String))
                    }
                }
                "coalesce" => {
                    // COALESCE returns the type of arguments, non-null if any arg is non-null
                    if let Some(inner_type) = get_first_arg_type(schema, ctx, &func.args)? {
                        // Strip Option - COALESCE makes things non-null
                        match inner_type {
                            RustType::Option(t) => *t,
                            t => t,
                        }
                    } else {
                        RustType::String
                    }
                }
                "now" => RustType::DateTime,
                _ => RustType::Custom(func_name.clone()),
            };

            Ok((func_name, rust_type))
        }
        Expr::Value(val) => {
            // Literal values - val is ValueWithSpan, access .value
            let rust_type = match &val.value {
                Value::Number(_, _) => RustType::I64,
                Value::SingleQuotedString(_) => RustType::String,
                Value::Boolean(_) => RustType::Bool,
                Value::Null => RustType::Option(Box::new(RustType::String)),
                _ => RustType::String,
            };
            Ok(("?column?".to_string(), rust_type))
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            // CAST changes the type
            let rust_type =
                crate::types::PostgresType::from_sql_name(&format!("{}", data_type)).to_rust_type();
            let (name, _) = infer_expr_type(schema, ctx, expr)?;
            Ok((name, rust_type))
        }
        Expr::BinaryOp { left, .. } => {
            // For binary ops, infer from left side (simplification)
            infer_expr_type(schema, ctx, left)
        }
        Expr::Nested(inner) => {
            // Parenthesized expression
            infer_expr_type(schema, ctx, inner)
        }
        _ => {
            // Default to String for unknown expressions
            Ok(("?column?".to_string(), RustType::String))
        }
    }
}

/// Helper to get the type of the first argument in a function call.
fn get_first_arg_type(
    schema: &Schema,
    ctx: &ResolveContext,
    args: &FunctionArguments,
) -> Result<Option<RustType>> {
    match args {
        FunctionArguments::List(list) => {
            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) = list.args.first() {
                let (_, inner_type) = infer_expr_type(schema, ctx, e)?;
                return Ok(Some(inner_type));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Find a column in any of the tables in context.
fn find_column_in_tables<'a>(
    schema: &'a Schema,
    ctx: &ResolveContext,
    col_name: &str,
) -> Result<(String, &'a crate::schema::Column)> {
    let mut found: Option<(String, &crate::schema::Column)> = None;

    for (alias, table_name) in &ctx.table_aliases {
        if let Some(table) = schema.get_table(table_name) {
            if let Some(col) = table.get_column(col_name) {
                if found.is_some() {
                    return Err(Error::AmbiguousColumn(col_name.to_string()));
                }
                found = Some((alias.clone(), col));
            }
        }
    }

    found.ok_or_else(|| Error::UnknownColumn {
        table: "<unknown>".to_string(),
        column: col_name.to_string(),
    })
}

/// Validate an INSERT statement.
fn validate_insert(schema: &Schema, insert: &sqlparser::ast::Insert) -> Result<QueryResult> {
    let table_name = insert.table.to_string();

    // Verify table exists
    let table = schema
        .get_table(&table_name)
        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

    // Verify columns exist
    for col_ident in &insert.columns {
        let col_name = &col_ident.value;
        if !table.has_column(col_name) {
            return Err(Error::UnknownColumn {
                table: table_name.clone(),
                column: col_name.clone(),
            });
        }
    }

    // If there's a RETURNING clause, infer those types
    if let Some(returning) = &insert.returning {
        let mut ctx = ResolveContext::default();
        ctx.table_aliases
            .insert(table_name.to_lowercase(), table_name.clone());

        let mut columns = Vec::new();
        for item in returning {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (name, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn { name, rust_type });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn {
                        name: alias.value.clone(),
                        rust_type,
                    });
                }
                SelectItem::Wildcard(_) => {
                    for col in &table.columns {
                        let mut rust_type = col.data_type.to_rust_type();
                        if col.nullable {
                            rust_type = rust_type.nullable();
                        }
                        columns.push(QueryColumn {
                            name: col.name.clone(),
                            rust_type,
                        });
                    }
                }
                _ => {}
            }
        }

        return Ok(QueryResult { columns });
    }

    // No RETURNING - return empty result
    Ok(QueryResult { columns: vec![] })
}

/// Validate an UPDATE statement.
fn validate_update(
    schema: &Schema,
    table: &sqlparser::ast::TableWithJoins,
    returning: Option<&[SelectItem]>,
) -> Result<QueryResult> {
    // Extract table name from the table reference
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| Error::InvalidQuery("Empty table name".to_string()))?,
        _ => return Err(Error::InvalidQuery("Unsupported table factor in UPDATE".to_string())),
    };

    // Verify table exists
    let table_schema = schema
        .get_table(&table_name)
        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

    // If there's a RETURNING clause, infer those types
    if let Some(returning) = returning {
        let mut ctx = ResolveContext::default();
        ctx.table_aliases
            .insert(table_name.to_lowercase(), table_name.clone());

        let mut columns = Vec::new();
        for item in returning {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (name, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn { name, rust_type });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn {
                        name: alias.value.clone(),
                        rust_type,
                    });
                }
                SelectItem::Wildcard(_) => {
                    for col in &table_schema.columns {
                        let mut rust_type = col.data_type.to_rust_type();
                        if col.nullable {
                            rust_type = rust_type.nullable();
                        }
                        columns.push(QueryColumn {
                            name: col.name.clone(),
                            rust_type,
                        });
                    }
                }
                _ => {}
            }
        }

        return Ok(QueryResult { columns });
    }

    // No RETURNING - return empty result
    Ok(QueryResult { columns: vec![] })
}

/// Validate a DELETE statement.
fn validate_delete(schema: &Schema, delete: &sqlparser::ast::Delete) -> Result<QueryResult> {
    use sqlparser::ast::FromTable;

    // Extract table name from the first table in from
    let from_tables = match &delete.from {
        FromTable::WithFromKeyword(tables) => tables,
        FromTable::WithoutKeyword(tables) => tables,
    };

    let table_with_joins = from_tables
        .first()
        .ok_or_else(|| Error::InvalidQuery("DELETE must have FROM clause".to_string()))?;

    let table_name = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| Error::InvalidQuery("Empty table name".to_string()))?,
        _ => return Err(Error::InvalidQuery("Unsupported table factor in DELETE".to_string())),
    };

    // Verify table exists
    let table_schema = schema
        .get_table(&table_name)
        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

    // If there's a RETURNING clause, infer those types
    if let Some(returning) = &delete.returning {
        let mut ctx = ResolveContext::default();
        ctx.table_aliases
            .insert(table_name.to_lowercase(), table_name.clone());

        let mut columns = Vec::new();
        for item in returning {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (name, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn { name, rust_type });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, rust_type) = infer_expr_type(schema, &ctx, expr)?;
                    columns.push(QueryColumn {
                        name: alias.value.clone(),
                        rust_type,
                    });
                }
                SelectItem::Wildcard(_) => {
                    for col in &table_schema.columns {
                        let mut rust_type = col.data_type.to_rust_type();
                        if col.nullable {
                            rust_type = rust_type.nullable();
                        }
                        columns.push(QueryColumn {
                            name: col.name.clone(),
                            rust_type,
                        });
                    }
                }
                _ => {}
            }
        }

        return Ok(QueryResult { columns });
    }

    // No RETURNING - return empty result
    Ok(QueryResult { columns: vec![] })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::from_sql(
            r#"
            CREATE TABLE users (
                id uuid NOT NULL,
                name text NOT NULL,
                email text NOT NULL,
                metadata jsonb NOT NULL DEFAULT '{}',
                CONSTRAINT users_pkey PRIMARY KEY (id)
            );

            CREATE TABLE profiles (
                id uuid NOT NULL,
                user_id uuid NOT NULL,
                bio text,
                avatar_url text,
                CONSTRAINT profiles_pkey PRIMARY KEY (id)
            );
        "#,
        )
        .unwrap()
    }

    #[test]
    fn test_validate_simple_select() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT id, name FROM users").unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_select_with_alias() {
        let schema = test_schema();
        let result =
            validate_query(&schema, "SELECT u.id, u.name as user_name FROM users u").unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "user_name");
    }

    #[test]
    fn test_validate_left_join_nullability() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, u.name, p.bio
            FROM users u
            LEFT JOIN profiles p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        // u.id - not nullable (from users, not left-joined)
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        // u.name - not nullable
        assert_eq!(result.columns[1].rust_type, RustType::String);
        // p.bio - nullable (from profiles which is LEFT JOINed, AND bio is nullable in schema)
        assert_eq!(
            result.columns[2].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
    }

    #[test]
    fn test_validate_count_aggregate() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT COUNT(*) FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].rust_type, RustType::I64);
    }

    #[test]
    fn test_validate_jsonb_column() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT id, metadata FROM users").unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[1].name, "metadata");
        assert_eq!(result.columns[1].rust_type, RustType::JsonValue);
    }

    #[test]
    fn test_validate_unknown_table() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT * FROM nonexistent");

        assert!(matches!(result, Err(Error::UnknownTable(_))));
    }

    #[test]
    fn test_validate_unknown_column() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT fake_column FROM users");

        assert!(matches!(result, Err(Error::UnknownColumn { .. })));
    }

    #[test]
    fn test_validate_insert_returning() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4) RETURNING id, name"
        ).unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }
}
