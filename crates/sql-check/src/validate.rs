//! Query validation against a schema.

use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::types::RustType;
use sqlparser::ast::{
    AssignmentTarget, Delete, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments,
    JoinOperator, Query, Select, SelectItem, SetExpr, SetOperator, Statement, TableFactor,
    TableWithJoins, Update, Value,
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
#[derive(Debug, Clone)]
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
        Statement::Update(update) => validate_update(schema, update),
        Statement::Delete(delete) => validate_delete(schema, delete),
        _ => Err(Error::InvalidQuery(
            "Only SELECT, INSERT, UPDATE, and DELETE are supported".to_string(),
        )),
    }
}

/// A CTE (Common Table Expression) definition with its column types.
#[derive(Debug, Clone)]
struct CteDefinition {
    /// Column definitions inferred from the CTE's query
    columns: Vec<QueryColumn>,
}

/// Context for resolving column references.
#[derive(Debug, Default)]
struct ResolveContext {
    /// Map from alias/table name -> table name in schema
    table_aliases: HashMap<String, String>,
    /// Tables whose columns are nullable due to JOIN type
    /// (right side of LEFT JOIN, left side of RIGHT JOIN, both sides of FULL OUTER JOIN)
    nullable_tables: Vec<String>,
    /// CTE definitions: name -> columns
    cte_definitions: HashMap<String, CteDefinition>,
}

impl ResolveContext {
    fn is_nullable_table(&self, table: &str) -> bool {
        self.nullable_tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(table))
    }

    fn mark_nullable(&mut self, table: &str) {
        let lower = table.to_lowercase();
        if !self.nullable_tables.iter().any(|t| t == &lower) {
            self.nullable_tables.push(lower);
        }
    }

    fn get_cte(&self, name: &str) -> Option<&CteDefinition> {
        self.cte_definitions.get(&name.to_lowercase())
    }

    fn add_cte(&mut self, name: String, columns: Vec<QueryColumn>) {
        self.cte_definitions
            .insert(name.to_lowercase(), CteDefinition { columns });
    }
}

/// Validate a SELECT query.
fn validate_select(schema: &Schema, query: &Query) -> Result<QueryResult> {
    // First, process CTEs if present
    let mut ctx = ResolveContext::default();

    if let Some(with_clause) = &query.with {
        for cte in &with_clause.cte_tables {
            // Get the CTE name
            let cte_name = cte.alias.name.value.clone();

            // Recursively validate the CTE's query to get its column types
            let cte_result = validate_select(schema, &cte.query)?;

            // If the CTE has explicit column aliases, use those names
            let columns = if !cte.alias.columns.is_empty() {
                // CTE has explicit column names: WITH cte(col1, col2) AS (...)
                cte_result
                    .columns
                    .into_iter()
                    .zip(cte.alias.columns.iter())
                    .map(|(mut col, alias_col)| {
                        col.name = alias_col.name.value.clone();
                        col
                    })
                    .collect()
            } else {
                cte_result.columns
            };

            ctx.add_cte(cte_name, columns);
        }
    }

    validate_set_expr(schema, query.body.as_ref(), ctx)
}

/// Validate a SetExpr (handles both simple SELECT and set operations like UNION).
fn validate_set_expr(
    schema: &Schema,
    set_expr: &SetExpr,
    ctx: ResolveContext,
) -> Result<QueryResult> {
    match set_expr {
        SetExpr::Select(select) => validate_select_body_with_ctx(schema, select, ctx),
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier: _,
        } => {
            // Validate both sides of the set operation
            let left_result = validate_set_expr(schema, left, ResolveContext::default())?;
            let right_result = validate_set_expr(schema, right, ResolveContext::default())?;

            // Verify column counts match
            if left_result.columns.len() != right_result.columns.len() {
                return Err(Error::InvalidQuery(format!(
                    "{} requires both sides to have the same number of columns (left: {}, right: {})",
                    set_op_name(op),
                    left_result.columns.len(),
                    right_result.columns.len()
                )));
            }

            // Use the left side's column names and types (PostgreSQL behavior)
            // In PostgreSQL, the first SELECT's column names are used for the result
            Ok(left_result)
        }
        SetExpr::Query(subquery) => validate_select(schema, subquery),
        _ => Err(Error::InvalidQuery(
            "Only SELECT and set operations (UNION/INTERSECT/EXCEPT) are supported".to_string(),
        )),
    }
}

/// Get the name of a set operation for error messages.
fn set_op_name(op: &SetOperator) -> &'static str {
    match op {
        SetOperator::Union => "UNION",
        SetOperator::Intersect => "INTERSECT",
        SetOperator::Except => "EXCEPT",
        SetOperator::Minus => "MINUS", // Oracle/DB2 equivalent of EXCEPT
    }
}

/// Validate the SELECT body with an existing context (preserves CTE definitions).
fn validate_select_body_with_ctx(
    schema: &Schema,
    select: &Select,
    mut ctx: ResolveContext,
) -> Result<QueryResult> {
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
                // For *, we need to add all columns from all tables (including CTEs)
                for (alias, table_ref) in &ctx.table_aliases {
                    // Check if this is a CTE reference
                    if let Some(cte_name) = table_ref.strip_prefix("_cte:") {
                        if let Some(cte) = ctx.get_cte(cte_name) {
                            for cte_col in &cte.columns {
                                let mut rust_type = cte_col.rust_type.clone();
                                if ctx.is_nullable_table(alias) {
                                    rust_type = rust_type.nullable();
                                }
                                columns.push(QueryColumn {
                                    name: cte_col.name.clone(),
                                    rust_type,
                                });
                            }
                        }
                    } else if let Some(table) = schema.get_table(table_ref) {
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
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                // For table.*, add all columns from that table (or CTE)
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

                let table_ref = ctx
                    .table_aliases
                    .get(&table_alias.to_lowercase())
                    .ok_or_else(|| Error::UnknownTable(table_alias.clone()))?;

                // Check if this is a CTE reference
                if let Some(cte_name) = table_ref.strip_prefix("_cte:") {
                    let cte = ctx
                        .get_cte(cte_name)
                        .ok_or_else(|| Error::UnknownTable(cte_name.to_string()))?;

                    for cte_col in &cte.columns {
                        let mut rust_type = cte_col.rust_type.clone();
                        if ctx.is_nullable_table(&table_alias) {
                            rust_type = rust_type.nullable();
                        }
                        columns.push(QueryColumn {
                            name: cte_col.name.clone(),
                            rust_type,
                        });
                    }
                } else {
                    let table = schema
                        .get_table(table_ref)
                        .ok_or_else(|| Error::UnknownTable(table_ref.clone()))?;

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
    }

    Ok(QueryResult { columns })
}

/// Resolve table references from FROM clause.
fn resolve_table_refs(
    schema: &Schema,
    twj: &TableWithJoins,
    ctx: &mut ResolveContext,
) -> Result<()> {
    // Get the alias of the first (left) table - we need this for RIGHT JOIN and FULL OUTER
    let first_table_alias = get_table_alias(&twj.relation);

    // Process the main table
    resolve_table_factor(schema, &twj.relation, ctx)?;

    // Process JOINs
    for join in &twj.joins {
        match &join.join_operator {
            // LEFT JOIN: right table columns are nullable
            JoinOperator::Left(_)
            | JoinOperator::LeftOuter(_)
            | JoinOperator::LeftSemi(_)
            | JoinOperator::LeftAnti(_) => {
                resolve_table_factor(schema, &join.relation, ctx)?;
                if let Some(alias) = get_table_alias(&join.relation) {
                    ctx.mark_nullable(&alias);
                }
            }
            // RIGHT JOIN: left (first) table columns are nullable
            JoinOperator::Right(_)
            | JoinOperator::RightOuter(_)
            | JoinOperator::RightSemi(_)
            | JoinOperator::RightAnti(_) => {
                resolve_table_factor(schema, &join.relation, ctx)?;
                if let Some(ref alias) = first_table_alias {
                    ctx.mark_nullable(alias);
                }
            }
            // FULL OUTER JOIN: both tables' columns are nullable
            JoinOperator::FullOuter(_) => {
                resolve_table_factor(schema, &join.relation, ctx)?;
                if let Some(ref alias) = first_table_alias {
                    ctx.mark_nullable(alias);
                }
                if let Some(alias) = get_table_alias(&join.relation) {
                    ctx.mark_nullable(&alias);
                }
            }
            // INNER JOIN, CROSS JOIN: no nullability changes
            _ => {
                resolve_table_factor(schema, &join.relation, ctx)?;
            }
        }
    }

    Ok(())
}

/// Get the alias (or table name) from a TableFactor.
fn get_table_alias(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            alias.as_ref().map(|a| a.name.value.clone()).or_else(|| {
                name.0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|i| i.value.clone())
            })
        }
        TableFactor::Derived { alias: Some(a), .. } => Some(a.name.value.clone()),
        _ => None,
    }
}

/// Resolve a single table factor (register it in context).
fn resolve_table_factor(
    schema: &Schema,
    factor: &TableFactor,
    ctx: &mut ResolveContext,
) -> Result<()> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|i| i.value.clone())
                .ok_or_else(|| Error::InvalidQuery("Empty table name".to_string()))?;

            // Use alias if provided, otherwise use table name
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| table_name.clone());

            // Check if this is a CTE reference first
            if ctx.get_cte(&table_name).is_some() {
                // It's a CTE - use the special marker "_cte:<name>"
                ctx.table_aliases.insert(
                    alias_name.to_lowercase(),
                    format!("_cte:{}", table_name.to_lowercase()),
                );
            } else {
                // Not a CTE - verify table exists in schema
                if !schema.has_table(&table_name) {
                    return Err(Error::UnknownTable(table_name));
                }

                ctx.table_aliases
                    .insert(alias_name.to_lowercase(), table_name.clone());
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

            // First, try to find in CTEs
            if let Some((table_alias, rust_type)) = find_column_in_ctes(ctx, col_name) {
                let mut rust_type = rust_type;
                if ctx.is_nullable_table(&table_alias) {
                    rust_type = rust_type.nullable();
                }
                return Ok((col_name.clone(), rust_type));
            }

            // Then try schema tables
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

            let table_ref = ctx
                .table_aliases
                .get(&table_alias.to_lowercase())
                .ok_or_else(|| Error::UnknownTable(table_alias.clone()))?;

            // Check if this is a CTE reference
            if let Some(cte_name) = table_ref.strip_prefix("_cte:") {
                // Look up the column in the CTE definition
                let cte = ctx
                    .get_cte(cte_name)
                    .ok_or_else(|| Error::UnknownTable(cte_name.to_string()))?;

                let cte_col = cte
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| Error::UnknownColumn {
                        table: cte_name.to_string(),
                        column: col_name.clone(),
                    })?;

                let mut rust_type = cte_col.rust_type.clone();
                if ctx.is_nullable_table(table_alias) {
                    rust_type = rust_type.nullable();
                }

                return Ok((col_name.clone(), rust_type));
            }

            // Regular table lookup
            let table = schema
                .get_table(table_ref)
                .ok_or_else(|| Error::UnknownTable(table_ref.clone()))?;

            let col = table
                .get_column(col_name)
                .ok_or_else(|| Error::UnknownColumn {
                    table: table_ref.clone(),
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

                // String functions that return String
                "upper" | "lower" | "initcap" => RustType::String,
                "concat" | "concat_ws" => RustType::String,
                "substring" | "substr" | "left" | "right" => RustType::String,
                "trim" | "ltrim" | "rtrim" | "btrim" => RustType::String,
                "replace" | "translate" | "reverse" | "repeat" => RustType::String,
                "lpad" | "rpad" => RustType::String,
                "split_part" => RustType::String,
                "overlay" | "format" => RustType::String,
                "quote_ident" | "quote_literal" | "quote_nullable" => RustType::String,
                "encode" | "decode" => RustType::String,
                "md5" | "sha256" | "sha384" | "sha512" => RustType::String,
                "to_hex" => RustType::String,
                "chr" => RustType::String,
                "regexp_replace" | "regexp_substr" | "regexp_match" => RustType::String,

                // String functions that return integers
                "length" | "char_length" | "character_length" | "octet_length" | "bit_length" => {
                    RustType::I32
                }
                "position" | "strpos" => RustType::I32,
                "ascii" => RustType::I32,

                // Date/time functions
                "extract" | "date_part" => RustType::F64,
                "date_trunc" => RustType::DateTime,
                "age" => RustType::Duration,
                "to_char" => RustType::String,
                "to_date" => RustType::Date,
                "to_timestamp" => RustType::DateTime,
                "current_date" => RustType::Date,
                "current_time" => RustType::Time,
                "current_timestamp" | "localtimestamp" | "localtime" => RustType::DateTime,
                "make_date" => RustType::Date,
                "make_time" => RustType::Time,
                "make_timestamp" | "make_timestamptz" => RustType::DateTime,
                "make_interval" => RustType::Duration,

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
        Expr::Extract { .. } => {
            // EXTRACT(field FROM timestamp) returns f64
            Ok(("extract".to_string(), RustType::F64))
        }
        Expr::Ceil { .. } => {
            // CEIL can be numeric or date/time, return f64 as a reasonable default
            Ok(("ceil".to_string(), RustType::F64))
        }
        Expr::Floor { .. } => {
            // FLOOR can be numeric or date/time, return f64 as a reasonable default
            Ok(("floor".to_string(), RustType::F64))
        }
        Expr::Position { .. } => {
            // POSITION(substring IN string) returns i32
            Ok(("position".to_string(), RustType::I32))
        }
        Expr::Substring { .. } => {
            // SUBSTRING returns String
            Ok(("substring".to_string(), RustType::String))
        }
        Expr::Trim { .. } => {
            // TRIM returns String
            Ok(("trim".to_string(), RustType::String))
        }
        Expr::Overlay { .. } => {
            // OVERLAY returns String
            Ok(("overlay".to_string(), RustType::String))
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

/// Find a column in CTE definitions.
fn find_column_in_ctes(ctx: &ResolveContext, col_name: &str) -> Option<(String, RustType)> {
    let mut found: Option<(String, RustType)> = None;

    for (alias, table_ref) in &ctx.table_aliases {
        if let Some(cte_name) = table_ref.strip_prefix("_cte:") {
            if let Some(cte) = ctx.get_cte(cte_name) {
                if let Some(col) = cte
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(col_name))
                {
                    if found.is_some() {
                        // Ambiguous - but we return None and let find_column_in_tables handle it
                        // (though it won't find anything, leading to proper error)
                        return None;
                    }
                    found = Some((alias.clone(), col.rust_type.clone()));
                }
            }
        }
    }

    found
}

/// Find a column in any of the tables in context (excluding CTEs).
fn find_column_in_tables<'a>(
    schema: &'a Schema,
    ctx: &ResolveContext,
    col_name: &str,
) -> Result<(String, &'a crate::schema::Column)> {
    let mut found: Option<(String, &crate::schema::Column)> = None;

    for (alias, table_name) in &ctx.table_aliases {
        // Skip CTE references
        if table_name.starts_with("_cte:") {
            continue;
        }

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
fn validate_update(schema: &Schema, update: &Update) -> Result<QueryResult> {
    // Get table name from the UPDATE target
    let table_name = extract_table_name_from_table_with_joins(&update.table)?;

    // Verify table exists
    let table = schema
        .get_table(&table_name)
        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

    // Verify columns in SET clause exist
    for assignment in &update.assignments {
        let col_names = extract_assignment_target_columns(&assignment.target)?;
        for col_name in col_names {
            if !table.has_column(&col_name) {
                return Err(Error::UnknownColumn {
                    table: table_name.clone(),
                    column: col_name,
                });
            }
        }
    }

    // If there's a RETURNING clause, infer those types
    if let Some(returning) = &update.returning {
        let mut ctx = ResolveContext::default();
        ctx.table_aliases
            .insert(table_name.to_lowercase(), table_name.clone());

        return infer_returning_types(schema, &ctx, table, returning);
    }

    // No RETURNING - return empty result
    Ok(QueryResult { columns: vec![] })
}

/// Validate a DELETE statement.
fn validate_delete(schema: &Schema, delete: &Delete) -> Result<QueryResult> {
    // Get table name from the FROM clause
    let table_name = extract_table_name_from_delete_from(&delete.from)?;

    // Verify table exists
    let table = schema
        .get_table(&table_name)
        .ok_or_else(|| Error::UnknownTable(table_name.clone()))?;

    // If there's a RETURNING clause, infer those types
    if let Some(returning) = &delete.returning {
        let mut ctx = ResolveContext::default();
        ctx.table_aliases
            .insert(table_name.to_lowercase(), table_name.clone());

        return infer_returning_types(schema, &ctx, table, returning);
    }

    // No RETURNING - return empty result
    Ok(QueryResult { columns: vec![] })
}

/// Extract table name from TableWithJoins.
fn extract_table_name_from_table_with_joins(twj: &TableWithJoins) -> Result<String> {
    match &twj.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| Error::InvalidQuery("Empty table name".to_string())),
        _ => Err(Error::InvalidQuery(
            "Complex table expressions not supported in UPDATE".to_string(),
        )),
    }
}

/// Extract table name from DELETE's FromTable.
fn extract_table_name_from_delete_from(from: &FromTable) -> Result<String> {
    match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if tables.is_empty() {
                return Err(Error::InvalidQuery(
                    "DELETE requires at least one table".to_string(),
                ));
            }
            extract_table_name_from_table_with_joins(&tables[0])
        }
    }
}

/// Extract column names from an assignment target.
fn extract_assignment_target_columns(target: &AssignmentTarget) -> Result<Vec<String>> {
    match target {
        AssignmentTarget::ColumnName(obj_name) => {
            // Single column, e.g., `name = 'value'`
            let col_name = obj_name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|i| i.value.clone())
                .ok_or_else(|| {
                    Error::InvalidQuery("Empty column name in assignment".to_string())
                })?;
            Ok(vec![col_name])
        }
        AssignmentTarget::Tuple(obj_names) => {
            // Tuple of columns, e.g., `(a, b) = (1, 2)`
            let mut cols = Vec::new();
            for obj_name in obj_names {
                let col_name = obj_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| {
                        Error::InvalidQuery("Empty column name in tuple assignment".to_string())
                    })?;
                cols.push(col_name);
            }
            Ok(cols)
        }
    }
}

/// Infer types for a RETURNING clause.
fn infer_returning_types(
    schema: &Schema,
    ctx: &ResolveContext,
    table: &crate::schema::Table,
    returning: &[SelectItem],
) -> Result<QueryResult> {
    let mut columns = Vec::new();

    for item in returning {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (name, rust_type) = infer_expr_type(schema, ctx, expr)?;
                columns.push(QueryColumn { name, rust_type });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (_, rust_type) = infer_expr_type(schema, ctx, expr)?;
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

    Ok(QueryResult { columns })
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

    #[test]
    fn test_validate_right_join_nullability() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, u.name, p.bio
            FROM users u
            RIGHT JOIN profiles p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        // u.id - nullable (users is on left side of RIGHT JOIN)
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Uuid))
        );
        // u.name - nullable (users is on left side of RIGHT JOIN)
        assert_eq!(
            result.columns[1].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
        // p.bio - nullable (bio is nullable in schema, but profiles is not nullable from JOIN)
        assert_eq!(
            result.columns[2].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
    }

    #[test]
    fn test_validate_right_join_non_nullable_column() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, p.id as profile_id
            FROM users u
            RIGHT JOIN profiles p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        // u.id - nullable (users is on left side of RIGHT JOIN)
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Uuid))
        );
        // p.id - NOT nullable (profiles is on right side of RIGHT JOIN, id is NOT NULL in schema)
        assert_eq!(result.columns[1].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_full_outer_join_nullability() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, u.name, p.id as profile_id, p.bio
            FROM users u
            FULL OUTER JOIN profiles p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 4);
        // u.id - nullable (FULL OUTER JOIN makes both sides nullable)
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Uuid))
        );
        // u.name - nullable
        assert_eq!(
            result.columns[1].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
        // p.id - nullable (even though NOT NULL in schema, FULL OUTER makes it nullable)
        assert_eq!(
            result.columns[2].rust_type,
            RustType::Option(Box::new(RustType::Uuid))
        );
        // p.bio - nullable (already nullable in schema + FULL OUTER)
        assert_eq!(
            result.columns[3].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
    }

    #[test]
    fn test_validate_cross_join() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, u.name, p.id as profile_id
            FROM users u
            CROSS JOIN profiles p
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        // CROSS JOIN: neither side becomes nullable
        // u.id - NOT nullable
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        // u.name - NOT nullable
        assert_eq!(result.columns[1].rust_type, RustType::String);
        // p.id - NOT nullable
        assert_eq!(result.columns[2].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_inner_join_not_nullable() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT u.id, u.name, p.id as profile_id
            FROM users u
            INNER JOIN profiles p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        // INNER JOIN: neither side becomes nullable from the join itself
        // u.id - NOT nullable
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        // u.name - NOT nullable
        assert_eq!(result.columns[1].rust_type, RustType::String);
        // p.id - NOT nullable
        assert_eq!(result.columns[2].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_update_simple() {
        let schema = test_schema();
        let result = validate_query(&schema, "UPDATE users SET name = 'Alice'").unwrap();

        // No RETURNING clause - empty result
        assert_eq!(result.columns.len(), 0);
    }

    #[test]
    fn test_validate_update_with_where() {
        let schema = test_schema();
        let result = validate_query(&schema, "UPDATE users SET name = $1 WHERE id = $2").unwrap();

        assert_eq!(result.columns.len(), 0);
    }

    #[test]
    fn test_validate_update_returning() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_update_returning_all() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "UPDATE users SET name = $1 WHERE id = $2 RETURNING *",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 4); // id, name, email, metadata
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[2].name, "email");
        assert_eq!(result.columns[3].name, "metadata");
    }

    #[test]
    fn test_validate_update_unknown_column() {
        let schema = test_schema();
        let result = validate_query(&schema, "UPDATE users SET nonexistent = 'value'");

        assert!(matches!(result, Err(Error::UnknownColumn { .. })));
    }

    #[test]
    fn test_validate_update_unknown_table() {
        let schema = test_schema();
        let result = validate_query(&schema, "UPDATE nonexistent SET name = 'value'");

        assert!(matches!(result, Err(Error::UnknownTable(_))));
    }

    #[test]
    fn test_validate_delete_simple() {
        let schema = test_schema();
        let result = validate_query(&schema, "DELETE FROM users").unwrap();

        // No RETURNING clause - empty result
        assert_eq!(result.columns.len(), 0);
    }

    #[test]
    fn test_validate_delete_with_where() {
        let schema = test_schema();
        let result = validate_query(&schema, "DELETE FROM users WHERE id = $1").unwrap();

        assert_eq!(result.columns.len(), 0);
    }

    #[test]
    fn test_validate_delete_returning() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "DELETE FROM users WHERE id = $1 RETURNING id, name",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_delete_returning_all() {
        let schema = test_schema();
        let result =
            validate_query(&schema, "DELETE FROM users WHERE id = $1 RETURNING *").unwrap();

        assert_eq!(result.columns.len(), 4); // id, name, email, metadata
    }

    #[test]
    fn test_validate_delete_unknown_table() {
        let schema = test_schema();
        let result = validate_query(&schema, "DELETE FROM nonexistent WHERE id = $1");

        assert!(matches!(result, Err(Error::UnknownTable(_))));
    }

    // CTE (Common Table Expression) tests

    #[test]
    fn test_validate_simple_cte() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH active_users AS (
                SELECT id, name FROM users
            )
            SELECT id, name FROM active_users
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_cte_with_alias() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH active_users AS (
                SELECT id, name FROM users
            )
            SELECT au.id, au.name FROM active_users au
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_cte_with_explicit_column_names() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH user_info(user_id, user_name) AS (
                SELECT id, name FROM users
            )
            SELECT user_id, user_name FROM user_info
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "user_id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "user_name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_cte_wildcard() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH active_users AS (
                SELECT id, name FROM users
            )
            SELECT * FROM active_users
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
    }

    #[test]
    fn test_validate_cte_qualified_wildcard() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH active_users AS (
                SELECT id, name FROM users
            )
            SELECT active_users.* FROM active_users
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
    }

    #[test]
    fn test_validate_cte_with_join() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH user_profiles AS (
                SELECT u.id, u.name, p.bio
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
            )
            SELECT id, name, bio FROM user_profiles
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
        // bio is nullable from the LEFT JOIN in the CTE
        assert_eq!(result.columns[2].name, "bio");
        assert_eq!(
            result.columns[2].rust_type,
            RustType::Option(Box::new(RustType::String))
        );
    }

    #[test]
    fn test_validate_multiple_ctes() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH
                user_data AS (SELECT id, name FROM users),
                profile_data AS (SELECT user_id, bio FROM profiles)
            SELECT u.id, u.name, p.bio
            FROM user_data u
            LEFT JOIN profile_data p ON p.user_id = u.id
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
        // bio is nullable because profile_data is LEFT JOINed
        assert_eq!(result.columns[2].name, "bio");
        assert_eq!(
            result.columns[2].rust_type,
            RustType::Option(Box::new(RustType::Option(Box::new(RustType::String))))
        );
    }

    #[test]
    fn test_validate_cte_unknown_column() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            WITH active_users AS (
                SELECT id, name FROM users
            )
            SELECT nonexistent FROM active_users
            "#,
        );

        assert!(matches!(result, Err(Error::UnknownColumn { .. })));
    }

    // SUM/AVG aggregate tests

    #[test]
    fn test_validate_sum_returns_decimal() {
        let schema = Schema::from_sql(
            r#"
            CREATE TABLE items (
                id uuid NOT NULL,
                quantity integer NOT NULL
            );
            "#,
        )
        .unwrap();

        let result = validate_query(&schema, "SELECT SUM(quantity) FROM items").unwrap();

        assert_eq!(result.columns.len(), 1);
        // SUM returns Option<Decimal>
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Decimal))
        );
    }

    #[test]
    fn test_validate_avg_returns_decimal() {
        let schema = Schema::from_sql(
            r#"
            CREATE TABLE items (
                id uuid NOT NULL,
                quantity integer NOT NULL
            );
            "#,
        )
        .unwrap();

        let result = validate_query(&schema, "SELECT AVG(quantity) FROM items").unwrap();

        assert_eq!(result.columns.len(), 1);
        // AVG returns Option<Decimal>
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Decimal))
        );
    }

    #[test]
    fn test_validate_sum_on_decimal_column() {
        let schema = Schema::from_sql(
            r#"
            CREATE TABLE products (
                id uuid NOT NULL,
                price numeric(10,2) NOT NULL
            );
            "#,
        )
        .unwrap();

        let result = validate_query(&schema, "SELECT SUM(price) FROM products").unwrap();

        assert_eq!(result.columns.len(), 1);
        // SUM on Decimal still returns Option<Decimal>
        assert_eq!(
            result.columns[0].rust_type,
            RustType::Option(Box::new(RustType::Decimal))
        );
    }

    // String function tests

    #[test]
    fn test_validate_upper_lower() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT UPPER(name) as upper_name, LOWER(name) as lower_name FROM users",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "upper_name");
        assert_eq!(result.columns[0].rust_type, RustType::String);
        assert_eq!(result.columns[1].name, "lower_name");
        assert_eq!(result.columns[1].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_concat() {
        let schema = test_schema();
        let result =
            validate_query(&schema, "SELECT CONCAT(name, email) as combined FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "combined");
        assert_eq!(result.columns[0].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_length_returns_i32() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT LENGTH(name) as name_len FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "name_len");
        assert_eq!(result.columns[0].rust_type, RustType::I32);
    }

    #[test]
    fn test_validate_substring() {
        let schema = test_schema();
        let result =
            validate_query(&schema, "SELECT SUBSTRING(name, 1, 3) as short FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "short");
        assert_eq!(result.columns[0].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_strpos_returns_i32() {
        let schema = test_schema();
        // Use STRPOS which is the function-call equivalent of POSITION
        let result =
            validate_query(&schema, "SELECT STRPOS(email, '@') as pos FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "pos");
        assert_eq!(result.columns[0].rust_type, RustType::I32);
    }

    #[test]
    fn test_validate_char_length_returns_i32() {
        let schema = test_schema();
        let result = validate_query(&schema, "SELECT CHAR_LENGTH(name) as len FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "len");
        assert_eq!(result.columns[0].rust_type, RustType::I32);
    }

    #[test]
    fn test_validate_trim_functions() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT TRIM(name) as t, LTRIM(name) as lt, RTRIM(name) as rt FROM users",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].rust_type, RustType::String);
        assert_eq!(result.columns[1].rust_type, RustType::String);
        assert_eq!(result.columns[2].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_replace() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT REPLACE(name, 'a', 'b') as replaced FROM users",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].rust_type, RustType::String);
    }

    // Date/time function tests

    fn test_schema_with_timestamps() -> Schema {
        Schema::from_sql(
            r#"
            CREATE TABLE orders (
                id uuid NOT NULL,
                user_id uuid NOT NULL,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            "#,
        )
        .unwrap()
    }

    #[test]
    fn test_validate_extract_returns_f64() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT id, EXTRACT(YEAR FROM created_at) as year FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
        assert_eq!(result.columns[1].name, "year");
        assert_eq!(result.columns[1].rust_type, RustType::F64);
    }

    #[test]
    fn test_validate_extract_month() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT EXTRACT(MONTH FROM created_at) as month FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "month");
        assert_eq!(result.columns[0].rust_type, RustType::F64);
    }

    #[test]
    fn test_validate_date_trunc_returns_datetime() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT DATE_TRUNC('day', created_at) as day FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "day");
        assert_eq!(result.columns[0].rust_type, RustType::DateTime);
    }

    #[test]
    fn test_validate_date_part_returns_f64() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT DATE_PART('hour', created_at) as hour FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "hour");
        assert_eq!(result.columns[0].rust_type, RustType::F64);
    }

    #[test]
    fn test_validate_age_returns_duration() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT AGE(updated_at, created_at) as duration FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "duration");
        assert_eq!(result.columns[0].rust_type, RustType::Duration);
    }

    #[test]
    fn test_validate_to_char_returns_string() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(
            &schema,
            "SELECT TO_CHAR(created_at, 'YYYY-MM-DD') as formatted FROM orders",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "formatted");
        assert_eq!(result.columns[0].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_now_returns_datetime() {
        let schema = test_schema_with_timestamps();
        let result = validate_query(&schema, "SELECT NOW() as current_time FROM orders").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "current_time");
        assert_eq!(result.columns[0].rust_type, RustType::DateTime);
    }

    #[test]
    fn test_validate_position_returns_i32() {
        let schema = test_schema();
        let result =
            validate_query(&schema, "SELECT POSITION('@' IN email) as pos FROM users").unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "pos");
        assert_eq!(result.columns[0].rust_type, RustType::I32);
    }

    // Set operation tests (UNION/INTERSECT/EXCEPT)

    #[test]
    fn test_validate_union() {
        let schema = test_schema();
        // Use columns that exist in both tables (id is uuid in both)
        let result = validate_query(
            &schema,
            "SELECT id FROM users UNION SELECT id FROM profiles",
        )
        .unwrap();

        // Result uses column names from left side
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_union_with_different_column_names() {
        let schema = test_schema();
        // Test that result uses left side's column names
        let result = validate_query(
            &schema,
            "SELECT name FROM users UNION SELECT bio FROM profiles",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        // Column name from left side (users.name)
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[0].rust_type, RustType::String);
    }

    #[test]
    fn test_validate_union_all() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT id FROM users UNION ALL SELECT id FROM profiles",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");
    }

    #[test]
    fn test_validate_intersect() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT id FROM users INTERSECT SELECT id FROM profiles",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_except() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT id FROM users EXCEPT SELECT id FROM profiles",
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, RustType::Uuid);
    }

    #[test]
    fn test_validate_union_column_count_mismatch() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            "SELECT id, name FROM users UNION SELECT id FROM profiles",
        );

        assert!(matches!(result, Err(Error::InvalidQuery(_))));
        if let Err(Error::InvalidQuery(msg)) = result {
            assert!(msg.contains("UNION"));
            assert!(msg.contains("same number of columns"));
        }
    }

    #[test]
    fn test_validate_multiple_unions() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT id FROM users
            UNION
            SELECT id FROM profiles
            UNION
            SELECT user_id FROM profiles
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");
    }

    #[test]
    fn test_validate_union_with_where() {
        let schema = test_schema();
        let result = validate_query(
            &schema,
            r#"
            SELECT id, name FROM users WHERE name = 'Alice'
            UNION
            SELECT id, bio FROM profiles WHERE bio IS NOT NULL
            "#,
        )
        .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
    }
}
