//! Schema representation and parsing.
//!
//! Parses pg_dump --schema-only output into Rust data structures.

use crate::error::{Error, Result};
use crate::types::PostgresType;
use sqlparser::ast::{
    CharacterLength, ColumnDef, ColumnOption, DataType, Expr, ObjectName, Statement,
    TableConstraint, TimezoneInfo,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

/// A database schema containing tables and their definitions.
#[derive(Debug, Default)]
pub struct Schema {
    tables: HashMap<String, Table>,
}

impl Schema {
    /// Create a new empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse schema from SQL DDL statements (like pg_dump output).
    pub fn from_sql(sql: &str) -> Result<Self> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::SchemaParse(e.to_string()))?;

        let mut schema = Schema::new();

        for statement in statements {
            match statement {
                Statement::CreateTable(create) => {
                    let table = Table::from_create_table(&create)?;
                    schema.tables.insert(table.name.clone(), table);
                }
                // We can add support for CREATE INDEX, CREATE TYPE, etc. later
                _ => {}
            }
        }

        Ok(schema)
    }

    /// Load schema from a file.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let sql = std::fs::read_to_string(path)?;
        Self::from_sql(&sql)
    }

    /// Get a table by name.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // Try exact match first
        if let Some(table) = self.tables.get(name) {
            return Some(table);
        }

        // Try case-insensitive match
        let name_lower = name.to_lowercase();
        self.tables
            .values()
            .find(|t| t.name.to_lowercase() == name_lower)
    }

    /// Get all table names.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.get_table(name).is_some()
    }
}

/// A database table.
#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    column_map: HashMap<String, usize>,
}

impl Table {
    /// Create a table from a CREATE TABLE statement.
    fn from_create_table(create: &sqlparser::ast::CreateTable) -> Result<Self> {
        let name = object_name_to_string(&create.name);
        let mut columns = Vec::new();
        let mut column_map = HashMap::new();

        // First pass: extract columns
        for (idx, col_def) in create.columns.iter().enumerate() {
            let column = Column::from_column_def(col_def)?;
            column_map.insert(column.name.to_lowercase(), idx);
            columns.push(column);
        }

        // Second pass: handle table constraints (PRIMARY KEY, UNIQUE, etc.)
        for constraint in &create.constraints {
            match constraint {
                TableConstraint::PrimaryKey(pk) => {
                    for pk_col in &pk.columns {
                        // IndexColumn has a column field with OrderByExpr
                        if let Expr::Identifier(ident) = &pk_col.column.expr {
                            let col_name = ident.value.to_lowercase();
                            if let Some(&idx) = column_map.get(&col_name) {
                                columns[idx].is_primary_key = true;
                                columns[idx].nullable = false; // PKs are never null
                            }
                        }
                    }
                }
                TableConstraint::Unique(unique) => {
                    for unique_col in &unique.columns {
                        if let Expr::Identifier(ident) = &unique_col.column.expr {
                            let col_name = ident.value.to_lowercase();
                            if let Some(&idx) = column_map.get(&col_name) {
                                columns[idx].is_unique = true;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Table {
            name,
            columns,
            column_map,
        })
    }

    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        let name_lower = name.to_lowercase();
        self.column_map
            .get(&name_lower)
            .map(|&idx| &self.columns[idx])
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    /// Get all column names.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|c| c.name.as_str())
    }
}

/// A table column.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: PostgresType,
    pub nullable: bool,
    pub has_default: bool,
    pub is_primary_key: bool,
    pub is_unique: bool,
}

impl Column {
    /// Create a column from a ColumnDef.
    fn from_column_def(col_def: &ColumnDef) -> Result<Self> {
        let name = col_def.name.value.clone();
        let data_type = data_type_to_postgres(&col_def.data_type)?;

        let mut nullable = true; // Default to nullable
        let mut has_default = false;
        let mut is_primary_key = false;
        let mut is_unique = false;

        for option in &col_def.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                ColumnOption::Default(_) => has_default = true,
                ColumnOption::PrimaryKey(_) => {
                    is_primary_key = true;
                    nullable = false;
                }
                ColumnOption::Unique(_) => {
                    is_unique = true;
                }
                _ => {}
            }
        }

        Ok(Column {
            name,
            data_type,
            nullable,
            has_default,
            is_primary_key,
            is_unique,
        })
    }
}

/// Convert an ObjectName to a simple string.
fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|i| i.value.clone()))
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract length from CharacterLength enum.
fn extract_char_length(len: &Option<CharacterLength>) -> Option<u32> {
    match len {
        Some(CharacterLength::IntegerLength { length, .. }) => Some(*length as u32),
        Some(CharacterLength::Max) => None,
        None => None,
    }
}

/// Convert sqlparser DataType to our PostgresType.
fn data_type_to_postgres(dt: &DataType) -> Result<PostgresType> {
    Ok(match dt {
        // Exact numeric types
        DataType::SmallInt(_) => PostgresType::SmallInt,
        DataType::Int(_) | DataType::Integer(_) => PostgresType::Integer,
        DataType::BigInt(_) => PostgresType::BigInt,
        DataType::Real => PostgresType::Real,
        DataType::Double(_) | DataType::DoublePrecision => PostgresType::DoublePrecision,
        DataType::Numeric(_) | DataType::Decimal(_) => PostgresType::Numeric,

        // Character types
        DataType::Text => PostgresType::Text,
        DataType::Varchar(len) => PostgresType::Varchar(extract_char_length(len)),
        DataType::Char(len) => PostgresType::Char(extract_char_length(len)),
        DataType::CharacterVarying(len) => PostgresType::Varchar(extract_char_length(len)),
        DataType::Character(len) => PostgresType::Char(extract_char_length(len)),

        // Binary
        DataType::Bytea => PostgresType::Bytea,

        // Boolean
        DataType::Boolean | DataType::Bool => PostgresType::Boolean,

        // Date/Time
        DataType::Timestamp(_, tz) => {
            if matches!(tz, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) {
                PostgresType::TimestampTz
            } else {
                PostgresType::Timestamp
            }
        }
        DataType::Date => PostgresType::Date,
        DataType::Time(_, tz) => {
            if matches!(tz, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) {
                PostgresType::TimeTz
            } else {
                PostgresType::Time
            }
        }
        DataType::Interval { .. } => PostgresType::Interval,

        // UUID
        DataType::Uuid => PostgresType::Uuid,

        // JSON
        DataType::JSON => PostgresType::Json,
        DataType::JSONB => PostgresType::Jsonb,

        // Array types
        DataType::Array(inner) => match inner {
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner_dt)
            | sqlparser::ast::ArrayElemTypeDef::SquareBracket(inner_dt, _)
            | sqlparser::ast::ArrayElemTypeDef::Parenthesis(inner_dt) => {
                PostgresType::Array(Box::new(data_type_to_postgres(inner_dt)?))
            }
            sqlparser::ast::ArrayElemTypeDef::None => {
                return Err(Error::SchemaParse("Array with no element type".to_string()));
            }
        },

        // Custom types (enums, etc.)
        DataType::Custom(name, _) => PostgresType::Custom(object_name_to_string(name)),

        // Fallback for other types
        other => PostgresType::Custom(format!("{:?}", other)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_table() {
        let sql = r#"
            CREATE TABLE users (
                id uuid NOT NULL,
                name text NOT NULL,
                email text NOT NULL,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                CONSTRAINT users_pkey PRIMARY KEY (id)
            );
        "#;

        let schema = Schema::from_sql(sql).unwrap();
        let table = schema.get_table("users").unwrap();

        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 4);

        let id_col = table.get_column("id").unwrap();
        assert_eq!(id_col.data_type, PostgresType::Uuid);
        assert!(!id_col.nullable);
        assert!(id_col.is_primary_key);

        let name_col = table.get_column("name").unwrap();
        assert_eq!(name_col.data_type, PostgresType::Text);
        assert!(!name_col.nullable);

        let created_col = table.get_column("created_at").unwrap();
        assert_eq!(created_col.data_type, PostgresType::TimestampTz);
        assert!(created_col.has_default);
    }

    #[test]
    fn test_parse_jsonb_column() {
        let sql = r#"
            CREATE TABLE items (
                id integer NOT NULL,
                metadata jsonb NOT NULL DEFAULT '{}'
            );
        "#;

        let schema = Schema::from_sql(sql).unwrap();
        let table = schema.get_table("items").unwrap();

        let metadata_col = table.get_column("metadata").unwrap();
        assert_eq!(metadata_col.data_type, PostgresType::Jsonb);
    }

    #[test]
    fn test_parse_nullable_columns() {
        let sql = r#"
            CREATE TABLE profiles (
                id uuid NOT NULL,
                bio text,
                avatar_url text
            );
        "#;

        let schema = Schema::from_sql(sql).unwrap();
        let table = schema.get_table("profiles").unwrap();

        let bio_col = table.get_column("bio").unwrap();
        assert!(bio_col.nullable);

        let avatar_col = table.get_column("avatar_url").unwrap();
        assert!(avatar_col.nullable);
    }
}
