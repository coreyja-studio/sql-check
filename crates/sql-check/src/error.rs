//! Error types for sql-check.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to parse schema: {0}")]
    SchemaParse(String),

    #[error("Failed to parse query: {0}")]
    QueryParse(String),

    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("Ambiguous column '{0}' - exists in multiple tables")]
    AmbiguousColumn(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
