//! sql-check: Compile-time SQL query validation against a schema file.
//!
//! Unlike SQLx (which requires a running Postgres instance at compile time),
//! sql-check validates queries against a schema file dumped from the database.

pub mod error;
pub mod schema;
pub mod types;
pub mod validate;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "sqlx-runtime")]
pub mod sqlx_runtime;

pub use error::{Error, Result};
pub use schema::{Column, Schema, Table};
pub use types::{PostgresType, RustType};
pub use validate::validate_query;

#[cfg(feature = "runtime")]
pub use runtime::{Query, QueryWithParams};

#[cfg(feature = "sqlx-runtime")]
pub use sqlx_runtime::{Query as SqlxQuery, SqlxQueryBuilder};
