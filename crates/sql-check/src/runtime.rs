//! Runtime query execution support.
//!
//! Provides types and functions for executing validated queries against a database.

use tokio_postgres::Row;

/// A validated query ready for execution.
pub struct Query<T> {
    sql: String,
    mapper: fn(&Row) -> T,
}

impl<T> Query<T> {
    /// Create a new query with a mapper function.
    pub fn new(sql: impl Into<String>, mapper: fn(&Row) -> T) -> Self {
        Self {
            sql: sql.into(),
            mapper,
        }
    }

    /// Get the SQL string.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Execute the query and fetch all results.
    pub async fn fetch_all(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<T>, tokio_postgres::Error> {
        let rows = client.query(&self.sql, &[]).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result.
    pub async fn fetch_one(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<T, tokio_postgres::Error> {
        let row = client.query_one(&self.sql, &[]).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result.
    pub async fn fetch_optional(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Option<T>, tokio_postgres::Error> {
        let rows = client.query(&self.sql, &[]).await?;
        Ok(rows.first().map(self.mapper))
    }
}

/// A query with parameters.
pub struct QueryWithParams<T, const N: usize> {
    sql: String,
    mapper: fn(&Row) -> T,
    params: [Box<dyn tokio_postgres::types::ToSql + Sync + Send>; N],
}

// Note: Full parameter support would need more sophisticated handling.
// For MVP, we'll focus on parameterless queries or build up from there.
