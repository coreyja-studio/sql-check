//! Runtime query execution support.
//!
//! Provides types and functions for executing validated queries against a database.

use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

/// A validated query ready for execution (no parameters).
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

    /// Bind parameters and return a BoundQuery.
    pub fn bind<P: ToSql + Sync>(self, params: Vec<P>) -> BoundQuery<T, P> {
        BoundQuery {
            sql: self.sql,
            mapper: self.mapper,
            params,
        }
    }

    /// Execute the query and fetch all results (no parameters).
    pub async fn fetch_all(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<T>, tokio_postgres::Error> {
        let rows = client.query(&self.sql, &[]).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result (no parameters).
    pub async fn fetch_one(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<T, tokio_postgres::Error> {
        let row = client.query_one(&self.sql, &[]).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result (no parameters).
    pub async fn fetch_optional(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Option<T>, tokio_postgres::Error> {
        let rows = client.query(&self.sql, &[]).await?;
        Ok(rows.first().map(self.mapper))
    }

    /// Execute the query without returning results (for INSERT/UPDATE/DELETE).
    pub async fn execute(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<u64, tokio_postgres::Error> {
        client.execute(&self.sql, &[]).await
    }
}

/// A query bound with parameters.
pub struct BoundQuery<T, P: ToSql + Sync> {
    sql: String,
    mapper: fn(&Row) -> T,
    params: Vec<P>,
}

impl<T, P: ToSql + Sync> BoundQuery<T, P> {
    /// Execute the query and fetch all results.
    pub async fn fetch_all(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<T>, tokio_postgres::Error> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|p| p as _).collect();
        let rows = client.query(&self.sql, &params).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result.
    pub async fn fetch_one(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<T, tokio_postgres::Error> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|p| p as _).collect();
        let row = client.query_one(&self.sql, &params).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result.
    pub async fn fetch_optional(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Option<T>, tokio_postgres::Error> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|p| p as _).collect();
        let rows = client.query(&self.sql, &params).await?;
        Ok(rows.first().map(self.mapper))
    }

    /// Execute the query without returning results (for INSERT/UPDATE/DELETE).
    pub async fn execute(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<u64, tokio_postgres::Error> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|p| p as _).collect();
        client.execute(&self.sql, &params).await
    }
}

/// A validated query with embedded parameters.
///
/// This is used when the query! macro is called with parameters.
/// The parameters are stored as trait object references.
pub struct QueryWithParams<'a, T> {
    sql: String,
    mapper: fn(&Row) -> T,
    params: Vec<&'a (dyn ToSql + Sync)>,
}

impl<'a, T> QueryWithParams<'a, T> {
    /// Create a new query with parameters.
    pub fn new(
        sql: impl Into<String>,
        mapper: fn(&Row) -> T,
        params: Vec<&'a (dyn ToSql + Sync)>,
    ) -> Self {
        Self {
            sql: sql.into(),
            mapper,
            params,
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
        let rows = client.query(&self.sql, &self.params).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result.
    pub async fn fetch_one(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<T, tokio_postgres::Error> {
        let row = client.query_one(&self.sql, &self.params).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result.
    pub async fn fetch_optional(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Option<T>, tokio_postgres::Error> {
        let rows = client.query(&self.sql, &self.params).await?;
        Ok(rows.first().map(self.mapper))
    }

    /// Execute the query without returning results (for INSERT/UPDATE/DELETE).
    pub async fn execute(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<u64, tokio_postgres::Error> {
        client.execute(&self.sql, &self.params).await
    }
}

/// Trait for converting tokio_postgres Row to typed struct.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Self;
}
