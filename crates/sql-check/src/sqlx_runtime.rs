//! SQLx-based runtime query execution support.
//!
//! Provides types and functions for executing validated queries against a database
//! using sqlx's connection pool.

use sqlx::postgres::PgRow;
use sqlx::PgPool;

/// A validated query ready for execution (no parameters).
pub struct Query<T> {
    sql: String,
    mapper: fn(&PgRow) -> T,
}

impl<T> Query<T> {
    /// Create a new query with a mapper function.
    pub fn new(sql: impl Into<String>, mapper: fn(&PgRow) -> T) -> Self {
        Self {
            sql: sql.into(),
            mapper,
        }
    }

    /// Get the SQL string.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Execute the query and fetch all results (no parameters).
    pub async fn fetch_all(&self, pool: &PgPool) -> Result<Vec<T>, sqlx::Error> {
        let rows: Vec<PgRow> = sqlx::query(&self.sql).fetch_all(pool).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result (no parameters).
    pub async fn fetch_one(&self, pool: &PgPool) -> Result<T, sqlx::Error> {
        let row: PgRow = sqlx::query(&self.sql).fetch_one(pool).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result (no parameters).
    pub async fn fetch_optional(&self, pool: &PgPool) -> Result<Option<T>, sqlx::Error> {
        let row: Option<PgRow> = sqlx::query(&self.sql).fetch_optional(pool).await?;
        Ok(row.as_ref().map(self.mapper))
    }

    /// Execute the query (for INSERT/UPDATE/DELETE without returning data).
    pub async fn execute(&self, pool: &PgPool) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(&self.sql).execute(pool).await?;
        Ok(result.rows_affected())
    }
}

/// A query builder that wraps sqlx::query for chained `.bind()` calls.
///
/// This type is created by the `sqlx_query!` macro and allows you to bind parameters
/// and then execute the query with methods like `fetch_all`, `fetch_one`, etc.
pub struct SqlxQueryBuilder<'q, T> {
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    mapper: fn(&PgRow) -> T,
}

impl<'q, T> SqlxQueryBuilder<'q, T> {
    /// Create a new query builder with a mapper function.
    #[must_use]
    pub fn new(sql: &'q str, mapper: fn(&PgRow) -> T) -> Self {
        Self {
            query: sqlx::query(sql),
            mapper,
        }
    }

    /// Bind a parameter to the query.
    #[must_use]
    pub fn bind<P>(mut self, value: P) -> Self
    where
        P: 'q + sqlx::Encode<'q, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send,
    {
        self.query = self.query.bind(value);
        self
    }

    /// Execute the query and fetch all results.
    pub async fn fetch_all(self, pool: &PgPool) -> Result<Vec<T>, sqlx::Error> {
        let rows: Vec<PgRow> = self.query.fetch_all(pool).await?;
        Ok(rows.iter().map(self.mapper).collect())
    }

    /// Execute the query and fetch one result.
    pub async fn fetch_one(self, pool: &PgPool) -> Result<T, sqlx::Error> {
        let row: PgRow = self.query.fetch_one(pool).await?;
        Ok((self.mapper)(&row))
    }

    /// Execute the query and fetch an optional result.
    pub async fn fetch_optional(self, pool: &PgPool) -> Result<Option<T>, sqlx::Error> {
        let row: Option<PgRow> = self.query.fetch_optional(pool).await?;
        Ok(row.as_ref().map(self.mapper))
    }

    /// Execute the query (for INSERT/UPDATE/DELETE without returning data).
    pub async fn execute(self, pool: &PgPool) -> Result<u64, sqlx::Error> {
        let result = self.query.execute(pool).await?;
        Ok(result.rows_affected())
    }
}
