# sql-check

Compile-time SQL validation for Rust against a schema file.

Unlike sqlx which requires a live database connection for compile-time checking, sql-check validates queries against a static schema file (e.g., from `pg_dump --schema-only`).

## Status

**Early Development** - Core MVP functionality works, but many SQL features are not yet supported.

### Working Features

- ✅ **SELECT** statements with column validation
- ✅ **INSERT** statements with column validation and RETURNING
- ✅ **UPDATE** statements with column validation and RETURNING
- ✅ **DELETE** statements with RETURNING
- ✅ **JOINs**: INNER, LEFT, RIGHT, FULL OUTER, CROSS (with nullability inference)
- ✅ **Multiple JOINs** and **self-joins**
- ✅ **Aggregate functions**: COUNT (returns i64)
- ✅ **MIN/MAX** on text and integer columns
- ✅ **GROUP BY** and **HAVING**
- ✅ **DISTINCT**
- ✅ **ORDER BY**, **LIMIT**, **OFFSET**
- ✅ **Subqueries** in WHERE clause (IN, EXISTS, NOT EXISTS)
- ✅ **COALESCE** (unwraps Option types)
- ✅ **IS NULL / IS NOT NULL**
- ✅ **Complex WHERE**: AND, OR, LIKE, IN, BETWEEN
- ✅ **CASE expressions**
- ✅ **CAST expressions** and PostgreSQL `::type` syntax
- ✅ **NOW()** function
- ✅ **Parameters** with `$1`, `$2`, etc.
- ✅ **Type inference** from schema (UUID, text, jsonb, timestamp, boolean, integer, decimal, etc.)
- ✅ **Nullability inference** from LEFT/RIGHT/FULL OUTER JOINs
- ✅ **Decimal/Numeric columns** (via rust_decimal)
- ✅ **CTEs** (WITH clause) with type inference from CTE definitions

### Known Limitations

- ❌ **Subqueries in FROM** - derived tables not yet supported
- ❌ **SUM/AVG aggregates** - always return Decimal
- ❌ **Window functions** (ROW_NUMBER, RANK, LAG, LEAD, etc.)
- ❌ **String functions** (UPPER, LOWER, CONCAT, SUBSTRING, LENGTH)
- ❌ **Date functions** (EXTRACT, DATE_TRUNC)
- ❌ **UNION**, **INTERSECT**, **EXCEPT**
- ❌ **Array operations** (ANY, array overlap)

## Usage

```rust
use sql_check_macros::query;

// Basic SELECT - types inferred from schema
let q = query!("SELECT id, name, email FROM users WHERE id = $1", user_id);
let user = q.fetch_one(&client).await?;
println!("{}: {}", user.id, user.name);

// INSERT with RETURNING
let q = query!(
    "INSERT INTO users (id, name, email) VALUES ($1, $2, $3) RETURNING id, created_at",
    user_id, name, email
);
let result = q.fetch_one(&client).await?;

// LEFT JOIN - bio is Option<String> due to potential NULL
let q = query!(
    "SELECT u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id"
);
for row in q.fetch_all(&client).await? {
    if let Some(bio) = row.bio {
        println!("{}: {}", row.name, bio);
    }
}
```

## Schema Configuration

By default, sql-check looks for `schema.sql` in your crate root. Override with the `SQL_CHECK_SCHEMA` environment variable:

```bash
SQL_CHECK_SCHEMA=/path/to/schema.sql cargo build
```

Generate the schema from your database:

```bash
pg_dump --schema-only mydb > schema.sql
```

## Test Coverage

### Unit Tests (57 tests)
Compile-time validation tests that verify the `query!` macro correctly parses and validates SQL without needing a database.

### Compile-Fail Tests (6 tests)
Tests using trybuild to verify that invalid SQL produces proper compile-time errors:
- Unknown table names
- Unknown column names
- Parameter count mismatches
- Invalid SQL syntax

### Integration Tests (27 tests)
Runtime tests against a real PostgreSQL database. Requires:
```bash
# Create test database and load schema
createdb sql_check_test
psql -d sql_check_test -f crates/sql-check-tests/schema.sql

# Run tests (use single thread to avoid race conditions)
cargo test -p sql-check-tests --test integration -- --test-threads=1
```

## Project Structure

```
crates/
  sql-check/          # Core library (schema parsing, validation, type inference)
  sql-check-macros/   # Proc macro (query!)
  sql-check-tests/    # Test suite
    src/lib.rs        # Unit tests (compile-time validation)
    tests/
      integration.rs  # Runtime tests against real Postgres
      compile_fail.rs # Compile-fail tests (trybuild)
      compile_fail/   # Individual compile-fail test cases
examples/
  sample-app/         # Example usage
```

## Running Tests

```bash
# Unit tests (no database needed)
cargo test -p sql-check-tests --lib

# Compile-fail tests (no database needed)
cargo test -p sql-check-tests --test compile_fail

# Integration tests (requires PostgreSQL)
cargo test -p sql-check-tests --test integration -- --test-threads=1

# All tests in the core library
cargo test -p sql-check
```

## License

MIT OR Apache-2.0
