# sql-check

Compile-time SQL query validation against a schema file - **no database connection required**.

Unlike SQLx (which requires a running Postgres instance at compile time), sql-check validates queries against a schema file dumped from your database. This makes CI simpler and avoids the "chicken and egg" problem of needing a database to compile code that sets up the database.

## Features

- **Compile-time validation**: Catch SQL errors before runtime
- **No database connection**: Validates against a `schema.sql` file
- **Type inference**: Generates typed result structs from your queries
- **JOIN support**: Handles LEFT JOIN nullability correctly
- **CRUD support**: Validates SELECT, INSERT, UPDATE, and DELETE statements
- **RETURNING clauses**: Full support for typed RETURNING results
- **Aggregate functions**: Correct types for COUNT, SUM, AVG, MIN, MAX

## Quick Start

1. Dump your schema:
   ```bash
   pg_dump --schema-only your_database > schema.sql
   ```

2. Add the dependency:
   ```toml
   [dependencies]
   sql-check = { version = "0.1", features = ["runtime"] }
   sql-check-macros = "0.1"
   tokio-postgres = "0.7"
   ```

3. Write validated queries:
   ```rust
   use sql_check_macros::query;

   // Validates at compile time that:
   // - `users` table exists
   // - `id` and `name` columns exist
   // - Types are correctly inferred
   let users = query!("SELECT id, name FROM users")
       .fetch_all(&client)
       .await?;

   for user in users {
       println!("{}: {}", user.id, user.name);  // Typed fields!
   }
   ```

## Runtime Backends

### tokio-postgres (default runtime)

```toml
[dependencies]
sql-check = { version = "0.1", features = ["runtime"] }
```

```rust
use sql_check_macros::query;

let user = query!("SELECT id, name FROM users WHERE id = $1", user_id)
    .fetch_one(&client)
    .await?;
```

### SQLx

For projects using SQLx connection pools:

```toml
[dependencies]
sql-check = { version = "0.1", features = ["sqlx-runtime"] }
```

```rust
use sql_check_macros::sqlx_query;

let users = sqlx_query!("SELECT id, name FROM users WHERE active = $1", true)
    .fetch_all(&pool)
    .await?;
```

## Schema Configuration

By default, sql-check looks for `schema.sql` in your crate root. Override with:

```bash
SQL_CHECK_SCHEMA=/path/to/schema.sql cargo build
```

## Type Mappings

| PostgreSQL | Rust |
|------------|------|
| `int2/smallint` | `i16` |
| `int4/integer` | `i32` |
| `int8/bigint` | `i64` |
| `real` | `f32` |
| `double precision` | `f64` |
| `numeric/decimal` | `rust_decimal::Decimal` |
| `text/varchar` | `String` |
| `bytea` | `Vec<u8>` |
| `boolean` | `bool` |
| `timestamp/timestamptz` | `chrono::DateTime<Utc>` |
| `date` | `chrono::NaiveDate` |
| `time` | `chrono::NaiveTime` |
| `uuid` | `uuid::Uuid` |
| `json/jsonb` | `serde_json::Value` |
| `inet` | `std::net::IpAddr` |

Nullable columns are wrapped in `Option<T>`. LEFT JOIN columns are automatically made nullable.

## How It Works

1. **At compile time**, the `query!` macro:
   - Parses your schema.sql file
   - Parses and validates your SQL query
   - Checks that tables and columns exist
   - Infers result types from the schema
   - Generates a typed result struct

2. **At runtime**, the generated code:
   - Executes the query through tokio-postgres or SQLx
   - Maps rows to the generated struct
   - Returns typed results

## Validation Examples

```rust
// Catches unknown tables
query!("SELECT * FROM nonexistent");
// error: SQL validation error: Unknown table: nonexistent

// Catches unknown columns
query!("SELECT fake_col FROM users");
// error: SQL validation error: Unknown column 'fake_col' in table 'users'

// Catches parameter count mismatches
query!("SELECT * FROM users WHERE id = $1");  // Missing parameter
// error: Expected 1 parameter(s) but got 0
```

## License

MIT
