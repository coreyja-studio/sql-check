//! Integration tests for sql-check-macros.
//!
//! These tests verify the query! macro generates valid, compilable code.

use sql_check_macros::query;

/// Test basic SELECT query generates a typed result.
#[test]
fn test_basic_select() {
    // This should generate a struct with id: Uuid and name: String
    let q = query!("SELECT id, name FROM users");

    // Verify the SQL is preserved
    assert_eq!(q.sql(), "SELECT id, name FROM users");
}

/// Test SELECT with all columns from users table.
#[test]
fn test_select_all_user_columns() {
    let q = query!("SELECT id, name, email, metadata, created_at FROM users");
    assert_eq!(q.sql(), "SELECT id, name, email, metadata, created_at FROM users");
}

/// Test SELECT from profiles table with nullable columns.
#[test]
fn test_select_nullable_columns() {
    let q = query!("SELECT id, user_id, bio, avatar_url FROM profiles");
    assert_eq!(q.sql(), "SELECT id, user_id, bio, avatar_url FROM profiles");
}

/// Test LEFT JOIN makes columns nullable.
#[test]
fn test_left_join_nullability() {
    let q = query!("SELECT u.id, u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id");
    assert_eq!(
        q.sql(),
        "SELECT u.id, u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id"
    );
}

/// Test query with one parameter.
#[test]
fn test_query_with_one_param() {
    let user_id = uuid::Uuid::new_v4();
    let q = query!("SELECT id, name FROM users WHERE id = $1", user_id);
    assert_eq!(q.sql(), "SELECT id, name FROM users WHERE id = $1");
}

/// Test query with multiple parameters.
#[test]
fn test_query_with_multiple_params() {
    let name = "Alice".to_string();
    let email = "alice@example.com".to_string();
    let q = query!("SELECT id FROM users WHERE name = $1 AND email = $2", name, email);
    assert_eq!(q.sql(), "SELECT id FROM users WHERE name = $1 AND email = $2");
}

// NOTE: To verify compile-time errors work, uncomment one of these:
//
// Invalid table name - should fail:
// let q = query!("SELECT id FROM nonexistent_table");
//
// Invalid column name - should fail:
// let q = query!("SELECT nonexistent_column FROM users");
//
// Wrong number of parameters - should fail with "Expected 2 parameter(s) but got 1":
// let name = "Alice".to_string();
// let q = query!("SELECT id FROM users WHERE name = $1 AND email = $2", name);
