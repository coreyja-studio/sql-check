//! Test that invalid SQL fails at compile time.
//!
//! Uncomment one of the test bodies to verify it fails.

// Import kept for when tests are uncommented
#[allow(unused_imports)]
use sql_check_macros::query;

#[test]
fn test_invalid_table_fails() {
    // Uncomment to test - should fail with "Unknown table: nonexistent_table"
    // let q = query!("SELECT id FROM nonexistent_table");
}

#[test]
fn test_invalid_column_fails() {
    // Uncomment to test - should fail with "Unknown column: nonexistent_column in table users"
    // let q = query!("SELECT nonexistent_column FROM users");
}
