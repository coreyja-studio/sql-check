//! Test that invalid SQL fails at compile time with proper error messages.
//!
//! Uses trybuild to verify that certain code fails to compile and produces
//! the expected error messages.
//!
//! NOTE: These tests require the SQL_CHECK_SCHEMA env var to be set.
//! Run with: SQL_CHECK_SCHEMA=<path>/schema.sql cargo test --test compile_fail

use std::path::PathBuf;

#[test]
fn compile_fail_tests() {
    // Set the schema path for trybuild compilation
    // The schema is in the crate root
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema.sql");
    std::env::set_var("SQL_CHECK_SCHEMA", &schema_path);

    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
