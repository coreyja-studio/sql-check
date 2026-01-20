//! Test that unsupported statement types fail with a clear error.

use sql_check_macros::query;

fn main() {
    // CREATE TABLE is not supported
    let _q = query!("CREATE TABLE test (id int)");
}
