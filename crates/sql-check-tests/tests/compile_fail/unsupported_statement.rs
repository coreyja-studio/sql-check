//! Test that unsupported statement types fail with a clear error.

use sql_check_macros::query;

fn main() {
    // UPDATE is not supported
    let _q = query!("UPDATE users SET name = 'test'");
}
