//! Test that selecting a non-existent column fails with a clear error.

use sql_check_macros::query;

fn main() {
    let _q = query!("SELECT nonexistent_column FROM users");
}
