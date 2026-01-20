//! Test that providing wrong number of parameters fails with a clear error.

use sql_check_macros::query;

fn main() {
    let name = "Alice".to_string();
    // SQL has $1 and $2 but only one parameter provided
    let _q = query!("SELECT id FROM users WHERE name = $1 AND email = $2", name);
}
