//! Test that querying a non-existent table fails with a clear error.

use sql_check_macros::query;

fn main() {
    let _q = query!("SELECT id FROM nonexistent_table");
}
