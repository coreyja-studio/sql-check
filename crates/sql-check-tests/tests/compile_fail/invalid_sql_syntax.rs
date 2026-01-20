//! Test that invalid SQL syntax fails with a clear error.

use sql_check_macros::query;

fn main() {
    // Missing FROM clause
    let _q = query!("SELECT id WHERE id = 1");
}
