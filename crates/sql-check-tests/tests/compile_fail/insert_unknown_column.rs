//! Test that INSERT with non-existent column fails.

use sql_check_macros::query;

fn main() {
    let _q = query!("INSERT INTO users (id, nonexistent_col) VALUES ($1, $2)",
        uuid::Uuid::new_v4(),
        "test".to_string()
    );
}
