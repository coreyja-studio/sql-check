//! Integration tests for sql-check-macros.
//!
//! These tests verify the query! macro generates valid, compilable code.
//!
//! Test categories:
//! - Working tests: Can be compiled and run now
//! - Ignored tests (compile): Marked #[ignore], can compile but not run
//! - Ignored tests (no compile): Commented out because they fail at compile time
//!
//! Known limitations (tests commented out):
//! - CTEs: WITH clause table resolution not implemented
//! - UPDATE/DELETE: Only SELECT and INSERT are supported
//! - Window functions: ROW_NUMBER, RANK, etc. not supported
//! - UNION/INTERSECT/EXCEPT: Set operations not supported
//! - RIGHT JOIN/FULL OUTER JOIN/CROSS JOIN: Not yet tested
//! - Array operations: ANY, array overlap not tested

// The query! macro is used in tests below, but clippy doesn't see proc macro usage
#[allow(unused_imports)]
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
    assert_eq!(
        q.sql(),
        "SELECT id, name, email, metadata, created_at FROM users"
    );
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
    let q =
        query!("SELECT u.id, u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id");
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
    let q = query!(
        "SELECT id FROM users WHERE name = $1 AND email = $2",
        name,
        email
    );
    assert_eq!(
        q.sql(),
        "SELECT id FROM users WHERE name = $1 AND email = $2"
    );
}

// ============================================================================
// INNER JOIN tests
// ============================================================================

#[test]
fn test_inner_join() {
    let user_id = uuid::Uuid::new_v4();
    let q = query!(
        "SELECT u.id, u.name, p.bio FROM users u INNER JOIN profiles p ON p.user_id = u.id WHERE u.id = $1",
        user_id
    );
    assert!(q.sql().contains("INNER JOIN"));
}

// ============================================================================
// Multiple JOINs tests
// ============================================================================

#[test]
fn test_multiple_joins() {
    let q = query!(
        r#"
        SELECT u.name, o.status, p.bio
        FROM users u
        INNER JOIN orders o ON o.user_id = u.id
        LEFT JOIN profiles p ON p.user_id = u.id
        "#
    );
    assert!(q.sql().contains("INNER JOIN"));
    assert!(q.sql().contains("LEFT JOIN"));
}

// ============================================================================
// Aggregate function tests
// ============================================================================
// Note: SUM/AVG always return Decimal in Postgres, which requires the
// with-rust_decimal-1 feature. COUNT returns i64 which works.

#[test]
fn test_count_star() {
    let q = query!("SELECT COUNT(*) as total FROM users");
    assert!(q.sql().contains("COUNT(*)"));
}

// SUM/AVG on any column return Decimal, so these tests are commented out
// until Decimal support is added via feature flag.
//
// #[test]
// fn test_sum_aggregate() {
//     let q = query!("SELECT SUM(quantity) as total FROM order_items");
//     assert!(q.sql().contains("SUM"));
// }
//
// #[test]
// fn test_avg_aggregate() {
//     let q = query!("SELECT AVG(quantity) as avg_qty FROM order_items");
//     assert!(q.sql().contains("AVG"));
// }

// MIN/MAX on text columns return Option<String> which works
#[test]
fn test_min_max_text() {
    let q = query!("SELECT MIN(name) as first_name, MAX(name) as last_name FROM users");
    assert!(q.sql().contains("MIN"));
    assert!(q.sql().contains("MAX"));
}

// MIN/MAX on integer columns return Option<i32> which might work
#[test]
fn test_min_max_integer() {
    let q = query!("SELECT MIN(stock_quantity) as min_stock, MAX(stock_quantity) as max_stock FROM products");
    assert!(q.sql().contains("MIN"));
    assert!(q.sql().contains("MAX"));
}

// ============================================================================
// GROUP BY / HAVING tests
// ============================================================================

#[test]
fn test_group_by() {
    let q = query!("SELECT status, COUNT(*) as count FROM orders GROUP BY status");
    assert!(q.sql().contains("GROUP BY"));
}

#[test]
fn test_group_by_having() {
    let threshold = 0i64;
    let q = query!(
        "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id HAVING COUNT(*) > $1",
        threshold
    );
    assert!(q.sql().contains("HAVING"));
}

// ============================================================================
// DISTINCT tests
// ============================================================================

#[test]
fn test_distinct() {
    let q = query!("SELECT DISTINCT status FROM orders");
    assert!(q.sql().contains("DISTINCT"));
}

#[test]
fn test_distinct_multiple_columns() {
    let q = query!("SELECT DISTINCT user_id, status FROM orders");
    assert!(q.sql().contains("DISTINCT"));
}

// ============================================================================
// ORDER BY / LIMIT / OFFSET tests
// ============================================================================

#[test]
fn test_order_by() {
    let q = query!("SELECT id, name FROM users ORDER BY name ASC");
    assert!(q.sql().contains("ORDER BY"));
}

#[test]
fn test_order_by_multiple() {
    let q = query!("SELECT id, status, created_at FROM orders ORDER BY status ASC, created_at DESC");
    assert!(q.sql().contains("ORDER BY"));
}

#[test]
fn test_limit() {
    let q = query!("SELECT id, name FROM users LIMIT 5");
    assert!(q.sql().contains("LIMIT"));
}

#[test]
fn test_limit_offset() {
    let q = query!("SELECT id, name FROM users ORDER BY name LIMIT 5 OFFSET 2");
    assert!(q.sql().contains("LIMIT"));
    assert!(q.sql().contains("OFFSET"));
}

// ============================================================================
// INSERT tests
// ============================================================================

#[test]
fn test_insert_returning() {
    let user_id = uuid::Uuid::new_v4();
    let name = "Test".to_string();
    let email = "test@example.com".to_string();
    let metadata = serde_json::json!({});
    let q = query!(
        "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4) RETURNING id, name",
        user_id,
        name,
        email,
        metadata
    );
    assert!(q.sql().contains("INSERT"));
    assert!(q.sql().contains("RETURNING"));
}

#[test]
fn test_insert_returning_all() {
    let cat_id = uuid::Uuid::new_v4();
    let name = "Test".to_string();
    let q = query!(
        "INSERT INTO categories (id, name) VALUES ($1, $2) RETURNING *",
        cat_id,
        name
    );
    assert!(q.sql().contains("RETURNING *"));
}

#[test]
fn test_insert_no_returning() {
    let user_id = uuid::Uuid::new_v4();
    let name = "Test User".to_string();
    let email = "test@example.com".to_string();
    let metadata = serde_json::json!({});
    let q = query!(
        "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
        user_id,
        name,
        email,
        metadata
    );
    assert!(q.sql().contains("INSERT"));
    assert!(!q.sql().contains("RETURNING"));
}

#[test]
fn test_insert_multiple_rows() {
    let cat1_id = uuid::Uuid::new_v4();
    let cat2_id = uuid::Uuid::new_v4();
    let name1 = "Category 1".to_string();
    let name2 = "Category 2".to_string();
    let q = query!(
        "INSERT INTO categories (id, name) VALUES ($1, $2), ($3, $4)",
        cat1_id,
        name1,
        cat2_id,
        name2
    );
    assert!(q.sql().contains("INSERT"));
}

// ============================================================================
// Subquery tests
// ============================================================================

#[test]
fn test_subquery_in_where() {
    let status = "completed".to_string();
    let q = query!(
        "SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = $1)",
        status
    );
    assert!(q.sql().contains("SELECT user_id FROM orders"));
}

#[test]
fn test_exists() {
    let q = query!(
        r#"
        SELECT id, name
        FROM users u
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        "#
    );
    assert!(q.sql().contains("EXISTS"));
}

#[test]
fn test_not_exists() {
    let q = query!(
        r#"
        SELECT id, name
        FROM users u
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        "#
    );
    assert!(q.sql().contains("NOT EXISTS"));
}

// ============================================================================
// NULL handling tests
// ============================================================================

#[test]
fn test_coalesce() {
    let q = query!(
        "SELECT id, COALESCE(description, 'No description') as description FROM products"
    );
    assert!(q.sql().contains("COALESCE"));
}

#[test]
fn test_where_is_null() {
    let q = query!("SELECT id, name FROM products WHERE category_id IS NULL");
    assert!(q.sql().contains("IS NULL"));
}

#[test]
fn test_where_is_not_null() {
    let q = query!("SELECT id, name FROM products WHERE description IS NOT NULL");
    assert!(q.sql().contains("IS NOT NULL"));
}

// ============================================================================
// Complex WHERE clause tests
// ============================================================================

#[test]
fn test_where_and_or() {
    let stock_threshold = 5i32;
    let q = query!(
        "SELECT id, name FROM products WHERE (stock_quantity > $1 AND is_active = true) OR stock_quantity = 0",
        stock_threshold
    );
    assert!(q.sql().contains("AND"));
    assert!(q.sql().contains("OR"));
}

#[test]
fn test_where_like() {
    let pattern = "%Laptop%".to_string();
    let q = query!(
        "SELECT id, name FROM products WHERE name LIKE $1",
        pattern
    );
    assert!(q.sql().contains("LIKE"));
}

#[test]
fn test_where_in() {
    let q = query!(
        "SELECT id, status FROM orders WHERE status IN ('pending', 'completed')"
    );
    assert!(q.sql().contains("IN"));
}

#[test]
fn test_where_between() {
    let low = 1i32;
    let high = 100i32;
    let q = query!(
        "SELECT id, name, stock_quantity FROM products WHERE stock_quantity BETWEEN $1 AND $2",
        low,
        high
    );
    assert!(q.sql().contains("BETWEEN"));
}

// ============================================================================
// Self-join tests
// ============================================================================

#[test]
fn test_self_join() {
    let cat_id = uuid::Uuid::new_v4();
    let q = query!(
        r#"
        SELECT c.id, c.name, p.name as parent_name
        FROM categories c
        LEFT JOIN categories p ON c.parent_id = p.id
        WHERE c.id = $1
        "#,
        cat_id
    );
    assert!(q.sql().contains("categories c"));
    assert!(q.sql().contains("categories p"));
}

// ============================================================================
// Type tests (new tables) - only non-Decimal columns
// ============================================================================

#[test]
fn test_products_table_non_decimal() {
    // Avoid price column which is Decimal
    let q = query!("SELECT id, name, stock_quantity, is_active, tags FROM products");
    assert!(q.sql().contains("products"));
}

#[test]
fn test_orders_table_non_decimal() {
    // Avoid total_amount column which is Decimal
    let q = query!("SELECT id, user_id, status, notes FROM orders");
    assert!(q.sql().contains("orders"));
}

#[test]
fn test_order_items_table_non_decimal() {
    // Avoid unit_price column which is Decimal
    let q = query!("SELECT id, order_id, product_id, quantity FROM order_items");
    assert!(q.sql().contains("order_items"));
}

#[test]
fn test_categories_table() {
    let q = query!("SELECT id, name, parent_id FROM categories");
    assert!(q.sql().contains("categories"));
}

// ============================================================================
// CASE expression tests
// ============================================================================

#[test]
fn test_case_expression() {
    let q = query!(
        r#"
        SELECT id, status,
               CASE
                   WHEN status = 'pending' THEN 'Waiting'
                   WHEN status = 'completed' THEN 'Done'
                   ELSE 'Unknown'
               END as status_label
        FROM orders
        "#
    );
    assert!(q.sql().contains("CASE"));
    assert!(q.sql().contains("WHEN"));
    assert!(q.sql().contains("ELSE"));
    assert!(q.sql().contains("END"));
}

#[test]
fn test_case_simple_form() {
    let q = query!(
        r#"
        SELECT id, is_active,
               CASE is_active
                   WHEN true THEN 'Active'
                   WHEN false THEN 'Inactive'
               END as active_label
        FROM products
        "#
    );
    assert!(q.sql().contains("CASE"));
}

// ============================================================================
// CAST expression tests
// ============================================================================

#[test]
fn test_cast_expression() {
    let q = query!(
        r#"
        SELECT id, CAST(stock_quantity AS text) as stock_text
        FROM products
        "#
    );
    assert!(q.sql().contains("CAST"));
}

#[test]
fn test_postgresql_cast_syntax() {
    let q = query!(
        r#"
        SELECT id, stock_quantity::text as stock_text
        FROM products
        "#
    );
    assert!(q.sql().contains("::"));
}

// ============================================================================
// Date/time function tests
// ============================================================================

#[test]
fn test_now_function() {
    let q = query!(
        r#"
        SELECT id, name, NOW() as current_time
        FROM users
        "#
    );
    assert!(q.sql().contains("NOW()"));
}

// ============================================================================
// Tests that don't compile yet (documented limitations)
// ============================================================================

// These tests are commented out because they fail at compile time.
// They document features that are not yet implemented.

// --- CTE (WITH clause) ---
// CTEs fail because the table names from WITH clause are not recognized.
//
// #[test]
// fn test_cte_simple() {
//     let q = query!(
//         r#"
//         WITH active_users AS (
//             SELECT id, name FROM users
//         )
//         SELECT id, name FROM active_users
//         "#
//     );
//     assert!(q.sql().contains("WITH"));
// }

// --- UPDATE statements ---
// UPDATE is not yet supported - only SELECT and INSERT work.
//
// #[test]
// fn test_update_simple() {
//     let name = "Updated".to_string();
//     let user_id = uuid::Uuid::new_v4();
//     let q = query!("UPDATE users SET name = $1 WHERE id = $2", name, user_id);
//     assert!(q.sql().contains("UPDATE"));
// }

// --- DELETE statements ---
// DELETE is not yet supported - only SELECT and INSERT work.
//
// #[test]
// fn test_delete_simple() {
//     let user_id = uuid::Uuid::new_v4();
//     let q = query!("DELETE FROM users WHERE id = $1", user_id);
//     assert!(q.sql().contains("DELETE"));
// }

// --- Window functions ---
// Window functions return unknown types (function name as type).
//
// #[test]
// fn test_row_number() {
//     let q = query!(
//         "SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM users"
//     );
//     assert!(q.sql().contains("ROW_NUMBER"));
// }

// --- String functions ---
// Functions like UPPER, LOWER, CONCAT return unknown types.
//
// #[test]
// fn test_upper_lower() {
//     let q = query!("SELECT id, UPPER(name) as upper_name FROM users");
//     assert!(q.sql().contains("UPPER"));
// }

// --- UNION / INTERSECT / EXCEPT ---
// Set operations are not yet supported.
//
// #[test]
// fn test_union() {
//     let q = query!(
//         "SELECT id, name FROM users UNION SELECT id, name FROM categories"
//     );
//     assert!(q.sql().contains("UNION"));
// }

// --- RIGHT JOIN / FULL OUTER JOIN / CROSS JOIN ---
// These join types are not yet tested/implemented.
//
// #[test]
// fn test_right_join() {
//     let q = query!(
//         "SELECT p.id, p.bio, u.name FROM profiles p RIGHT JOIN users u ON u.id = p.user_id"
//     );
//     assert!(q.sql().contains("RIGHT JOIN"));
// }

// --- Decimal columns ---
// Columns with numeric(10,2) type now supported via tokio-postgres with-rust_decimal-1 feature.

#[test]
fn test_products_with_price() {
    let q = query!("SELECT id, name, price FROM products");
    assert!(q.sql().contains("price"));
}

#[test]
fn test_orders_total_amount() {
    let q = query!("SELECT id, user_id, total_amount FROM orders");
    assert!(q.sql().contains("total_amount"));
}

#[test]
fn test_order_items_unit_price() {
    let q = query!("SELECT id, order_id, quantity, unit_price FROM order_items");
    assert!(q.sql().contains("unit_price"));
}

#[test]
fn test_select_all_decimal_columns() {
    // Test selecting all decimal columns together
    let q = query!(
        r#"
        SELECT p.price, o.total_amount, oi.unit_price
        FROM products p
        INNER JOIN order_items oi ON oi.product_id = p.id
        INNER JOIN orders o ON o.id = oi.order_id
        "#
    );
    assert!(q.sql().contains("price"));
    assert!(q.sql().contains("total_amount"));
    assert!(q.sql().contains("unit_price"));
}

// --- SUM/AVG aggregates ---
// SUM and AVG always return Decimal, even on integer columns.
//
// #[test]
// fn test_sum_integer() {
//     let q = query!("SELECT SUM(quantity) FROM order_items");
//     assert!(q.sql().contains("SUM"));
// }

// --- Array operations ---
// ANY, array overlap operators not yet tested.
//
// #[test]
// fn test_array_any() {
//     let tag = "electronics".to_string();
//     let q = query!("SELECT id FROM products WHERE $1 = ANY(tags)", tag);
//     assert!(q.sql().contains("ANY"));
// }

// --- Date functions ---
// EXTRACT, DATE_TRUNC return unknown types.
//
// #[test]
// fn test_extract() {
//     let q = query!("SELECT id, EXTRACT(YEAR FROM created_at) as year FROM orders");
//     assert!(q.sql().contains("EXTRACT"));
// }

// ============================================================================
// NOTE: To verify compile-time errors work, uncomment one of these:
// ============================================================================
//
// Invalid table name - should fail:
// let q = query!("SELECT id FROM nonexistent_table");
//
// Invalid column name - should fail:
// let q = query!("SELECT nonexistent_column FROM users");
//
// Wrong number of parameters - should fail:
// let name = "Alice".to_string();
// let q = query!("SELECT id FROM users WHERE name = $1 AND email = $2", name);
