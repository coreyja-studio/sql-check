//! Integration tests for sql-check-macros.
//!
//! These tests verify the query! macro generates valid, compilable code.
//!
//! Test categories:
//! - Working tests: Can be compiled and run now
//! - Ignored tests (compile): Marked #[ignore], can compile but not run
//! - Ignored tests (no compile): Commented out because they fail at compile time
//!
//! Supported statements:
//! - SELECT (with JOINs, aggregates, subqueries, UNION/INTERSECT/EXCEPT, etc.)
//! - INSERT (with RETURNING)
//! - UPDATE (with RETURNING)
//! - DELETE (with RETURNING)
//!
//! Known limitations (tests commented out):
//! - (none currently)

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

// SUM/AVG on any column return Decimal (Option<rust_decimal::Decimal>).
// This works now that rust_decimal is enabled with db-tokio-postgres feature.

#[test]
fn test_sum_aggregate() {
    let q = query!("SELECT SUM(quantity) as total FROM order_items");
    assert!(q.sql().contains("SUM"));
}

#[test]
fn test_avg_aggregate() {
    let q = query!("SELECT AVG(quantity) as avg_qty FROM order_items");
    assert!(q.sql().contains("AVG"));
}

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
    let q = query!(
        "SELECT MIN(stock_quantity) as min_stock, MAX(stock_quantity) as max_stock FROM products"
    );
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
    let q =
        query!("SELECT id, status, created_at FROM orders ORDER BY status ASC, created_at DESC");
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
    let q =
        query!("SELECT id, COALESCE(description, 'No description') as description FROM products");
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
    let q = query!("SELECT id, name FROM products WHERE name LIKE $1", pattern);
    assert!(q.sql().contains("LIKE"));
}

#[test]
fn test_where_in() {
    let q = query!("SELECT id, status FROM orders WHERE status IN ('pending', 'completed')");
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
// ============================================================================
// CTE (Common Table Expression) tests
// ============================================================================

#[test]
fn test_cte_simple() {
    let q = query!(
        r#"
        WITH active_users AS (
            SELECT id, name FROM users
        )
        SELECT id, name FROM active_users
        "#
    );
    assert!(q.sql().contains("WITH"));
}

#[test]
fn test_cte_with_alias() {
    let q = query!(
        r#"
        WITH user_summary AS (
            SELECT id, name, email FROM users
        )
        SELECT us.id, us.name FROM user_summary us
        "#
    );
    assert!(q.sql().contains("WITH"));
}

#[test]
fn test_cte_multiple() {
    let q = query!(
        r#"
        WITH
            user_data AS (SELECT id, name FROM users),
            profile_data AS (SELECT user_id, bio FROM profiles)
        SELECT u.id, u.name, p.bio
        FROM user_data u
        LEFT JOIN profile_data p ON p.user_id = u.id
        "#
    );
    assert!(q.sql().contains("WITH"));
}

// ============================================================================
// UPDATE statement tests
// ============================================================================

#[test]
fn test_update_simple() {
    let name = "Updated".to_string();
    let user_id = uuid::Uuid::new_v4();
    let q = query!("UPDATE users SET name = $1 WHERE id = $2", name, user_id);
    assert!(q.sql().contains("UPDATE"));
}

#[test]
fn test_update_multiple_columns() {
    let name = "Updated".to_string();
    let email = "new@example.com".to_string();
    let user_id = uuid::Uuid::new_v4();
    let q = query!(
        "UPDATE users SET name = $1, email = $2 WHERE id = $3",
        name,
        email,
        user_id
    );
    assert!(q.sql().contains("SET"));
}

#[test]
fn test_update_returning() {
    let name = "Updated".to_string();
    let user_id = uuid::Uuid::new_v4();
    let q = query!(
        "UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name",
        name,
        user_id
    );
    assert!(q.sql().contains("RETURNING"));
}

#[test]
fn test_update_returning_all() {
    let name = "Updated".to_string();
    let user_id = uuid::Uuid::new_v4();
    let q = query!(
        "UPDATE users SET name = $1 WHERE id = $2 RETURNING *",
        name,
        user_id
    );
    assert!(q.sql().contains("RETURNING *"));
}

// ============================================================================
// DELETE statement tests
// ============================================================================

#[test]
fn test_delete_simple() {
    let user_id = uuid::Uuid::new_v4();
    let q = query!("DELETE FROM users WHERE id = $1", user_id);
    assert!(q.sql().contains("DELETE"));
}

#[test]
fn test_delete_returning() {
    let user_id = uuid::Uuid::new_v4();
    let q = query!(
        "DELETE FROM users WHERE id = $1 RETURNING id, name",
        user_id
    );
    assert!(q.sql().contains("RETURNING"));
}

#[test]
fn test_delete_returning_all() {
    let user_id = uuid::Uuid::new_v4();
    let q = query!("DELETE FROM users WHERE id = $1 RETURNING *", user_id);
    assert!(q.sql().contains("RETURNING *"));
}

// --- Window functions ---
// Window functions are now supported!

#[test]
fn test_row_number() {
    let q =
        query!("SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM users");
    assert!(q.sql().contains("ROW_NUMBER"));
}

#[test]
fn test_rank() {
    let q = query!("SELECT id, name, RANK() OVER (ORDER BY created_at) as rank FROM users");
    assert!(q.sql().contains("RANK"));
}

#[test]
fn test_dense_rank() {
    let q =
        query!("SELECT id, name, DENSE_RANK() OVER (ORDER BY created_at) as dense_rank FROM users");
    assert!(q.sql().contains("DENSE_RANK"));
}

#[test]
fn test_lag() {
    let q = query!("SELECT id, name, LAG(name) OVER (ORDER BY created_at) as prev_name FROM users");
    assert!(q.sql().contains("LAG"));
}

#[test]
fn test_lead() {
    let q =
        query!("SELECT id, name, LEAD(name) OVER (ORDER BY created_at) as next_name FROM users");
    assert!(q.sql().contains("LEAD"));
}

#[test]
fn test_first_value() {
    let q = query!(
        "SELECT id, name, FIRST_VALUE(name) OVER (ORDER BY created_at) as first_name FROM users"
    );
    assert!(q.sql().contains("FIRST_VALUE"));
}

#[test]
fn test_last_value() {
    let q = query!(
        "SELECT id, name, LAST_VALUE(name) OVER (ORDER BY created_at) as last_name FROM users"
    );
    assert!(q.sql().contains("LAST_VALUE"));
}

#[test]
fn test_window_with_partition() {
    let q = query!(
        r#"
        SELECT id, name,
               ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) as row_num
        FROM users
        "#
    );
    assert!(q.sql().contains("PARTITION BY"));
}

// --- String functions ---
// String functions now supported!

#[test]
fn test_upper_lower() {
    let q = query!("SELECT id, UPPER(name) as upper_name, LOWER(name) as lower_name FROM users");
    assert!(q.sql().contains("UPPER"));
    assert!(q.sql().contains("LOWER"));
}

#[test]
fn test_concat() {
    let q = query!("SELECT id, CONCAT(name, ' - ', email) as full_info FROM users");
    assert!(q.sql().contains("CONCAT"));
}

#[test]
fn test_length() {
    let q = query!("SELECT id, LENGTH(name) as name_length FROM users");
    assert!(q.sql().contains("LENGTH"));
}

#[test]
fn test_substring() {
    let q = query!("SELECT id, SUBSTRING(name, 1, 3) as short_name FROM users");
    assert!(q.sql().contains("SUBSTRING"));
}

#[test]
fn test_trim_functions() {
    let q = query!("SELECT id, TRIM(name) as trimmed_name, LTRIM(name) as ltrimmed, RTRIM(name) as rtrimmed FROM users");
    assert!(q.sql().contains("TRIM"));
    assert!(q.sql().contains("LTRIM"));
    assert!(q.sql().contains("RTRIM"));
}

#[test]
fn test_replace() {
    let q = query!("SELECT id, REPLACE(name, 'old', 'new') as replaced_name FROM users");
    assert!(q.sql().contains("REPLACE"));
}

#[test]
fn test_strpos() {
    // STRPOS is the function-call equivalent of POSITION in PostgreSQL
    let q = query!("SELECT id, STRPOS(email, '@') as at_position FROM users");
    assert!(q.sql().contains("STRPOS"));
}

#[test]
fn test_split_part() {
    let q = query!("SELECT id, SPLIT_PART(email, '@', 2) as domain FROM users");
    assert!(q.sql().contains("SPLIT_PART"));
}

#[test]
fn test_lpad_rpad() {
    let q = query!("SELECT id, LPAD(name, 10, ' ') as padded_name FROM users");
    assert!(q.sql().contains("LPAD"));
}

// --- UNION / INTERSECT / EXCEPT ---
// Set operations are now supported!

#[test]
fn test_union() {
    let q = query!("SELECT id, name FROM users UNION SELECT id, name FROM categories");
    assert!(q.sql().contains("UNION"));
}

#[test]
fn test_union_all() {
    let q = query!("SELECT id, name FROM users UNION ALL SELECT id, name FROM categories");
    assert!(q.sql().contains("UNION ALL"));
}

#[test]
fn test_intersect() {
    let q = query!("SELECT id FROM users INTERSECT SELECT user_id FROM profiles");
    assert!(q.sql().contains("INTERSECT"));
}

#[test]
fn test_except() {
    let q = query!("SELECT id FROM users EXCEPT SELECT user_id FROM profiles");
    assert!(q.sql().contains("EXCEPT"));
}

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
// SUM and AVG always return Decimal (Option<rust_decimal::Decimal>), even on integer columns.

#[test]
fn test_sum_integer() {
    let q = query!("SELECT SUM(quantity) FROM order_items");
    assert!(q.sql().contains("SUM"));
}

#[test]
fn test_avg_decimal() {
    // AVG on a Decimal column also returns Option<Decimal>
    let q = query!("SELECT AVG(price) as avg_price FROM products");
    assert!(q.sql().contains("AVG"));
}

#[test]
fn test_sum_with_group_by() {
    // SUM with GROUP BY returns Option<Decimal>
    let q =
        query!("SELECT order_id, SUM(quantity) as total_qty FROM order_items GROUP BY order_id");
    assert!(q.sql().contains("SUM"));
    assert!(q.sql().contains("GROUP BY"));
}

// --- Array operations ---

#[test]
fn test_array_any() {
    let tag = "electronics".to_string();
    let q = query!("SELECT id FROM products WHERE $1 = ANY(tags)", tag);
    assert!(q.sql().contains("ANY"));
}

#[test]
fn test_array_contains() {
    // Test the @> (contains) operator
    let q = query!("SELECT id FROM products WHERE tags @> ARRAY['electronics']");
    assert!(q.sql().contains("@>"));
}

#[test]
fn test_array_overlap() {
    // Test the && (overlap) operator
    let q = query!("SELECT id FROM products WHERE tags && ARRAY['electronics', 'gadgets']");
    assert!(q.sql().contains("&&"));
}

#[test]
fn test_array_is_contained_by() {
    // Test the <@ (is contained by) operator
    let q = query!("SELECT id FROM products WHERE tags <@ ARRAY['electronics', 'gadgets', 'tech']");
    assert!(q.sql().contains("<@"));
}

#[test]
fn test_select_array_column() {
    // Selecting an array column should work and return Vec<String>
    let q = query!("SELECT id, tags FROM products");
    assert!(q.sql().contains("tags"));
}

#[test]
fn test_array_literal_in_select() {
    // Test selecting an array literal
    let q = query!("SELECT ARRAY['a', 'b', 'c'] as arr FROM products");
    assert!(q.sql().contains("ARRAY"));
}

// --- Date functions ---

#[test]
fn test_extract() {
    let q = query!("SELECT id, EXTRACT(YEAR FROM created_at) as year FROM orders");
    assert!(q.sql().contains("EXTRACT"));
}

#[test]
fn test_date_trunc() {
    let q = query!("SELECT id, DATE_TRUNC('day', created_at) as day FROM orders");
    assert!(q.sql().contains("DATE_TRUNC"));
}

#[test]
fn test_date_part() {
    let q = query!("SELECT id, DATE_PART('hour', created_at) as hour FROM orders");
    assert!(q.sql().contains("DATE_PART"));
}

// AGE() returns interval type which doesn't have FromSql implementation in postgres-types.
// The type inference works (returns Duration) but runtime execution requires a custom type.
// #[test]
// fn test_age() {
//     let q = query!("SELECT id, AGE(updated_at, created_at) as duration FROM orders");
//     assert!(q.sql().contains("AGE"));
// }

#[test]
fn test_to_char() {
    let q = query!("SELECT id, TO_CHAR(created_at, 'YYYY-MM-DD') as formatted FROM orders");
    assert!(q.sql().contains("TO_CHAR"));
}

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
