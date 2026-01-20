//! Integration tests that run against a real Postgres database.
//!
//! Requires a running Postgres with database `sql_check_test` and the schema loaded.
//! Run: `cat crates/sql-check-tests/schema.sql | sudo -u postgres psql -d sql_check_test`
//!
//! Note: Run with `--test-threads=1` to avoid race conditions between tests:
//! `cargo test -p sql-check-tests --test integration -- --test-threads=1`
//!
//! Known limitations (tests commented out or skipped):
//! - Decimal columns: Requires postgres-types with-rust_decimal-1 feature
//! - CTEs (WITH clause): Table resolution not implemented
//! - Subqueries in FROM: Same issue as CTEs

use sql_check_macros::query;
use tokio_postgres::NoTls;

/// Helper to connect to the test database.
async fn connect() -> tokio_postgres::Client {
    // Use DATABASE_URL env var if set (for CI), otherwise use local socket
    let connection_string = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=/var/run/postgresql dbname=sql_check_test".to_string());

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("Failed to connect to database");

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
}

// ============================================================================
// Basic SELECT tests
// ============================================================================

#[tokio::test]
async fn test_insert_and_select_user() {
    let client = connect().await;

    // Clean up any existing test data
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    // Insert a user
    let user_id = uuid::Uuid::new_v4();
    let name = "Alice".to_string();
    let email = format!("alice-{}@example.com", user_id);
    let metadata = serde_json::json!({"role": "admin"});

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[&user_id, &name, &email, &metadata],
        )
        .await
        .unwrap();

    // Query the user back using our macro
    let q = query!(
        "SELECT id, name, email, metadata FROM users WHERE id = $1",
        user_id
    );
    let user = q.fetch_one(&client).await.unwrap();

    assert_eq!(user.id, user_id);
    assert_eq!(user.name, "Alice");
    assert_eq!(user.email, email);
    assert_eq!(user.metadata["role"], "admin");
}

#[tokio::test]
async fn test_select_all_users() {
    let client = connect().await;

    // Insert test data with unique names
    let user1_id = uuid::Uuid::new_v4();
    let user2_id = uuid::Uuid::new_v4();
    let bob_name = format!("Bob-{}", &user1_id.to_string()[..8]);
    let carol_name = format!("Carol-{}", &user2_id.to_string()[..8]);

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user1_id,
                &bob_name,
                &format!("bob-{}@example.com", user1_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user2_id,
                &carol_name,
                &format!("carol-{}@example.com", user2_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    // Query all users - just verify we can fetch and the query works
    let q = query!("SELECT id, name FROM users");
    let users = q.fetch_all(&client).await.unwrap();

    // Should have at least our 2 users
    assert!(users.len() >= 2);

    // Verify our specific users are in the results
    let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&bob_name.as_str()));
    assert!(names.contains(&carol_name.as_str()));
}

// ============================================================================
// LEFT JOIN tests
// ============================================================================

#[tokio::test]
async fn test_left_join_with_nullable() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    // Insert user without profile
    let user_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user_id,
                &"Dave".to_string(),
                &format!("dave-{}@example.com", user_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    // Query with LEFT JOIN - bio should be None
    let q = query!(
        "SELECT u.id, u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id = $1",
        user_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, user_id);
    assert_eq!(result.name, "Dave");
    assert!(result.bio.is_none()); // No profile, so bio is NULL

    // Now add a profile
    let profile_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO profiles (id, user_id, bio) VALUES ($1, $2, $3)",
            &[&profile_id, &user_id, &"Software developer".to_string()],
        )
        .await
        .unwrap();

    // Query again - bio should now have a value
    let result = q.fetch_one(&client).await.unwrap();
    assert_eq!(result.bio, Some("Software developer".to_string()));
}

// ============================================================================
// fetch_optional tests
// ============================================================================

#[tokio::test]
async fn test_fetch_optional() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    let nonexistent_id = uuid::Uuid::new_v4();

    // Query for nonexistent user
    let q = query!("SELECT id, name FROM users WHERE id = $1", nonexistent_id);
    let result = q.fetch_optional(&client).await.unwrap();

    assert!(result.is_none());

    // Insert a user
    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &nonexistent_id,
                &"Eve".to_string(),
                &format!("eve-{}@example.com", nonexistent_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    // Query again - should find the user
    let result = q.fetch_optional(&client).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "Eve");
}

// ============================================================================
// INNER JOIN tests
// ============================================================================

#[tokio::test]
async fn test_inner_join() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    // Insert user with profile
    let user_id = uuid::Uuid::new_v4();
    let profile_id = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user_id,
                &"Frank".to_string(),
                &format!("frank-{}@example.com", user_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO profiles (id, user_id, bio) VALUES ($1, $2, $3)",
            &[&profile_id, &user_id, &"Developer bio".to_string()],
        )
        .await
        .unwrap();

    // INNER JOIN - bio is nullable in schema, still wrapped in Option
    let q = query!(
        "SELECT u.id, u.name, p.bio FROM users u INNER JOIN profiles p ON p.user_id = u.id WHERE u.id = $1",
        user_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, user_id);
    assert_eq!(result.name, "Frank");
    assert_eq!(result.bio, Some("Developer bio".to_string()));
}

// ============================================================================
// COUNT aggregate tests (returns i64, not Decimal)
// ============================================================================

#[tokio::test]
async fn test_count_aggregate() {
    let client = connect().await;

    // COUNT(*) test
    let q = query!("SELECT COUNT(*) as total FROM users");
    let result = q.fetch_one(&client).await.unwrap();

    // count returns i64
    assert!(result.total >= 0);
}

// ============================================================================
// GROUP BY tests
// ============================================================================

#[tokio::test]
async fn test_group_by() {
    let client = connect().await;

    // Clean up and add test data
    client.execute("DELETE FROM order_items", &[]).await.unwrap();
    client.execute("DELETE FROM orders", &[]).await.unwrap();
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    let user_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user_id,
                &"GroupTest".to_string(),
                &format!("group-{}@example.com", user_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    // Insert orders with different statuses
    let order1 = uuid::Uuid::new_v4();
    let order2 = uuid::Uuid::new_v4();
    let order3 = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO orders (id, user_id, status, total_amount) VALUES ($1, $2, 'pending', 0)",
            &[&order1, &user_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO orders (id, user_id, status, total_amount) VALUES ($1, $2, 'pending', 0)",
            &[&order2, &user_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO orders (id, user_id, status, total_amount) VALUES ($1, $2, 'completed', 0)",
            &[&order3, &user_id],
        )
        .await
        .unwrap();

    // GROUP BY with COUNT
    let q = query!("SELECT status, COUNT(*) as count FROM orders GROUP BY status");
    let results = q.fetch_all(&client).await.unwrap();

    // Each result has status and count
    for result in &results {
        assert!(!result.status.is_empty());
        assert!(result.count > 0);
    }

    // Should have 'pending' and 'completed' groups
    let pending = results.iter().find(|r| r.status == "pending");
    let completed = results.iter().find(|r| r.status == "completed");

    assert!(pending.is_some());
    assert!(completed.is_some());
    assert_eq!(pending.unwrap().count, 2);
    assert_eq!(completed.unwrap().count, 1);
}

// ============================================================================
// DISTINCT tests
// ============================================================================

#[tokio::test]
async fn test_distinct() {
    let client = connect().await;

    // DISTINCT on single column
    let q = query!("SELECT DISTINCT status FROM orders");
    let results = q.fetch_all(&client).await.unwrap();

    // Results should have unique statuses
    let statuses: Vec<&str> = results.iter().map(|r| r.status.as_str()).collect();
    let unique: std::collections::HashSet<_> = statuses.iter().collect();
    assert_eq!(statuses.len(), unique.len());
}

// ============================================================================
// ORDER BY / LIMIT / OFFSET tests
// ============================================================================

#[tokio::test]
async fn test_order_by() {
    let client = connect().await;

    // ORDER BY single column
    let q = query!("SELECT id, name FROM users ORDER BY name ASC");
    let results = q.fetch_all(&client).await.unwrap();

    // Verify ordering
    for i in 1..results.len() {
        assert!(results[i - 1].name <= results[i].name);
    }
}

#[tokio::test]
async fn test_limit() {
    let client = connect().await;

    // LIMIT clause
    let q = query!("SELECT id, name FROM users LIMIT 5");
    let results = q.fetch_all(&client).await.unwrap();

    assert!(results.len() <= 5);
}

#[tokio::test]
async fn test_limit_offset() {
    let client = connect().await;

    // First get total count
    let count_q = query!("SELECT COUNT(*) as total FROM users");
    let count_result = count_q.fetch_one(&client).await.unwrap();
    let total = count_result.total as usize;

    // LIMIT with OFFSET
    let q = query!("SELECT id, name FROM users ORDER BY name LIMIT 5 OFFSET 2");
    let results = q.fetch_all(&client).await.unwrap();

    // Should have min(5, total-2) results
    let expected_max = if total > 2 { std::cmp::min(5, total - 2) } else { 0 };
    assert!(results.len() <= expected_max);
}

// ============================================================================
// INSERT RETURNING tests
// ============================================================================

#[tokio::test]
async fn test_insert_returning() {
    let client = connect().await;

    let user_id = uuid::Uuid::new_v4();
    let name = "Jack".to_string();
    let email = format!("jack-{}@example.com", user_id);
    let metadata = serde_json::json!({"role": "user"});

    // INSERT with RETURNING
    let q = query!(
        "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4) RETURNING id, name, created_at",
        user_id,
        name,
        email,
        metadata
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, user_id);
    assert_eq!(result.name, "Jack");
    // created_at should be set by default
    let _ = result.created_at;
}

#[tokio::test]
async fn test_insert_returning_all() {
    let client = connect().await;

    // Clean up categories first
    client.execute("DELETE FROM products", &[]).await.unwrap();
    client.execute("DELETE FROM categories", &[]).await.unwrap();

    let cat_id = uuid::Uuid::new_v4();
    let name = "Books".to_string();

    // INSERT RETURNING *
    let q = query!(
        "INSERT INTO categories (id, name) VALUES ($1, $2) RETURNING *",
        cat_id,
        name
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, cat_id);
    assert_eq!(result.name, "Books");
    assert!(result.parent_id.is_none()); // nullable column
}

// ============================================================================
// Subquery in WHERE tests
// ============================================================================

#[tokio::test]
async fn test_subquery_in_where() {
    let client = connect().await;

    let status = "completed".to_string();

    // Subquery in WHERE clause
    let q = query!(
        r#"
        SELECT id, name
        FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE status = $1)
        "#,
        status
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

// ============================================================================
// NULL handling tests
// ============================================================================

#[tokio::test]
async fn test_nullable_column_handling() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM order_items", &[]).await.unwrap();
    client.execute("DELETE FROM orders", &[]).await.unwrap();
    client.execute("DELETE FROM products", &[]).await.unwrap();
    client.execute("DELETE FROM categories", &[]).await.unwrap();

    // Insert product without category (nullable FK) - avoid price column
    let product_id = uuid::Uuid::new_v4();
    client
        .execute(
            "INSERT INTO products (id, name, price, stock_quantity) VALUES ($1, $2, 50.00, $3)",
            &[
                &product_id,
                &"Standalone Product".to_string(),
                &5i32,
            ],
        )
        .await
        .unwrap();

    // Select non-Decimal columns
    let q = query!(
        "SELECT id, name, category_id, description FROM products WHERE id = $1",
        product_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, product_id);
    assert!(result.category_id.is_none()); // Nullable FK
    assert!(result.description.is_none()); // Nullable text
}

#[tokio::test]
async fn test_coalesce() {
    let client = connect().await;

    // COALESCE removes Option wrapper
    let q = query!(
        "SELECT id, COALESCE(description, 'No description') as description FROM products"
    );
    let results = q.fetch_all(&client).await.unwrap();

    for result in results {
        // description is now a String, not Option<String>
        // It should either be the actual description or 'No description'
        assert!(!result.description.is_empty());
    }
}

// ============================================================================
// Self-join tests
// ============================================================================

#[tokio::test]
async fn test_self_join() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM order_items", &[]).await.unwrap();
    client.execute("DELETE FROM orders", &[]).await.unwrap();
    client.execute("DELETE FROM products", &[]).await.unwrap();
    client.execute("DELETE FROM categories", &[]).await.unwrap();

    // Insert parent and child categories
    let parent_id = uuid::Uuid::new_v4();
    let child_id = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO categories (id, name) VALUES ($1, $2)",
            &[&parent_id, &"Electronics".to_string()],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO categories (id, name, parent_id) VALUES ($1, $2, $3)",
            &[&child_id, &"Laptops".to_string(), &parent_id],
        )
        .await
        .unwrap();

    // Self-join to get category with parent name
    let q = query!(
        r#"
        SELECT c.id, c.name, p.name as parent_name
        FROM categories c
        LEFT JOIN categories p ON c.parent_id = p.id
        WHERE c.id = $1
        "#,
        child_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    assert_eq!(result.id, child_id);
    assert_eq!(result.name, "Laptops");
    // parent_name should be "Electronics" (LEFT JOIN returned a match)
    // It's Option<String> because p is LEFT JOINed
    assert_eq!(result.parent_name, Some("Electronics".to_string()));
}

// ============================================================================
// Complex WHERE clause tests
// ============================================================================

#[tokio::test]
async fn test_where_and_or() {
    let client = connect().await;

    let stock_threshold = 5i32;
    let q = query!(
        r#"
        SELECT id, name
        FROM products
        WHERE (stock_quantity > $1 AND is_active = true) OR stock_quantity = 0
        "#,
        stock_threshold
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_where_like() {
    let client = connect().await;

    let pattern = "%Product%".to_string();
    let q = query!(
        "SELECT id, name FROM products WHERE name LIKE $1",
        pattern
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_where_in() {
    let client = connect().await;

    // IN with literal list
    let q = query!(
        "SELECT id, status FROM orders WHERE status IN ('pending', 'completed')"
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_where_between() {
    let client = connect().await;

    let low = 1i32;
    let high = 100i32;

    let q = query!(
        "SELECT id, name, stock_quantity FROM products WHERE stock_quantity BETWEEN $1 AND $2",
        low,
        high
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_where_is_null() {
    let client = connect().await;

    let q = query!("SELECT id, name FROM products WHERE category_id IS NULL");
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_where_is_not_null() {
    let client = connect().await;

    let q = query!("SELECT id, name FROM products WHERE description IS NOT NULL");
    let _ = q.fetch_all(&client).await.unwrap();
}

// ============================================================================
// EXISTS tests
// ============================================================================

#[tokio::test]
async fn test_exists() {
    let client = connect().await;

    let q = query!(
        r#"
        SELECT id, name
        FROM users u
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        "#
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

#[tokio::test]
async fn test_not_exists() {
    let client = connect().await;

    let q = query!(
        r#"
        SELECT id, name
        FROM users u
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        "#
    );
    let _ = q.fetch_all(&client).await.unwrap();
}

// ============================================================================
// CASE expression tests
// ============================================================================

#[tokio::test]
async fn test_case_expression() {
    let client = connect().await;

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
    let results = q.fetch_all(&client).await.unwrap();

    for result in results {
        // status_label should be one of the CASE values
        assert!(["Waiting", "Done", "Unknown"].contains(&result.status_label.as_str()));
    }
}

// ============================================================================
// CAST expression tests
// ============================================================================

#[tokio::test]
async fn test_cast_expression() {
    let client = connect().await;

    let q = query!(
        r#"
        SELECT id, CAST(stock_quantity AS text) as stock_text
        FROM products
        "#
    );
    let results = q.fetch_all(&client).await.unwrap();

    for result in results {
        // stock_text should be a numeric string
        assert!(result.stock_text.parse::<i32>().is_ok());
    }
}

// ============================================================================
// RIGHT JOIN tests
// ============================================================================

#[tokio::test]
async fn test_right_join() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    // Insert a profile first (we'll create a user for it)
    let user_id = uuid::Uuid::new_v4();
    let profile_id = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user_id,
                &"RightJoinUser".to_string(),
                &format!("rightjoin-{}@example.com", user_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO profiles (id, user_id, bio) VALUES ($1, $2, $3)",
            &[&profile_id, &user_id, &"Test bio".to_string()],
        )
        .await
        .unwrap();

    // RIGHT JOIN - users columns should be Option because they might not exist
    // for profiles without users (though FK prevents that in this schema)
    let q = query!(
        r#"
        SELECT u.id as user_id, u.name, p.id as profile_id, p.bio
        FROM users u
        RIGHT JOIN profiles p ON p.user_id = u.id
        WHERE p.id = $1
        "#,
        profile_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    // u.id is Option<Uuid> due to RIGHT JOIN
    assert_eq!(result.user_id, Some(user_id));
    // u.name is Option<String> due to RIGHT JOIN
    assert_eq!(result.name, Some("RightJoinUser".to_string()));
    // p.id is Uuid (not nullable from the join)
    assert_eq!(result.profile_id, profile_id);
    // p.bio is Option<String> (nullable in schema)
    assert_eq!(result.bio, Some("Test bio".to_string()));
}

// ============================================================================
// FULL OUTER JOIN tests
// ============================================================================

#[tokio::test]
async fn test_full_outer_join() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM profiles", &[]).await.unwrap();
    client.execute("DELETE FROM users", &[]).await.unwrap();

    // Insert a user with a profile
    let user_id = uuid::Uuid::new_v4();
    let profile_id = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO users (id, name, email, metadata) VALUES ($1, $2, $3, $4)",
            &[
                &user_id,
                &"FullOuterUser".to_string(),
                &format!("fullouter-{}@example.com", user_id),
                &serde_json::json!({}),
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO profiles (id, user_id, bio) VALUES ($1, $2, $3)",
            &[&profile_id, &user_id, &"FULL OUTER bio".to_string()],
        )
        .await
        .unwrap();

    // FULL OUTER JOIN - both sides' columns should be Option
    let q = query!(
        r#"
        SELECT u.id as user_id, u.name, p.id as profile_id, p.bio
        FROM users u
        FULL OUTER JOIN profiles p ON p.user_id = u.id
        WHERE u.id = $1
        "#,
        user_id
    );
    let result = q.fetch_one(&client).await.unwrap();

    // All columns are Option due to FULL OUTER JOIN
    assert_eq!(result.user_id, Some(user_id));
    assert_eq!(result.name, Some("FullOuterUser".to_string()));
    assert_eq!(result.profile_id, Some(profile_id));
    assert_eq!(result.bio, Some("FULL OUTER bio".to_string()));
}

// ============================================================================
// CROSS JOIN tests
// ============================================================================

#[tokio::test]
async fn test_cross_join() {
    let client = connect().await;

    // Clean up
    client.execute("DELETE FROM order_items", &[]).await.unwrap();
    client.execute("DELETE FROM orders", &[]).await.unwrap();
    client.execute("DELETE FROM products", &[]).await.unwrap();
    client.execute("DELETE FROM categories", &[]).await.unwrap();

    // Insert some categories
    let cat1 = uuid::Uuid::new_v4();
    let cat2 = uuid::Uuid::new_v4();

    client
        .execute(
            "INSERT INTO categories (id, name) VALUES ($1, $2)",
            &[&cat1, &"Category A".to_string()],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO categories (id, name) VALUES ($1, $2)",
            &[&cat2, &"Category B".to_string()],
        )
        .await
        .unwrap();

    // CROSS JOIN - cartesian product, neither side is nullable from the join
    let q = query!(
        r#"
        SELECT c1.id as id1, c1.name as name1, c2.id as id2, c2.name as name2
        FROM categories c1
        CROSS JOIN categories c2
        ORDER BY c1.name, c2.name
        "#
    );
    let results = q.fetch_all(&client).await.unwrap();

    // Should have 4 rows (2 x 2 cartesian product)
    assert_eq!(results.len(), 4);

    // All columns are NOT Option (CROSS JOIN doesn't make things nullable)
    // Types are directly accessible without unwrapping
    for result in &results {
        let _ = result.id1; // Uuid, not Option<Uuid>
        let _ = result.name1; // String, not Option<String>
        let _ = result.id2; // Uuid
        let _ = result.name2; // String
    }
}

// ============================================================================
// Tests requiring features not yet available (documented)
// ============================================================================

// --- Decimal columns ---
// Tests using price, total_amount, unit_price columns are not included
// because rust_decimal::Decimal doesn't implement ToSql/FromSql without
// the postgres-types with-rust_decimal-1 feature.

// --- CTE (WITH clause) ---
// CTEs like "WITH active_users AS (...) SELECT ... FROM active_users"
// fail because the table name from the WITH clause is not recognized.

// --- Subqueries in FROM ---
// "SELECT ... FROM (SELECT ...) sub" fails similarly to CTEs.

// --- SUM/AVG aggregates ---
// SUM and AVG always return Decimal, even on integer columns.

// --- Window functions ---
// ROW_NUMBER(), RANK(), LAG(), LEAD() return unknown types.

// --- String/Date functions ---
// UPPER(), LOWER(), EXTRACT(), DATE_TRUNC() return unknown types.

// --- UPDATE/DELETE statements ---
// Only SELECT and INSERT are currently supported.
