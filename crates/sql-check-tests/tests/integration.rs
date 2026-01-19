//! Integration tests that run against a real Postgres database.
//!
//! Requires a running Postgres with database `sql_check_test` and the schema loaded.
//! Run: `cat examples/sample-app/schema.sql | sudo -u postgres psql -d sql_check_test`
//!
//! Note: Run with `--test-threads=1` to avoid race conditions between tests:
//! `cargo test -p sql-check-tests --test integration -- --test-threads=1`

use sql_check_macros::query;
use tokio_postgres::NoTls;

/// Helper to connect to the test database.
async fn connect() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        "host=/var/run/postgresql dbname=sql_check_test",
        NoTls,
    )
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

#[tokio::test]
async fn test_insert_and_select_user() {
    let client = connect().await;

    // Clean up any existing test data
    client
        .execute("DELETE FROM profiles", &[])
        .await
        .unwrap();
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
    let q = query!("SELECT id, name, email, metadata FROM users WHERE id = $1", user_id);
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

#[tokio::test]
async fn test_left_join_with_nullable() {
    let client = connect().await;

    // Clean up
    client
        .execute("DELETE FROM profiles", &[])
        .await
        .unwrap();
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
            &[&profile_id, &user_id, &"Developer".to_string()],
        )
        .await
        .unwrap();

    // Query again - bio should now have a value
    let result = q.fetch_one(&client).await.unwrap();
    assert_eq!(result.bio, Some("Developer".to_string()));
}

#[tokio::test]
async fn test_fetch_optional() {
    let client = connect().await;

    // Clean up
    client
        .execute("DELETE FROM profiles", &[])
        .await
        .unwrap();
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
