//! Sample SQLx app - this is our migration target.
//!
//! This app demonstrates the query patterns we need sql-check to support:
//! - Basic SELECT with WHERE
//! - INSERT with RETURNING
//! - LEFT JOIN (nullability testing)
//! - JSONB columns
//! - Aggregates (COUNT)

use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use uuid::Uuid;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, FromRow)]
struct User {
    id: Uuid,
    name: String,
    email: String,
    metadata: serde_json::Value, // JSONB column
}

#[derive(Debug, FromRow)]
struct UserWithProfile {
    user_id: Uuid,
    user_name: String,
    // Profile fields are nullable because of LEFT JOIN
    profile_bio: Option<String>,
    profile_avatar_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserMetadata {
    signup_source: String,
    preferences: serde_json::Value,
}

// ============================================================================
// Queries - These are what we want sql-check to validate
// ============================================================================

/// Query 1: Basic SELECT with WHERE
async fn get_user_by_id(pool: &PgPool, user_id: Uuid) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        SELECT id, name, email, metadata
        FROM users
        WHERE id = $1
        "#,
        user_id
    )
    .fetch_optional(pool)
    .await
}

/// Query 2: INSERT with RETURNING
async fn create_user(
    pool: &PgPool,
    name: &str,
    email: &str,
    metadata: serde_json::Value,
) -> Result<User, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        INSERT INTO users (id, name, email, metadata)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, email, metadata
        "#,
        Uuid::new_v4(),
        name,
        email,
        metadata
    )
    .fetch_one(pool)
    .await
}

/// Query 3: LEFT JOIN - profile fields become nullable
async fn get_user_with_profile(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<UserWithProfile>, sqlx::Error> {
    sqlx::query_as!(
        UserWithProfile,
        r#"
        SELECT
            u.id as user_id,
            u.name as user_name,
            p.bio as profile_bio,
            p.avatar_url as profile_avatar_url
        FROM users u
        LEFT JOIN profiles p ON p.user_id = u.id
        WHERE u.id = $1
        "#,
        user_id
    )
    .fetch_optional(pool)
    .await
}

/// Query 4: Aggregate - COUNT returns i64
async fn count_users(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let result = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM users
        "#
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Query 5: SELECT multiple rows
async fn list_users(pool: &PgPool, limit: i64) -> Result<Vec<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        SELECT id, name, email, metadata
        FROM users
        ORDER BY name
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres:///sql_check_sample".into());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    println!("Connected to database!");

    // Demo the queries
    let user_count = count_users(&pool).await?;
    println!("Total users: {}", user_count);

    // Create a test user
    let metadata = serde_json::json!({
        "signup_source": "cli",
        "preferences": {
            "theme": "dark",
            "notifications": true
        }
    });

    let new_user = create_user(&pool, "Test User", "test@example.com", metadata).await?;
    println!("Created user: {:?}", new_user);

    // Fetch the user back
    if let Some(user) = get_user_by_id(&pool, new_user.id).await? {
        println!("Fetched user: {:?}", user);
    }

    // Get user with profile (LEFT JOIN)
    if let Some(user_with_profile) = get_user_with_profile(&pool, new_user.id).await? {
        println!("User with profile: {:?}", user_with_profile);
    }

    // List all users
    let users = list_users(&pool, 10).await?;
    println!("All users: {:?}", users);

    Ok(())
}
