use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use tracing::info;

/// Fetch all TeamSpeak users that are currently online from the DB.
/// Returns Vec of (ts_unique_id, current_client_id).
pub async fn fetch_online_users(pool: &PgPool) -> Result<Vec<(String, u16)>> {
    info!("Fetching online TS3 users from database...");

    let rows = sqlx::query(
        r#"
        SELECT "TsUniqueId", "CurrentClientId"
        FROM "TeamSpeakUsers"
        WHERE "IsOnline" = true
          AND "CurrentClientId" IS NOT NULL
        "#,
    )
    .fetch_all(pool)
    .await
    .context("Failed to fetch online TeamSpeak users from DB")?;

    let mut users = Vec::with_capacity(rows.len());
    for row in rows {
        let uid: String = row.try_get("TsUniqueId").unwrap_or_default();
        let client_id_i32: i32 = row.try_get("CurrentClientId").unwrap_or(0);

        if uid.is_empty() || client_id_i32 <= 0 {
            continue;
        }

        let client_id = client_id_i32 as u16;
        users.push((uid, client_id));
    }

    info!("Loaded {} online TS3 user(s) from database", users.len());
    Ok(users)
}

pub async fn connect(database_url: &str) -> Result<PgPool> {
    let pool = PgPool::connect(database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    info!("Connected to PostgreSQL database");
    Ok(pool)
}
