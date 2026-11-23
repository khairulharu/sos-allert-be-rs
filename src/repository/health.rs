//! Readiness probes used by `GET /healthz/ready`.
//!
//! Liveness must not touch the stores: the Go service hard-failed at startup,
//! which made a Redis blip a crash loop. Here the process boots, `/healthz`
//! answers, and readiness reports which dependency is down.

use redis::AsyncCommands;
use sqlx::{PgPool, Row};

/// `SELECT 1` against Postgres.
pub async fn database_ready(pool: &PgPool) -> Result<(), String> {
    let row = sqlx::query("SELECT 1 AS ok")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("postgres: {e}"))?;
    let ok: i32 = row.try_get("ok").map_err(|e| format!("postgres: {e}"))?;
    if ok == 1 {
        Ok(())
    } else {
        Err("postgres: unexpected result".into())
    }
}

/// `PING` against Redis.
pub async fn redis_ready(client: &redis::Client) -> Result<(), String> {
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("redis: {e}"))?;
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("redis: {e}"))?;
    if pong.eq_ignore_ascii_case("PONG") {
        Ok(())
    } else {
        Err(format!("redis: unexpected reply {pong}"))
    }
}

/// Keeps the `AsyncCommands` import used once stats helpers land.
#[allow(dead_code)]
async fn _touch_commands(conn: &mut redis::aio::MultiplexedConnection) -> redis::RedisResult<()> {
    let _: () = conn.set("__noop", 1).await?;
    Ok(())
}
