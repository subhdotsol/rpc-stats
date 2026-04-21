pub mod keys;
pub mod streams;

use anyhow::{Context, Result};
pub use deadpool_redis::redis;
use deadpool_redis::{Config, Pool, Runtime};
use tracing::warn;


/// Shared Redis connection pool — cheap to clone, backed by deadpool.
pub type RedisPool = Pool;

/// Create a connection pool from a `redis://` URL.
pub fn create_pool(url: &str) -> Result<RedisPool> {
    let cfg = Config::from_url(url);
    cfg.create_pool(Some(Runtime::Tokio1))
        .context("failed to create Redis connection pool")
}

// ── Low-level helpers ─────────────────────────────────────────────────────────

/// GET a raw string value. Returns `None` on cache miss or connection error.
/// Errors are logged as warnings rather than propagated so a Redis outage
/// never takes down the API — it degrades to DB reads instead.
pub async fn get(pool: &RedisPool, key: &str) -> Option<String> {
    let mut conn = pool.get().await
        .map_err(|e| warn!("Redis: pool.get failed for key={key}: {e}"))
        .ok()?;

    redis::cmd("GET")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| warn!("Redis: GET {key} failed: {e}"))
        .ok()
}

/// SET key value EX ttl_secs.
pub async fn set_ex(pool: &RedisPool, key: &str, value: &str, ttl_secs: u64) -> Result<()> {
    let mut conn = pool.get().await
        .context("Redis: pool.get failed")?;

    redis::cmd("SETEX")
        .arg(key)
        .arg(ttl_secs)
        .arg(value)
        .query_async(&mut conn)
        .await
        .with_context(|| format!("Redis: SETEX {key} failed"))
}


/// Serialize `value` to JSON and SET with TTL.
/// A serialization failure is returned as an error; a Redis write failure
/// is only logged (so callers never fail on a cache write).
pub async fn set_json_ex<T: serde::Serialize>(
    pool: &RedisPool,
    key: &str,
    value: &T,
    ttl_secs: u64,
) -> Result<()> {
    let json = serde_json::to_string(value)
        .with_context(|| format!("failed to serialize value for key={key}"))?;

    if let Err(e) = set_ex(pool, key, &json, ttl_secs).await {
        warn!("Redis write-back failed for key={key}: {e}");
    }
    Ok(())
}

/// Try to deserialize a cached JSON value for `key`.
/// Returns `None` on cache miss, deserialization error, or connection failure.
pub async fn get_json<T: serde::de::DeserializeOwned>(
    pool: &RedisPool,
    key: &str,
) -> Option<T> {
    let raw = get(pool, key).await?;
    serde_json::from_str(&raw)
        .map_err(|e| warn!("Redis: failed to deserialize key={key}: {e}"))
        .ok()
}
