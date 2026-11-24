//! Redis GEO store: `users:locations` (GEO set) and `users:last_seen` (hash).
//!
//! Longitude always comes before latitude — that is the Redis API order, and
//! getting it backwards silently puts users in the wrong hemisphere.

use std::collections::HashMap;

use async_trait::async_trait;
use redis::Value;

use crate::domain::LocationCache;
use crate::error::{AppError, AppResult};
use crate::repository::redis_conn::RedisHandle;
use crate::usecase::location::{GeoHit, LocationGeoStore};

pub const USER_LOCATION_KEY: &str = "users:locations";
pub const USER_LAST_SEEN_KEY: &str = "users:last_seen";

#[derive(Clone)]
pub struct RedisLocationCacheRepository {
    redis: RedisHandle,
}

impl RedisLocationCacheRepository {
    pub fn new(redis: RedisHandle) -> Self {
        Self { redis }
    }
}

#[async_trait]
impl LocationGeoStore for RedisLocationCacheRepository {
    async fn save(&self, location: &LocationCache, now_ms: i64) -> AppResult<()> {
        let mut conn = self.redis.conn().await?;

        redis::cmd("GEOADD")
            .arg(USER_LOCATION_KEY)
            .arg(location.longitude)
            .arg(location.latitude)
            .arg(&location.user_id)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis geoadd: {e}")))?;

        redis::cmd("HSET")
            .arg(USER_LAST_SEEN_KEY)
            .arg(&location.user_id)
            .arg(now_ms)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis hset last_seen: {e}")))?;

        Ok(())
    }

    async fn get(&self, user_id: &str) -> AppResult<Option<LocationCache>> {
        let mut conn = self.redis.conn().await?;
        let raw: Value = redis::cmd("GEOPOS")
            .arg(USER_LOCATION_KEY)
            .arg(user_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis geopos: {e}")))?;

        let items = match raw {
            Value::Array(items) => items,
            _ => Vec::new(),
        };
        let Some(Value::Array(point)) = items.into_iter().next() else {
            return Ok(None);
        };
        let (Some(longitude), Some(latitude)) = (
            point.first().and_then(value_to_f64),
            point.get(1).and_then(value_to_f64),
        ) else {
            return Ok(None);
        };

        Ok(Some(LocationCache::new(user_id, longitude, latitude)))
    }

    async fn last_seen(&self, user_id: &str) -> AppResult<Option<i64>> {
        let mut conn = self.redis.conn().await?;
        let raw: Value = redis::cmd("HGET")
            .arg(USER_LAST_SEEN_KEY)
            .arg(user_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis hget last_seen: {e}")))?;
        Ok(value_to_i64(&raw))
    }

    async fn last_seen_batch(&self, user_ids: &[String]) -> AppResult<HashMap<String, i64>> {
        let mut out = HashMap::with_capacity(user_ids.len());
        if user_ids.is_empty() {
            return Ok(out);
        }

        let mut conn = self.redis.conn().await?;
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(USER_LAST_SEEN_KEY);
        for id in user_ids {
            cmd.arg(id);
        }

        let raw: Value = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis hmget last_seen: {e}")))?;

        let values = match raw {
            Value::Array(values) => values,
            _ => return Ok(out),
        };
        for (index, value) in values.iter().enumerate() {
            if let (Some(id), Some(seen)) = (user_ids.get(index), value_to_i64(value)) {
                out.insert(id.clone(), seen);
            }
        }

        Ok(out)
    }

    async fn search_with_distance(
        &self,
        longitude: f64,
        latitude: f64,
        radius_meters: f64,
        count: usize,
    ) -> AppResult<Vec<GeoHit>> {
        let mut conn = self.redis.conn().await?;
        let raw: Value = redis::cmd("GEOSEARCH")
            .arg(USER_LOCATION_KEY)
            .arg("FROMLONLAT")
            .arg(longitude)
            .arg(latitude)
            .arg("BYRADIUS")
            .arg(radius_meters)
            .arg("m")
            .arg("ASC")
            .arg("COUNT")
            .arg(count)
            .arg("WITHDIST")
            .arg("WITHCOORD")
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis geosearch: {e}")))?;

        let mut hits = Vec::new();
        let Value::Array(rows) = raw else {
            return Ok(hits);
        };
        for row in rows {
            let Value::Array(fields) = row else { continue };
            let Some(user_id) = fields.first().and_then(value_to_string) else {
                continue;
            };
            let Some(distance) = fields.get(1).and_then(value_to_f64) else {
                continue;
            };
            let Some(Value::Array(point)) = fields.get(2) else {
                continue;
            };
            let (Some(lon), Some(lat)) = (
                point.first().and_then(value_to_f64),
                point.get(1).and_then(value_to_f64),
            ) else {
                continue;
            };
            hits.push(GeoHit {
                user_id,
                longitude: lon,
                latitude: lat,
                distance_meters: distance,
            });
        }

        Ok(hits)
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes.as_ref()).to_string()),
        Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Double(number) => Some(*number),
        Value::Int(number) => Some(*number as f64),
        Value::BulkString(bytes) => std::str::from_utf8(bytes.as_ref())
            .ok()
            .and_then(|text| text.parse().ok()),
        Value::SimpleString(text) => text.parse().ok(),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(number) => Some(*number),
        Value::BulkString(bytes) => std::str::from_utf8(bytes.as_ref())
            .ok()
            .and_then(|text| text.parse().ok()),
        Value::SimpleString(text) => text.parse().ok(),
        Value::Double(number) => Some(*number as i64),
        _ => None,
    }
}
