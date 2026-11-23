//! Storage layer: Postgres via sqlx, Redis for sessions/geo/rate limits.
//!
//! Repository *traits* (ports) land in later phases, one per aggregate, so the
//! use cases can be unit-tested with fakes and no live infrastructure:
//!
//! | trait                | backing store            | phase |
//! |----------------------|--------------------------|-------|
//! | `UserRepository`     | `users`                  | 1     |
//! | `SessionRepository`  | Redis `auth:*`           | 1     |
//! | `ContactRepository`  | `contacts`               | 2     |
//! | `AddressRepository`  | `addresses`              | 2     |
//! | `LocationRepository` | Redis GEO + `locations`  | 2     |
//! | `AlertStore`         | `alerts` + satellites    | 3     |
//! | `IdempotencyStore`   | Redis `idem:*`           | 3     |
//! | `RateLimiter`        | Redis `ratelimit:*`      | 3     |
//! | `PushSubscriptionRepository` | `push_subscriptions` | 4 |
//!
//! Generic CRUD in the Go service (`repository.Repository[T]`) becomes small
//! per-aggregate impls here; sqlx has no GORM-style enum of dialect quirks to
//! inherit, and explicit SQL is what keeps the radius queries off a seq scan.

pub mod health;
