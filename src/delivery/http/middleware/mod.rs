//! Middleware.
//!
//! Phase 1 ports `AuthMiddleware` here: raw `Authorization` token (no `Bearer`
//! prefix, unlike `/metrics`), Redis session lookup with the in-process
//! `SessionCache`, `401` for unknown tokens and `503` when the store errors so
//! clients keep their token during an outage.

pub mod metrics_token;
