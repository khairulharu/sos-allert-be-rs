//! Business rules. Use cases own transactions and are generic over the
//! repository ports, so they run in tests with fakes (no database).
//!
//! Port order (mirrors the Go `internal/usecase` package):
//!
//! 1. `auth` - register/login, bcrypt, Redis session create/replace/verify. **done**
//! 2. `contact`, `address` - CRUD scoped to the session user.
//! 3. `location` - heartbeat (Redis GEO + `locations` upsert), nearby search.
//! 4. `trigger_sos` - idempotency, per-user rate limit, adaptive radius,
//!    grace-period outbox, fan-out, fail-open recipient lookup.
//! 5. `sos_lifecycle` - cancel/acknowledge/resolve/respond, breadcrumbs,
//!    media upload, history/inbox/summary with batched detail queries.
//! 6. `triage` - offline keyword classifier, then optional HTTP providers.
//! 7. `stt` - Whisper transcribe + silence detection sentinel.
//! 8. `notify` - FCM, APNs, Twilio SMS, Web Push (VAPID).

pub mod address;
pub mod auth;
pub mod contact;
pub mod emergency;
pub mod location;

#[cfg(test)]
mod tests;
