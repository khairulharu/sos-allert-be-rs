# Port plan: Go -> Rust

Source: `khairulharu/sos-allert-be` @ `7b8ff5a`.
Scope agreed with the user: **full parity**, database moved to **Postgres**,
clean architecture kept, tests kept out of logic files.

## What is being ported

| item | count |
|---|---|
| Go production lines | 12,724 |
| Go test lines | 12,148 |
| HTTP endpoints (incl. `/metrics`, `/ws/sos/:alertId/track`) | 42 |
| Postgres tables | 11 |
| Kafka topics | `users`, `contacts`, `addresses`, `sos.dispatched` + `sos.dispatched.dlq` |
| Go packages to port | config, entity, model+converter, repository, usecase, delivery/http, delivery/messaging, gateway, storage, tracking, metrics |

## Decisions

- **Postgres, no PostGIS.** Radius search stays in Redis GEO
  (`users:locations`), exactly like the Go service. Postgres stores the
  historical `locations` row and the alert trail. Adding PostGIS later is a
  separate, additive migration, not a prerequisite.
- **Timestamps stay millisecond `bigint`.** No `timestamptz` conversion, so data
  can be moved from MySQL without a timezone step and the wire format does not
  change.
- **bcrypt stays bcrypt** (`bcrypt` crate), not argon2, so existing hashes keep
  verifying.
- **Kafka client: `rskafka`** (pure Rust). No librdkafka, no cmake, no C
  toolchain surprises in CI. If throughput or consumer-group behaviour proves
  insufficient at phase 5, that is a measured decision with numbers, not a
  default.
- **Migrations keep the Go version numbers and filenames**, so the timeline can
  be compared one-to-one. `sqlx::migrate` tracks them in `_sqlx_migrations`
  (Go used `schema_migrations`); a fresh Postgres database starts clean.
- **Migrations never run on boot.** `web migrate` applies them explicitly.
- **Tests**: unit tests in `src/<module>/tests.rs` (logic files stay test-free),
  cross-module/HTTP tests in `tests/`. Anything needing a live Postgres/Redis is
  env-gated and skipped by default.

## Schema translation rules applied

1. `engine = InnoDB` dropped; `AUTO_INCREMENT` -> `bigserial`; `double` ->
   `double precision`; `float` -> `real`.
2. Inline `INDEX`/`UNIQUE KEY` inside `CREATE TABLE` -> separate
   `CREATE INDEX` / `CREATE UNIQUE INDEX`.
3. MySQL creates an index for every foreign key; Postgres does not. Explicit
   indexes were added to keep the same read paths off a seq scan:
   `idx_contacts_user_id`, `idx_addresses_contact_id`, `idx_locations_user_id`,
   `idx_emergency_contacts_user_id`.
4. Multitable `DELETE ... JOIN` (duplicate responder cleanup) ->
   `DELETE ... USING`.
5. `ADD COLUMN ... AFTER x` dropped (column order is not part of the contract).
6. Constraint names kept (`fk_*`, `uq_*`) so error messages stay recognisable.

## Phases

Each phase ends with `cargo fmt`/`clippy -D warnings`/`cargo test` green plus
the phase's own evidence (curl transcript or test names). No phase is "done"
because it compiles.

### 0. Skeleton (done)

- crate, two binaries (`web`, `worker`), module tree, config loader with the Go
  key set and defaults, error type with the HTTP mapping, domain entities for
  all 11 tables, Postgres migrations, health/readiness, metrics guard,
  CI workflow.
- Evidence: `cargo test --all-targets` -> 7 geo tests + 5 router smoke tests.

### 1. Auth and users (done)

- `POST /api/users`, `POST /api/users/_login`, `GET/PATCH /api/users/_current`,
  `DELETE /api/users`.
- Redis session repo with the Lua single-session replace, `SessionRepository`
  parity, `auth:session_invalidate` pub/sub, in-process session cache bounded by
  `auth.session_cache_ttl_seconds`.
- Auth middleware (raw token, 401 vs 503), per-IP auth rate limit + lockout
  (fails open), bcrypt verify, safe PIN set/check.
- Done. Unit tests use trait fakes (the `redis` crate has no in-process
  server); the Lua session script and the pub/sub invalidation are covered
  against a live Redis. Verified with a live curl transcript:
  register/login/current/logout, plus a second login invalidating the first
  token (401 on the old one).

### 2. Contacts, addresses, location (done)

- CRUD for contacts and addresses scoped to the session user.
- `PUT /api/location` (Redis GEO + `users:last_seen` + `locations` upsert),
  `GET /api/location`, `GET /api/location/nearby` (GEOSEARCH with count,
  batched `HMGET` freshness, coordinate masking).
- Done. `nearby` proves one `HMGET` per candidate set (counter in the fake),
  drops self/stale/unknown users, masks to 3 decimals and snaps distance to a
  100 m bucket. The Postgres upsert is verified against the live database
  (one row per user after two heartbeats).
- Deliberate deviations from Go, both documented in code:
  * contact search uses `ILIKE` (Postgres `LIKE` is case-sensitive, MySQL's
    default collation was not);
  * contact paging orders by `created_at DESC, id` — the Go service left the
    order to the storage engine, which makes page boundaries unstable.
- One Go behaviour carried over on purpose: `email` has no `omitempty` in the
  Go validator, so an empty email on contact create/update is `400`. Confirmed
  by running the exact tag through go-playground/validator v10.27.0.

### 2b. Emergency contacts (done)

- `GET/POST /api/emergency-contacts`: E.164 only (8-15 digits, non-zero first
  digit), separators normalized, 10 per user, one number per user, `422` on
  violation. Legacy non-E.164 rows stay readable and block duplicates in
  normalized form.

### 3. SOS core

- `trigger_sos`: idempotency before the limiter, per-user hourly limit,
  adaptive radius (1/2/5 km, cap 50), persist then fail-open fan-out,
  `201` with `recipients_notified`.
- Lifecycle: cancel (bcrypt PIN), acknowledge, resolve, respond
  (`ON CONFLICT DO NOTHING` -> 409), breadcrumbs (24 h past / 60 s future
  clamp), history/inbox/summary with the batched `buildAlertDetails`.
- Durable grace-period outbox loop + `markDispatched` guard; housekeeping loop
  (expire 24 h, purge breadcrumb trails 30 days).
- `WS /ws/sos/:alertId/track` with in-band auth frame, connection budgets
  (global/per-IP/per-user), 5 s write deadline, 25 s ping / 60 s pong.
- Metrics: active-SOS gauge, fan-out failures.
- Acceptance: ported Go tests for outbox (due scan, exactly-once mark, cancel
  skip, restart recovery, fail-open), guard/idempotency, and a live ws client
  transcript.

### 4. Media and notifications

- `POST /api/sos/:alertId/media` (multipart, ≤25 MB, local storage backend),
  media list/get, escalation when triage widens the radius.
- Web Push (VAPID) subscribe/unsubscribe + allowed-host SSRF guard; FCM, APNs,
  Twilio SMS; `NotifyRecipients` push-only path.
- Acceptance: notifier tests with a fake HTTP transport (no live push service),
  push subscription repository tests env-gated.

### 5. Kafka worker

- Consumers for `users`/`contacts`/`addresses` (idempotent upserts) and
  `sos.dispatched` (legacy group id), retry-same-message, DLQ publish with
  `dlq.source.*` headers, offset only after success, failed DLQ leaves the
  offset uncommitted.
- Acceptance: consumer tests against a fake broker abstraction; one integration
  run against a real broker env-gated.

### 6. AI triage and STT

- Offline keyword classifier first, then jev > openai > ollama chain, fail-open,
  severity clamping, `severity`/`summary` persistence (0 is a real score).
- Whisper transcription with the silence sentinel (`no_speech_prob`,
  compression ratio, hallucination phrases), `transcription_status = empty`.
- `POST /api/sos/classify` (rate limited, fails closed with 503) and
  `pendingClassify` single-use Redis handoff.
- Acceptance: ported classifier/chain tests with a stub HTTP client; no live
  provider calls in CI.

### 7. Parity and cutover notes

- Replay `api-spec.json` (the Go repo's OpenAPI) against the Rust router:
  every documented path/method must be registered, and response shapes compared
  field by field.
- Reuse `perf/k6/k6-sos-api.js` for a before/after read-mix run.
- Write `docs/CUTOVER.md`: MySQL -> Postgres data move, session invalidation,
  and the fact that deploying the session change logs everyone out.

## Test infrastructure used locally

No Docker on this machine (socket is root-only, no sudo), so the services were
installed user-space from Debian packages with `dpkg-deb -x`:

```bash
# Postgres 17.12 on 127.0.0.1:55432 (trust auth, data in ~/.local/pgdata)
# Redis 8.0.2 on 127.0.0.1:56379
export TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/sos_rs
export TEST_REDIS_URL=redis://127.0.0.1:56379
cargo test --all-targets
```

Without those variables the integration tests print a skip line and pass, so
CI stays green on a machine with no services.

## Risks

- **Size.** Full parity is roughly 12.7k lines of Go, with SOS lifecycle alone
  at ~2.1k lines. Phases must land in order; a half-ported lifecycle is worse
  than no lifecycle.
- **Behaviour drift in batched reads.** The Go code deliberately batches
  responder counts, latest breadcrumbs and last-seen lookups to keep query count
  constant. A naive port reintroduces N+1; the plan's tests check round-trip
  counts, not just output.
- **No local Postgres yet.** Migrations are compile-checked by
  `sqlx::migrate` (filenames and order) but have not been executed against a
  server. First real run needs a Postgres instance.
- **Redis has no in-process fake** for the `redis` crate; session and rate-limit
  tests use trait fakes, and the real Lua script needs an env-gated run.

## Open questions for the user

1. Supabase project (URL + connection string) is not created yet — needed before
   any migration can actually run.
2. GitHub push and PR creation need the token already saved; repo
   `sos-allert-be-rs` does not exist yet.
3. Does the existing MySQL data need to be migrated, or does the Postgres
   database start empty?
