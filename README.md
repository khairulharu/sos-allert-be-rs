# sos-allert-be-rs

Rust port of [khairulharu/sos-allert-be](https://github.com/khairulharu/sos-allert-be)
(the Go service), with the database moved from MySQL to **Postgres**. Same clean
architecture, same HTTP contract, same SOS semantics.

Status: **phase 0 (skeleton)**. Only health/metrics routes exist; see
[docs/PORT-PLAN.md](docs/PORT-PLAN.md) for the phase list.

## Stack

| concern | Go | Rust |
|---|---|---|
| http | Fiber v2 | axum 0.8 + tower-http |
| db | GORM + MySQL | sqlx + Postgres |
| migrations | golang-migrate | sqlx::migrate (same filenames/order) |
| sessions / geo | go-redis | redis 1.x (async) |
| kafka | IBM/sarama | rskafka (pure Rust) — phase 5 |
| push | webpush-go, APNs, Twilio | `web-push`-equivalent, reqwest — phase 4 |
| tests | `_test.go` per package | `src/<module>/tests.rs` + `tests/` |

## Run

```bash
cp config.json.example config.json     # then edit; never commit it
cargo run --bin web                    # :3000
cargo run --bin web -- migrate         # apply migrations (explicit DDL)
cargo run --bin worker                 # kafka consumers (phase 5)
```

`CONFIG_PATH=/path/to/config.json` overrides the search path
(`./config.json`, then `../config.json`).

## Check

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

No database is needed for these: unit tests use fakes, and the router smoke
tests point at unreachable ports on purpose.

## Layout

```
src/domain/        entities, one module per table
src/repository/    storage traits + sqlx/redis implementations
src/usecase/       business rules, owns transactions
src/delivery/http  handlers, middleware, router
src/delivery/messaging  kafka consumers
src/gateway/       kafka producer, push/sms/storage adapters
src/config/        config.json structs and defaults
src/geo/           haversine + ETA (ported 1:1)
migrations/        Postgres, same versions as the Go repo
```

## Contract notes carried over

- Timestamps are millisecond `bigint`, not timestamps.
- `Authorization` carries the **raw** session token, no `Bearer` prefix
  (`/metrics` is the only exception when `metrics.token` is set).
- Sessions live in Redis only (`auth:token:*`, `auth:user:*`) with a TTL.
- Radius search uses Redis GEO; Postgres keeps the historical `locations` row.
  PostGIS is deliberately not required.
- Passwords and the safe PIN are bcrypt, so hashes imported from MySQL still
  verify.
- Triage severity `NULL` means "never classified"; `0` is a real score.
