# AGENTS.md

Rust port of the Go service `khairulharu/sos-allert-be`. The Go repo is the
contract: when behaviour is unclear, read it (`internal/usecase`,
`internal/delivery/http/route/route.go`, `db/migrations`) instead of guessing.

## Commands

```bash
cargo run --bin web                  # :3000
cargo run --bin web -- migrate       # apply migrations; explicit, never on boot
cargo run --bin worker               # kafka consumers (phase 5)
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## Rules

- Work on a branch, open a PR, never push to `main`.
- Migrations are DDL: never run them against a live database without the
  user's approval card. Never edit an applied migration; add a new one with the
  next version.
- Never commit `config.json`, `ca.pem`, `*.p8`, or any key. `config.json.example`
  holds the full key set and stays in sync when keys change.
- Do not disable or skip a failing test to make CI green.
- The build uses `profile.debug = 0`: the machine's 16G disk cannot hold a
  debuginfo build of this tree. Do not re-enable it without freeing space.

## Contract details that are easy to get wrong

- Timestamps are millisecond `bigint` everywhere (Go `autoCreateTime:milli`).
- `Authorization` is the raw token. No `Bearer` prefix. `401` for an unknown
  token, `503` when the session store errors so clients keep their token.
- Sessions: `auth:token:<token>` -> user id, `auth:user:<user_id>` -> token,
  both with `auth.token_ttl_hours`. Login replaces the old token (single
  session). Logout deletes both and publishes `auth:session_invalidate`.
- Redis GEO key `users:locations` takes **longitude before latitude**.
  `users:last_seen` drives the >2 min stale filter; read it with one `HMGET`
  per candidate set, never one call per candidate.
- Alert statuses: `TRIGGERED`, `ACKNOWLEDGED`, `RESOLVED`, `CANCELLED`,
  `EXPIRED`. Expiry only flips status; there is no `expired_at`.
- `alerts.severity = NULL` means never classified; `0` is a real score.
- Grace-period outbox is at-least-once: crash after send, before mark, re-sends.
  Losing an SOS is worse than a duplicate.
- Fan-out is fail-open after the alert row is persisted: a recipient-lookup
  error yields an empty list, `recipients_notified: 0`, and still runs the
  notifier. Only an alert-persist failure is a 5xx.

## Local test services

Docker is not usable on this machine (socket is root-only). Postgres and Redis
were installed user-space; start them before integration tests:

```bash
PGBIN=$HOME/.local/pgsrv/usr/lib/postgresql/17/bin
LD_LIBRARY_PATH=$HOME/.local/pgsrv/usr/lib/x86_64-linux-gnu   $PGBIN/pg_ctl -D $HOME/.local/pgdata -l /tmp/opencode/pg.log   -o "-p 55432 -k /tmp/opencode -c listen_addresses=127.0.0.1" start
LD_LIBRARY_PATH=$HOME/.local/pgsrv/usr/lib/x86_64-linux-gnu \
  $HOME/.local/pgsrv/usr/bin/redis-server --port 56379 --bind 127.0.0.1 \
  --dir $HOME/.local/redis --daemonize yes --save '' --appendonly no
export TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/sos_rs
export TEST_REDIS_URL=redis://127.0.0.1:56379
```

## Kafka locally

`rdkafka` needs cmake and a broker. On this machine both are user-space:

```bash
export PATH="$HOME/.local/cmake-venv/bin:$PATH"     # cmake for cmake-build
# Kafka 3.9.1 KRaft in /tmp/opencode/kafka, config /tmp/opencode/kraft.properties
/tmp/opencode/kafka/bin/kafka-server-start.sh /tmp/opencode/kraft.properties &
export TEST_KAFKA_BROKERS=127.0.0.1:9092
```

Only `sos.dispatched` has a dead-letter publisher (it needs
`kafka.producer.enabled: true`); the entity topics drop after their retries,
exactly like the Go worker.

## Notifications

`fcm`, `apns` and `twilio` are all off by default; an unconfigured channel logs
and continues instead of failing the trigger. APNs reads
`apns.private_key_path` (a .p8 PKCS#8 EC key); its provider token is cached for
50 minutes — do not mint one per device, Apple answers
`403 TooManyProviderTokenUpdates`. Provider request shapes are asserted against
a local mock server in `src/gateway/notify/tests.rs`.

## Proxy header trust

`web.proxy_header` is only believed when the direct peer is listed in
`web.trusted_proxies` (exact address or CIDR). The header is parsed from the
right, skipping trusted hops, so a client-supplied prefix cannot pick its own
rate-limit bucket. An empty `web.trusted_proxies` means the header is ignored
and the limiter keys on the peer address — never fall back to trusting it.

## HTTP middleware

The router applies, outermost first: panic guard, the two helmet headers, CORS,
a 50 MB body limit. CORS is locked to `https://allert.my.id` and
`http://localhost:5173`; a new client origin must be added there (same list as
the Go service).

## Metrics

`/metrics` is registered only when `metrics.token` is non-empty, and the token
is compared in constant time (raw or `Bearer`). Never make it public: it exposes
incident counts.

## Web Push

`webpush.*` in config: VAPID public/private keys (base64url, raw P-256 scalar),
`subscriber` (bare email inside the VAPID `sub` claim), `ttl_seconds`, and
`allowed_hosts` (the SSRF guard; empty falls back to the FCM/Mozilla/Apple
hosts, and an entry starting with `.` matches a subdomain suffix). A stored
endpoint is re-checked against that list at send time. `404`/`410` from the push
service deletes the subscription.

## AI triage and STT

`ai.enabled` builds the chain from `ai.provider`: `jev` -> (openai when
`ai.openai.api_key` is set) -> offline, `openai` -> offline, `ollama` ->
offline, anything else -> offline only. Every provider validates its answer
strictly (known type, score present, 0..1) and a bad answer falls through.
Jev's `choice` confidence floor is 0.5.

`ai.stt.enabled` wires the OpenAI-compatible `/audio/transcriptions` client
(Groq by default). Silence is detected from `no_speech_prob`,
`compression_ratio`, `avg_logprob`, clip duration and the stock subtitle
phrases — never report a silent clip as a transcript.

`POST /api/sos/classify` is rate limited per user and **fails closed** (503) on
a limiter error: it spends provider tokens unlike the SOS trigger.

## Tests

- Unit tests live in `src/<module>/tests.rs`, declared with
  `#[cfg(test)] mod tests;`, so logic files carry no test code.
- Cross-module and HTTP-level tests live in `tests/`.
- Tests that need Postgres/Redis are gated by env vars and skipped when unset;
  the default `cargo test` runs with no infrastructure.
- Port a Go test when porting its behaviour, not after. `*_test.go` in the Go
  repo is the expected-coverage checklist.
