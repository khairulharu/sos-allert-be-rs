//! Shared harness for the integration tests.
//!
//! Not every test binary uses every helper.
#![allow(dead_code)]

//!
//! These tests run only when `TEST_DATABASE_URL` and `TEST_REDIS_URL` are set;
//! otherwise they print a skip line and pass, so `cargo test` stays green on a
//! machine without a database.

use axum::body::Body;
use axum::http::{Request, Response};
use axum::Router;
use http_body_util::BodyExt;
use sos_allert::bootstrap::{build_state, migrate, AppState};
use sos_allert::config::Config;
use sos_allert::delivery::http::routes::router;
use tokio::sync::OnceCell;
use tower::ServiceExt;

pub struct Harness {
    pub config: Config,
    pub state: AppState,
    pub app: Router,
}

pub fn env_database() -> Option<(u16, String, String, String)> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    // postgres://user[:pass]@host:port/db
    let rest = url.strip_prefix("postgres://")?;
    let (creds, hostpart) = rest.split_once('@')?;
    let (user, pass) = match creds.split_once(':') {
        Some((user, pass)) => (user.to_owned(), pass.to_owned()),
        None => (creds.to_owned(), String::new()),
    };
    let (hostport, db) = hostpart.split_once('/')?;
    let (host, port) = hostport.split_once(':')?;
    Some((
        port.parse().ok()?,
        format!("{host}:{port}"),
        user,
        format!("{pass}|{db}"),
    ))
}

/// Build a config pointing at the test services, or `None` when the env vars
/// are absent.
pub fn test_config() -> Option<Config> {
    let (port, host, user, pass_db) = env_database()?;
    let (pass, db) = pass_db.split_once('|')?;
    let redis_url = std::env::var("TEST_REDIS_URL").ok()?;

    let mut config = Config::default();
    config.database.username = user;
    config.database.password = pass.to_owned();
    config.database.host = host.split(':').next().unwrap_or("127.0.0.1").to_owned();
    config.database.port = port;
    config.database.name = db.to_owned();
    config.database.sslmode = "disable".into();
    config.database.pool.max = 5;
    config.database.pool.idle = 1;

    // redis://host:port/db
    let rest = redis_url.strip_prefix("redis://").unwrap_or(&redis_url);
    let addr = rest.split('/').next().unwrap_or(rest);
    config.redis.addr = addr.to_owned();

    // Keep guest routes usable in tests: the limiter is per-IP and the test
    // client has no address.
    config.auth.rate_limit_per_minute = 0;
    config.auth.session_cache_ttl_seconds = 30;
    config.web.port = 0;
    Some(config)
}

static MIGRATED: OnceCell<()> = OnceCell::const_new();

async fn ensure_migrated(state: &AppState) {
    MIGRATED
        .get_or_init(|| async {
            migrate(state.db()).await.expect("migrations run");
        })
        .await;
}

pub async fn harness() -> Option<Harness> {
    let config = match test_config() {
        Some(config) => config,
        None => {
            eprintln!("skip: TEST_DATABASE_URL / TEST_REDIS_URL not set");
            return None;
        }
    };

    let state = build_state(config.clone()).expect("state builds");
    ensure_migrated(&state).await;
    let app = router(state.clone());
    Some(Harness { config, state, app })
}

/// Send one request through the router and return status + JSON body.
pub async fn send(
    app: &Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", token);
    }
    let request_body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };

    let response: Response<Body> = app
        .clone()
        .oneshot(builder.body(request_body).unwrap())
        .await
        .expect("router responds");

    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

pub fn unique_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

/// Best-effort cleanup so the test database does not grow forever.
pub async fn delete_user(state: &AppState, id: &str) {
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(state.db())
        .await;
}

/// Register and log in, returning the session token. Panics on failure so
/// tests fail loudly instead of continuing with an empty header.
pub async fn signup_and_login(app: &Router, id: &str, password: &str) -> String {
    let (status, body) = send(
        app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": id, "password": password, "name": "Test"})),
    )
    .await;
    assert!(
        status == 200 || status == 409,
        "register failed: {status} {body}"
    );

    let (status, body) = send(
        app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": password})),
    )
    .await;
    assert_eq!(status, 200, "login failed: {body}");
    body["data"]["token"].as_str().expect("token").to_owned()
}
