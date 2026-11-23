//! `config.json` loading.
//!
//! Key set and defaults mirror the Go service (`internal/config/*.go`,
//! `config.json.example`). The file is not committed; set `CONFIG_PATH` or
//! place `config.json` at the repo root or one level up.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Settings used only when the key is absent from the file.
pub const DEFAULT_TOKEN_TTL_HOURS: i64 = 168;
pub const DEFAULT_SOS_RATE_LIMIT_PER_HOUR: u32 = 3;
pub const DEFAULT_MAX_RECIPIENTS: usize = 50;
pub const DEFAULT_WEB_PORT: u16 = 3000;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct Config {
    pub app: AppConfig,
    pub web: WebConfig,
    pub log: LogConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub kafka: KafkaConfig,
    pub sos: SosConfig,
    pub auth: AuthConfig,
    pub metrics: MetricsConfig,
    pub fcm: FcmConfig,
    pub apns: ApnsConfig,
    pub twilio: TwilioConfig,
    pub webpush: WebPushConfig,
    pub storage: StorageConfig,
    pub ai: AiConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub name: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: "sos-backend".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct WebConfig {
    pub port: u16,
    pub prefork: bool,
    /// Header carrying the client IP behind a proxy, e.g. `X-Forwarded-For`.
    pub proxy_header: String,
    pub trusted_proxies: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LogConfig {
    /// 0=panic .. 6=trace, same scale as logrus in the Go service.
    pub level: u8,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: 4 }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub name: String,
    pub pool: PoolConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            username: String::new(),
            password: String::new(),
            host: "127.0.0.1".into(),
            port: 5432,
            name: "postgres".into(),
            pool: PoolConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PoolConfig {
    pub idle: u32,
    pub max: u32,
    /// Seconds a connection may be reused (`0` falls back to 300).
    pub lifetime: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            idle: 10,
            max: 100,
            lifetime: 300,
        }
    }
}

impl PoolConfig {
    /// `0` means "unlimited / never recycle" to the driver, so fall back.
    pub fn lifetime_secs(&self) -> u64 {
        if self.lifetime == 0 {
            300
        } else {
            self.lifetime
        }
    }
}

impl DatabaseConfig {
    /// `postgres://user:pass@host:port/db?sslmode=require`.
    ///
    /// Supabase requires TLS; the pooler port (6543) is used in production.
    pub fn dsn(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode=require",
            urlencode(&self.username),
            urlencode(&self.password),
            self.host,
            self.port,
            self.name
        )
    }

    /// `postgres://.../db?sslmode=disable`, for local test databases.
    pub fn dsn_no_tls(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            urlencode(&self.username),
            urlencode(&self.password),
            self.host,
            self.port,
            self.name
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RedisConfig {
    pub addr: String,
    pub user: String,
    pub pass: String,
    pub db: i64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:6379".into(),
            user: String::new(),
            pass: String::new(),
            db: 0,
        }
    }
}

impl RedisConfig {
    /// Redis URL, redacted credentials are added only when set.
    pub fn url(&self) -> String {
        if self.user.is_empty() && self.pass.is_empty() {
            format!("redis://{}/{}", self.addr, self.db)
        } else {
            format!(
                "redis://{}:{}@{}/{}",
                urlencode(&self.user),
                urlencode(&self.pass),
                self.addr,
                self.db
            )
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct KafkaConfig {
    pub bootstrap: KafkaBootstrap,
    pub group: KafkaGroup,
    pub auto: KafkaAuto,
    pub producer: KafkaProducer,
    pub consumer: KafkaConsumer,
    pub connect: KafkaConnect,
    pub dlq: KafkaDlq,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaBootstrap {
    pub servers: String,
}

impl Default for KafkaBootstrap {
    fn default() -> Self {
        Self {
            servers: "localhost:9092".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaGroup {
    pub id: String,
}

impl Default for KafkaGroup {
    fn default() -> Self {
        Self {
            id: "sos-allert".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct KafkaAuto {
    pub offset: KafkaOffset,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaOffset {
    pub reset: String,
}

impl Default for KafkaOffset {
    fn default() -> Self {
        Self {
            reset: "earliest".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct KafkaProducer {
    /// Off by default: the web process then notifies inline.
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaConsumer {
    pub max_retries: u32,
    pub retry_backoff_seconds: u64,
}

impl Default for KafkaConsumer {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_backoff_seconds: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaConnect {
    pub max_attempts: u32,
    pub retry_backoff_seconds: u64,
}

impl Default for KafkaConnect {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            retry_backoff_seconds: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaDlq {
    pub topic: String,
}

impl Default for KafkaDlq {
    fn default() -> Self {
        Self {
            topic: "sos.dispatched.dlq".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SosConfig {
    pub grace_period_seconds: u64,
    pub dispatch_poll_seconds: u64,
    pub housekeeping_interval_seconds: u64,
    pub rate_limit_per_hour: u32,
    pub classify_rate_limit_per_hour: u32,
    pub rate_limit_disabled: bool,
    pub max_recipients: usize,
    pub ws_max_connections: usize,
    pub ws_max_per_ip: usize,
    pub ws_max_per_user: usize,
}

impl Default for SosConfig {
    fn default() -> Self {
        Self {
            grace_period_seconds: 10,
            dispatch_poll_seconds: 1,
            housekeeping_interval_seconds: 60,
            rate_limit_per_hour: DEFAULT_SOS_RATE_LIMIT_PER_HOUR,
            classify_rate_limit_per_hour: 10,
            rate_limit_disabled: false,
            max_recipients: DEFAULT_MAX_RECIPIENTS,
            ws_max_connections: 500,
            ws_max_per_ip: 10,
            ws_max_per_user: 3,
        }
    }
}

impl SosConfig {
    /// The poll interval is floored at 100 ms like the Go config.
    pub fn dispatch_poll_interval_ms(&self) -> u64 {
        (self.dispatch_poll_seconds * 1000).max(100)
    }

    /// Housekeeping sleeps at least one second.
    pub fn housekeeping_interval_secs(&self) -> u64 {
        self.housekeeping_interval_seconds.max(1)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AuthConfig {
    pub rate_limit_per_minute: u32,
    pub lockout_minutes: u64,
    pub token_ttl_hours: i64,
    pub session_cache_ttl_seconds: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 5,
            lockout_minutes: 15,
            token_ttl_hours: DEFAULT_TOKEN_TTL_HOURS,
            session_cache_ttl_seconds: 30,
        }
    }
}

impl AuthConfig {
    /// Sessions always expire: `0` or negative falls back to the default.
    pub fn token_ttl_hours_or_default(&self) -> i64 {
        if self.token_ttl_hours <= 0 {
            DEFAULT_TOKEN_TTL_HOURS
        } else {
            self.token_ttl_hours
        }
    }

    /// In-process session cache is capped by the token TTL, `0` disables it.
    pub fn session_cache_ttl_secs(&self) -> u64 {
        let ttl = (self.token_ttl_hours_or_default().max(1) as u64) * 3600;
        self.session_cache_ttl_seconds.min(ttl)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct MetricsConfig {
    /// When set, `/metrics` requires `Authorization: Bearer <token>`.
    pub token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct FcmConfig {
    pub enabled: bool,
    pub project_id: String,
    pub access_token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ApnsConfig {
    pub enabled: bool,
    pub key_id: String,
    pub team_id: String,
    pub bundle_id: String,
    pub private_key_path: String,
    pub production: bool,
}

impl Default for ApnsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_id: String::new(),
            team_id: String::new(),
            bundle_id: String::new(),
            private_key_path: "AuthKey.p8".into(),
            production: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct TwilioConfig {
    pub enabled: bool,
    pub account_sid: String,
    pub auth_token: String,
    pub from_number: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct WebPushConfig {
    pub enabled: bool,
    pub vapid_public_key: String,
    pub vapid_private_key: String,
    /// Bare email; the push service gets `mailto:` in front of it.
    pub subscriber: String,
    pub ttl_seconds: u32,
    /// Push service hosts a stored subscription may point at (SSRF guard).
    pub allowed_hosts: Vec<String>,
}

impl Default for WebPushConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            vapid_public_key: String::new(),
            vapid_private_key: String::new(),
            subscriber: "sos@example.com".into(),
            ttl_seconds: 300,
            allowed_hosts: [
                "fcm.googleapis.com",
                "updates.push.services.mozilla.com",
                "web.push.apple.com",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct StorageConfig {
    /// `local` (default) or `supabase` storage later.
    pub backend: String,
    pub local_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "local".into(),
            local_dir: "./uploads".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct AiConfig {
    pub enabled: bool,
    pub provider: String,
    pub timeout_seconds: u64,
    pub stt: SttConfig,
    pub jev: AiProviderConfig,
    pub ollama: AiProviderConfig,
    pub openai: AiProviderConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SttConfig {
    pub enabled: bool,
    pub provider: String,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub language: String,
    pub timeout_seconds: u64,
}

impl Default for SttConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "groq".into(),
            base_url: "https://api.groq.com/openai/v1".into(),
            api_key: String::new(),
            model: "whisper-large-v3-turbo".into(),
            language: "id".into(),
            timeout_seconds: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct AiProviderConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}

impl Config {
    /// Read `CONFIG_PATH`, else `./config.json`, else `../config.json`.
    ///
    /// Mirrors the Go loader: a missing file is fatal because the service
    /// cannot start without database/redis settings.
    pub fn load() -> anyhow::Result<Self> {
        if let Ok(path) = std::env::var("CONFIG_PATH") {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("read CONFIG_PATH {path}: {e}"))?;
            return Self::from_json_str(&text);
        }

        for candidate in Self::candidate_paths() {
            if candidate.is_file() {
                let text = std::fs::read_to_string(&candidate)
                    .map_err(|e| anyhow::anyhow!("read {}: {e}", candidate.display()))?;
                return Self::from_json_str(&text);
            }
        }

        anyhow::bail!(
            "config.json not found (looked in {})",
            Self::candidate_paths()
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    pub fn from_json_str(text: &str) -> anyhow::Result<Self> {
        let cfg: Self = serde_json::from_str(text)?;
        Ok(cfg)
    }

    pub fn candidate_paths() -> Vec<PathBuf> {
        let mut paths = vec![
            PathBuf::from("../config.json"),
            PathBuf::from("./config.json"),
        ];
        if let Ok(dir) = std::env::current_dir() {
            paths.push(dir.join("config.json"));
        }
        paths
    }

    /// Nothing else may run before this: an unset database name means the
    /// operator pointed the service at an empty placeholder file.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.database.name.is_empty() {
            anyhow::bail!("database.name is empty in config");
        }
        if self.redis.addr.is_empty() {
            anyhow::bail!("redis.addr is empty in config");
        }
        Ok(())
    }

    pub fn web_port(&self) -> u16 {
        if self.web.port == 0 {
            DEFAULT_WEB_PORT
        } else {
            self.web.port
        }
    }
}

/// Percent-encode the characters that break a URL authority.
fn urlencode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Path helper used by tests and tooling.
pub fn explain_search_paths() -> String {
    Config::candidate_paths()
        .iter()
        .map(|p: &PathBuf| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}
