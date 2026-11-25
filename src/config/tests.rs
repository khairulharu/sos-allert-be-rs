use super::RedisConfig;

#[test]
fn redis_url_defaults_to_plain_scheme() {
    let cfg = RedisConfig::default();

    assert_eq!(cfg.url(), "redis://127.0.0.1:6379/0");
}

#[test]
fn redis_url_uses_rediss_with_encoded_credentials() {
    let cfg = RedisConfig {
        addr: "redis-12345.c1.us-east-1-2.ec2.redns.redis-cloud.com:12345".into(),
        user: "default".into(),
        pass: "p@ss/word".into(),
        db: 0,
        tls: true,
    };

    assert_eq!(
        cfg.url(),
        "rediss://default:p%40ss%2Fword@redis-12345.c1.us-east-1-2.ec2.redns.redis-cloud.com:12345/0"
    );
}
