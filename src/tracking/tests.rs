//! Hub tests: fan-out, isolation per alert, and the three connection budgets.

use super::*;

#[tokio::test]
async fn publish_reaches_every_subscriber_of_that_alert_only() {
    let hub = Hub::new(10, 10, 3);
    let mut a1 = hub.subscribe("alert-1");
    let mut a1_second = hub.subscribe("alert-1");
    let mut a2 = hub.subscribe("alert-2");

    hub.publish("alert-1", b"LOCATION_UPDATE".to_vec());

    assert_eq!(a1.recv().await.unwrap(), b"LOCATION_UPDATE");
    assert_eq!(a1_second.recv().await.unwrap(), b"LOCATION_UPDATE");
    assert!(a2.try_recv().is_err(), "other alerts must not receive it");
}

#[tokio::test]
async fn publish_without_subscribers_is_not_a_panic() {
    let hub = Hub::new(10, 10, 3);
    hub.publish("nobody-listening", b"x".to_vec());
}

#[tokio::test]
async fn global_cap_rejects_extra_connections() {
    let hub = Hub::new(2, 10, 3);
    let first = hub.acquire("1.1.1.1").expect("slot 1");
    let second = hub.acquire("2.2.2.2").expect("slot 2");
    assert!(hub.acquire("3.3.3.3").is_none(), "global cap not enforced");

    drop(first);
    assert!(hub.acquire("3.3.3.3").is_some(), "slot not released");
    drop(second);
}

#[tokio::test]
async fn per_ip_cap_rejects_extra_sockets_without_touching_other_ips() {
    let hub = Hub::new(100, 2, 3);
    let a = hub.acquire("9.9.9.9").unwrap();
    let b = hub.acquire("9.9.9.9").unwrap();
    assert!(hub.acquire("9.9.9.9").is_none(), "per-IP cap not enforced");
    assert!(hub.acquire("8.8.8.8").is_some(), "another IP was blocked");
    drop(a);
    drop(b);
}

#[tokio::test]
async fn per_user_cap_counts_only_claimed_identities() {
    let hub = Hub::new(100, 10, 1);
    let mut first = hub.acquire("1.1.1.1").unwrap();
    let mut second = hub.acquire("1.1.1.1").unwrap();

    assert!(first.claim_user("user-1"));
    assert!(!second.claim_user("user-1"), "second socket allowed");
    assert!(second.claim_user("user-2"), "other user blocked");

    drop(first);
    let mut third = hub.acquire("1.1.1.1").unwrap();
    assert!(third.claim_user("user-1"), "slot not released on drop");
    drop(second);
    drop(third);
}
