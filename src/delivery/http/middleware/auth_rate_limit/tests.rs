//! The limiter key must not be forgeable: only a trusted peer's proxy header
//! is believed, and a forged prefix is skipped.

use std::net::SocketAddr;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;

use super::{client_ip, ip_is_trusted};

const LOOPBACK: &[&str] = &["127.0.0.1", "::1"];

fn list(entries: &[&str]) -> Vec<String> {
    entries.iter().map(|entry| (*entry).to_owned()).collect()
}

fn request(peer: &str, header: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri("/api/users/_login");
    if let Some(value) = header {
        builder = builder.header("x-forwarded-for", value);
    }
    let mut request = builder.body(Body::empty()).unwrap();
    let addr: SocketAddr = peer.parse().unwrap();
    request.extensions_mut().insert(ConnectInfo(addr));
    request
}

#[test]
fn an_untrusted_peer_cannot_choose_its_bucket() {
    // Rotating X-Forwarded-For used to key the limiter; now the peer wins.
    for spoofed in ["203.0.113.101", "203.0.113.102", "198.51.100.7"] {
        let request = request("192.0.2.10:4000", Some(spoofed));
        assert_eq!(
            client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
            "192.0.2.10"
        );
    }
}

#[test]
fn a_trusted_peer_is_believed() {
    let request = request("127.0.0.1:5000", Some("203.0.113.7"));
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
        "203.0.113.7"
    );
}

#[test]
fn a_trusted_peer_ignores_a_forged_prefix() {
    // Cloudflare appends the real client address; the attacker-supplied
    // prefix must not become the bucket.
    let request = request("127.0.0.1:5000", Some("203.0.113.99, 198.51.100.23"));
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
        "198.51.100.23"
    );
}

#[test]
fn trusted_hops_in_the_chain_are_skipped() {
    let request = request(
        "127.0.0.1:5000",
        Some("203.0.113.99, 198.51.100.23, 127.0.0.1"),
    );
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
        "198.51.100.23"
    );
}

#[test]
fn a_chain_of_only_trusted_hops_falls_back_to_the_peer() {
    let request = request("127.0.0.1:5000", Some("127.0.0.1, 127.0.0.1"));
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
        "127.0.0.1"
    );
}

#[test]
fn without_a_proxy_header_config_the_peer_is_used() {
    let request = request("198.51.100.5:5000", Some("203.0.113.7"));
    assert_eq!(client_ip(&request, "", &list(LOOPBACK)), "198.51.100.5");
}

#[test]
fn empty_trusted_proxies_means_no_header_is_believed() {
    // The old behaviour trusted the header when the list was empty, which is
    // exactly the bypass; an unset list now means "peer only".
    let request = request("127.0.0.1:5000", Some("203.0.113.7"));
    assert_eq!(client_ip(&request, "x-forwarded-for", &[]), "127.0.0.1");
}

#[test]
fn a_request_without_a_peer_falls_back_to_unknown() {
    let request = Request::builder()
        .uri("/api/users")
        .header("x-forwarded-for", "203.0.113.7")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(LOOPBACK)),
        "unknown"
    );
}

#[test]
fn cidr_entries_are_honoured() {
    let cidrs = list(&["10.0.0.0/8", "2001:db8::/32"]);
    assert!(ip_is_trusted("10.1.2.3".parse().unwrap(), &cidrs));
    assert!(!ip_is_trusted("11.1.2.3".parse().unwrap(), &cidrs));
    assert!(ip_is_trusted("2001:db8:1::5".parse().unwrap(), &cidrs));
    assert!(!ip_is_trusted("2001:db9::1".parse().unwrap(), &cidrs));
    // /0 matches everything of that family.
    assert!(ip_is_trusted(
        "8.8.8.8".parse().unwrap(),
        &list(&["0.0.0.0/0"])
    ));
}

#[test]
fn hop_parsing_accepts_ports_and_rejects_garbage() {
    // Ports are tolerated on a hop; garbage counts as the client address.
    let with_port = request("127.0.0.1:5000", Some("203.0.113.9:443"));
    assert_eq!(
        client_ip(&with_port, "x-forwarded-for", &list(LOOPBACK)),
        "203.0.113.9:443"
    );

    let garbage = request("127.0.0.1:5000", Some("not-an-ip"));
    assert_eq!(
        client_ip(&garbage, "x-forwarded-for", &list(LOOPBACK)),
        "not-an-ip"
    );
}

#[test]
fn an_ipv6_client_address_survives() {
    let request = request("[::1]:5000", Some("2001:db8::7"));
    assert_eq!(
        client_ip(&request, "x-forwarded-for", &list(&["::1"])),
        "2001:db8::7"
    );
}
