//! Per-IP auth limiter for the guest routes.
//!
//! Fails **open**: a Redis outage must never lock people out of authentication
//! (`NewAuthRateLimit` in the Go service). Blocked attempts get `429` plus
//! `Retry-After` in seconds, floored at 1.

use std::net::IpAddr;
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::bootstrap::AppState;
use crate::error::AppError;

/// Client IP for the limiter key.
///
/// The proxy header is honoured **only** when the direct peer is listed in
/// `web.trusted_proxies`: otherwise any client could send a fresh
/// `X-Forwarded-For` on every attempt and never hit the lockout. With a
/// trusted peer, the header is parsed from the right, skipping trusted hops,
/// so a client-supplied prefix cannot shift the bucket.
pub fn client_ip(request: &Request, proxy_header: &str, trusted_proxies: &[String]) -> String {
    let peer = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip());

    if !proxy_header.is_empty() {
        if let Some(peer) = peer.filter(|ip| ip_is_trusted(*ip, trusted_proxies)) {
            let _ = peer;
            if let Some(value) = request
                .headers()
                .get(proxy_header)
                .and_then(|value| value.to_str().ok())
            {
                // Right-most non-trusted hop: what the nearest trusted proxy
                // actually saw. Cloudflare appends the client address, so a
                // forged prefix is ignored.
                if let Some(client) = value.split(',').rev().map(str::trim).find(|candidate| {
                    !candidate.is_empty() && !hop_is_trusted(candidate, trusted_proxies)
                }) {
                    return client.to_owned();
                }
            }
        }
    }

    peer.map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// `true` when `ip` matches an entry of `web.trusted_proxies`: either an exact
/// address or a CIDR block (`10.0.0.0/8`, `::1/128`).
pub fn ip_is_trusted(ip: IpAddr, trusted_proxies: &[String]) -> bool {
    trusted_proxies.iter().any(|entry| entry_matches(ip, entry))
}

/// A header hop may carry a port (`203.0.113.7:443`) or be malformed; an
/// unparsable hop is treated as untrusted, i.e. as the client.
fn hop_is_trusted(hop: &str, trusted_proxies: &[String]) -> bool {
    parse_hop(hop).is_some_and(|ip| ip_is_trusted(ip, trusted_proxies))
}

fn parse_hop(hop: &str) -> Option<IpAddr> {
    let hop = hop.trim();
    if let Ok(ip) = hop.parse::<IpAddr>() {
        return Some(ip);
    }
    // `[v6]:port`
    if let Some(inner) = hop.strip_prefix('[') {
        if let Some((addr, _)) = inner.split_once(']') {
            return addr.parse().ok();
        }
        return None;
    }
    // `v4:port`
    if let Some((addr, port)) = hop.rsplit_once(':') {
        if port.chars().all(|c| c.is_ascii_digit()) {
            return addr.parse().ok();
        }
    }
    None
}

fn entry_matches(ip: IpAddr, entry: &str) -> bool {
    let entry = entry.trim();
    if entry.is_empty() {
        return false;
    }
    if let Ok(exact) = entry.parse::<IpAddr>() {
        return exact == ip;
    }
    let Some((network, bits)) = entry.split_once('/') else {
        return false;
    };
    let (Ok(network), Ok(bits)) = (network.parse::<IpAddr>(), bits.parse::<u8>()) else {
        return false;
    };
    match (network, ip) {
        (IpAddr::V4(net), IpAddr::V4(candidate)) => {
            let bits = bits.min(32);
            let mask = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - u32::from(bits))
            };
            (u32::from(net) & mask) == (u32::from(candidate) & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(candidate)) => {
            let bits = bits.min(128);
            let mask = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - u32::from(bits))
            };
            (u128::from(net) & mask) == (u128::from(candidate) & mask)
        }
        _ => false,
    }
}

pub async fn auth_rate_limit(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let config = state.config.clone();
    let limit = config.auth.rate_limit_per_minute;
    let lockout = Duration::from_secs(config.auth.lockout_minutes * 60);

    if limit == 0 || lockout.is_zero() {
        return next.run(request).await;
    }

    let ip = client_ip(
        &request,
        &config.web.proxy_header,
        &config.web.trusted_proxies,
    );
    match state
        .rate_limiter()
        .allow_auth(&ip, limit, Duration::from_secs(60), lockout)
        .await
    {
        Ok((true, _)) => next.run(request).await,
        Ok((false, retry_after)) => {
            let seconds = retry_after.as_secs().max(1);
            let mut response = AppError::TooManyRequests {
                message: "too many authentication attempts, try again later".into(),
                retry_after_s: seconds,
            }
            .into_response();
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
            response
        }
        Err(error) => {
            tracing::warn!(error = %error, "auth rate limiter unavailable, proceeding");
            next.run(request).await
        }
    }
}

#[cfg(test)]
mod tests;
