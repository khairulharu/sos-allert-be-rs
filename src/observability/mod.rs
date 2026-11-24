//! Prometheus text metrics (`internal/metrics/metrics.go`).
//!
//! Three observability signals, hand-rolled so the service needs no metrics
//! dependency: dispatch latency histogram, failed push/SMS deliveries, and the
//! active-incident gauge. `/metrics` is only registered when a token is
//! configured, so the body is never public.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

/// Upper bounds in milliseconds, sorted.
pub const LATENCY_BUCKETS: [f64; 7] = [50.0, 100.0, 150.0, 200.0, 500.0, 1000.0, 2000.0];

#[derive(Default)]
struct Histogram {
    /// One observation lands in exactly one bucket (the first bound it fits),
    /// so rendering has to accumulate them into Prometheus' cumulative form.
    bucket_counts: [u64; LATENCY_BUCKETS.len()],
    count: u64,
    sum_ms: f64,
}

pub struct Metrics {
    histogram: Mutex<Histogram>,
    push_failures: AtomicU64,
    fanout_failures: AtomicU64,
    active_sos: AtomicI64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            histogram: Mutex::new(Histogram::default()),
            push_failures: AtomicU64::new(0),
            fanout_failures: AtomicU64::new(0),
            active_sos: AtomicI64::new(0),
        }
    }

    /// One `POST /api/sos` dispatch latency, measured around the
    /// publish/notify call.
    pub fn observe_dispatch_duration(&self, ms: f64) {
        let mut histogram = self.histogram.lock().expect("metrics poisoned");
        histogram.count += 1;
        histogram.sum_ms += ms;
        for (index, bound) in LATENCY_BUCKETS.iter().enumerate() {
            if ms <= *bound {
                histogram.bucket_counts[index] += 1;
                return;
            }
        }
    }

    /// A failed push/SMS delivery attempt.
    pub fn inc_push_failures(&self) {
        self.push_failures.fetch_add(1, Ordering::SeqCst);
    }

    /// A trigger whose recipient lookup failed and was dispatched with an
    /// empty fan-out.
    pub fn inc_fanout_failures(&self) {
        self.fanout_failures.fetch_add(1, Ordering::SeqCst);
    }

    pub fn set_active_sos(&self, count: i64) {
        self.active_sos.store(count, Ordering::SeqCst);
    }

    /// Prometheus text exposition.
    pub fn render(&self) -> String {
        let (count, sum_ms, buckets) = {
            let histogram = self.histogram.lock().expect("metrics poisoned");
            (histogram.count, histogram.sum_ms, histogram.bucket_counts)
        };

        let mut out = String::with_capacity(1024);
        out.push_str(
            "# HELP sos_dispatch_latency_ms Dispatch latency for POST /api/sos in milliseconds.\n",
        );
        out.push_str("# TYPE sos_dispatch_latency_ms histogram\n");
        let mut cumulative = 0_u64;
        for (index, bound) in LATENCY_BUCKETS.iter().enumerate() {
            cumulative += buckets[index];
            let _ = writeln!(
                out,
                "sos_dispatch_latency_ms_bucket{{le=\"{}\"}} {cumulative}",
                format_bound(*bound)
            );
        }
        let _ = writeln!(out, "sos_dispatch_latency_ms_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "sos_dispatch_latency_ms_sum {}", format_bound(sum_ms));
        let _ = writeln!(out, "sos_dispatch_latency_ms_count {count}");

        out.push_str("# HELP push_delivery_failures_total Failed push/SMS delivery attempts.\n");
        out.push_str("# TYPE push_delivery_failures_total counter\n");
        let _ = writeln!(
            out,
            "push_delivery_failures_total {}",
            self.push_failures.load(Ordering::SeqCst)
        );

        out.push_str("# HELP sos_fanout_failures_total Triggers dispatched with an empty fan-out because the recipient lookup failed.\n");
        out.push_str("# TYPE sos_fanout_failures_total counter\n");
        let _ = writeln!(
            out,
            "sos_fanout_failures_total {}",
            self.fanout_failures.load(Ordering::SeqCst)
        );

        out.push_str("# HELP active_sos_count Currently active SOS incidents.\n");
        out.push_str("# TYPE active_sos_count gauge\n");
        let _ = writeln!(
            out,
            "active_sos_count {}",
            self.active_sos.load(Ordering::SeqCst)
        );
        out
    }
}

/// `%g`-ish formatting: whole numbers without a trailing `.0`, fractions in
/// their shortest form. Buckets are integers, the sum is not.
fn format_bound(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// Constant-time comparison for the metrics token. `Authorization` may carry
/// the raw token or `Bearer <token>`, matching the Go handler.
pub fn token_matches(presented: &str, expected: &str) -> bool {
    let presented = presented.trim();
    let presented = presented
        .strip_prefix("Bearer ")
        .unwrap_or(presented)
        .trim();
    let a = presented.as_bytes();
    let b = expected.as_bytes();

    // Length is not secret; contents are compared without early exit.
    let mut diff = (a.len() ^ b.len()) as u8;
    for (index, byte) in b.iter().enumerate() {
        let left = a.get(index).copied().unwrap_or(0);
        diff |= left ^ byte;
    }
    diff == 0 && !expected.is_empty()
}

impl crate::usecase::sos::SosMetrics for Metrics {
    fn observe_dispatch_duration(&self, ms: f64) {
        Metrics::observe_dispatch_duration(self, ms);
    }
    fn inc_push_failures(&self) {
        Metrics::inc_push_failures(self);
    }
    fn inc_fanout_failures(&self) {
        Metrics::inc_fanout_failures(self);
    }
    fn set_active_sos(&self, count: i64) {
        Metrics::set_active_sos(self, count);
    }
}

#[cfg(test)]
mod tests;
