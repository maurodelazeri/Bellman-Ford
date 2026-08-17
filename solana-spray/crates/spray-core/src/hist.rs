//! A lock-free log-bucket latency histogram.
//!
//! Recording happens on the hot path from every worker thread at up to tens of
//! thousands of samples a second, so it must be a single relaxed atomic add and
//! nothing else — no locks, no allocation, no clock reads beyond the one the
//! caller already made.
//!
//! Buckets are powers of two from 1µs up, which gives roughly 30% relative
//! precision. That is plenty: we are distinguishing "sub-millisecond" from "a
//! few milliseconds" from "a slot", not chasing the third significant figure.

use std::sync::atomic::{AtomicU64, Ordering};

const BUCKETS: usize = 32;

pub struct LogHistogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Default for LogHistogram {
    fn default() -> Self {
        LogHistogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

impl LogHistogram {
    #[inline]
    pub fn record(&self, micros: u64) {
        let idx = (64 - micros.max(1).leading_zeros()) as usize - 1;
        let idx = idx.min(BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(micros, Ordering::Relaxed);
        self.max.fetch_max(micros, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Upper bound of the bucket containing the requested quantile. Reported as
    /// an upper bound rather than interpolated, so the number is never
    /// optimistic.
    pub fn quantile(&self, q: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * q).ceil() as u64;
        let mut seen = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            seen += b.load(Ordering::Relaxed);
            if seen >= target {
                return 1u64 << (i + 1);
            }
        }
        self.max.load(Ordering::Relaxed)
    }

    pub fn mean(&self) -> f64 {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 {
            return 0.0;
        }
        self.sum.load(Ordering::Relaxed) as f64 / c as f64
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count(),
            "mean_us": self.mean().round(),
            "p50_us": self.quantile(0.50),
            "p90_us": self.quantile(0.90),
            "p99_us": self.quantile(0.99),
            "p999_us": self.quantile(0.999),
            "max_us": self.max.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_bound_the_samples() {
        let h = LogHistogram::default();
        for _ in 0..990 {
            h.record(1_000);
        }
        for _ in 0..10 {
            h.record(500_000);
        }
        assert!(h.quantile(0.50) <= 2_048, "p50 was {}", h.quantile(0.50));
        assert!(h.quantile(0.999) >= 500_000);
        assert_eq!(h.count(), 1000);
    }
}
