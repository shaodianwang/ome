//! Per-session egress rate limiting.

use std::time::Duration;

/// A token bucket over lines: one second's allowance of burst capacity,
/// refilled by elapsed wall time. `admit` spends whole tokens; lines beyond
/// the balance are the caller's to drop and count.
#[derive(Debug)]
pub(super) struct TokenBucket {
    capacity: f64,
    tokens: f64,
    per_sec: f64,
}

impl TokenBucket {
    /// Starts full, so a fresh session can flush its tail history immediately.
    pub fn new(lines_per_sec: usize) -> Self {
        let per_sec = lines_per_sec as f64;
        Self {
            capacity: per_sec,
            tokens: per_sec,
            per_sec,
        }
    }

    /// Credit the elapsed interval, never beyond the burst capacity.
    pub fn refill(&mut self, elapsed: Duration) {
        self.tokens = (self.tokens + self.per_sec * elapsed.as_secs_f64()).min(self.capacity);
    }

    /// How many of `requested` lines fit the current balance; spends them.
    pub fn admit(&mut self, requested: usize) -> usize {
        let available = self.tokens.max(0.0) as usize;
        let admitted = requested.min(available);
        self.tokens -= admitted as f64;
        admitted
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TokenBucket;

    #[test]
    fn a_fresh_bucket_admits_one_second_of_burst_then_rejects() {
        let mut bucket = TokenBucket::new(100);
        assert_eq!(bucket.admit(60), 60);
        assert_eq!(bucket.admit(60), 40);
        assert_eq!(bucket.admit(10), 0);
    }

    #[test]
    fn refill_restores_tokens_proportionally_to_elapsed_time() {
        let mut bucket = TokenBucket::new(100);
        assert_eq!(bucket.admit(100), 100);

        bucket.refill(Duration::from_millis(250));
        assert_eq!(bucket.admit(100), 25);

        bucket.refill(Duration::from_millis(500));
        assert_eq!(bucket.admit(10), 10);
        assert_eq!(bucket.admit(100), 40);
    }

    #[test]
    fn refill_never_exceeds_the_burst_capacity() {
        let mut bucket = TokenBucket::new(50);
        bucket.refill(Duration::from_secs(3600));
        assert_eq!(bucket.admit(1000), 50);
    }
}
