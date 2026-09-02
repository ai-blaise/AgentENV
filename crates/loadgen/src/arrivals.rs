//! Open-loop arrival schedule.

use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Draws the gaps between arrivals of a Poisson process at a fixed rate.
///
/// A closed loop measures a system under a load that shrinks whenever the
/// system slows down, which is exactly the wrong instrument for a saturation
/// question: the offered rate is a function of the answer. Poisson arrivals
/// keep the offered rate fixed, so a queue that grows is visible as a queue
/// rather than as a lower request count.
pub struct PoissonArrivals {
    rate_per_sec: f64,
    rng: SmallRng,
}

impl PoissonArrivals {
    /// Creates a schedule at `rate_per_sec` arrivals per second.
    ///
    /// The seed is explicit so a run can be replayed; the binary derives it
    /// from the wall clock unless one is given.
    pub fn new(rate_per_sec: f64, seed: u64) -> Self {
        assert!(
            rate_per_sec > 0.0,
            "open-loop arrival rate must be positive, got {rate_per_sec}"
        );
        Self {
            rate_per_sec,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Returns the wait before the next arrival.
    ///
    /// Inter-arrival times of a Poisson process are exponential with mean
    /// `1/rate`, drawn here by inverting the CDF. The uniform is taken from
    /// the half-open range ending below 1 so `ln` never sees zero.
    pub fn next_gap(&mut self) -> Duration {
        let uniform: f64 = self.rng.random_range(f64::EPSILON..1.0);
        Duration::from_secs_f64(-uniform.ln() / self.rate_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::PoissonArrivals;

    /// The schedule must offer the rate it was asked for.
    ///
    /// Getting the inversion backwards (multiplying by the rate rather than
    /// dividing) still produces plausible-looking exponential gaps, and every
    /// throughput number the run reports would then be wrong by the square of
    /// the rate without anything failing.
    #[test]
    fn poisson_gaps_average_to_the_requested_rate() {
        let mut arrivals = PoissonArrivals::new(500.0, 0x5eed);

        let samples = 200_000;
        let total: f64 = (0..samples)
            .map(|_| arrivals.next_gap().as_secs_f64())
            .sum();
        let mean = total / f64::from(samples);

        let expected = 1.0 / 500.0;
        assert!(
            (mean - expected).abs() < expected * 0.02,
            "mean inter-arrival gap was {mean}s, expected about {expected}s"
        );
    }

    /// An exponential distribution is not a constant one: a schedule that
    /// returned the mean every time would pass the test above and would not
    /// be an open loop at all.
    #[test]
    fn poisson_gaps_are_dispersed() {
        let mut arrivals = PoissonArrivals::new(100.0, 7);

        let gaps: Vec<f64> = (0..10_000)
            .map(|_| arrivals.next_gap().as_secs_f64())
            .collect();
        let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
        let variance = gaps.iter().map(|gap| (gap - mean).powi(2)).sum::<f64>() / gaps.len() as f64;

        // For an exponential, the standard deviation equals the mean.
        let deviation = variance.sqrt();
        assert!(
            (deviation - mean).abs() < mean * 0.05,
            "gap spread {deviation}s does not match an exponential with mean {mean}s"
        );
    }
}
