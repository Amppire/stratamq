//! Deterministic synthetic workload generation (uniform, Zipfian, hot-key).
//!
//! The same [`WorkloadSpec`] always yields the same key sequence, on every
//! machine, so benchmark runs stay comparable.

use rand::RngExt;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Zipf};
use thiserror::Error;

/// How keys are chosen from the key space.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyDistribution {
    /// Every key is equally likely.
    Uniform,
    /// A few low-numbered keys get most of the traffic. Larger `exponent`
    /// means a steeper skew; 1.0 is the classic Zipf law.
    Zipfian { exponent: f64 },
    /// All traffic hits a single key (key 0).
    HotKey,
}

/// Everything needed to reproduce a key sequence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkloadSpec {
    pub distribution: KeyDistribution,
    /// Number of distinct keys; generated ids are in `0..key_space`.
    pub key_space: u64,
    /// PRNG seed. Same seed, same sequence.
    pub seed: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GeneratorError {
    #[error("key_space must be at least 1")]
    EmptyKeySpace,
    #[error("invalid Zipfian exponent: {0}")]
    InvalidExponent(String),
}

/// Produces a reproducible stream of key ids for a [`WorkloadSpec`].
#[derive(Debug, Clone)]
pub struct WorkloadGenerator {
    rng: ChaCha8Rng,
    sampler: Sampler,
}

#[derive(Debug, Clone)]
enum Sampler {
    Uniform { key_space: u64 },
    Zipfian(Zipf<f64>),
    HotKey,
}

impl WorkloadGenerator {
    /// Builds a generator, rejecting an empty key space or a bad exponent.
    pub fn new(spec: WorkloadSpec) -> Result<Self, GeneratorError> {
        if spec.key_space == 0 {
            return Err(GeneratorError::EmptyKeySpace);
        }
        let sampler = match spec.distribution {
            KeyDistribution::Uniform => Sampler::Uniform {
                key_space: spec.key_space,
            },
            KeyDistribution::Zipfian { exponent } => {
                // `Zipf` samples ranks in 1..=n as floats; precision is
                // ample for any realistic key space.
                let zipf = Zipf::new(spec.key_space as f64, exponent)
                    .map_err(|e| GeneratorError::InvalidExponent(e.to_string()))?;
                Sampler::Zipfian(zipf)
            }
            KeyDistribution::HotKey => Sampler::HotKey,
        };
        Ok(Self {
            rng: ChaCha8Rng::seed_from_u64(spec.seed),
            sampler,
        })
    }

    /// Returns the next key id, in `0..key_space`. Convert to bytes with
    /// `to_be_bytes()` when a trait needs `&[u8]`.
    pub fn next_key_id(&mut self) -> u64 {
        match &self.sampler {
            Sampler::Uniform { key_space } => self.rng.random_range(0..*key_space),
            // Rank 1 is the most popular key; map it to id 0.
            Sampler::Zipfian(zipf) => (zipf.sample(&mut self.rng) as u64).saturating_sub(1),
            Sampler::HotKey => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(distribution: KeyDistribution, key_space: u64, seed: u64) -> WorkloadSpec {
        WorkloadSpec {
            distribution,
            key_space,
            seed,
        }
    }

    fn draw(spec: WorkloadSpec, n: usize) -> Vec<u64> {
        let mut generator = WorkloadGenerator::new(spec).unwrap();
        (0..n).map(|_| generator.next_key_id()).collect()
    }

    fn counts(keys: &[u64], key_space: u64) -> Vec<usize> {
        let mut counts = vec![0; key_space as usize];
        for &key in keys {
            counts[key as usize] += 1;
        }
        counts
    }

    #[test]
    fn same_seed_gives_same_sequence_for_every_distribution() {
        for distribution in [
            KeyDistribution::Uniform,
            KeyDistribution::Zipfian { exponent: 1.0 },
            KeyDistribution::HotKey,
        ] {
            let s = spec(distribution, 1_000, 42);
            assert_eq!(draw(s, 500), draw(s, 500), "{distribution:?}");
        }
    }

    #[test]
    fn different_seed_gives_different_sequence() {
        let a = draw(spec(KeyDistribution::Uniform, 1_000, 1), 100);
        let b = draw(spec(KeyDistribution::Uniform, 1_000, 2), 100);
        assert_ne!(a, b);
    }

    #[test]
    fn keys_stay_inside_the_key_space() {
        for distribution in [
            KeyDistribution::Uniform,
            KeyDistribution::Zipfian { exponent: 1.2 },
            KeyDistribution::HotKey,
        ] {
            let keys = draw(spec(distribution, 50, 7), 10_000);
            assert!(keys.iter().all(|&k| k < 50), "{distribution:?}");
        }
    }

    #[test]
    fn uniform_spreads_evenly() {
        let keys = draw(spec(KeyDistribution::Uniform, 10, 3), 100_000);
        // Expect ~10_000 per key; allow +/-10%.
        for (key, count) in counts(&keys, 10).into_iter().enumerate() {
            assert!((9_000..=11_000).contains(&count), "key {key}: {count}");
        }
    }

    #[test]
    fn zipfian_is_skewed_toward_low_keys() {
        let keys = draw(
            spec(KeyDistribution::Zipfian { exponent: 1.0 }, 1_000, 3),
            100_000,
        );
        let counts = counts(&keys, 1_000);
        assert!(counts[0] > counts[9] * 3, "{} vs {}", counts[0], counts[9]);
        assert!(counts[0] > counts[999] * 50);
        // Classic Zipf over 1000 keys puts ~13% of traffic on the top key.
        assert!(counts[0] > 10_000, "top key got {}", counts[0]);
    }

    #[test]
    fn hot_key_always_returns_key_zero() {
        let keys = draw(spec(KeyDistribution::HotKey, 1_000, 3), 1_000);
        assert!(keys.iter().all(|&k| k == 0));
    }

    #[test]
    fn empty_key_space_is_rejected() {
        let err = WorkloadGenerator::new(spec(KeyDistribution::Uniform, 0, 1)).unwrap_err();
        assert_eq!(err, GeneratorError::EmptyKeySpace);
    }

    #[test]
    fn negative_zipf_exponent_is_rejected() {
        let result =
            WorkloadGenerator::new(spec(KeyDistribution::Zipfian { exponent: -1.0 }, 100, 1));
        assert!(matches!(result, Err(GeneratorError::InvalidExponent(_))));
    }
}
