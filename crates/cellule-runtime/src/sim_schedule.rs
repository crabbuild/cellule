//! Deterministic schedule generator shared by the seeded simulators.
//!
//! Each simulator seeds its own stream, so a failing schedule replays from its
//! seed alone and two simulators never march through the same sequence. This
//! module is compiled for tests only.

/// One deterministic operation schedule.
#[derive(Debug)]
pub(crate) struct Schedule {
    state: u64,
}

impl Schedule {
    pub(crate) fn new(seed: u64, stream: u64) -> Self {
        Self {
            state: seed ^ stream,
        }
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state >> 17
    }

    pub(crate) fn pick<T: Copy>(&mut self, choices: &[T]) -> T {
        choices[(self.next() as usize) % choices.len()]
    }

    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 { 0 } else { self.next() % bound }
    }
}

/// Enumerates every operation sequence of `length` steps over `choices`.
///
/// Sampling finds the bugs that happen often; a short exhaustive enumeration
/// finds the ones that need an exact interleaving. Both run the same
/// invariants, so a counterexample here is a regression test with the exact
/// sequence in its name.
pub(crate) fn sequences<T: Copy>(choices: &[T], length: usize) -> Vec<Vec<T>> {
    let mut sequences = vec![Vec::new()];
    for _ in 0..length {
        let mut extended = Vec::with_capacity(sequences.len() * choices.len());
        for prefix in &sequences {
            for choice in choices {
                let mut sequence = prefix.clone();
                sequence.push(*choice);
                extended.push(sequence);
            }
        }
        sequences = extended;
    }
    sequences
}
