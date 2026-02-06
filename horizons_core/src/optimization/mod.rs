pub mod engine;
pub mod traits;
pub mod wiring;

// Continual learning requires tying together optimizer + memory + evaluation.
#[cfg(all(feature = "memory", feature = "evaluation"))]
pub mod continual;
