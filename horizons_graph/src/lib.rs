//! horizons_graph: Core graph execution engine (ported from rust_backend/graph).

#![forbid(unsafe_code)]

pub mod definition;
pub mod engine;
pub mod error;
pub mod ir;
pub mod llm;
pub mod mapping;
pub mod python;
pub mod registry;
pub mod run;
pub mod template;
pub mod tools;
pub mod trace;

#[cfg(feature = "rlm_v1")]
pub mod rlm_v1;

pub use engine::{GraphEngine, GraphRun};
pub use error::{GraphError, Result};
