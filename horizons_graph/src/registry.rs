//! Built-in graph registry for zero-shot verifier graphs.
//!
//! This module provides a registry of built-in graph definitions that can be
//! resolved by graph_id without requiring Redis storage.
//!
//! The registry supports:
//! - Canonical graph IDs (e.g., "verifier_rubric_rlm")
//! - Aliases (e.g., "zero_shot_verifier_rubric_rlm", "rlm", "rlm_v1")

use std::collections::HashMap;
use std::sync::LazyLock;

/// Graph definition with YAML content
#[derive(Debug, Clone)]
pub struct GraphDefinition {
    pub id: &'static str,
    pub name: &'static str,
    pub yaml: &'static str,
}

// Embed YAML files at compile time.
//
// Note: these live in `horizons_graph/graphs/` (not in `Horizons/graphs/`).
const VERIFIER_RUBRIC_SINGLE_YAML: &str = include_str!("../graphs/verifier_rubric_single.yaml");
const VERIFIER_RUBRIC_RLM_YAML: &str = include_str!("../graphs/verifier_rubric_rlm.yaml");
const VERIFIER_FEWSHOT_SINGLE_YAML: &str = include_str!("../graphs/verifier_fewshot_single.yaml");
const VERIFIER_FEWSHOT_RLM_YAML: &str = include_str!("../graphs/verifier_fewshot_rlm.yaml");
const VERIFIER_CONTRASTIVE_SINGLE_YAML: &str =
    include_str!("../graphs/verifier_contrastive_single.yaml");
const VERIFIER_CONTRASTIVE_RLM_YAML: &str = include_str!("../graphs/verifier_contrastive_rlm.yaml");

/// Canonical graph IDs
pub const VERIFIER_RUBRIC_SINGLE: &str = "verifier_rubric_single";
pub const VERIFIER_RUBRIC_RLM: &str = "verifier_rubric_rlm";
pub const VERIFIER_FEWSHOT_SINGLE: &str = "verifier_fewshot_single";
pub const VERIFIER_FEWSHOT_RLM: &str = "verifier_fewshot_rlm";
pub const VERIFIER_CONTRASTIVE_SINGLE: &str = "verifier_contrastive_single";
pub const VERIFIER_CONTRASTIVE_RLM: &str = "verifier_contrastive_rlm";

/// Built-in graph definitions
static GRAPH_DEFINITIONS: LazyLock<HashMap<&'static str, GraphDefinition>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    map.insert(
        VERIFIER_RUBRIC_SINGLE,
        GraphDefinition {
            id: VERIFIER_RUBRIC_SINGLE,
            name: "zero_shot_verifier_rubric_single",
            yaml: VERIFIER_RUBRIC_SINGLE_YAML,
        },
    );

    map.insert(
        VERIFIER_RUBRIC_RLM,
        GraphDefinition {
            id: VERIFIER_RUBRIC_RLM,
            name: "zero_shot_verifier_rubric_rlm",
            yaml: VERIFIER_RUBRIC_RLM_YAML,
        },
    );

    map.insert(
        VERIFIER_FEWSHOT_SINGLE,
        GraphDefinition {
            id: VERIFIER_FEWSHOT_SINGLE,
            name: "zero_shot_verifier_fewshot_single",
            yaml: VERIFIER_FEWSHOT_SINGLE_YAML,
        },
    );

    map.insert(
        VERIFIER_FEWSHOT_RLM,
        GraphDefinition {
            id: VERIFIER_FEWSHOT_RLM,
            name: "zero_shot_verifier_fewshot_rlm",
            yaml: VERIFIER_FEWSHOT_RLM_YAML,
        },
    );

    map.insert(
        VERIFIER_CONTRASTIVE_SINGLE,
        GraphDefinition {
            id: VERIFIER_CONTRASTIVE_SINGLE,
            name: "zero_shot_verifier_contrastive_single",
            yaml: VERIFIER_CONTRASTIVE_SINGLE_YAML,
        },
    );

    map.insert(
        VERIFIER_CONTRASTIVE_RLM,
        GraphDefinition {
            id: VERIFIER_CONTRASTIVE_RLM,
            name: "zero_shot_verifier_contrastive_rlm",
            yaml: VERIFIER_CONTRASTIVE_RLM_YAML,
        },
    );

    map
});

/// Aliases for graph IDs (maps alias -> canonical ID)
static ALIASES: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    // Full names with zero_shot_ prefix
    map.insert("zero_shot_verifier_rubric_single", VERIFIER_RUBRIC_SINGLE);
    map.insert("zero_shot_verifier_rubric_rlm", VERIFIER_RUBRIC_RLM);
    map.insert("zero_shot_verifier_rubric_rlm_v1", VERIFIER_RUBRIC_RLM);
    map.insert("zero_shot_verifier_fewshot_single", VERIFIER_FEWSHOT_SINGLE);
    map.insert("zero_shot_verifier_fewshot_rlm", VERIFIER_FEWSHOT_RLM);
    map.insert(
        "zero_shot_verifier_contrastive_single",
        VERIFIER_CONTRASTIVE_SINGLE,
    );
    map.insert(
        "zero_shot_verifier_contrastive_rlm",
        VERIFIER_CONTRASTIVE_RLM,
    );

    // Short names with rubric_ prefix
    map.insert("rubric_single", VERIFIER_RUBRIC_SINGLE);
    map.insert("rubric_rlm", VERIFIER_RUBRIC_RLM);
    map.insert("rubric_rlm_v1", VERIFIER_RUBRIC_RLM);

    // Short names with fewshot_ prefix
    map.insert("fewshot_single", VERIFIER_FEWSHOT_SINGLE);
    map.insert("fewshot_rlm", VERIFIER_FEWSHOT_RLM);

    // Short names with contrastive_ prefix
    map.insert("contrastive_single", VERIFIER_CONTRASTIVE_SINGLE);
    map.insert("contrastive_rlm", VERIFIER_CONTRASTIVE_RLM);

    // Very short aliases
    map.insert("single", VERIFIER_RUBRIC_SINGLE);
    map.insert("rlm", VERIFIER_RUBRIC_RLM);
    map.insert("rlm_v1", VERIFIER_RUBRIC_RLM);

    map
});

/// Normalize a graph ID to its canonical form.
///
/// Handles aliases and prefixes for convenience.
///
/// # Examples
/// - `normalize_graph_id("zero_shot_verifier_rubric_rlm") == "verifier_rubric_rlm"`
/// - `normalize_graph_id("rlm") == "verifier_rubric_rlm"`
/// - `normalize_graph_id("verifier_rubric_single") == "verifier_rubric_single"`
pub fn normalize_graph_id(graph_id: &str) -> String {
    let graph_id_lower = graph_id.to_lowercase();
    let trimmed = graph_id_lower.trim();

    // Check direct match in definitions
    if GRAPH_DEFINITIONS.contains_key(trimmed) {
        return trimmed.to_string();
    }

    // Check aliases
    if let Some(&canonical) = ALIASES.get(trimmed) {
        return canonical.to_string();
    }

    // Return as-is if not found
    trimmed.to_string()
}

/// Get a built-in graph definition by ID.
///
/// Returns the graph definition if found in the built-in registry,
/// or None if not a built-in graph.
pub fn get_builtin_graph(graph_id: &str) -> Option<&'static GraphDefinition> {
    let normalized = normalize_graph_id(graph_id);
    GRAPH_DEFINITIONS.get(normalized.as_str())
}

/// Get the YAML content for a built-in graph by ID.
///
/// Returns the YAML string if found, or None if not a built-in graph.
pub fn get_builtin_graph_yaml(graph_id: &str) -> Option<&'static str> {
    get_builtin_graph(graph_id).map(|def| def.yaml)
}

/// Check if a graph ID refers to a built-in graph.
pub fn is_builtin_graph(graph_id: &str) -> bool {
    get_builtin_graph(graph_id).is_some()
}

/// List all built-in graph IDs.
pub fn list_builtin_graphs() -> Vec<&'static str> {
    let mut ids: Vec<_> = GRAPH_DEFINITIONS.keys().copied().collect();
    ids.sort_unstable();
    ids
}
