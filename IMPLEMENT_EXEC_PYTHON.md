# Implementation Instructions: `exec_python` Monty Sandbox for RLM v2

## Goal

Add an `exec_python` tool to the RLM v1 tool set that runs LLM-generated Python
code inside the Monty interpreter (Rust-native Python), fully in-process. This
replaces `codex_exec` (Docker container) as the default code execution path for
RLM runs.

When finished, the RLM can execute Python code with <5ms cold start, no Docker,
no Python backend, no network — just a single Rust binary.

---

## Context You Need

Read these files before starting:

| File | Why |
|------|-----|
| `Horizons/better_rlm.txt` | Full design doc — architecture, sandbox properties, open questions |
| `testing/horizons-tests/mocked/tests/rlm_exec_python_scenarios.rs` | 11 integration test scenarios with test data fixtures |
| `horizons_graph/src/rlm_v1/mod.rs` | Core types: `RlmRunRequest`, `LmClient`, `LmResponse`, `ToolCall`, `ToolExecutor` |
| `horizons_graph/src/rlm_v1/tools/mod.rs` | `ToolExecutor` trait, `ToolResult`, `ToolRegistry`, `build_tool_registry()` |
| `horizons_graph/src/rlm_v1/tools/builtin.rs` | Existing tool definitions (the list you're adding `exec_python` to) |
| `horizons_graph/src/rlm_v1/runner.rs` | Main RLM loop — `run_rlm()`, tool dispatch, `execute_tool_call()` |
| `horizons_graph/src/tools.rs` | `DefaultToolExecutor` — where Rust-native tools are routed (grep, search, view_lines) |
| `horizons_graph/src/python.rs` | Existing Monty integration for graph nodes (reference implementation) |
| `horizons_graph/Cargo.toml` | Monty dependency declaration (optional feature `monty`) |

---

## Step-by-step Implementation

### Step 1: Enable Monty feature

**File:** `horizons_graph/Cargo.toml`

Change the default features to include `monty`:

```toml
[features]
default = ["rlm_v1", "monty"]
```

Verify `monty` dependency compiles:
```bash
cd Horizons && cargo check -p horizons_graph
```

If Monty's git dependency has breaking API changes, check the pydantic/monty repo
for the latest API. The existing usage in `python.rs` (lines 112-323) shows the
current working pattern:
```rust
use monty::{MontyRun, NoLimitTracker, StdPrint, MontyObject};
let runner = MontyRun::new(code, script_name, vec![], vec![])?;
let result = runner.run(vec![], NoLimitTracker, &mut printer)?;
```

### Step 2: Create `python_sandbox.rs`

**New file:** `horizons_graph/src/rlm_v1/tools/python_sandbox.rs`

This is the core implementation. Create a `MontyREPL` struct that:

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde_json::{json, Value};
use tokio::sync::oneshot;

/// Result of a single Python execution in the sandbox.
#[derive(Clone, Debug)]
pub struct PythonExecResult {
    pub stdout: String,
    pub return_value: Option<String>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub variables: Vec<String>,
}

/// Persistent Monty REPL that survives across exec_python calls within one RLM run.
///
/// The REPL holds:
/// - A namespace (variables created by previous executions)
/// - Injected context data (from materialized files)
/// - A channel sender for llm_query callbacks
pub struct MontyREPL {
    /// Accumulated Python namespace (variable_name -> serialized value).
    /// Monty doesn't natively support persistent namespaces across runs,
    /// so we serialize variables after each execution and inject them as
    /// a preamble on the next run.
    namespace: HashMap<String, String>,

    /// Injected context content (from LocalToolState materialized files).
    /// Keyed by variable name (e.g. "context", "context_0", "files").
    contexts: HashMap<String, String>,

    /// Captured stdout buffer from the last execution.
    last_stdout: String,
}
```

**Key methods to implement:**

#### `MontyREPL::new()`
Initialize empty namespace and contexts.

#### `MontyREPL::inject_context(name, content)`
Store context data that will be available as Python variables.
Call this before the first exec to inject materialized file content.

```rust
pub fn inject_context(&mut self, name: &str, content: String) {
    self.contexts.insert(name.to_string(), content);
}
```

#### `MontyREPL::execute(code, timeout, llm_query_fn) -> PythonExecResult`

This is the critical method. It must:

1. **Build a preamble** that re-injects persisted variables + context:
   ```python
   # Auto-injected context
   context = """<content>"""
   context_0 = context
   # Auto-injected persisted variables from previous executions
   x = 42
   results = [1, 2, 3]
   ```

2. **Build the full code**: preamble + user code

3. **Run in Monty** with timeout enforcement:
   ```rust
   // Monty::run is synchronous — run in a blocking thread with timeout
   let code_clone = full_code.clone();
   let handle = tokio::task::spawn_blocking(move || {
       let runner = MontyRun::new(code_clone, "exec.py", vec![], vec![])?;
       let mut printer = CapturePrint::new(); // Custom StdPrint that captures output
       let result = runner.run(vec![], NoLimitTracker, &mut printer)?;
       Ok((result, printer.output()))
   });

   let result = tokio::time::timeout(
       Duration::from_millis(timeout_ms),
       handle,
   ).await;
   ```

4. **Extract variables** from the result. Since Monty returns the value of the
   last expression, and we need to persist variables, use this strategy:

   After the user's code, append a variable-extraction epilogue:
   ```python
   # Epilogue: collect all user-defined variables
   __vars__ = {k: repr(v) for k, v in locals().items() if not k.startswith('_')}
   __vars__
   ```

   Parse the returned `MontyObject::Dict` to update `self.namespace`.

5. **Handle errors**: If Monty raises an exception, capture it as `error` string.

6. **Handle timeout**: If `tokio::time::timeout` fires, return a `TimeoutError`.

#### `CapturePrint` (custom Monty printer)

Monty's `StdPrint` writes to real stdout. Implement a custom printer that
captures output to a `String` buffer:

```rust
struct CapturePrint {
    buffer: String,
}

impl CapturePrint {
    fn new() -> Self { Self { buffer: String::new() } }
    fn output(&self) -> String { self.buffer.clone() }
}

// Implement the Monty print trait — check monty's source for the exact trait.
// In python.rs it uses `StdPrint` which implements monty's printer interface.
```

#### `llm_query` host callback

The LLM's Python code can call `llm_query(prompt)`. Since Monty is synchronous
and `LmClient::call` is async, bridge with a channel:

```rust
// Before spawning the blocking task, create a channel pair
// The Monty code calls llm_query() which:
// 1. Sends the prompt through a channel to the async runtime
// 2. Blocks waiting for the response on a oneshot channel
// 
// The async runtime receives the request, calls LmClient::call(),
// and sends the response back.
```

**For Phase 1, you can skip `llm_query`** and return a stub error
("llm_query not yet implemented in sandbox"). The test scenarios S7/S8
test this but can be deferred. Get S1-S6, S9-S11 working first.

### Step 3: Register `exec_python` tool

**File:** `horizons_graph/src/rlm_v1/tools/builtin.rs`

Add `exec_python` to the `builtin_tools()` function. Insert it after `view_lines`
and before `codex_exec`:

```rust
json!({
    "type": "function",
    "function": {
        "name": "exec_python",
        "description": "Execute Python code in a sandboxed interpreter. \
            The `context` variable contains the materialized data. \
            Variables persist across calls. Use print() for output. \
            No filesystem or network access. Available stdlib: json, re, \
            math, collections, itertools, string.",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python code to execute"
                },
                "timeout_ms": {
                    "type": "integer",
                    "description": "Max execution time in ms (default 5000)"
                }
            },
            "required": ["code"]
        }
    }
}),
```

**File:** `horizons_graph/src/rlm_v1/tools/mod.rs`

Add capability mapping:
```rust
fn tool_capabilities_for(tool_name: &str) -> &'static [&'static str] {
    match tool_name {
        // ... existing entries ...
        "exec_python" => &["compute"],
        _ => &[],
    }
}
```

### Step 4: Wire `exec_python` into the tool executor

**File:** `horizons_graph/src/tools.rs`

In `DefaultToolExecutor::execute_impl()`, add `exec_python` to the Rust-native
routing (NOT `execute_remote`). It goes alongside `local_grep`, `local_search`, etc.

```rust
async fn execute_impl(
    &self,
    tool_name: &str,
    args: &Value,
    context: &Value,
    local_state: Option<&SharedLocalToolState>,
    graph_inputs: Option<&Value>,
) -> Result<Value> {
    match tool_name {
        "local_grep" => self.local_grep(args, local_state).await,
        "local_search" => self.local_search(args, local_state).await,
        "local_regex" => self.local_regex(args, local_state).await,
        "view_lines" => self.view_lines(args, local_state, graph_inputs).await,
        "materialize_context" => self.materialize_context(args, local_state, graph_inputs).await,
        "exec_python" => self.exec_python(args, local_state, graph_inputs).await,  // NEW
        _ => self.execute_remote(tool_name, args, context).await,
    }
}
```

But there's a problem: `DefaultToolExecutor` is stateless (no `&mut self`),
and `MontyREPL` needs mutable state that persists across calls. Options:

**Option A (recommended):** Add `Arc<Mutex<MontyREPL>>` to `DefaultToolExecutor`.
Initialize it lazily on first `exec_python` call. The RLM runner creates one
`DefaultToolExecutor` per `run_rlm()` call, so the REPL naturally scopes to
one run.

**Option B:** Add `MontyREPL` to `LocalToolState` (which already uses `Arc<RwLock>`).
This keeps tool state co-located.

Go with Option B — it's cleaner. Add to `LocalToolState`:

```rust
pub struct LocalToolState {
    materialized: HashMap<String, MaterializedFile>,
    python_repl: Option<MontyREPL>,  // NEW: lazy-initialized on first exec_python
}
```

Then `exec_python` implementation:

```rust
async fn exec_python(
    &self,
    args: &Value,
    local_state: Option<&SharedLocalToolState>,
    graph_inputs: Option<&Value>,
) -> Result<Value> {
    let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;
    let code = args.get("code").and_then(|v| v.as_str())
        .ok_or_else(|| GraphError::bad_request("exec_python: missing 'code' parameter"))?;
    let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(5000);

    let mut state = state.write().await;

    // Lazy-init the Monty REPL
    if state.python_repl.is_none() {
        let mut repl = MontyREPL::new();
        // Inject all materialized files as context
        for (filename, mat_file) in &state.materialized {
            repl.inject_context(filename, mat_file.content.clone());
        }
        // Also inject "context" as alias for the first materialized file
        if let Some(first) = state.materialized.values().next() {
            repl.inject_context("context", first.content.clone());
        }
        state.python_repl = Some(repl);
    }

    let repl = state.python_repl.as_mut().unwrap();

    // Re-inject any newly materialized files since last exec
    for (filename, mat_file) in &state.materialized {
        repl.inject_context(filename, mat_file.content.clone());
    }

    let result = repl.execute(code, timeout_ms, None /* llm_query_fn */);

    Ok(json!({
        "stdout": result.stdout,
        "return_value": result.return_value,
        "error": result.error,
        "execution_time_ms": result.execution_time_ms,
        "variables": result.variables,
        "_local": true,
        "_rust": true,
        "_sandbox": "monty",
    }))
}
```

### Step 5: Bridge to the RLM v1 `ToolExecutor` trait

The `DefaultToolExecutor` in `tools.rs` implements the *graph engine's*
`ToolExecutor` trait. The RLM v1 has its *own* `ToolExecutor` trait
(in `rlm_v1/tools/mod.rs`). The bridge is in `engine.rs` — the
`GraphToolExecutor` wrapper.

**No changes needed here.** The `GraphToolExecutor` already forwards all
tool calls to the graph engine's `DefaultToolExecutor`. Since `exec_python`
is now handled by `DefaultToolExecutor`, it flows through automatically.

### Step 6: Update the RLM v1 runner for exec_python awareness

**File:** `horizons_graph/src/rlm_v1/runner.rs`

In `execute_tool_call()`, `exec_python` should be treated as a regular tool
(dispatched to `ToolExecutor`), NOT as a terminal tool. No changes needed
unless you want special handling (e.g., tracking exec_python calls separately
in stats). The existing code already handles unknown tools by dispatching to
`tools.execute()`.

One optional improvement: in the `record_tool_result()` function, set
`execution_mode` to `"monty"` for `exec_python` results. The `ToolResult`
already has this field — just ensure it propagates from the executor.

### Step 7: Module registration

**File:** `horizons_graph/src/rlm_v1/tools/mod.rs`

Add the new module:
```rust
pub mod builtin;
pub mod python_sandbox;  // NEW
```

### Step 8: Make the test scenarios compile and run

**File:** `testing/horizons-tests/mocked/tests/rlm_exec_python_scenarios.rs`

The test file uses a `TestToolExecutor` with stubbed responses. To run tests
against the real implementation:

1. Replace `TestToolExecutor` with the real `DefaultToolExecutor` (or wrap it)
2. Remove `simulate_python_exec()` — the real Monty handles execution
3. Keep `ScriptedLm` — the LLM responses are still scripted
4. The test fixtures (`fixture_application_logs()`, etc.) remain as-is

For now, get the stubs passing first, then swap in real Monty.

The test file may need import path adjustments depending on how
`horizons_graph` re-exports types. Check `horizons_graph/src/lib.rs` for
what's publicly available.

---

## What NOT to Do

- **Do NOT modify `runner.rs`'s main loop.** The exec_python tool plugs in
  through the existing `ToolExecutor` trait. No loop changes needed.

- **Do NOT remove `codex_exec`.** Keep it in `builtin_tools()` for backward
  compatibility. It can be gated behind the `"shell"` capability later.

- **Do NOT implement `llm_query` callback in Phase 1.** Get basic exec working
  first. The callback bridge (Monty sync → async LmClient) is the trickiest
  part and can be Phase 2.

- **Do NOT use Python's `exec()` or subprocess.** The whole point is in-process
  Monty execution. No `python3` subprocess.

- **Do NOT modify `python.rs`.** That file handles graph-node Python execution
  (conditions, reduce_fn). The RLM sandbox is separate.

---

## Verification

Run these checks after each step:

```bash
# Step 1: Compiles with monty
cargo check -p horizons_graph

# Step 2-4: Unit test the sandbox directly
cargo test -p horizons_graph python_sandbox

# Step 5-7: Full build
cargo build -p horizons_graph

# Step 8: Integration scenarios
cd testing && cargo test --test rlm_exec_python_scenarios
```

---

## File Summary (what you create / modify)

| Action | File |
|--------|------|
| **CREATE** | `horizons_graph/src/rlm_v1/tools/python_sandbox.rs` |
| MODIFY | `horizons_graph/Cargo.toml` (enable monty by default) |
| MODIFY | `horizons_graph/src/rlm_v1/tools/mod.rs` (add module + capability) |
| MODIFY | `horizons_graph/src/rlm_v1/tools/builtin.rs` (add exec_python schema) |
| MODIFY | `horizons_graph/src/tools.rs` (route exec_python + add MontyREPL to LocalToolState) |
| MODIFY | `testing/horizons-tests/mocked/tests/rlm_exec_python_scenarios.rs` (swap stubs for real) |

---

## Acceptance Criteria

- [ ] `exec_python` tool executes Python code via Monty in-process
- [ ] Variables persist across multiple `exec_python` calls in one RLM run
- [ ] Materialized file content is accessible as `context` variable
- [ ] `print()` output captured and returned in tool result
- [ ] Timeout enforcement: infinite loops killed after `timeout_ms`
- [ ] Memory safety: large allocations rejected (best-effort via Monty limits)
- [ ] Python errors (exceptions) returned gracefully, don't crash Rust process
- [ ] No filesystem access, no network access from sandbox
- [ ] Test scenarios S1-S6, S9-S11 pass with real Monty execution
- [ ] `codex_exec` still works (not removed, just not default for RLM)
- [ ] `cargo build` succeeds with `--features monty` and without
