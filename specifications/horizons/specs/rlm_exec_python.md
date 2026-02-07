# RLM `exec_python` (Monty Sandbox) Specification

## Scope

This spec defines the behavior of the RLM v1 tool `exec_python`, which executes
LLM-generated Python code in-process via the Rust-native Monty interpreter.

It also documents:
- the tool schema surfaced to the LLM
- result shape and tracing fields
- sandbox limits (time/memory) and security properties
- state persistence rules across multiple tool calls within a single run
- interaction with materialized inputs (`materialize_context`)

Out of scope:
- Docker/Daytona `codex_exec` behavior (kept for compatibility)
- any “remote Python backend” tool execution
- Tier 3 WASM/wasmtime sandboxing


## Tool Schema

The tool is registered in the RLM v1 builtin tool registry as a function tool:

- name: `exec_python`
- parameters:
  - `code` (string, required): Python source to execute
  - `timeout_ms` (integer, optional, default `5000`): max wall-clock execution time


## Execution Model

### Where It Runs

`exec_python` runs entirely inside the Rust process (no subprocess, no container).
It is routed through the graph engine’s `DefaultToolExecutor` “local tool” path.

### Context Injection

The sandbox receives injected variables:

- `files`: a `dict[str, str]` mapping materialized filename -> content
- `context_files`: `list[str]` of filenames available in `files`
- `context`: the default context string (best-effort selection; see below)
- `context_0`, `context_1`, ...: file contents in deterministic filename order

Default `context` selection order:
1. The first materialized item whose `field_name == "context"` (if present)
2. A materialized file named `context.txt` (if present)
3. Otherwise, the lexicographically-first materialized filename’s content
4. Otherwise, `graph_inputs["context"]` (string or JSON-pretty-printed)

### Stdout Capture

`print()` output is captured via Monty’s `CollectStringPrint` and returned as
`stdout` in the tool result.

### Timeouts And Limits

Enforcement uses two layers:

1. Monty resource tracker:
   - `max_duration = timeout_ms`
   - `max_memory = 64 MiB` (best-effort, per execution)
2. Host timeout backstop:
   - `tokio::time::timeout(timeout_ms + 250ms)` around the blocking task

Notes:
- Monty execution is synchronous and is invoked via `tokio::task::spawn_blocking`.
- Tokio cannot forcefully stop a running thread; the host timeout is a backstop
  in case Monty’s own limit doesn’t trip, but the blocking task may continue.

### Error Handling

Errors are returned in-band in the tool result:

- `success: false`
- `error: <string>`
- `stdout` may contain partial output from before the error.


## State Persistence Across Calls

`exec_python` maintains a per-run REPL-like state scoped to the run’s local tool
state:

- Variables persist across multiple `exec_python` calls within the same run.
- State is reset when the run ends (new run => fresh state).

### Persistence Mechanism

Monty does not currently implement `locals()`/`globals()`, so the sandbox cannot
enumerate user-defined variables after execution.

Instead, persistence is implemented as:
1. Extract a best-effort set of assigned names from the user’s `code`
   (heuristics over lines: `x = ...`, `x += ...`, `def name`, `class Name`,
   `import ... as ...`, `from ... import ... as ...`, simple `for x in ...`).
2. After the user code runs, explicitly persist only those tracked names:
   - If a name is defined and its value is a “persistable” primitive/container
     (`int`, `float`, `bool`, `str`, `list`, `dict`, `tuple`, `set`, `None`),
     store `repr(value)` into a returned dict.
3. On the next execution, re-inject persisted names by emitting Python source:
   `name = <repr from previous run>` (wrapped in `try/except`).

Limitations:
- Variables not detected by the heuristic may not persist.
- Non-primitive objects (modules, regex objects, iterators, etc.) will not be
  persisted by default.
- Values are persisted by `repr()` and re-injected as source; some values may
  not round-trip cleanly.


## Tool Result Shape

`exec_python` returns a JSON object with:

- `success` (bool)
- `stdout` (string)
- `return_value` (string|null)
  - Best-effort: if the user assigns `return_value` or `result`, that value’s
    persisted representation is surfaced here when available.
- `error` (string|null)
- `execution_time_ms` (integer)
- `variables` (array of strings): current persisted variable names

Tracing/diagnostic fields:
- `_local: true`
- `_rust: true`
- `_sandbox: "monty"`


## Security Properties

Intended sandbox properties (best-effort; enforced by Monty’s feature set and
the tool wiring):

- No filesystem access (the interpreter runs without host file primitives)
- No network access (no sockets or HTTP APIs exposed)
- Execution is in-process but memory/time constrained as described above

`codex_exec` remains available for cases that require a full OS shell sandbox.


## Compatibility Notes

Build configuration:
- `horizons_graph` enables `monty` by default.
- It must still compile without `monty` by building with `--no-default-features`
  and explicitly enabling only needed features (e.g. `--features rlm_v1`).

