use serde_json::{json, Map, Value};
use std::process::Stdio;
use tokio::process::Command;

use crate::error::{GraphError, Result};

/// Execute python "modes" used by the graph engine.
///
/// This is used for:
/// - `python_function` nodes (fn_str + inputs)
/// - `reduce_fn` reductions (fn_str + results)
/// - conditional edges (code + state/node_output)
/// - output-mapping fallbacks (code + state/node_output)
///
/// By default this runs via a local `python3` subprocess. A Monty backend can
/// be added behind the `monty` feature.
pub async fn run_python(args: &Value) -> Result<Value> {
    match selected_backend() {
        PythonBackend::Subprocess => run_python_subprocess(args).await,
        PythonBackend::Monty => {
            #[cfg(feature = "monty")]
            {
                return run_python_monty(args);
            }
            #[cfg(not(feature = "monty"))]
            {
                Err(GraphError::unavailable(
                    "python backend 'monty' requested, but horizons_graph was built without feature 'monty'",
                ))
            }
        }
    }
}

fn normalize_mode(args: &Map<String, Value>) -> &str {
    args.get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("python_function")
}

async fn run_python_subprocess(args: &Value) -> Result<Value> {
    let args_obj = args
        .as_object()
        .ok_or_else(|| GraphError::bad_request("python args must be an object"))?;
    let mode = normalize_mode(args_obj);

    let wrapper = build_python_wrapper(mode)?;
    let payload = serde_json::to_string(args).map_err(|e| {
        GraphError::internal(format!("failed to serialize python args to json: {e}"))
    })?;

    let mut child = Command::new("python3")
        .arg("-c")
        .arg(wrapper)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| GraphError::internal(format!("failed to spawn python3: {e}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| GraphError::internal(format!("failed writing python stdin: {e}")))?;
    }

    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .map_err(|_| GraphError::internal("python execution timed out after 30s"))?
        .map_err(|e| GraphError::internal(format!("python process failed: {e}")))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        let msg = if stderr.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            stderr.trim().to_string()
        };
        return Err(GraphError::bad_request(format!("python execution failed: {msg}")));
    }

    serde_json::from_str::<Value>(stdout.trim()).map_err(|e| {
        GraphError::internal(format!(
            "failed to parse python output as json: {e}; output={}",
            truncate_for_error(&stdout, 1000)
        ))
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PythonBackend {
    Subprocess,
    Monty,
}

fn selected_backend() -> PythonBackend {
    match std::env::var("HORIZONS_GRAPH_PYTHON_BACKEND")
        .unwrap_or_else(|_| "subprocess".to_string())
        .trim()
        .to_lowercase()
        .as_str()
    {
        "monty" => PythonBackend::Monty,
        _ => PythonBackend::Subprocess,
    }
}

#[cfg(feature = "monty")]
fn run_python_monty(args: &Value) -> Result<Value> {
    use monty::{MontyObject, MontyRun, NoLimitTracker, StdPrint};

    let args_obj = args
        .as_object()
        .ok_or_else(|| GraphError::bad_request("python args must be an object"))?;
    let mode = normalize_mode(args_obj);

    let script_name = "graph.py";

    let code = match mode {
        // Best-effort support for pure-python nodes. Monty is intentionally limited:
        // no filesystem/env/network; limited stdlib; no third-party libs.
        //
        // For complex python that imports modules (e.g. `import json`) you should
        // use the default subprocess backend instead.
        "python_function" => {
            let fn_str = args_obj
                .get("fn_str")
                .and_then(|v| v.as_str())
                .ok_or_else(|| GraphError::bad_request("python_function missing fn_str"))?;
            let inputs = args_obj.get("inputs").cloned().unwrap_or(json!({}));
            if !inputs.is_object() {
                return Err(GraphError::bad_request(
                    "python_function inputs must be an object when using monty backend",
                ));
            }
            let input_literal = to_python_literal(&inputs)?;
            let fn_name = extract_function_name(fn_str).unwrap_or("main");
            format!(
                r#"{fn_str}
_inputs = {input_literal}
{fn_name}(**_inputs)
"#,
                fn_str = fn_str,
                input_literal = input_literal,
                fn_name = fn_name
            )
        }
        "reduce_fn" => {
            let fn_str = args_obj
                .get("fn_str")
                .and_then(|v| v.as_str())
                .ok_or_else(|| GraphError::bad_request("reduce_fn missing fn_str"))?;
            let results = args_obj.get("results").cloned().unwrap_or(json!([]));
            let results_literal = to_python_literal(&results)?;
            let fn_name = extract_function_name(fn_str).unwrap_or("main");
            format!(
                r#"{fn_str}
_results = {results_literal}
{fn_name}(_results)
"#,
                fn_str = fn_str,
                results_literal = results_literal,
                fn_name = fn_name
            )
        }
        "condition" | "output_mapping" => {
            let code = args_obj
                .get("code")
                .and_then(|v| v.as_str())
                .ok_or_else(|| GraphError::bad_request("missing code"))?;
            let state = args_obj.get("state").cloned().unwrap_or(Value::Null);
            let node_output = args_obj
                .get("node_output")
                .cloned()
                .unwrap_or(Value::Null);
            let state_literal = to_python_literal(&state)?;
            let node_output_literal = to_python_literal(&node_output)?;
            // Return the value of the last expression (Monty behaves like a REPL for the final line).
            format!(
                r#"state = {state_literal}
node_output = {node_output_literal}
{code}
"#,
                state_literal = state_literal,
                node_output_literal = node_output_literal,
                code = code
            )
        }
        other => {
            return Err(GraphError::bad_request(format!(
                "monty backend does not support mode '{other}'"
            )));
        }
    };

    let runner =
        MontyRun::new(code, script_name, Vec::<String>::new(), Vec::<String>::new()).map_err(
            |err| GraphError::internal(format!("monty init failed: {err:?}")),
        )?;

    let mut printer = StdPrint;
    let result = runner
        .run(Vec::<MontyObject>::new(), NoLimitTracker, &mut printer)
        .map_err(|err| GraphError::bad_request(format!("monty execution failed: {err:?}")))?;

    // MontyObject's exact shape is intentionally not depended on here.
    // For graph nodes that need structured JSON outputs, prefer the subprocess backend.
    Ok(Value::String(format!("{result:?}")))
}

#[cfg(feature = "monty")]
fn to_python_literal(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok("None".to_string()),
        Value::Bool(true) => Ok("True".to_string()),
        Value::Bool(false) => Ok("False".to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::String(s) => Ok(serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())),
        Value::Array(items) => {
            let mut out = String::from("[");
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                out.push_str(&to_python_literal(item)?);
            }
            out.push(']');
            Ok(out)
        }
        Value::Object(map) => {
            let mut out = String::from("{");
            for (idx, (k, v)) in map.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                let key = serde_json::to_string(k).unwrap_or_else(|_| "\"\"".to_string());
                out.push_str(&key);
                out.push_str(": ");
                out.push_str(&to_python_literal(v)?);
            }
            out.push('}');
            Ok(out)
        }
    }
}

#[cfg(feature = "monty")]
fn extract_function_name(fn_str: &str) -> Option<&str> {
    for line in fn_str.lines() {
        let trimmed = line.trim();
        if let Some(after_def) = trimmed.strip_prefix("def ") {
            if let Some(paren_pos) = after_def.find('(') {
                let name = after_def[..paren_pos].trim();
                if !name.is_empty() {
                    return Some(name);
                }
            }
        }
    }
    None
}

fn truncate_for_error(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    format!("{}...(truncated)", &text[..max])
}

fn build_python_wrapper(mode: &str) -> Result<String> {
    // The wrapper reads the full args payload from stdin and branches on mode.
    // It returns JSON to stdout, and on any exception returns {"error": "..."}.
    //
    // Note: we keep this wrapper intentionally self-contained (no external deps),
    // since it runs in minimal environments.
    let script = format!(
        r#"
import json
import sys
import traceback

def _extract_function_name(fn_str: str) -> str:
    for line in fn_str.splitlines():
        line = line.strip()
        if line.startswith("def "):
            after = line[4:]
            if "(" in after:
                name = after.split("(", 1)[0].strip()
                if name:
                    return name
    return "main"

def _as_obj(v):
    if isinstance(v, dict):
        return v
    return {{"value": v}}

def _run():
    args = json.loads(sys.stdin.read() or "{{}}")
    mode = args.get("mode") or "{mode}"

    # Common context for code evaluation.
    state = args.get("state", {{}})
    node_output = args.get("node_output", None)
    inputs = args.get("inputs", {{}})
    results = args.get("results", [])

    if mode in ("python_function", "reduce_fn"):
        fn_str = args.get("fn_str")
        if not isinstance(fn_str, str) or not fn_str.strip():
            raise ValueError("missing fn_str")
        # Execute in a scratch namespace (avoid leaking globals between runs).
        ns = {{}}
        exec(fn_str, ns, ns)
        fn_name = args.get("fn_name") or _extract_function_name(fn_str)
        fn = ns.get(fn_name)
        if not callable(fn):
            raise ValueError(f"function '{{fn_name}}' not found in fn_str")
        if mode == "python_function":
            if not isinstance(inputs, dict):
                raise ValueError("inputs must be an object for python_function")
            return fn(**inputs)
        return fn(results)

    if mode in ("condition", "output_mapping"):
        code = args.get("code")
        if not isinstance(code, str) or not code.strip():
            raise ValueError("missing code")
        ns = {{"state": state, "node_output": node_output}}
        # If code defines a function, call it; otherwise eval as an expression.
        if "def " in code:
            exec(code, ns, ns)
            fn_name = args.get("fn_name") or _extract_function_name(code)
            fn = ns.get(fn_name)
            if not callable(fn):
                raise ValueError(f"function '{{fn_name}}' not found in code")
            return fn(state=state, node_output=node_output)
        return eval(code, ns, ns)

    raise ValueError(f"unsupported mode '{{mode}}'")

try:
    result = _run()
    print(json.dumps(result))
except Exception as e:
    # Keep error payload JSON; upstream can turn this into GraphError if needed.
    payload = {{
        "error": str(e),
        "type": e.__class__.__name__,
        "traceback": traceback.format_exc(),
    }}
    print(json.dumps(payload))
    sys.exit(1)
"#,
        mode = mode
    );
    Ok(script)
}

pub fn python_error_to_graph_error(value: &Value) -> GraphError {
    if let Some(msg) = value.get("error").and_then(|v| v.as_str()) {
        return GraphError::bad_request(msg.to_string());
    }
    GraphError::bad_request(value.to_string())
}

pub fn python_args_for_python_function(fn_str: &str, inputs: &Value) -> Value {
    json!({
        "mode": "python_function",
        "fn_str": fn_str,
        "inputs": inputs,
    })
}
