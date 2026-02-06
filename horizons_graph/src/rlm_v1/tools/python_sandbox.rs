#![cfg_attr(not(feature = "monty"), allow(dead_code))]

use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Result of a single Python execution in the sandbox.
#[derive(Clone, Debug)]
pub struct PythonExecResult {
    pub stdout: String,
    pub return_value: Option<String>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub variables: Vec<String>,
}

/// Persistent Monty REPL that survives across `exec_python` calls within one RLM run.
///
/// Notes:
/// - We don't keep a live interpreter instance; instead we persist a best-effort set of
///   user variables by re-injecting them as Python source on each run.
/// - We always inject a `files` dict and `context`/`context_N` aliases derived from
///   materialized inputs.
#[derive(Clone, Debug, Default)]
pub struct MontyREPL {
    /// Persisted Python namespace: variable_name -> python literal (repr-like) string.
    namespace: HashMap<String, String>,

    /// Injected context content. Keys are typically filenames; a special key `context`
    /// may be set to force the default `context` string.
    contexts: HashMap<String, String>,

    /// Captured stdout buffer from the last execution.
    last_stdout: String,
}

impl MontyREPL {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inject_context(&mut self, name: &str, content: String) {
        self.contexts.insert(name.to_string(), content);
    }

    pub fn last_stdout(&self) -> &str {
        &self.last_stdout
    }

    pub async fn execute(&mut self, code: &str, timeout_ms: u64) -> PythonExecResult {
        #[cfg(feature = "monty")]
        {
            return self.execute_monty(code, timeout_ms).await;
        }
        #[cfg(not(feature = "monty"))]
        {
            let _ = (code, timeout_ms);
            return PythonExecResult {
                stdout: String::new(),
                return_value: None,
                error: Some(
                    "exec_python unavailable: horizons_graph was built without feature 'monty'"
                        .to_string(),
                ),
                execution_time_ms: 0,
                variables: Vec::new(),
            };
        }
    }

    #[cfg(feature = "monty")]
    async fn execute_monty(&mut self, code: &str, timeout_ms: u64) -> PythonExecResult {
        use monty::{CollectStringPrint, LimitedTracker, MontyObject, MontyRun, ResourceLimits};

        let start = std::time::Instant::now();

        let (full_code, reserved_names) = build_full_code(&self.namespace, &self.contexts, code);

        // Run Monty in a blocking task so we don't block the async runtime.
        //
        // Monty itself enforces time limits via the ResourceTracker. We still wrap
        // in a host timeout as a backstop; if it ever fires, the blocking task may
        // continue running (Tokio can't forcibly kill a thread).
        let host_timeout = std::time::Duration::from_millis(timeout_ms.saturating_add(250));
        let full_code_for_task = full_code.clone();

        let handle = tokio::task::spawn_blocking(
            move || -> Result<(Option<MontyObject>, String, Option<String>), String> {
                let runner = MontyRun::new(
                    full_code_for_task,
                    "exec.py",
                    Vec::<String>::new(),
                    Vec::<String>::new(),
                )
                .map_err(|e| format!("monty init failed: {e:?}"))?;

                let limits = ResourceLimits::new()
                    .max_duration(std::time::Duration::from_millis(timeout_ms))
                    // 64 MiB per execution (best-effort).
                    .max_memory(64 * 1024 * 1024)
                    // Keep GC reasonably frequent under allocation-heavy code.
                    .gc_interval(2_000);
                let tracker = LimitedTracker::new(limits);

                let mut printer = CollectStringPrint::new();
                let run_res = runner.run(Vec::<MontyObject>::new(), tracker, &mut printer);
                let out = printer.into_output();

                match run_res {
                    Ok(obj) => Ok((Some(obj), out, None)),
                    Err(e) => Ok((None, out, Some(format!("monty execution failed: {e:?}")))),
                }
            },
        );

        let run_res = tokio::time::timeout(host_timeout, handle).await;

        let mut stdout = String::new();
        let mut error: Option<String> = None;
        let mut updated_namespace: Option<HashMap<String, String>> = None;

        match run_res {
            Err(_) => {
                error = Some(format!(
                    "exec_python timed out after {timeout_ms}ms (host timeout backstop)"
                ));
            }
            Ok(join_res) => match join_res {
                Err(join_err) => {
                    error = Some(format!("exec_python internal error: {join_err}"));
                }
                Ok(Err(exec_err)) => error = Some(exec_err),
                Ok(Ok((maybe_obj, out, maybe_err))) => {
                    stdout = out;
                    if let Some(e) = maybe_err {
                        error = Some(e);
                    } else if let Some(obj) = maybe_obj {
                        match extract_vars_dict(&obj) {
                            Ok(vars) => updated_namespace = Some(vars),
                            Err(e) => error = Some(e),
                        }
                    } else {
                        error = Some("monty execution failed without a result object".to_string());
                    }
                }
            },
        }

        self.last_stdout = stdout.clone();

        // Update namespace only if we successfully extracted variables.
        let variables: Vec<String> = if let Some(vars) = updated_namespace.take() {
            self.namespace = vars;
            let mut names = self.namespace.keys().cloned().collect::<Vec<_>>();
            names.sort();
            names
        } else {
            let mut names = self.namespace.keys().cloned().collect::<Vec<_>>();
            names.sort();
            names
        };

        // Best-effort return value: if user sets `return_value` or `result`, surface it.
        let return_value = self
            .namespace
            .get("return_value")
            .cloned()
            .or_else(|| self.namespace.get("result").cloned());

        // If we persisted reserved names (shouldn't happen), drop them defensively.
        if !reserved_names.is_empty() {
            for name in reserved_names {
                self.namespace.remove(&name);
            }
        }

        PythonExecResult {
            stdout,
            return_value,
            error,
            execution_time_ms: start.elapsed().as_millis() as u64,
            variables,
        }
    }
}

#[cfg(feature = "monty")]
fn extract_vars_dict(result: &monty::MontyObject) -> Result<HashMap<String, String>, String> {
    use monty::MontyObject;

    let MontyObject::Dict(pairs) = result else {
        return Err(format!(
            "unexpected monty return value (expected dict of persisted vars), got: {}",
            result.py_repr()
        ));
    };

    let mut out: HashMap<String, String> = HashMap::new();
    for (k, v) in pairs {
        let key = match k {
            MontyObject::String(s) => s.clone(),
            _ => continue,
        };
        let val = match v {
            MontyObject::String(s) => s.clone(),
            MontyObject::Repr(s) => s.clone(),
            other => other.py_repr(),
        };
        out.insert(key, val);
    }

    Ok(out)
}

fn build_full_code(
    namespace: &HashMap<String, String>,
    contexts: &HashMap<String, String>,
    user_code: &str,
) -> (String, HashSet<String>) {
    // Stable file ordering for deterministic `context_0`, `context_1`, ...
    let mut files: Vec<(String, String)> = contexts
        .iter()
        .filter(|(k, _)| k.as_str() != "context")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));

    // Default context preference:
    // - explicit `contexts["context"]`
    // - otherwise first file (by sorted filename)
    let default_context = contexts
        .get("context")
        .cloned()
        .or_else(|| files.first().map(|(_, v)| v.clone()))
        .unwrap_or_default();

    let mut reserved: HashSet<String> = HashSet::new();
    reserved.insert("files".to_string());
    reserved.insert("context".to_string());
    reserved.insert("context_files".to_string());

    let mut preamble = String::new();
    preamble.push_str("# Auto-injected context\n");

    // files = { "trace.json": "...", ... }
    preamble.push_str("files = {\n");
    for (name, content) in &files {
        let key = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_string());
        let val = serde_json::to_string(content).unwrap_or_else(|_| "\"\"".to_string());
        preamble.push_str("  ");
        preamble.push_str(&key);
        preamble.push_str(": ");
        preamble.push_str(&val);
        preamble.push_str(",\n");
    }
    preamble.push_str("}\n");

    preamble.push_str("context_files = list(files.keys())\n");

    let default_context_lit =
        serde_json::to_string(&default_context).unwrap_or_else(|_| "\"\"".to_string());
    preamble.push_str("context = ");
    preamble.push_str(&default_context_lit);
    preamble.push_str("\n");

    // context_0..context_N derived from sorted files
    for (idx, (_name, content)) in files.iter().enumerate() {
        let var = format!("context_{idx}");
        reserved.insert(var.clone());
        let lit = serde_json::to_string(content).unwrap_or_else(|_| "\"\"".to_string());
        preamble.push_str(&var);
        preamble.push_str(" = ");
        preamble.push_str(&lit);
        preamble.push_str("\n");
    }

    preamble.push('\n');
    preamble.push_str("# Auto-injected persisted variables\n");

    // Re-inject previous variables as Python literals. Wrap each assignment in
    // a try/except so one bad value doesn't poison the whole run.
    let mut names: Vec<&String> = namespace.keys().collect();
    names.sort();
    for name in names {
        if name.starts_with('_') {
            continue;
        }
        if reserved.contains(name) {
            continue;
        }
        if let Some(value_src) = namespace.get(name) {
            preamble.push_str("try:\n  ");
            preamble.push_str(name);
            preamble.push_str(" = ");
            preamble.push_str(value_src);
            preamble.push_str("\nexcept Exception:\n  pass\n");
        }
    }

    // Epilogue: persist locals as repr strings, excluding injected helpers.
    //
    // We only persist a conservative set of types that are likely to roundtrip as literals.
    // (e.g. modules, compiled regexes, file handles won't persist cleanly.)
    let mut reserved_list = reserved.iter().cloned().collect::<Vec<_>>();
    reserved_list.sort();
    let reserved_py = serde_json::to_string(&reserved_list).unwrap_or_else(|_| "[]".to_string());

    let epilogue = format!(
        r#"
# Epilogue: collect user-defined variables
def __persistable(v):
    return isinstance(v, (int, float, bool, str, list, dict, tuple, set, type(None)))

__reserved__ = set({reserved_py})
__vars__ = {{
    k: repr(v)
    for (k, v) in locals().items()
    if k not in __reserved__ and not k.startswith('_') and __persistable(v)
}}
__vars__
"#
    );

    let mut full = String::new();
    full.push_str(&preamble);
    full.push('\n');
    full.push_str(user_code);
    full.push('\n');
    full.push_str(&epilogue);

    (full, reserved)
}

// Keep unused imports if the file is built without monty.
#[allow(dead_code)]
fn _keep_serde_json_imports(_: &Value) {}
