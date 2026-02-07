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
///   user variables by capturing a dict of values at the end of each run and re-injecting
///   them as Monty inputs on the next run.
/// - We always inject `files`/`context`/`context_N` aliases derived from materialized inputs.
#[derive(Clone, Debug, Default)]
pub struct MontyREPL {
    /// Persisted Python namespace for `exec_python` calls within a single RLM run.
    ///
    /// This is only used when Monty is enabled; for non-monty builds the field is unused.
    #[cfg(feature = "monty")]
    namespace: HashMap<String, monty::MontyObject>,

    #[cfg(not(feature = "monty"))]
    namespace: HashMap<String, String>,

    /// Names we should attempt to persist after each execution.
    ///
    /// Monty doesn't implement `locals()`/`globals()`, so we can't enumerate variables
    /// dynamically. Instead we heuristically extract assigned names from the user's code
    /// and then explicitly persist only those names.
    tracked_vars: HashSet<String>,

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

        self.tracked_vars.extend(discover_assigned_names(code));
        let mut names = self.tracked_vars.iter().cloned().collect::<Vec<_>>();
        names.sort();

        let (input_names, inputs, reserved_names) =
            build_monty_inputs(&self.namespace, &self.contexts);
        let full_code = build_full_code(&names, &reserved_names, code);

        // Run Monty in a blocking task so we don't block the async runtime.
        //
        // Monty itself enforces time limits via the ResourceTracker. We still wrap
        // in a host timeout as a backstop; if it ever fires, the blocking task may
        // continue running (Tokio can't forcibly kill a thread).
        let host_timeout = std::time::Duration::from_millis(timeout_ms.saturating_add(250));
        let full_code_for_task = full_code.clone();
        let input_names_for_task = input_names.clone();
        let inputs_for_task = inputs.clone();

        let handle = tokio::task::spawn_blocking(
            move || -> Result<(Option<MontyObject>, String, Option<String>), String> {
                fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
                    if let Some(s) = payload.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic payload".to_string()
                    }
                }

                let mut printer = CollectStringPrint::new();

                let unwind_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runner = MontyRun::new(
                        full_code_for_task,
                        "exec.py",
                        input_names_for_task,
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

                    runner
                        .run(inputs_for_task, tracker, &mut printer)
                        .map_err(|e| format!("monty execution failed: {e:?}"))
                }));

                let out = printer.into_output();
                match unwind_res {
                    Ok(Ok(obj)) => Ok((Some(obj), out, None)),
                    Ok(Err(err)) => Ok((None, out, Some(err))),
                    Err(payload) => Ok((
                        None,
                        out,
                        Some(format!(
                            "monty panicked: {}",
                            panic_payload_to_string(payload)
                        )),
                    )),
                }
            },
        );

        let run_res = tokio::time::timeout(host_timeout, handle).await;

        let mut stdout = String::new();
        let mut error: Option<String> = None;
        let mut updated_namespace: Option<HashMap<String, MontyObject>> = None;

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
            .map(|v| v.py_repr())
            .or_else(|| self.namespace.get("result").map(|v| v.py_repr()));

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
fn extract_vars_dict(
    result: &monty::MontyObject,
) -> Result<HashMap<String, monty::MontyObject>, String> {
    use monty::MontyObject;

    let MontyObject::Dict(pairs) = result else {
        return Err(format!(
            "unexpected monty return value (expected dict of persisted vars), got: {}",
            result.py_repr()
        ));
    };

    let mut out: HashMap<String, MontyObject> = HashMap::new();
    for (k, v) in pairs.into_iter() {
        let key = match k {
            MontyObject::String(s) => s.clone(),
            _ => continue,
        };
        out.insert(key, v.clone());
    }

    Ok(out)
}

#[cfg(feature = "monty")]
fn build_monty_inputs(
    namespace: &HashMap<String, monty::MontyObject>,
    contexts: &HashMap<String, String>,
) -> (Vec<String>, Vec<monty::MontyObject>, HashSet<String>) {
    use monty::MontyObject;

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

    let mut input_names: Vec<String> = Vec::new();
    let mut inputs: Vec<MontyObject> = Vec::new();

    // Inject files/context as Monty inputs to avoid massive string literals in source.
    reserved.insert("files".to_string());
    input_names.push("files".to_string());
    inputs.push(MontyObject::dict(
        files
            .iter()
            .map(|(name, content)| {
                (
                    MontyObject::String(name.clone()),
                    MontyObject::String(content.clone()),
                )
            })
            .collect::<Vec<_>>(),
    ));

    reserved.insert("context_files".to_string());
    input_names.push("context_files".to_string());
    inputs.push(MontyObject::List(
        files
            .iter()
            .map(|(name, _)| MontyObject::String(name.clone()))
            .collect(),
    ));

    reserved.insert("context".to_string());
    input_names.push("context".to_string());
    inputs.push(MontyObject::String(default_context));

    for (idx, (_name, content)) in files.iter().enumerate() {
        let var = format!("context_{idx}");
        reserved.insert(var.clone());
        input_names.push(var);
        inputs.push(MontyObject::String(content.clone()));
    }

    // Persisted variables from previous runs (best-effort).
    let mut persisted_names: Vec<&String> = namespace.keys().collect();
    persisted_names.sort();
    for name in persisted_names {
        if name.starts_with('_') || reserved.contains(name) {
            continue;
        }
        if let Some(value) = namespace.get(name) {
            input_names.push(name.clone());
            inputs.push(value.clone());
        }
    }

    (input_names, inputs, reserved)
}

fn build_full_code(tracked_vars: &[String], reserved: &HashSet<String>, user_code: &str) -> String {
    // Epilogue: persist a selected set of names.
    //
    // Monty doesn't implement `locals()`/`globals()`, so we can't introspect variable names.
    // We instead persist `tracked_vars`, derived from the user's code (best-effort).
    let mut epilogue = String::new();
    epilogue.push_str("\n# Epilogue: collect selected variables\n");
    epilogue.push_str("def __persistable(v):\n");
    epilogue.push_str(
        "    return isinstance(v, (int, float, bool, str, list, dict, tuple, set, type(None)))\n",
    );
    epilogue.push_str("__vars__ = {}\n");
    for name in tracked_vars {
        if name.starts_with('_') || reserved.contains(name) {
            continue;
        }
        let key = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_string());
        epilogue.push_str("try:\n");
        epilogue.push_str("    if __persistable(");
        epilogue.push_str(name);
        epilogue.push_str("):\n");
        epilogue.push_str("        __vars__[");
        epilogue.push_str(&key);
        epilogue.push_str("] = ");
        epilogue.push_str(name);
        epilogue.push_str("\n");
        epilogue.push_str("except Exception:\n");
        epilogue.push_str("    pass\n");
    }
    epilogue.push_str("__vars__\n");

    let mut full = String::new();
    full.push_str(user_code);
    full.push('\n');
    full.push_str(&epilogue);
    full
}

fn discover_assigned_names(code: &str) -> HashSet<String> {
    let mut out = HashSet::new();

    for raw_line in code.lines() {
        let line = raw_line.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // def name(...):
        if let Some(rest) = line.strip_prefix("def ") {
            if let Some(name) = rest
                .split(|c: char| c == '(' || c.is_whitespace() || c == ':')
                .next()
            {
                if is_ident(name) {
                    out.insert(name.to_string());
                }
            }
            continue;
        }

        // class Name:
        if let Some(rest) = line.strip_prefix("class ") {
            if let Some(name) = rest
                .split(|c: char| c == '(' || c.is_whitespace() || c == ':')
                .next()
            {
                if is_ident(name) {
                    out.insert(name.to_string());
                }
            }
            continue;
        }

        // import a as b, c
        if let Some(rest) = line.strip_prefix("import ") {
            for item in rest.split(',') {
                let item = item.trim();
                if item.is_empty() {
                    continue;
                }
                let mut parts = item.split_whitespace();
                let module = parts.next().unwrap_or("");
                let mut name = module.split('.').next().unwrap_or("").to_string();
                if let Some(maybe_as) = parts.next() {
                    if maybe_as == "as" {
                        if let Some(alias) = parts.next() {
                            name = alias.to_string();
                        }
                    }
                }
                if is_ident(&name) {
                    out.insert(name);
                }
            }
            continue;
        }

        // from m import x as y, z
        if let Some(rest) = line.strip_prefix("from ") {
            if let Some((_, imports)) = rest.split_once(" import ") {
                for item in imports.split(',') {
                    let item = item.trim();
                    if item.is_empty() || item == "*" {
                        continue;
                    }
                    let mut parts = item.split_whitespace();
                    let imported = parts.next().unwrap_or("");
                    let mut name = imported.to_string();
                    if let Some(maybe_as) = parts.next() {
                        if maybe_as == "as" {
                            if let Some(alias) = parts.next() {
                                name = alias.to_string();
                            }
                        }
                    }
                    if is_ident(&name) {
                        out.insert(name);
                    }
                }
            }
            continue;
        }

        // for x in ...:
        if let Some(rest) = line.strip_prefix("for ") {
            if let Some((lhs, _)) = rest.split_once(" in ") {
                for token in lhs.split(',') {
                    let t = token.trim().trim_matches(|c: char| c == '(' || c == ')');
                    if is_ident(t) {
                        out.insert(t.to_string());
                    }
                }
            }
            continue;
        }

        // Simple/augmented assignments.
        if let Some(names) = try_extract_assignment_lhs_names(line) {
            for name in names {
                if is_ident(&name) {
                    out.insert(name);
                }
            }
        }
    }

    out
}

fn try_extract_assignment_lhs_names(line: &str) -> Option<Vec<String>> {
    // Quick path for common `name = ...` or `name += ...`
    let mut i = 0usize;
    let bytes = line.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // Parse leading identifier.
    if !is_ident_start(bytes[0]) {
        return None;
    }
    i += 1;
    while i < bytes.len() && is_ident_continue(bytes[i]) {
        i += 1;
    }
    let name = &line[..i];

    // Skip whitespace.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let rest = &line[i..];

    const OPS: &[&str] = &[
        "=", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=", "<<=", ">>=", "**=", "//=",
    ];
    if OPS.iter().any(|op| rest.starts_with(op)) {
        return Some(vec![name.to_string()]);
    }

    // Fallback: destructuring `a, b = ...`
    let eq = line.find('=')?;
    // Reject comparisons like `==`, `!=`, `<=`, `>=`, `:=`
    let after = &line[eq..];
    if after.starts_with("==")
        || after.starts_with("!=")
        || after.starts_with(">=")
        || after.starts_with("<=")
        || after.starts_with(":=")
    {
        return None;
    }

    let lhs = line[..eq].trim();
    if lhs.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    for token in lhs.split(',') {
        let t = token
            .trim()
            .trim_matches(|c: char| c == '(' || c == ')' || c == '[' || c == ']');
        if is_ident(t) {
            out.push(t.to_string());
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

fn is_ident(s: &str) -> bool {
    let mut bytes = s.as_bytes().iter().copied();
    let Some(first) = bytes.next() else {
        return false;
    };
    if !is_ident_start(first) {
        return false;
    }
    bytes.all(is_ident_continue)
}

fn is_ident_start(b: u8) -> bool {
    (b'A'..=b'Z').contains(&b) || (b'a'..=b'z').contains(&b) || b == b'_'
}

fn is_ident_continue(b: u8) -> bool {
    is_ident_start(b) || (b'0'..=b'9').contains(&b)
}

// Keep unused imports if the file is built without monty.
#[allow(dead_code)]
fn _keep_serde_json_imports(_: &Value) {}
