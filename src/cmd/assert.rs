use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, mlr, yq};
use crate::domain::report::PipelineStageDiagnostic;
use crate::domain::rules::{AssertReport, AssertRules};
use crate::engine::r#assert::{self, AssertValidationError};
use crate::io::{self, Format, IoError};

/// Input arguments for assert command execution API.
#[derive(Debug, Clone)]
pub struct AssertCommandArgs {
    pub input: Option<PathBuf>,
    pub from: Option<Format>,
    pub rules: Option<PathBuf>,
    pub schema: Option<PathBuf>,
}

/// Optional input normalization profile applied before assert validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssertInputNormalizeMode {
    GithubActionsJobs,
    GitlabCiJobs,
}

impl AssertInputNormalizeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GithubActionsJobs => "github-actions-jobs",
            Self::GitlabCiJobs => "gitlab-ci-jobs",
        }
    }
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AssertCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for assert normalization stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssertPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl AssertPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps(normalize: Option<AssertInputNormalizeMode>) -> Vec<String> {
    let mut steps = vec![
        "load_rules".to_string(),
        "resolve_input_format".to_string(),
        "read_input_values".to_string(),
        "validate_assert_rules".to_string(),
    ];
    if normalize.is_some() {
        steps.insert(3, "normalize_assert_input".to_string());
    }
    steps
}

/// Determinism guards applied by the `assert` command.
pub fn deterministic_guards(normalize: Option<AssertInputNormalizeMode>) -> Vec<String> {
    let mut guards = vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "rules_schema_deny_unknown_fields".to_string(),
    ];
    if let Some(mode) = normalize {
        guards.push("normalize_pipeline_stage_order_yq_jq_mlr".to_string());
        guards.push("normalize_pipeline_deterministic_sort_mlr".to_string());
        guards.push(format!(
            "assert_input_normalized_{}",
            mode.as_str().replace('-', "_")
        ));
    }
    guards
}

/// Machine-readable help payload for `assert --rules` rule files.
pub fn rules_help_payload() -> Value {
    json!({
        "schema": "dataq.assert.rules.v1",
        "description": "Rule file schema for `dataq assert --rules`",
        "top_level_keys": {
            "extends": "string | array<string> (optional, parent-relative path)",
            "required_keys": "array<string>",
            "forbid_keys": "array<string>",
            "fields": "object<string, field_rule>",
            "count": {
                "min": "usize (optional)",
                "max": "usize (optional)"
            }
        },
        "field_rule": {
            "type": "string|number|integer|boolean|object|array|null (optional)",
            "nullable": "bool (optional)",
            "enum": "array<any> (optional)",
            "pattern": "string regex (optional)",
            "range": {
                "min": "number (optional)",
                "max": "number (optional)"
            }
        },
        "path_notation": "dot-delimited object path (example: meta.blocked)",
        "constraints": [
            "extends entries resolve relative to the referencing rules file",
            "extends references are applied before the current file (current file wins)",
            "fields.<path> must define at least one of type/nullable/enum/pattern/range",
            "count.min must be <= count.max",
            "fields.<path>.range.min must be <= fields.<path>.range.max",
            "unknown keys are rejected"
        ],
        "example": {
            "extends": ["./base.rules.yaml"],
            "required_keys": ["id", "status"],
            "forbid_keys": ["debug", "meta.blocked"],
            "fields": {
                "id": {
                    "type": "integer"
                },
                "score": {
                    "type": "number",
                    "nullable": true,
                    "range": {
                        "min": 0,
                        "max": 100
                    }
                },
                "status": {
                    "enum": ["active", "archived"]
                },
                "name": {
                    "pattern": "^[a-z]+_[0-9]+$"
                }
            },
            "count": {
                "min": 1,
                "max": 1000
            }
        }
    })
}

/// Machine-readable help payload for `assert --schema` JSON Schema mode.
pub fn schema_help_payload() -> Value {
    json!({
        "schema": "dataq.assert.schema_help.v1",
        "description": "JSON Schema validation help for `dataq assert --schema`",
        "mode": {
            "validator": "jsonschema crate (Rust native)",
            "input_contract": "schema file must contain exactly one JSON/YAML value",
            "source_selection": "`--schema` and `--rules` are mutually exclusive"
        },
        "usage": [
            "dataq assert --schema schema.json < input.json",
            "dataq assert --input input.json --schema schema.json"
        ],
        "result_contract": {
            "exit_code_0": "all rows matched schema",
            "exit_code_2": "one or more mismatches",
            "exit_code_3": "input/usage error (for example invalid schema)"
        },
        "mismatch_shape": {
            "path": "$[row].<field> (canonicalized from JSON Pointer)",
            "rule_kind": "schema",
            "reason": "schema_mismatch",
            "actual": "actual value at instance path",
            "expected": {
                "schema_path": "JSON Pointer into schema",
                "message": "validator error message"
            }
        },
        "example_schema": {
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": { "type": "integer" },
                "score": { "type": "number", "maximum": 10 }
            }
        }
    })
}

pub fn run_with_stdin<R: Read>(args: &AssertCommandArgs, stdin: R) -> AssertCommandResponse {
    run_with_stdin_and_normalize(args, stdin, None)
}

pub fn run_with_stdin_and_normalize<R: Read>(
    args: &AssertCommandArgs,
    stdin: R,
    normalize: Option<AssertInputNormalizeMode>,
) -> AssertCommandResponse {
    run_with_stdin_and_normalize_with_trace(args, stdin, normalize).0
}

pub fn run_with_stdin_and_normalize_with_trace<R: Read>(
    args: &AssertCommandArgs,
    stdin: R,
    normalize: Option<AssertInputNormalizeMode>,
) -> (AssertCommandResponse, AssertPipelineTrace) {
    match execute(args, stdin, normalize) {
        Ok(result) => (report_response(result.report), result.trace),
        Err(error) => {
            let response = match error.kind {
                CommandErrorKind::InputUsage(message) => AssertCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                CommandErrorKind::Internal(message) => AssertCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": message,
                    }),
                },
            };
            (response, error.trace)
        }
    }
}

fn execute<R: Read>(
    args: &AssertCommandArgs,
    stdin: R,
    normalize: Option<AssertInputNormalizeMode>,
) -> Result<ExecuteResult, CommandError> {
    let source = resolve_validation_source(args)?;
    let input_format = io::resolve_input_format(args.from, args.input.as_deref())
        .map_err(map_io_as_input_usage)?;
    let values = load_input_values(args, stdin, input_format)?;
    let (values, trace) = normalize_input_values(values, normalize)?;
    let report = match source {
        ValidationSource::Rules(rules) => assert::execute_assert(&values, &rules),
        ValidationSource::Schema(schema) => assert::execute_assert_with_schema(&values, &schema),
    }
    .map_err(map_engine_error)?;
    Ok(ExecuteResult { report, trace })
}

enum ValidationSource {
    Rules(AssertRules),
    Schema(Value),
}

fn resolve_validation_source(args: &AssertCommandArgs) -> Result<ValidationSource, CommandError> {
    match (&args.rules, &args.schema) {
        (Some(_), Some(_)) => Err(CommandError::input_usage(
            "`--rules` and `--schema` are mutually exclusive".to_string(),
        )),
        (None, None) => Err(CommandError::input_usage(
            "either `--rules` or `--schema` must be provided".to_string(),
        )),
        (Some(rules_path), None) => load_rules(rules_path.as_path()).map(ValidationSource::Rules),
        (None, Some(schema_path)) => {
            load_schema(schema_path.as_path()).map(ValidationSource::Schema)
        }
    }
}

fn load_rules(path: &Path) -> Result<AssertRules, CommandError> {
    let mut stack = Vec::new();
    let resolved = load_rules_recursive(path, &mut stack)?;
    Ok(resolved.rules)
}

#[derive(Debug, Clone)]
struct ResolvedAssertRules {
    rules: AssertRules,
    has_count: bool,
}

fn load_rules_recursive(
    path: &Path,
    stack: &mut Vec<PathBuf>,
) -> Result<ResolvedAssertRules, CommandError> {
    let (raw_rules, has_count) = load_rules_file(path)?;
    let canonical_path = path.canonicalize().map_err(|err| {
        CommandError::input_usage(format!(
            "failed to canonicalize rules file `{}`: {err}",
            path.display()
        ))
    })?;

    if let Some(cycle_start) = stack
        .iter()
        .position(|existing| existing == &canonical_path)
    {
        let mut cycle_chain: Vec<String> = stack[cycle_start..]
            .iter()
            .map(|entry| entry.display().to_string())
            .collect();
        cycle_chain.push(canonical_path.display().to_string());
        return Err(CommandError::input_usage(format!(
            "rules extends cycle detected: {}",
            cycle_chain.join(" -> ")
        )));
    }

    stack.push(canonical_path.clone());
    let result = (|| {
        let mut merged = ResolvedAssertRules {
            rules: AssertRules::default(),
            has_count: false,
        };
        if let Some(extends) = raw_rules.extends.clone() {
            for extends_entry in extends.into_paths() {
                let extended_path =
                    resolve_extended_rules_path(&canonical_path, extends_entry.as_str());
                let extended = load_rules_recursive(extended_path.as_path(), stack)?;
                merged = merge_resolved_rules(merged, extended);
            }
        }

        let current = ResolvedAssertRules {
            rules: AssertRules {
                extends: None,
                required_keys: raw_rules.required_keys,
                forbid_keys: raw_rules.forbid_keys,
                fields: raw_rules.fields,
                count: raw_rules.count,
            },
            has_count,
        };
        Ok(merge_resolved_rules(merged, current))
    })();
    stack.pop();
    result
}

fn resolve_extended_rules_path(current_file_path: &Path, extends_entry: &str) -> PathBuf {
    let extends_path = Path::new(extends_entry);
    if extends_path.is_absolute() {
        extends_path.to_path_buf()
    } else {
        current_file_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(extends_path)
    }
}

fn merge_resolved_rules(
    base: ResolvedAssertRules,
    overlay: ResolvedAssertRules,
) -> ResolvedAssertRules {
    let required_keys = merge_unique_paths(base.rules.required_keys, overlay.rules.required_keys);
    let forbid_keys = merge_unique_paths(base.rules.forbid_keys, overlay.rules.forbid_keys);
    let mut fields = base.rules.fields;
    for (path, rule) in overlay.rules.fields {
        fields.insert(path, rule);
    }
    let count = if overlay.has_count {
        overlay.rules.count
    } else {
        base.rules.count
    };

    ResolvedAssertRules {
        rules: AssertRules {
            extends: None,
            required_keys,
            forbid_keys,
            fields,
            count,
        },
        has_count: base.has_count || overlay.has_count,
    }
}

fn merge_unique_paths(left: Vec<String>, right: Vec<String>) -> Vec<String> {
    let mut merged = BTreeSet::new();
    merged.extend(left);
    merged.extend(right);
    merged.into_iter().collect()
}

fn load_rules_file(path: &Path) -> Result<(AssertRules, bool), CommandError> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|err| {
        CommandError::input_usage(format!(
            "unable to resolve rules format from `{}`: {err}",
            path.display()
        ))
    })?;
    let file = File::open(path).map_err(|err| {
        CommandError::input_usage(format!(
            "failed to open rules file `{}`: {err}",
            path.display()
        ))
    })?;
    let values = io::reader::read_values(file, format).map_err(map_io_as_input_usage)?;
    if values.len() != 1 {
        return Err(CommandError::input_usage(
            "rules file must contain exactly one object".to_string(),
        ));
    }
    let rules_value = values.into_iter().next().unwrap_or(Value::Null);
    let has_count = rules_value
        .as_object()
        .map(|object| object.contains_key("count"))
        .unwrap_or(false);
    let rules: AssertRules = serde_json::from_value(rules_value)
        .map_err(|err| CommandError::input_usage(format!("invalid rules schema: {err}")))?;
    Ok((rules, has_count))
}

fn load_schema(path: &Path) -> Result<Value, CommandError> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|err| {
        CommandError::input_usage(format!(
            "unable to resolve schema format from `{}`: {err}",
            path.display()
        ))
    })?;
    let file = File::open(path).map_err(|err| {
        CommandError::input_usage(format!(
            "failed to open schema file `{}`: {err}",
            path.display()
        ))
    })?;
    let values = io::reader::read_values(file, format).map_err(map_io_as_input_usage)?;
    if values.len() != 1 {
        return Err(CommandError::input_usage(
            "schema file must contain exactly one value".to_string(),
        ));
    }
    Ok(values.into_iter().next().unwrap_or(Value::Null))
}

fn load_input_values<R: Read>(
    args: &AssertCommandArgs,
    stdin: R,
    format: Format,
) -> Result<Vec<Value>, CommandError> {
    if let Some(path) = &args.input {
        let file = File::open(path).map_err(|err| {
            CommandError::input_usage(format!(
                "failed to open input file `{}`: {err}",
                path.display()
            ))
        })?;
        io::reader::read_values(file, format).map_err(map_io_as_input_usage)
    } else {
        io::reader::read_values(stdin, format).map_err(map_io_as_input_usage)
    }
}

fn map_io_as_input_usage(error: IoError) -> CommandError {
    CommandError::input_usage(error.to_string())
}

fn map_engine_error(error: AssertValidationError) -> CommandError {
    match error {
        AssertValidationError::InputUsage(message) => CommandError::input_usage(message),
        AssertValidationError::Internal(message) => CommandError::internal(message),
    }
}

fn report_response(report: AssertReport) -> AssertCommandResponse {
    let exit_code = if report.matched { 0 } else { 2 };
    match serde_json::to_value(&report) {
        Ok(payload) => AssertCommandResponse { exit_code, payload },
        Err(_) => AssertCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": "failed to serialize assert report"
            }),
        },
    }
}

struct ExecuteResult {
    report: AssertReport,
    trace: AssertPipelineTrace,
}

struct CommandError {
    kind: CommandErrorKind,
    trace: AssertPipelineTrace,
}

enum CommandErrorKind {
    InputUsage(String),
    Internal(String),
}

impl CommandError {
    fn input_usage(message: String) -> Self {
        Self {
            kind: CommandErrorKind::InputUsage(message),
            trace: AssertPipelineTrace::default(),
        }
    }

    fn input_usage_with_trace(message: String, trace: AssertPipelineTrace) -> Self {
        Self {
            kind: CommandErrorKind::InputUsage(message),
            trace,
        }
    }

    fn internal(message: String) -> Self {
        Self {
            kind: CommandErrorKind::Internal(message),
            trace: AssertPipelineTrace::default(),
        }
    }
}

fn normalize_input_values(
    values: Vec<Value>,
    normalize: Option<AssertInputNormalizeMode>,
) -> Result<(Vec<Value>, AssertPipelineTrace), CommandError> {
    match normalize {
        None => Ok((values, AssertPipelineTrace::default())),
        Some(mode) => normalize_with_pipeline(values, mode),
    }
}

fn normalize_with_pipeline(
    values: Vec<Value>,
    mode: AssertInputNormalizeMode,
) -> Result<(Vec<Value>, AssertPipelineTrace), CommandError> {
    let mut trace = AssertPipelineTrace::default();

    trace.mark_tool_used("yq");
    let yq_input_rows = values.len();
    let yq_rows = match mode {
        AssertInputNormalizeMode::GithubActionsJobs => yq::extract_github_actions_jobs(&values),
        AssertInputNormalizeMode::GitlabCiJobs => yq::extract_gitlab_ci_jobs(&values),
    };
    let yq_rows = match yq_rows {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "normalize_yq_extract",
                    "yq",
                    yq_input_rows,
                    rows.len(),
                ));
            rows
        }
        Err(yq::YqError::Unavailable) => {
            let message = format!("normalize mode `{}` requires `yq` in PATH", mode.as_str());
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "normalize_yq_extract",
                    "yq",
                    yq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(message, trace));
        }
        Err(error) => {
            let message = format!(
                "failed to normalize assert input with yq (`{}`): {error}",
                mode.as_str()
            );
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "normalize_yq_extract",
                    "yq",
                    yq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(message, trace));
        }
    };

    trace.mark_tool_used("jq");
    let jq_input_rows = yq_rows.len();
    let jq_rows = match mode {
        AssertInputNormalizeMode::GithubActionsJobs => jq::normalize_github_actions_jobs(&yq_rows),
        AssertInputNormalizeMode::GitlabCiJobs => jq::normalize_gitlab_ci_jobs(&yq_rows),
    };
    let jq_rows = match jq_rows {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    "normalize_jq_project",
                    "jq",
                    jq_input_rows,
                    rows.len(),
                ));
            rows
        }
        Err(jq::JqError::Unavailable) => {
            let message = format!("normalize mode `{}` requires `jq` in PATH", mode.as_str());
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "normalize_jq_project",
                    "jq",
                    jq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(message, trace));
        }
        Err(error) => {
            let message = format!(
                "failed to normalize assert input with jq (`{}`): {error}",
                mode.as_str()
            );
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "normalize_jq_project",
                    "jq",
                    jq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(message, trace));
        }
    };

    trace.mark_tool_used("mlr");
    let mlr_input_rows = jq_rows.len();
    let mlr_rows = match mode {
        AssertInputNormalizeMode::GithubActionsJobs => mlr::sort_github_actions_jobs(&jq_rows),
        AssertInputNormalizeMode::GitlabCiJobs => mlr::sort_gitlab_ci_jobs(&jq_rows),
    };
    match mlr_rows {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    3,
                    "normalize_mlr_sort",
                    "mlr",
                    mlr_input_rows,
                    rows.len(),
                ));
            Ok((rows, trace))
        }
        Err(mlr::MlrError::Unavailable) => {
            let message = format!("normalize mode `{}` requires `mlr` in PATH", mode.as_str());
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "normalize_mlr_sort",
                    "mlr",
                    mlr_input_rows,
                ));
            Err(CommandError::input_usage_with_trace(message, trace))
        }
        Err(error) => {
            let message = format!(
                "failed to normalize assert input with mlr (`{}`): {error}",
                mode.as_str()
            );
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "normalize_mlr_sort",
                    "mlr",
                    mlr_input_rows,
                ));
            Err(CommandError::input_usage_with_trace(message, trace))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::process::Command;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::cmd::r#assert::{AssertCommandArgs, run_with_stdin};
    use crate::io::Format;

    #[test]
    fn maps_success_to_exit_zero() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["id"],
                "fields": {"id": {"type": "integer"}},
                "count": {"min": 1, "max": 1},
                "forbid_keys": []
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: None,
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1}]"#));
        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["matched"], json!(true));
    }

    #[test]
    fn maps_mismatch_to_exit_two() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["id"],
                "fields": {"id": {"type": "integer"}},
                "count": {"min": 1, "max": 1},
                "forbid_keys": []
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: None,
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"oops"}]"#));
        assert_eq!(response.exit_code, 2);
        assert_eq!(response.payload["mismatch_count"], json!(1));
    }

    #[test]
    fn maps_input_usage_to_exit_three() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.invalid");
        std::fs::write(&rules_path, "{}").expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: None,
        };

        let response = run_with_stdin(&args, Cursor::new("[]"));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn maps_schema_mismatch_to_exit_two() {
        let dir = tempdir().expect("tempdir");
        let schema_path = dir.path().join("schema.json");
        std::fs::write(
            &schema_path,
            r#"{
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "integer"}
                }
            }"#,
        )
        .expect("write schema");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: None,
            schema: Some(schema_path),
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"oops"}]"#));
        assert_eq!(response.exit_code, 2);
        assert_eq!(response.payload["mismatch_count"], json!(1));
        assert_eq!(
            response.payload["mismatches"][0]["reason"],
            json!("schema_mismatch")
        );
    }

    #[test]
    fn maps_rules_schema_conflict_to_exit_three() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        let schema_path = dir.path().join("schema.json");
        std::fs::write(&rules_path, "{}").expect("write rules");
        std::fs::write(&schema_path, "{}").expect("write schema");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: Some(schema_path),
        };

        let response = run_with_stdin(&args, Cursor::new("[]"));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn github_actions_jobs_normalizer_extracts_job_rows() {
        if !normalize_tools_available() {
            return;
        }
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["job_id", "runs_on", "steps_count", "uses_unpinned_action"],
                "fields": {
                    "job_id": {"type": "string"},
                    "runs_on": {"type": "string"},
                    "steps_count": {"type": "integer", "range": {"min": 1}},
                    "uses_unpinned_action": {"type": "boolean", "enum": [false]}
                },
                "count": {"min": 1}
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Yaml),
            rules: Some(rules_path),
            schema: None,
        };
        let input = r#"
name: CI
on:
  push: {}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#;

        let response = super::run_with_stdin_and_normalize(
            &args,
            Cursor::new(input),
            Some(super::AssertInputNormalizeMode::GithubActionsJobs),
        );
        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["matched"], json!(true));
    }

    #[test]
    fn gitlab_jobs_normalizer_extracts_job_rows() {
        if !normalize_tools_available() {
            return;
        }
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["job_name", "stage", "script_count", "uses_only_except"],
                "fields": {
                    "job_name": {"type": "string"},
                    "stage": {"type": "string"},
                    "script_count": {"type": "integer", "range": {"min": 1}},
                    "uses_only_except": {"type": "boolean", "enum": [false]}
                },
                "count": {"min": 1}
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Yaml),
            rules: Some(rules_path),
            schema: None,
        };
        let input = r#"
stages: [build]
build:
  stage: build
  script:
    - echo ok
"#;

        let response = super::run_with_stdin_and_normalize(
            &args,
            Cursor::new(input),
            Some(super::AssertInputNormalizeMode::GitlabCiJobs),
        );
        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["matched"], json!(true));
    }

    fn normalize_tools_available() -> bool {
        Command::new("jq").arg("--version").output().is_ok()
            && Command::new("yq").arg("--version").output().is_ok()
            && Command::new("mlr").arg("--version").output().is_ok()
    }
}
