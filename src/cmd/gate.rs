use std::cmp::Ordering;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::cmd::r#assert::{
    self, AssertCommandArgs, AssertCommandResponse, AssertInputNormalizeMode, AssertPipelineTrace,
};
use crate::domain::rules::{AssertReport, MismatchEntry};
use crate::engine::r#assert as assert_engine;
use crate::io::{self, Format, IoError};

const GATE_SCHEMA_PRESETS: [&str; 2] = ["github-actions-jobs", "gitlab-ci-jobs"];

/// Input arguments for `gate schema` command execution API.
#[derive(Debug, Clone)]
pub struct GateSchemaCommandArgs {
    pub schema: PathBuf,
    pub input: Option<PathBuf>,
    pub from: Option<String>,
}

/// Preset IDs accepted by `gate schema --from`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateSchemaPreset {
    GithubActionsJobs,
    GitlabCiJobs,
}

impl GateSchemaPreset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GithubActionsJobs => "github-actions-jobs",
            Self::GitlabCiJobs => "gitlab-ci-jobs",
        }
    }

    fn normalize_mode(&self) -> AssertInputNormalizeMode {
        match self {
            Self::GithubActionsJobs => AssertInputNormalizeMode::GithubActionsJobs,
            Self::GitlabCiJobs => AssertInputNormalizeMode::GitlabCiJobs,
        }
    }

    fn stdin_format(&self) -> Format {
        Format::Yaml
    }
}

/// Resolve optional `gate schema --from` value into a known preset.
pub fn resolve_preset(raw: Option<&str>) -> Result<Option<GateSchemaPreset>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    if raw.trim().is_empty() {
        return Err("`--from` cannot be empty".to_string());
    }

    let preset = match raw {
        "github-actions-jobs" => GateSchemaPreset::GithubActionsJobs,
        "gitlab-ci-jobs" => GateSchemaPreset::GitlabCiJobs,
        other => {
            return Err(format!(
                "unsupported `--from` preset `{other}`; supported presets: {}",
                GATE_SCHEMA_PRESETS.join(", ")
            ));
        }
    };

    Ok(Some(preset))
}

/// Ordered pipeline-step names used for `gate schema --emit-pipeline` diagnostics.
pub fn schema_pipeline_steps() -> Vec<String> {
    vec![
        "gate_schema_ingest".to_string(),
        "gate_schema_validate".to_string(),
    ]
}

/// Determinism guards applied by `gate schema`.
pub fn schema_deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_schema_validation".to_string(),
        "gate_schema_exit_mapping_0_2_3_1".to_string(),
        "gate_schema_mismatch_order_stable".to_string(),
        "gate_schema_error_path_format_stable".to_string(),
        "gate_schema_preset_resolution_explicit".to_string(),
    ]
}

pub fn run_schema_with_stdin_and_trace<R: Read>(
    args: &GateSchemaCommandArgs,
    stdin: R,
) -> (AssertCommandResponse, AssertPipelineTrace) {
    let preset = match resolve_preset(args.from.as_deref()) {
        Ok(value) => value,
        Err(message) => {
            return (
                AssertCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                AssertPipelineTrace::default(),
            );
        }
    };

    let input = normalize_input_path(args.input.as_deref());
    let stdin_format = preset
        .as_ref()
        .map(GateSchemaPreset::stdin_format)
        .unwrap_or(Format::Json);
    let normalize_mode = preset.as_ref().map(GateSchemaPreset::normalize_mode);

    let assert_args = AssertCommandArgs {
        input: input.clone(),
        from: if input.is_some() && preset.is_none() {
            None
        } else {
            Some(stdin_format)
        },
        rules: None,
        schema: Some(args.schema.clone()),
    };

    r#assert::run_with_stdin_and_normalize_with_trace(&assert_args, stdin, normalize_mode)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourcePresetTransform {
    PassThrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourcePresetDefinition {
    name: &'static str,
    preset: GatePolicySourcePreset,
    transform: SourcePresetTransform,
}

const SOURCE_PRESET_REGISTRY: [SourcePresetDefinition; 5] = [
    SourcePresetDefinition {
        name: "scan-text",
        preset: GatePolicySourcePreset::ScanText,
        transform: SourcePresetTransform::PassThrough,
    },
    SourcePresetDefinition {
        name: "ingest-doc",
        preset: GatePolicySourcePreset::IngestDoc,
        transform: SourcePresetTransform::PassThrough,
    },
    SourcePresetDefinition {
        name: "ingest-api",
        preset: GatePolicySourcePreset::IngestApi,
        transform: SourcePresetTransform::PassThrough,
    },
    SourcePresetDefinition {
        name: "ingest-notes",
        preset: GatePolicySourcePreset::IngestNotes,
        transform: SourcePresetTransform::PassThrough,
    },
    SourcePresetDefinition {
        name: "ingest-book",
        preset: GatePolicySourcePreset::IngestBook,
        transform: SourcePresetTransform::PassThrough,
    },
];

/// Supported gate source preset names.
pub const SUPPORTED_SOURCE_PRESETS: [&str; 5] = [
    "scan-text",
    "ingest-doc",
    "ingest-api",
    "ingest-notes",
    "ingest-book",
];

/// Source preset used by `gate policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatePolicySourcePreset {
    ScanText,
    IngestDoc,
    IngestApi,
    IngestNotes,
    IngestBook,
}

impl GatePolicySourcePreset {
    pub fn as_str(self) -> &'static str {
        source_definition_for_preset(self).name
    }

    pub fn parse_cli_name(value: &str) -> Result<Self, String> {
        SOURCE_PRESET_REGISTRY
            .iter()
            .find(|definition| definition.name == value)
            .map(|definition| definition.preset)
            .ok_or_else(|| {
                format!(
                    "unknown source `{value}`: expected one of {}",
                    SUPPORTED_SOURCE_PRESETS
                        .iter()
                        .map(|name| format!("`{name}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            })
    }
}

/// Input arguments for `gate policy` command execution API.
#[derive(Debug, Clone)]
pub struct GatePolicyCommandArgs {
    pub rules: PathBuf,
    pub input: Option<PathBuf>,
    pub source: Option<GatePolicySourcePreset>,
}

/// Structured command response for `gate policy` execution.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GatePolicyCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct GatePolicyReport {
    matched: bool,
    violations: usize,
    details: Vec<GatePolicyViolation>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct GatePolicyViolation {
    path: String,
    rule_id: String,
    message: String,
    actual: Value,
    expected: Value,
}

pub fn run_policy_with_stdin<R: Read>(
    args: &GatePolicyCommandArgs,
    stdin: R,
) -> GatePolicyCommandResponse {
    match execute_policy(args, stdin) {
        Ok(report) => {
            let exit_code = if report.matched { 0 } else { 2 };
            match serde_json::to_value(report) {
                Ok(payload) => GatePolicyCommandResponse { exit_code, payload },
                Err(error) => GatePolicyCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": format!("failed to serialize gate policy report: {error}"),
                    }),
                },
            }
        }
        Err(CommandError::InputUsage(message)) => GatePolicyCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": message,
            }),
        },
        Err(CommandError::Internal(message)) => GatePolicyCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": message,
            }),
        },
    }
}

fn execute_policy<R: Read>(
    args: &GatePolicyCommandArgs,
    stdin: R,
) -> Result<GatePolicyReport, CommandError> {
    let rules =
        r#assert::load_rules_from_path(args.rules.as_path()).map_err(CommandError::InputUsage)?;
    let values = load_policy_input_values(args, stdin)?;
    let values = apply_source_preset(values, args.source)?;
    let report = assert_engine::execute_assert(&values, &rules).map_err(map_assert_error)?;
    Ok(to_gate_policy_report(report))
}

fn load_policy_input_values<R: Read>(
    args: &GatePolicyCommandArgs,
    stdin: R,
) -> Result<Vec<Value>, CommandError> {
    if let Some(path) = args.input.as_deref()
        && !is_stdin_path(path)
    {
        let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
            CommandError::InputUsage(format!(
                "unable to resolve gate input format from `{}`: {error}",
                path.display()
            ))
        })?;
        let file = File::open(path).map_err(|error| {
            CommandError::InputUsage(format!(
                "failed to open gate input file `{}`: {error}",
                path.display()
            ))
        })?;
        return io::reader::read_values(file, format).map_err(map_io_as_input_usage);
    }

    io::reader::read_values(stdin, Format::Json).map_err(map_io_as_input_usage)
}

/// Treat `-` as stdin for command-level input resolution.
pub fn is_stdin_path(path: &Path) -> bool {
    path == Path::new("-") || path == Path::new("/dev/stdin")
}

fn normalize_input_path(path: Option<&Path>) -> Option<PathBuf> {
    path.and_then(|value| {
        if is_stdin_path(value) {
            None
        } else {
            Some(value.to_path_buf())
        }
    })
}

fn apply_source_preset(
    values: Vec<Value>,
    source: Option<GatePolicySourcePreset>,
) -> Result<Vec<Value>, CommandError> {
    let Some(source) = source else {
        return Ok(values);
    };

    let definition = source_definition_for_preset(source);
    match definition.transform {
        SourcePresetTransform::PassThrough => Ok(values),
    }
}

fn source_definition_for_preset(preset: GatePolicySourcePreset) -> &'static SourcePresetDefinition {
    SOURCE_PRESET_REGISTRY
        .iter()
        .find(|definition| definition.preset == preset)
        .expect("source preset must exist in registry")
}

fn map_io_as_input_usage(error: IoError) -> CommandError {
    CommandError::InputUsage(error.to_string())
}

fn map_assert_error(error: assert_engine::AssertValidationError) -> CommandError {
    match error {
        assert_engine::AssertValidationError::InputUsage(message) => {
            CommandError::InputUsage(message)
        }
        assert_engine::AssertValidationError::Internal(message) => CommandError::Internal(message),
    }
}

fn to_gate_policy_report(report: AssertReport) -> GatePolicyReport {
    let mut details: Vec<GatePolicyViolation> = report
        .mismatches
        .into_iter()
        .map(to_gate_policy_violation)
        .collect();

    details.sort_by(compare_violations);

    let violations = details.len();
    GatePolicyReport {
        matched: violations == 0,
        violations,
        details,
    }
}

fn to_gate_policy_violation(entry: MismatchEntry) -> GatePolicyViolation {
    let rule_id = format!("{}.{}", entry.rule_kind, entry.reason);
    GatePolicyViolation {
        message: format!("policy_violation[path={}][rule_id={}]", entry.path, rule_id),
        path: entry.path,
        rule_id,
        actual: entry.actual,
        expected: entry.expected,
    }
}

fn compare_violations(left: &GatePolicyViolation, right: &GatePolicyViolation) -> Ordering {
    left.path
        .cmp(&right.path)
        .then(left.rule_id.cmp(&right.rule_id))
        .then(left.message.cmp(&right.message))
        .then(compare_value(&left.actual, &right.actual))
        .then(compare_value(&left.expected, &right.expected))
}

fn compare_value(left: &Value, right: &Value) -> Ordering {
    let left_key = serde_json::to_string(left).unwrap_or_default();
    let right_key = serde_json::to_string(right).unwrap_or_default();
    left_key.cmp(&right_key)
}

/// Ordered pipeline-step names used for `gate policy --emit-pipeline` diagnostics.
pub fn policy_pipeline_steps() -> Vec<String> {
    vec![
        "gate_policy_source".to_string(),
        "gate_policy_assert_rules".to_string(),
    ]
}

/// Determinism guards planned for the `gate policy` command.
pub fn policy_deterministic_guards(source: Option<GatePolicySourcePreset>) -> Vec<String> {
    let mut guards = vec![
        "rust_native_execution".to_string(),
        "gate_policy_source_registry_static".to_string(),
        "gate_policy_violation_order_path_then_rule_id".to_string(),
        "gate_policy_message_template_fixed".to_string(),
    ];
    if let Some(source) = source {
        guards.push(format!(
            "gate_policy_source_preset_{}",
            source.as_str().replace('-', "_")
        ));
    }
    guards
}

#[derive(Debug)]
enum CommandError {
    InputUsage(String),
    Internal(String),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        GatePolicyCommandArgs, GatePolicySourcePreset, GateSchemaPreset, is_stdin_path,
        resolve_preset, run_policy_with_stdin,
    };

    #[test]
    fn resolves_supported_schema_presets() {
        assert_eq!(
            resolve_preset(Some("github-actions-jobs")).expect("preset"),
            Some(GateSchemaPreset::GithubActionsJobs)
        );
        assert_eq!(
            resolve_preset(Some("gitlab-ci-jobs")).expect("preset"),
            Some(GateSchemaPreset::GitlabCiJobs)
        );
    }

    #[test]
    fn rejects_unknown_schema_preset_with_actionable_error() {
        let error = resolve_preset(Some("unknown")).expect_err("unknown preset should fail");
        assert!(error.contains("unsupported `--from` preset `unknown`"));
        assert!(error.contains("github-actions-jobs"));
        assert!(error.contains("gitlab-ci-jobs"));
    }

    #[test]
    fn source_parser_reports_unknown_value_clearly() {
        let error = GatePolicySourcePreset::parse_cli_name("unknown").expect_err("unknown source");
        assert!(error.contains("unknown source `unknown`"));
        assert!(error.contains("`scan-text`"));
        assert!(error.contains("`ingest-book`"));
    }

    #[test]
    fn gate_policy_reports_violation_count_with_sorted_details() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["id", "score"],
                "forbid_keys": ["meta.blocked"],
                "fields": {
                    "id": {"type": "integer"},
                    "score": {"type": "number", "range": {"min": 0, "max": 10}}
                },
                "count": {"min": 1, "max": 1}
            }"#,
        )
        .expect("write rules");

        let args = GatePolicyCommandArgs {
            rules: rules_path,
            input: None,
            source: Some(GatePolicySourcePreset::ScanText),
        };
        let response = run_policy_with_stdin(
            &args,
            Cursor::new(r#"[{"score": 20, "id": "x", "meta": {"blocked": true}}]"#),
        );

        assert_eq!(response.exit_code, 2);
        assert_eq!(response.payload["matched"], json!(false));
        assert_eq!(response.payload["violations"], json!(3));
        let details = response.payload["details"].as_array().expect("details");
        assert_eq!(details[0]["path"], json!("$[0].id"));
        assert_eq!(details[1]["path"], json!("$[0].meta.blocked"));
        assert_eq!(details[2]["path"], json!("$[0].score"));
    }

    #[test]
    fn treats_dash_as_stdin_path() {
        assert!(is_stdin_path(std::path::Path::new("-")));
        assert!(is_stdin_path(std::path::Path::new("/dev/stdin")));
        assert!(!is_stdin_path(std::path::Path::new("./input.json")));
    }
}
