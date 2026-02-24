use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, rg};
use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::scan::ScanTextMatch;

/// Input arguments for `scan text` command execution API.
#[derive(Debug, Clone)]
pub struct ScanTextCommandArgs {
    pub pattern: String,
    pub path: PathBuf,
    pub glob: Vec<String>,
    pub max_matches: Option<usize>,
    pub policy_mode: bool,
    pub jq_project: bool,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ScanTextCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for scan stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanTextPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl ScanTextPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ScanTextSummary {
    total_matches: usize,
    returned_matches: usize,
    files_with_matches: usize,
    truncated: bool,
    policy_mode: bool,
    forbidden_matches: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ScanTextReport {
    matches: Vec<ScanTextMatch>,
    summary: ScanTextSummary,
}

pub fn run_with_trace(
    args: &ScanTextCommandArgs,
) -> (ScanTextCommandResponse, ScanTextPipelineTrace) {
    let mut trace = ScanTextPipelineTrace::default();

    if args.pattern.trim().is_empty() {
        return (
            ScanTextCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": "`pattern` cannot be empty",
                }),
            },
            trace,
        );
    }

    let invocation_root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            return (
                ScanTextCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": format!("failed to resolve current working directory: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    trace.mark_tool_used("rg");
    let rg_args = rg::RgCommandArgs {
        pattern: args.pattern.as_str(),
        path: args.path.as_path(),
        globs: &args.glob,
    };
    let raw_output = match rg::execute_json(&rg_args) {
        Ok(output) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "scan_text_rg_execute",
                    "rg",
                    0,
                    output.lines().count(),
                ));
            output
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "scan_text_rg_execute",
                    "rg",
                    0,
                ));
            let message = match error {
                rg::RgError::Unavailable => "scan text requires `rg` in PATH".to_string(),
                rg::RgError::Execution(message) => {
                    format!("failed to scan text with rg: {message}")
                }
                rg::RgError::Spawn(source) => format!("failed to spawn rg: {source}"),
                rg::RgError::Utf8(source) => format!("failed to decode rg output: {source}"),
            };
            return (
                ScanTextCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                trace,
            );
        }
    };

    let mut matches =
        match crate::engine::scan::parse_rg_json_stream(&raw_output, invocation_root.as_path()) {
            Ok(matches) => {
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::success(
                        2,
                        "scan_text_parse",
                        "rust",
                        raw_output.lines().count(),
                        matches.len(),
                    ));
                matches
            }
            Err(error) => {
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::failure(
                        2,
                        "scan_text_parse",
                        "rust",
                        raw_output.lines().count(),
                    ));
                return (
                    ScanTextCommandResponse {
                        exit_code: 1,
                        payload: json!({
                            "error": "internal_error",
                            "message": format!("failed to parse rg output: {error}"),
                        }),
                    },
                    trace,
                );
            }
        };

    if args.jq_project {
        trace.mark_tool_used("jq");
        let projected = match project_with_jq(&matches) {
            Ok(projected) => {
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::success(
                        3,
                        "scan_text_jq_project",
                        "jq",
                        matches.len(),
                        projected.len(),
                    ));
                projected
            }
            Err(error) => {
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::failure(
                        3,
                        "scan_text_jq_project",
                        "jq",
                        matches.len(),
                    ));
                return (
                    ScanTextCommandResponse {
                        exit_code: 3,
                        payload: json!({
                            "error": "input_usage_error",
                            "message": format!("failed to project scan matches with jq: {error}"),
                        }),
                    },
                    trace,
                );
            }
        };
        matches = projected;
    }

    let total_matches = matches.len();
    if let Some(limit) = args.max_matches {
        if matches.len() > limit {
            matches.truncate(limit);
        }
    }
    let returned_matches = matches.len();

    let files_with_matches = matches
        .iter()
        .map(|entry| entry.path.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let truncated = returned_matches < total_matches;
    let summary = ScanTextSummary {
        total_matches,
        returned_matches,
        files_with_matches,
        truncated,
        policy_mode: args.policy_mode,
        forbidden_matches: total_matches,
    };
    let report = ScanTextReport { matches, summary };
    let payload = match serde_json::to_value(report) {
        Ok(payload) => payload,
        Err(error) => {
            return (
                ScanTextCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": format!("failed to serialize scan report: {error}"),
                    }),
                },
                trace,
            );
        }
    };
    let exit_code = if args.policy_mode && total_matches > 0 {
        2
    } else {
        0
    };

    (ScanTextCommandResponse { exit_code, payload }, trace)
}

fn project_with_jq(matches: &[ScanTextMatch]) -> Result<Vec<ScanTextMatch>, jq::JqError> {
    let input = serde_json::to_value(matches).map_err(jq::JqError::Serialize)?;
    let rows = input.as_array().cloned().ok_or(jq::JqError::OutputShape)?;
    let projected_rows = jq::project_scan_text_matches(&rows)?;

    projected_rows
        .into_iter()
        .map(|value| serde_json::from_value(value).map_err(jq::JqError::Parse))
        .collect()
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "scan_text_rg_execute".to_string(),
        "scan_text_parse".to_string(),
        "scan_text_jq_project".to_string(),
    ]
}

/// Determinism guards planned for the `scan text` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rg_execution_with_explicit_arg_arrays".to_string(),
        "stable_relative_paths_from_invocation_root".to_string(),
        "deterministic_match_sort_path_line_column".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ]
}
