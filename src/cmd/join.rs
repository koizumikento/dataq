use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::join::{self, JoinError, JoinHow};
use crate::io;

/// Input arguments for join command execution API.
#[derive(Debug, Clone)]
pub struct JoinCommandArgs {
    pub left: JoinCommandInput,
    pub right: JoinCommandInput,
    pub on: String,
    pub how: JoinHow,
}

/// Input source descriptor for join command execution.
#[derive(Debug, Clone)]
pub enum JoinCommandInput {
    Path(PathBuf),
    Inline(Vec<Value>),
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct JoinCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for join stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl JoinPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_with_trace(args: &JoinCommandArgs) -> (JoinCommandResponse, JoinPipelineTrace) {
    let mut trace = JoinPipelineTrace::default();

    let left = match resolve_input_rows(&args.left, "left") {
        Ok(values) => values,
        Err(message) => {
            return (
                JoinCommandResponse {
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

    let right = match resolve_input_rows(&args.right, "right") {
        Ok(values) => values,
        Err(message) => {
            return (
                JoinCommandResponse {
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

    let input_records = left.len() + right.len();
    match join::join_values(&left, &right, &args.on, args.how) {
        Ok(rows) => {
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "join_mlr_execute",
                    "mlr",
                    input_records,
                    rows.len(),
                ));
            (
                JoinCommandResponse {
                    exit_code: 0,
                    payload: Value::Array(rows),
                },
                trace,
            )
        }
        Err(JoinError::Mlr(error)) => {
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "join_mlr_execute",
                    "mlr",
                    input_records,
                ));
            (
                JoinCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to join inputs with mlr: {error}"),
                    }),
                },
                trace,
            )
        }
        Err(error) => (
            JoinCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": error.to_string(),
                }),
            },
            trace,
        ),
    }
}

fn resolve_input_rows(source: &JoinCommandInput, role: &'static str) -> Result<Vec<Value>, String> {
    match source {
        JoinCommandInput::Path(path) => load_input_rows(path.as_path(), role),
        JoinCommandInput::Inline(values) => Ok(values.clone()),
    }
}

fn load_input_rows(path: &Path, role: &'static str) -> Result<Vec<Value>, String> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
        format!(
            "unable to resolve {role} input format from `{}`: {error}",
            path.display()
        )
    })?;
    let file = File::open(path).map_err(|error| {
        format!(
            "failed to open {role} input file `{}`: {error}",
            path.display()
        )
    })?;
    io::reader::read_values(file, format)
        .map_err(|error| format!("failed to read {role} input values: {error}"))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_join_inputs".to_string(),
        "read_join_values".to_string(),
        "validate_join_keys".to_string(),
        "execute_join_with_mlr".to_string(),
        "write_join_output".to_string(),
    ]
}

/// Determinism guards planned for the `join` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "mlr_execution_with_explicit_arg_arrays".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_join_output_sorting".to_string(),
    ]
}
