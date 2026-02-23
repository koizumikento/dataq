use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::aggregate::{self, AggregateError, AggregateMetric};
use crate::io;

/// Input arguments for aggregate command execution API.
#[derive(Debug, Clone)]
pub struct AggregateCommandArgs {
    pub input: PathBuf,
    pub group_by: String,
    pub metric: AggregateMetric,
    pub target: String,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AggregateCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for aggregate stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregatePipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl AggregatePipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_with_trace(
    args: &AggregateCommandArgs,
) -> (AggregateCommandResponse, AggregatePipelineTrace) {
    let mut trace = AggregatePipelineTrace::default();

    let values = match load_input_rows(args.input.as_path()) {
        Ok(values) => values,
        Err(message) => {
            return (
                AggregateCommandResponse {
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

    let input_records = values.len();
    match aggregate::aggregate_values(&values, &args.group_by, args.metric, &args.target) {
        Ok(rows) => {
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "aggregate_mlr_execute",
                    "mlr",
                    input_records,
                    rows.len(),
                ));
            (
                AggregateCommandResponse {
                    exit_code: 0,
                    payload: Value::Array(rows),
                },
                trace,
            )
        }
        Err(AggregateError::Mlr(error)) => {
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "aggregate_mlr_execute",
                    "mlr",
                    input_records,
                ));
            (
                AggregateCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to aggregate input with mlr: {error}"),
                    }),
                },
                trace,
            )
        }
        Err(error) => (
            AggregateCommandResponse {
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

fn load_input_rows(path: &Path) -> Result<Vec<Value>, String> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
        format!(
            "unable to resolve input format from `{}`: {error}",
            path.display()
        )
    })?;
    let file = File::open(path)
        .map_err(|error| format!("failed to open input file `{}`: {error}", path.display()))?;
    io::reader::read_values(file, format).map_err(|error| format!("failed to read input: {error}"))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_aggregate_input".to_string(),
        "read_aggregate_values".to_string(),
        "validate_aggregate_keys".to_string(),
        "execute_aggregate_with_mlr".to_string(),
        "write_aggregate_output".to_string(),
    ]
}

/// Determinism guards planned for the `aggregate` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "mlr_execution_with_explicit_arg_arrays".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_aggregate_output_sorting".to_string(),
    ]
}
