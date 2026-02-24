use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::transform::{self, TransformRowsetError};
use crate::io;

/// Input arguments for `transform rowset` execution.
#[derive(Debug, Clone)]
pub struct TransformRowsetCommandArgs {
    pub input: TransformRowsetCommandInput,
    pub jq_filter: String,
    pub mlr: Vec<String>,
}

/// Input source descriptor for `transform rowset`.
#[derive(Debug, Clone)]
pub enum TransformRowsetCommandInput {
    Path(PathBuf),
    Inline(Vec<Value>),
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TransformRowsetCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for transform stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformRowsetPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl TransformRowsetPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_rowset_with_trace(
    args: &TransformRowsetCommandArgs,
) -> (TransformRowsetCommandResponse, TransformRowsetPipelineTrace) {
    let mut trace = TransformRowsetPipelineTrace::default();
    let values = match resolve_input_rows(&args.input) {
        Ok(values) => values,
        Err(message) => {
            return (
                TransformRowsetCommandResponse {
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

    match transform::execute_rowset(&values, &args.jq_filter, &args.mlr) {
        Ok(result) => {
            trace.mark_tool_used("jq");
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    result.jq_input_records,
                    result.jq_output_records,
                ));
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    "transform_rowset_mlr",
                    "mlr",
                    result.jq_output_records,
                    result.mlr_output_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 0,
                    payload: Value::Array(result.rows),
                },
                trace,
            )
        }
        Err(TransformRowsetError::Jq {
            input_records,
            source,
        }) => {
            trace.mark_tool_used("jq");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    input_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to transform rowset with jq: {source}"),
                    }),
                },
                trace,
            )
        }
        Err(TransformRowsetError::Mlr {
            jq_input_records,
            jq_output_records,
            source,
        }) => {
            trace.mark_tool_used("jq");
            trace.mark_tool_used("mlr");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    jq_input_records,
                    jq_output_records,
                ));
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "transform_rowset_mlr",
                    "mlr",
                    jq_output_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to transform rowset with mlr: {source}"),
                    }),
                },
                trace,
            )
        }
        Err(error) => (
            TransformRowsetCommandResponse {
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

fn resolve_input_rows(source: &TransformRowsetCommandInput) -> Result<Vec<Value>, String> {
    match source {
        TransformRowsetCommandInput::Path(path) => load_input_rows(path.as_path()),
        TransformRowsetCommandInput::Inline(values) => Ok(values.clone()),
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
        "transform_rowset_jq".to_string(),
        "transform_rowset_mlr".to_string(),
    ]
}

/// Determinism guards planned for the `transform rowset` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "jq_execution_with_explicit_arg_arrays".to_string(),
        "mlr_execution_with_explicit_arg_arrays".to_string(),
        "deterministic_row_sort_before_and_after_mlr".to_string(),
        "canonical_float_formatting_for_output".to_string(),
    ]
}
