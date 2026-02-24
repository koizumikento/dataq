use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, nb};
use crate::domain::report::PipelineStageDiagnostic;
pub use crate::engine::ingest::IngestDocInputFormat;
use crate::engine::ingest::{self, IngestDocError};

/// Input arguments for `ingest doc` command execution API.
#[derive(Debug, Clone)]
pub struct IngestDocCommandArgs {
    pub input: Option<PathBuf>,
    pub from: IngestDocInputFormat,
}

/// Input arguments for `ingest notes` command execution.
#[derive(Debug, Clone)]
pub struct IngestNotesCommandArgs {
    pub tags: Vec<String>,
    pub since: Option<String>,
    pub until: Option<String>,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestDocCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestNotesCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for `ingest notes` stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestNotesPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl IngestNotesPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_with_stdin<R: Read>(args: &IngestDocCommandArgs, stdin: R) -> IngestDocCommandResponse {
    match execute(args, stdin) {
        Ok(payload) => IngestDocCommandResponse {
            exit_code: 0,
            payload,
        },
        Err(error) => map_error(error),
    }
}

pub fn run_notes_with_trace(
    args: &IngestNotesCommandArgs,
) -> (IngestNotesCommandResponse, IngestNotesPipelineTrace) {
    let mut trace = IngestNotesPipelineTrace::default();

    if args.tags.iter().any(|tag| tag.trim().is_empty()) {
        return (
            IngestNotesCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": "`--tag` values cannot be empty",
                }),
            },
            trace,
        );
    }

    trace.mark_tool_used("nb");
    let exported_rows = match nb::export_or_list_notes() {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "ingest_notes_nb_export",
                    "nb",
                    0,
                    rows.len(),
                ));
            rows
        }
        Err(nb::NbError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "ingest_notes_nb_export",
                    "nb",
                    0,
                ));
            return (
                IngestNotesCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": "ingest notes requires `nb` in PATH",
                    }),
                },
                trace,
            );
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "ingest_notes_nb_export",
                    "nb",
                    0,
                ));
            return (
                IngestNotesCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to export notes with nb: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    trace.mark_tool_used("jq");
    let projected_rows = match jq::normalize_ingest_notes(&exported_rows) {
        Ok(rows) => rows,
        Err(jq::JqError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_notes_jq_normalize",
                    "jq",
                    exported_rows.len(),
                ));
            return (
                IngestNotesCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": "ingest notes requires `jq` in PATH",
                    }),
                },
                trace,
            );
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_notes_jq_normalize",
                    "jq",
                    exported_rows.len(),
                ));
            return (
                IngestNotesCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to normalize notes with jq: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    let normalized_rows = match ingest::finalize_notes(
        projected_rows,
        &args.tags,
        args.since.as_deref(),
        args.until.as_deref(),
    ) {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    "ingest_notes_jq_normalize",
                    "jq",
                    exported_rows.len(),
                    rows.len(),
                ));
            rows
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_notes_jq_normalize",
                    "jq",
                    exported_rows.len(),
                ));
            return (
                IngestNotesCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to normalize projected notes: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    (
        IngestNotesCommandResponse {
            exit_code: 0,
            payload: Value::Array(normalized_rows),
        },
        trace,
    )
}

fn execute<R: Read>(args: &IngestDocCommandArgs, stdin: R) -> Result<Value, IngestDocError> {
    let input = load_input_bytes(args, stdin)?;
    let report = ingest::ingest_document(&input, args.from)?;
    serde_json::to_value(report)
        .map_err(|error| IngestDocError::ProjectionSchema(error.to_string()))
}

fn load_input_bytes<R: Read>(
    args: &IngestDocCommandArgs,
    mut stdin: R,
) -> Result<Vec<u8>, IngestDocError> {
    if let Some(path) = &args.input {
        let mut file = File::open(path).map_err(|error| {
            IngestDocError::Input(format!(
                "failed to open input file `{}`: {error}",
                path.display()
            ))
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|error| {
            IngestDocError::Input(format!(
                "failed to read input file `{}`: {error}",
                path.display()
            ))
        })?;
        Ok(bytes)
    } else {
        let mut bytes = Vec::new();
        stdin
            .read_to_end(&mut bytes)
            .map_err(|error| IngestDocError::Input(format!("failed to read stdin: {error}")))?;
        Ok(bytes)
    }
}

fn map_error(error: IngestDocError) -> IngestDocCommandResponse {
    IngestDocCommandResponse {
        exit_code: 3,
        payload: json!({
            "error": "input_usage_error",
            "message": error.to_string(),
        }),
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    ingest::pipeline_steps()
}

/// Determinism guards planned for `ingest doc` command.
pub fn deterministic_guards() -> Vec<String> {
    ingest::deterministic_guards()
}

/// Ordered pipeline-step names used for `ingest notes` diagnostics.
pub fn notes_pipeline_steps() -> Vec<String> {
    vec![
        "ingest_notes_nb_export".to_string(),
        "ingest_notes_jq_normalize".to_string(),
    ]
}

/// Determinism guards planned for `ingest notes` command.
pub fn notes_deterministic_guards() -> Vec<String> {
    vec![
        "nb_and_jq_execution_with_explicit_arg_arrays".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "ingest_notes_sorted_by_created_at_then_id".to_string(),
        "ingest_notes_timestamps_normalized_to_utc".to_string(),
    ]
}
