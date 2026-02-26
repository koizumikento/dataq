use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, mdbook, nb};
use crate::domain::report::PipelineStageDiagnostic;
pub use crate::engine::ingest::IngestDocInputFormat;
use crate::engine::ingest::{self, IngestBookOptions, IngestDocError};

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

/// Input arguments for `ingest book` command execution API.
#[derive(Debug, Clone)]
pub struct IngestBookCommandArgs {
    pub root: PathBuf,
    pub include_files: bool,
    pub verify_mdbook_meta: bool,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestBookCommandResponse {
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

/// Trace details used by `--emit-pipeline` for ingest-book stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestBookPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl IngestBookPipelineTrace {
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

pub fn run_book_with_trace(
    args: &IngestBookCommandArgs,
) -> (IngestBookCommandResponse, IngestBookPipelineTrace) {
    let mut trace = IngestBookPipelineTrace::default();
    let parsed = match ingest::ingest_book(&IngestBookOptions {
        root: args.root.clone(),
        include_files: args.include_files,
    }) {
        Ok(report) => report,
        Err(error) => {
            return (
                IngestBookCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": error.to_string(),
                    }),
                },
                trace,
            );
        }
    };

    if args.verify_mdbook_meta {
        let diagnostic = match mdbook::verify_book_metadata(args.root.as_path()) {
            Ok(()) => {
                PipelineStageDiagnostic::success(2, "ingest_book_mdbook_meta", "mdbook", 1, 1)
            }
            Err(mdbook::MdbookError::Unavailable) => {
                let diagnostic =
                    PipelineStageDiagnostic::failure(2, "ingest_book_mdbook_meta", "mdbook", 1);
                trace.stage_diagnostics.push(diagnostic);
                return (
                    IngestBookCommandResponse {
                        exit_code: 3,
                        payload: json!({
                            "error": "input_usage_error",
                            "message": "mdbook metadata verification requires `mdbook` in PATH",
                        }),
                    },
                    trace,
                );
            }
            Err(error) => {
                let diagnostic =
                    PipelineStageDiagnostic::failure(2, "ingest_book_mdbook_meta", "mdbook", 1);
                trace.stage_diagnostics.push(diagnostic);
                return (
                    IngestBookCommandResponse {
                        exit_code: 3,
                        payload: json!({
                            "error": "input_usage_error",
                            "message": format!("failed to verify mdbook metadata: {error}"),
                        }),
                    },
                    trace,
                );
            }
        };
        trace.stage_diagnostics.push(diagnostic);
    }

    let parsed_value = match serde_json::to_value(parsed) {
        Ok(value) => value,
        Err(error) => {
            return (
                IngestBookCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": format!("failed to serialize ingest report: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    trace.mark_tool_used("jq");
    let projected = match jq::project_ingest_book(&parsed_value) {
        Ok(value) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    3,
                    "ingest_book_jq_project",
                    "jq",
                    1,
                    1,
                ));
            value
        }
        Err(jq::JqError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "ingest_book_jq_project",
                    "jq",
                    1,
                ));
            return (
                IngestBookCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": "ingest book requires `jq` in PATH",
                    }),
                },
                trace,
            );
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "ingest_book_jq_project",
                    "jq",
                    1,
                ));
            return (
                IngestBookCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to project ingest output with jq: {error}"),
                    }),
                },
                trace,
            );
        }
    };

    (
        IngestBookCommandResponse {
            exit_code: 0,
            payload: projected,
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

pub fn pipeline_steps_book() -> Vec<String> {
    vec![
        "ingest_book_summary_parse".to_string(),
        "ingest_book_mdbook_meta".to_string(),
        "ingest_book_jq_project".to_string(),
    ]
}

pub fn deterministic_guards_book() -> Vec<String> {
    vec![
        "summary_order_preserved_from_summary_md".to_string(),
        "path_normalization_slash_no_trailing_separator".to_string(),
        "jq_execution_with_explicit_arg_arrays".to_string(),
        "optional_mdbook_metadata_verification_stage".to_string(),
    ]
}

pub fn resolve_verify_mdbook_meta() -> bool {
    match env::var("DATAQ_INGEST_BOOK_VERIFY_MDBOOK") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn notes_and_book_trace_mark_tool_used_deduplicates_entries() {
        let mut notes_trace = IngestNotesPipelineTrace::default();
        notes_trace.mark_tool_used("jq");
        notes_trace.mark_tool_used("jq");
        notes_trace.mark_tool_used("nb");
        assert_eq!(
            notes_trace.used_tools,
            vec!["jq".to_string(), "nb".to_string()]
        );

        let mut book_trace = IngestBookPipelineTrace::default();
        book_trace.mark_tool_used("jq");
        book_trace.mark_tool_used("jq");
        assert_eq!(book_trace.used_tools, vec!["jq".to_string()]);
    }

    #[test]
    fn run_notes_rejects_empty_tag_values_before_running_tools() {
        let args = IngestNotesCommandArgs {
            tags: vec!["  ".to_string()],
            since: None,
            until: None,
        };

        let (response, trace) = run_notes_with_trace(&args);
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
        assert_eq!(
            response.payload["message"],
            json!("`--tag` values cannot be empty")
        );
        assert!(trace.used_tools.is_empty());
        assert!(trace.stage_diagnostics.is_empty());
    }

    #[test]
    fn run_book_with_missing_root_returns_input_usage_error() {
        let args = IngestBookCommandArgs {
            root: PathBuf::from("/definitely-missing/dataq-book-root"),
            include_files: false,
            verify_mdbook_meta: false,
        };

        let (response, trace) = run_book_with_trace(&args);
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
        assert!(trace.used_tools.is_empty());
    }

    #[test]
    fn load_input_bytes_reads_stdin_and_file_sources() {
        let stdin_args = IngestDocCommandArgs {
            input: None,
            from: IngestDocInputFormat::Md,
        };
        let stdin_bytes =
            load_input_bytes(&stdin_args, Cursor::new(b"stdin-input")).expect("read stdin input");
        assert_eq!(stdin_bytes, b"stdin-input");

        let temp = tempdir().expect("tempdir");
        let input_path = temp.path().join("doc.md");
        fs::write(&input_path, b"# Title\n").expect("write file");
        let file_args = IngestDocCommandArgs {
            input: Some(input_path),
            from: IngestDocInputFormat::Md,
        };
        let file_bytes =
            load_input_bytes(&file_args, Cursor::new(Vec::<u8>::new())).expect("read file input");
        assert_eq!(file_bytes, b"# Title\n");
    }

    #[test]
    fn load_input_bytes_reports_missing_file_as_input_error() {
        let args = IngestDocCommandArgs {
            input: Some(PathBuf::from("/definitely-missing/dataq-ingest-input.md")),
            from: IngestDocInputFormat::Md,
        };

        let error = load_input_bytes(&args, Cursor::new(Vec::<u8>::new()))
            .expect_err("missing input file should error");
        assert!(matches!(error, IngestDocError::Input(_)));
        assert!(
            error
                .to_string()
                .contains("failed to open input file `/definitely-missing/dataq-ingest-input.md`")
        );
    }

    #[test]
    fn run_with_stdin_maps_input_errors_to_command_contract() {
        let args = IngestDocCommandArgs {
            input: Some(PathBuf::from("/definitely-missing/dataq-ingest-doc.md")),
            from: IngestDocInputFormat::Md,
        };

        let response = run_with_stdin(&args, Cursor::new(Vec::<u8>::new()));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn map_error_returns_input_usage_shape() {
        let response = map_error(IngestDocError::MissingPandoc);
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
        assert_eq!(
            response.payload["message"],
            json!("ingest doc requires `pandoc` in PATH")
        );
    }

    #[test]
    fn pipeline_metadata_helpers_are_stable() {
        assert_eq!(
            notes_pipeline_steps(),
            vec![
                "ingest_notes_nb_export".to_string(),
                "ingest_notes_jq_normalize".to_string()
            ]
        );
        assert!(
            notes_deterministic_guards()
                .iter()
                .any(|guard| guard == "ingest_notes_timestamps_normalized_to_utc")
        );
        assert_eq!(
            pipeline_steps_book(),
            vec![
                "ingest_book_summary_parse".to_string(),
                "ingest_book_mdbook_meta".to_string(),
                "ingest_book_jq_project".to_string()
            ]
        );
        assert!(
            deterministic_guards_book()
                .iter()
                .any(|guard| guard == "optional_mdbook_metadata_verification_stage")
        );
    }
}
