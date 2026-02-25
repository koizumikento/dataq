use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, mlr, yq};
use crate::domain::ingest::IngestYamlJobsMode;
use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::ingest;
use crate::io::{self, Format, IoError};

/// Input arguments for ingest yaml-jobs command execution API.
#[derive(Debug, Clone)]
pub struct IngestYamlJobsCommandArgs {
    pub input: IngestYamlJobsInput,
    pub mode: IngestYamlJobsMode,
}

/// Input source descriptor for ingest yaml-jobs command execution.
#[derive(Debug, Clone)]
pub enum IngestYamlJobsInput {
    Path(PathBuf),
    Stdin,
    Inline(Vec<Value>),
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestYamlJobsCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for ingest yaml-jobs stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestYamlJobsPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl IngestYamlJobsPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "ingest_yaml_jobs_yq_extract".to_string(),
        "ingest_yaml_jobs_jq_normalize".to_string(),
        "ingest_yaml_jobs_mlr_shape".to_string(),
    ]
}

/// Determinism guards planned for the `ingest yaml-jobs` command.
pub fn deterministic_guards(mode: IngestYamlJobsMode) -> Vec<String> {
    vec![
        "pipeline_stage_order_yq_jq_mlr".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_sort_by_job_identifier".to_string(),
        format!("ingest_yaml_jobs_mode_{}", mode.as_str().replace('-', "_")),
    ]
}

pub fn run_with_stdin<R: Read>(
    args: &IngestYamlJobsCommandArgs,
    stdin: R,
) -> IngestYamlJobsCommandResponse {
    run_with_stdin_and_trace(args, stdin).0
}

pub fn run_with_stdin_and_trace<R: Read>(
    args: &IngestYamlJobsCommandArgs,
    stdin: R,
) -> (IngestYamlJobsCommandResponse, IngestYamlJobsPipelineTrace) {
    match execute(args, stdin) {
        Ok((rows, trace)) => (
            IngestYamlJobsCommandResponse {
                exit_code: 0,
                payload: Value::Array(rows),
            },
            trace,
        ),
        Err(error) => {
            let response = match error.kind {
                CommandErrorKind::InputUsage(message) => IngestYamlJobsCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                CommandErrorKind::Internal(message) => IngestYamlJobsCommandResponse {
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
    args: &IngestYamlJobsCommandArgs,
    stdin: R,
) -> Result<(Vec<Value>, IngestYamlJobsPipelineTrace), CommandError> {
    let input_values = load_input_values(&args.input, stdin)?;
    let mut trace = IngestYamlJobsPipelineTrace::default();

    trace.mark_tool_used("yq");
    let yq_input_rows = input_values.len();
    let yq_rows = match run_yq_stage(args.mode, &input_values) {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "ingest_yaml_jobs_yq_extract",
                    "yq",
                    yq_input_rows,
                    rows.len(),
                ));
            rows
        }
        Err(yq::YqError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "ingest_yaml_jobs_yq_extract",
                    "yq",
                    yq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!("ingest mode `{}` requires `yq` in PATH", args.mode.as_str()),
                trace,
            ));
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "ingest_yaml_jobs_yq_extract",
                    "yq",
                    yq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!(
                    "failed to extract yaml jobs with yq (`{}`): {error}",
                    args.mode.as_str()
                ),
                trace,
            ));
        }
    };

    trace.mark_tool_used("jq");
    let jq_input_rows = yq_rows.len();
    let jq_rows = match run_jq_stage(args.mode, &yq_rows) {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    "ingest_yaml_jobs_jq_normalize",
                    "jq",
                    jq_input_rows,
                    rows.len(),
                ));
            rows
        }
        Err(jq::JqError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_yaml_jobs_jq_normalize",
                    "jq",
                    jq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!("ingest mode `{}` requires `jq` in PATH", args.mode.as_str()),
                trace,
            ));
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_yaml_jobs_jq_normalize",
                    "jq",
                    jq_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!(
                    "failed to normalize yaml jobs with jq (`{}`): {error}",
                    args.mode.as_str()
                ),
                trace,
            ));
        }
    };

    trace.mark_tool_used("mlr");
    let mlr_input_rows = jq_rows.len();
    let mlr_rows = match run_mlr_stage(args.mode, &jq_rows) {
        Ok(rows) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    3,
                    "ingest_yaml_jobs_mlr_shape",
                    "mlr",
                    mlr_input_rows,
                    rows.len(),
                ));
            rows
        }
        Err(mlr::MlrError::Unavailable) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "ingest_yaml_jobs_mlr_shape",
                    "mlr",
                    mlr_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!(
                    "ingest mode `{}` requires `mlr` in PATH",
                    args.mode.as_str()
                ),
                trace,
            ));
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    3,
                    "ingest_yaml_jobs_mlr_shape",
                    "mlr",
                    mlr_input_rows,
                ));
            return Err(CommandError::input_usage_with_trace(
                format!(
                    "failed to shape yaml jobs with mlr (`{}`): {error}",
                    args.mode.as_str()
                ),
                trace,
            ));
        }
    };

    let shaped = ingest::shape_rows(args.mode, mlr_rows).map_err(|error| {
        CommandError::internal(format!(
            "failed to enforce normalized output schema (`{}`): {error}",
            args.mode.as_str()
        ))
    })?;

    Ok((shaped, trace))
}

fn run_yq_stage(mode: IngestYamlJobsMode, values: &[Value]) -> Result<Vec<Value>, yq::YqError> {
    match mode {
        IngestYamlJobsMode::GithubActions => yq::extract_github_actions_jobs(values),
        IngestYamlJobsMode::GitlabCi => yq::extract_gitlab_ci_jobs(values),
        IngestYamlJobsMode::GenericMap => yq::extract_generic_map_jobs(values),
    }
}

fn run_jq_stage(mode: IngestYamlJobsMode, values: &[Value]) -> Result<Vec<Value>, jq::JqError> {
    match mode {
        IngestYamlJobsMode::GithubActions => jq::normalize_github_actions_jobs(values),
        IngestYamlJobsMode::GitlabCi => jq::normalize_gitlab_ci_jobs(values),
        IngestYamlJobsMode::GenericMap => jq::normalize_generic_map_jobs(values),
    }
}

fn run_mlr_stage(mode: IngestYamlJobsMode, values: &[Value]) -> Result<Vec<Value>, mlr::MlrError> {
    match mode {
        IngestYamlJobsMode::GithubActions => mlr::sort_github_actions_jobs(values),
        IngestYamlJobsMode::GitlabCi => mlr::sort_gitlab_ci_jobs(values),
        IngestYamlJobsMode::GenericMap => mlr::sort_generic_map_jobs(values),
    }
}

fn load_input_values<R: Read>(
    input: &IngestYamlJobsInput,
    stdin: R,
) -> Result<Vec<Value>, CommandError> {
    match input {
        IngestYamlJobsInput::Path(path) => load_input_values_from_path(path),
        IngestYamlJobsInput::Stdin => {
            io::reader::read_values(stdin, Format::Yaml).map_err(map_io_as_input_usage)
        }
        IngestYamlJobsInput::Inline(values) => Ok(values.clone()),
    }
}

fn load_input_values_from_path(path: &Path) -> Result<Vec<Value>, CommandError> {
    let file = File::open(path).map_err(|error| {
        CommandError::input_usage(format!(
            "failed to open input file `{}`: {error}",
            path.display()
        ))
    })?;
    io::reader::read_values(file, Format::Yaml).map_err(map_io_as_input_usage)
}

pub fn path_is_stdin(path: &Path) -> bool {
    path == Path::new("-")
}

fn map_io_as_input_usage(error: IoError) -> CommandError {
    CommandError::input_usage(error.to_string())
}

struct CommandError {
    kind: CommandErrorKind,
    trace: IngestYamlJobsPipelineTrace,
}

enum CommandErrorKind {
    InputUsage(String),
    Internal(String),
}

impl CommandError {
    fn input_usage(message: String) -> Self {
        Self {
            kind: CommandErrorKind::InputUsage(message),
            trace: IngestYamlJobsPipelineTrace::default(),
        }
    }

    fn input_usage_with_trace(message: String, trace: IngestYamlJobsPipelineTrace) -> Self {
        Self {
            kind: CommandErrorKind::InputUsage(message),
            trace,
        }
    }

    fn internal(message: String) -> Self {
        Self {
            kind: CommandErrorKind::Internal(message),
            trace: IngestYamlJobsPipelineTrace::default(),
        }
    }
}
