use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

pub use crate::engine::ingest::IngestDocInputFormat;
use crate::engine::ingest::{self, IngestDocError};

/// Input arguments for `ingest doc` command execution API.
#[derive(Debug, Clone)]
pub struct IngestDocCommandArgs {
    pub input: Option<PathBuf>,
    pub from: IngestDocInputFormat,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestDocCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
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
