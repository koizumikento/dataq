use std::process::Command;

use serde_json::Value;
use thiserror::Error;

/// Errors produced while executing `nb` note export/list commands.
#[derive(Debug, Error)]
pub enum NbError {
    #[error("`nb` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn nb: {0}")]
    Spawn(std::io::Error),
    #[error("nb execution failed: {0}")]
    Execution(String),
    #[error("nb output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("nb output must be a JSON array or object containing `items`/`notes`")]
    OutputShape,
}

/// Runs `nb` and returns note rows from JSON output.
///
/// The adapter first tries `nb list --format json` and falls back to
/// `nb export --format json` for compatibility with installations that expose
/// only one of those commands.
pub fn export_or_list_notes() -> Result<Vec<Value>, NbError> {
    let list_args = ["list", "--format", "json"];
    match run_nb_command(&list_args) {
        Ok(rows) => Ok(rows),
        Err(NbError::Execution(list_error)) => {
            let export_args = ["export", "--format", "json"];
            match run_nb_command(&export_args) {
                Ok(rows) => Ok(rows),
                Err(NbError::Execution(export_error)) => Err(NbError::Execution(format!(
                    "`nb list --format json` failed: {list_error}; `nb export --format json` failed: {export_error}"
                ))),
                Err(other) => Err(other),
            }
        }
        Err(other) => Err(other),
    }
}

fn run_nb_command(args: &[&str]) -> Result<Vec<Value>, NbError> {
    let nb_bin = std::env::var("DATAQ_NB_BIN").unwrap_or_else(|_| "nb".to_string());
    let output = match Command::new(&nb_bin).args(args).output() {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(NbError::Unavailable),
        Err(err) => return Err(NbError::Spawn(err)),
    };

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode nb stderr".to_string());
        return Err(NbError::Execution(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(NbError::Parse)?;
    match parsed {
        Value::Array(rows) => Ok(rows),
        Value::Object(mut object) => {
            if let Some(Value::Array(rows)) = object.remove("items") {
                return Ok(rows);
            }
            if let Some(Value::Array(rows)) = object.remove("notes") {
                return Ok(rows);
            }
            Err(NbError::OutputShape)
        }
        _ => Err(NbError::OutputShape),
    }
}
