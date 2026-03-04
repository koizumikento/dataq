use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;
use serde_json::Value;
use tempfile::TempDir;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckJsonschemaMismatch {
    pub row_index: usize,
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckJsonschemaValidation {
    Matched,
    Mismatched(Vec<CheckJsonschemaMismatch>),
}

#[derive(Debug, Error)]
pub enum CheckJsonschemaError {
    #[error("`check-jsonschema` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn check-jsonschema: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write temporary schema/instance file: {0}")]
    TempFile(std::io::Error),
    #[error("failed to serialize schema: {0}")]
    SerializeSchema(serde_json::Error),
    #[error("failed to serialize row {row_index} for schema validation: {source}")]
    SerializeRow {
        row_index: usize,
        source: serde_json::Error,
    },
    #[error("check-jsonschema output is not valid JSON: {0}")]
    ParseOutput(serde_json::Error),
    #[error("check-jsonschema output is missing required fields: {0}")]
    OutputShape(String),
    #[error("check-jsonschema input parse errors: {0}")]
    InputParse(String),
    #[error("check-jsonschema execution failed: {0}")]
    Execution(String),
}

pub fn validate_rows(
    values: &[Value],
    schema: &Value,
) -> Result<CheckJsonschemaValidation, CheckJsonschemaError> {
    let check_jsonschema_bin =
        std::env::var("DATAQ_CHECK_JSONSCHEMA_BIN").unwrap_or_else(|_| "check-jsonschema".into());
    validate_rows_with_bin(values, schema, check_jsonschema_bin.as_str())
}

fn validate_rows_with_bin(
    values: &[Value],
    schema: &Value,
    bin: &str,
) -> Result<CheckJsonschemaValidation, CheckJsonschemaError> {
    if values.is_empty() {
        return Ok(CheckJsonschemaValidation::Matched);
    }

    let tempdir = TempDir::new().map_err(CheckJsonschemaError::TempFile)?;
    let schema_path = tempdir.path().join("schema.json");
    write_json_file(schema_path.as_path(), schema)
        .map_err(CheckJsonschemaError::SerializeSchema)?;

    let mut instance_paths = Vec::with_capacity(values.len());
    let mut row_by_filename = BTreeMap::new();
    for (index, value) in values.iter().enumerate() {
        let path = tempdir.path().join(format!("row-{index:06}.json"));
        write_json_file(path.as_path(), value).map_err(|source| {
            CheckJsonschemaError::SerializeRow {
                row_index: index,
                source,
            }
        })?;
        row_by_filename.insert(path.display().to_string(), index);
        instance_paths.push(path);
    }

    let output = run_check_jsonschema(bin, schema_path.as_path(), &instance_paths)?;
    let stderr_text = decode_text(output.stderr.as_slice());
    let report: JsonReport = match serde_json::from_slice(output.stdout.as_slice()) {
        Ok(report) => report,
        Err(error) => {
            if output.status.success() {
                return Err(CheckJsonschemaError::ParseOutput(error));
            }
            let message = if stderr_text.is_empty() {
                format!("check-jsonschema failed and did not emit JSON output: {error}")
            } else {
                stderr_text
            };
            return Err(CheckJsonschemaError::Execution(message));
        }
    };

    let parse_error_messages = collect_parse_error_messages(report.parse_errors.as_slice());
    if !parse_error_messages.is_empty() {
        return Err(CheckJsonschemaError::InputParse(
            parse_error_messages.join("; "),
        ));
    }

    let mut mismatches = normalize_mismatches(report.errors, &row_by_filename)?;
    mismatches.sort_by(|left, right| {
        (left.row_index, left.path.clone(), left.message.clone()).cmp(&(
            right.row_index,
            right.path.clone(),
            right.message.clone(),
        ))
    });

    if mismatches.is_empty() {
        if output.status.success() {
            return Ok(CheckJsonschemaValidation::Matched);
        }
        let message = if stderr_text.is_empty() {
            "unknown check-jsonschema failure".to_string()
        } else {
            stderr_text
        };
        return Err(CheckJsonschemaError::Execution(message));
    }

    if output.status.success() {
        return Ok(CheckJsonschemaValidation::Mismatched(mismatches));
    }

    Ok(CheckJsonschemaValidation::Mismatched(mismatches))
}

fn write_json_file(path: &Path, value: &Value) -> Result<(), serde_json::Error> {
    let file = File::create(path).map_err(serde_json::Error::io)?;
    serde_json::to_writer(file, value)
}

fn run_check_jsonschema(
    bin: &str,
    schema_path: &Path,
    instance_paths: &[PathBuf],
) -> Result<std::process::Output, CheckJsonschemaError> {
    let mut command = Command::new(bin);
    command
        .arg("--output-format")
        .arg("JSON")
        .arg("--schemafile")
        .arg(schema_path);
    for path in instance_paths {
        command.arg(path);
    }

    match command.output() {
        Ok(output) => Ok(output),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(CheckJsonschemaError::Unavailable)
        }
        Err(error) => Err(CheckJsonschemaError::Spawn(error)),
    }
}

fn normalize_mismatches(
    errors: Vec<JsonValidationError>,
    row_by_filename: &BTreeMap<String, usize>,
) -> Result<Vec<CheckJsonschemaMismatch>, CheckJsonschemaError> {
    let mut mismatches = Vec::with_capacity(errors.len());
    for error in errors {
        let row_index =
            resolve_row_index(error.filename.as_str(), row_by_filename).ok_or_else(|| {
                CheckJsonschemaError::OutputShape(format!(
                    "unable to map validation filename `{}` to input row",
                    error.filename
                ))
            })?;

        mismatches.push(CheckJsonschemaMismatch {
            row_index,
            path: error.path.unwrap_or_else(|| "$".to_string()),
            message: error
                .message
                .unwrap_or_else(|| "validation failed".to_string()),
        });
    }
    Ok(mismatches)
}

fn resolve_row_index(filename: &str, row_by_filename: &BTreeMap<String, usize>) -> Option<usize> {
    if let Some(index) = row_by_filename.get(filename) {
        return Some(*index);
    }

    let basename = Path::new(filename)
        .file_name()
        .and_then(|entry| entry.to_str())?;
    if !basename.starts_with("row-") || !basename.ends_with(".json") {
        return None;
    }
    let digits = basename
        .trim_start_matches("row-")
        .trim_end_matches(".json");
    digits.parse::<usize>().ok()
}

fn collect_parse_error_messages(parse_errors: &[JsonParseError]) -> Vec<String> {
    let mut messages: Vec<String> = parse_errors
        .iter()
        .map(|entry| match (&entry.filename, &entry.message) {
            (Some(filename), Some(message)) => format!("{filename}: {message}"),
            (Some(filename), None) => filename.clone(),
            (None, Some(message)) => message.clone(),
            (None, None) => "unknown parse error".to_string(),
        })
        .collect();
    messages.sort();
    messages
}

fn decode_text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|_| "failed to decode check-jsonschema stderr".to_string())
        .trim()
        .to_string()
}

#[derive(Debug, Deserialize)]
struct JsonReport {
    #[allow(dead_code)]
    status: Option<String>,
    #[serde(default)]
    errors: Vec<JsonValidationError>,
    #[serde(default)]
    parse_errors: Vec<JsonParseError>,
}

#[derive(Debug, Deserialize)]
struct JsonValidationError {
    filename: String,
    path: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonParseError {
    filename: Option<String>,
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;

    use super::{CheckJsonschemaError, CheckJsonschemaValidation, validate_rows_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let error = validate_rows_with_bin(
            &[json!({"id": 1})],
            &json!({"type":"object"}),
            "/missing/check-jsonschema",
        )
        .expect_err("missing binary should fail");
        assert!(matches!(error, CheckJsonschemaError::Unavailable));
    }

    #[test]
    fn maps_nonzero_json_report_to_mismatches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-check-jsonschema"),
            r#"
target=""
for arg in "$@"; do
  target="$arg"
done
printf '{"status":"fail","errors":[{"filename":"%s","path":"$.id","message":"bad type"}],"parse_errors":[]}\n' "$target"
exit 1
"#,
        );
        let values = vec![json!({"id":"x"})];
        let schema = json!({"type":"object","properties":{"id":{"type":"integer"}}});

        let result = validate_rows_with_bin(&values, &schema, bin.to_str().expect("utf8 path"))
            .expect("schema validation result");
        match result {
            CheckJsonschemaValidation::Matched => panic!("expected mismatch"),
            CheckJsonschemaValidation::Mismatched(mismatches) => {
                assert_eq!(mismatches.len(), 1);
                assert_eq!(mismatches[0].row_index, 0);
                assert_eq!(mismatches[0].path, "$.id");
                assert_eq!(mismatches[0].message, "bad type");
            }
        }
    }

    #[test]
    fn maps_parse_errors_to_input_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-check-jsonschema"),
            r#"
printf '{"status":"fail","errors":[],"parse_errors":[{"filename":"row-000000.json","message":"invalid json"}]}\n'
exit 1
"#,
        );
        let values = vec![json!({"id": 1})];
        let schema = json!({"type":"object"});

        let error = validate_rows_with_bin(&values, &schema, bin.to_str().expect("utf8 path"))
            .expect_err("parse error should fail");
        assert!(matches!(error, CheckJsonschemaError::InputParse(_)));
    }

    fn write_test_script(path: PathBuf, body: &str) -> PathBuf {
        let tmp_path = path.with_extension("tmp");
        let script = format!("#!/bin/sh\n{body}\n");
        fs::write(&tmp_path, script).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        fs::rename(&tmp_path, &path).expect("rename script");
        path
    }
}
