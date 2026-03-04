use std::io::Write;
use std::process::{Command, Stdio};

use jsonschema::validator_for;
use serde_json::{Map, Value};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::domain::rules::{AssertReport, MismatchEntry};

use super::AssertValidationError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaValidationEngine {
    #[default]
    Jsonschema,
    Ajv,
}

impl SchemaValidationEngine {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Jsonschema => "jsonschema",
            Self::Ajv => "ajv",
        }
    }
}

pub fn validate(values: &[Value], schema: &Value) -> Result<AssertReport, AssertValidationError> {
    validate_with_engine(values, schema, SchemaValidationEngine::Jsonschema)
}

pub fn validate_with_engine(
    values: &[Value],
    schema: &Value,
    engine: SchemaValidationEngine,
) -> Result<AssertReport, AssertValidationError> {
    match engine {
        SchemaValidationEngine::Jsonschema => validate_with_jsonschema(values, schema),
        SchemaValidationEngine::Ajv => validate_with_ajv(values, schema),
    }
}

fn validate_with_jsonschema(
    values: &[Value],
    schema: &Value,
) -> Result<AssertReport, AssertValidationError> {
    let validator = validator_for(schema)
        .map_err(|error| AssertValidationError::InputUsage(format!("invalid schema: {error}")))?;

    let mut mismatches = Vec::new();
    for (row_index, value) in values.iter().enumerate() {
        for error in validator.iter_errors(value) {
            let instance_pointer = error.instance_path().as_str().to_string();
            let schema_path = normalize_schema_path(error.schema_path().as_str());
            let keyword = schema_keyword_from_path(&schema_path);
            let message = error.to_string();

            mismatches.push(build_schema_mismatch(
                row_index,
                value,
                &instance_pointer,
                &schema_path,
                keyword.as_deref(),
                &message,
                SchemaValidationEngine::Jsonschema,
            ));
        }
    }

    build_report(mismatches)
}

fn validate_with_ajv(
    values: &[Value],
    schema: &Value,
) -> Result<AssertReport, AssertValidationError> {
    let mut mismatches = Vec::new();
    for (row_index, value) in values.iter().enumerate() {
        let row_errors = run_ajv_for_row(schema, value)?;
        for error in row_errors {
            mismatches.push(build_schema_mismatch(
                row_index,
                value,
                &error.instance_path,
                &error.schema_path,
                error.keyword.as_deref(),
                &error.message,
                SchemaValidationEngine::Ajv,
            ));
        }
    }

    build_report(mismatches)
}

fn build_report(mut mismatches: Vec<MismatchEntry>) -> Result<AssertReport, AssertValidationError> {
    sort_mismatches(&mut mismatches);
    Ok(AssertReport {
        matched: mismatches.is_empty(),
        mismatch_count: mismatches.len(),
        mismatches,
    })
}

fn build_schema_mismatch(
    row_index: usize,
    root: &Value,
    instance_path: &str,
    schema_path: &str,
    keyword: Option<&str>,
    message: &str,
    engine: SchemaValidationEngine,
) -> MismatchEntry {
    MismatchEntry {
        path: row_path_from_json_pointer(row_index, root, instance_path),
        rule_kind: "schema".to_string(),
        reason: "schema_mismatch".to_string(),
        actual: value_at_pointer(root, instance_path),
        expected: schema_expected_payload(engine, instance_path, schema_path, keyword, message),
    }
}

fn schema_expected_payload(
    engine: SchemaValidationEngine,
    instance_path: &str,
    schema_path: &str,
    keyword: Option<&str>,
    message: &str,
) -> Value {
    let mut payload = Map::new();
    payload.insert(
        "engine".to_string(),
        Value::String(engine.as_str().to_string()),
    );
    payload.insert(
        "instance_path".to_string(),
        Value::String(instance_path.to_string()),
    );
    payload.insert(
        "schema_path".to_string(),
        Value::String(schema_path.to_string()),
    );
    if let Some(keyword) = keyword {
        payload.insert("keyword".to_string(), Value::String(keyword.to_string()));
    }
    payload.insert("message".to_string(), Value::String(message.to_string()));
    Value::Object(payload)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AjvErrorEntry {
    instance_path: String,
    schema_path: String,
    keyword: Option<String>,
    message: String,
}

#[derive(Debug, Error)]
enum AjvRunError {
    #[error("schema engine `ajv` requires `ajv` in PATH")]
    Unavailable,
    #[error("failed to create temporary ajv file: {0}")]
    Temp(std::io::Error),
    #[error("failed to serialize ajv input: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to spawn ajv: {0}")]
    Spawn(std::io::Error),
    #[error("{0}")]
    Usage(String),
}

fn run_ajv_for_row(
    schema: &Value,
    value: &Value,
) -> Result<Vec<AjvErrorEntry>, AssertValidationError> {
    let ajv_bin = std::env::var("DATAQ_AJV_BIN").unwrap_or_else(|_| "ajv".to_string());
    run_ajv_for_row_with_bin(schema, value, &ajv_bin).map_err(map_ajv_error)
}

fn map_ajv_error(error: AjvRunError) -> AssertValidationError {
    match error {
        AjvRunError::Unavailable | AjvRunError::Usage(_) => {
            AssertValidationError::InputUsage(error.to_string())
        }
        AjvRunError::Temp(_) | AjvRunError::Serialize(_) | AjvRunError::Spawn(_) => {
            AssertValidationError::Internal(error.to_string())
        }
    }
}

fn run_ajv_for_row_with_bin(
    schema: &Value,
    value: &Value,
    bin: &str,
) -> Result<Vec<AjvErrorEntry>, AjvRunError> {
    let schema_file = write_json_tempfile(schema)?;
    let data_file = write_json_tempfile(value)?;

    let output = match Command::new(bin)
        .arg("validate")
        .arg("--all-errors")
        .arg("--errors=json")
        .arg("--strict=false")
        .arg("-s")
        .arg(schema_file.path())
        .arg("-d")
        .arg(data_file.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AjvRunError::Unavailable);
        }
        Err(error) => return Err(AjvRunError::Spawn(error)),
    };

    if output.status.success() {
        return Ok(Vec::new());
    }

    if let Some(errors) = parse_ajv_validation_errors(&output.stdout, &output.stderr) {
        return Ok(errors);
    }

    let fallback = first_non_empty_line(&output.stderr)
        .or_else(|| first_non_empty_line(&output.stdout))
        .unwrap_or("ajv validation failed without parseable JSON diagnostics");
    Err(AjvRunError::Usage(fallback.to_string()))
}

fn write_json_tempfile(value: &Value) -> Result<NamedTempFile, AjvRunError> {
    let mut file = NamedTempFile::new().map_err(AjvRunError::Temp)?;
    serde_json::to_writer(file.as_file_mut(), value).map_err(AjvRunError::Serialize)?;
    file.as_file_mut().flush().map_err(AjvRunError::Temp)?;
    Ok(file)
}

fn parse_ajv_validation_errors(stdout: &[u8], stderr: &[u8]) -> Option<Vec<AjvErrorEntry>> {
    parse_ajv_validation_errors_from_bytes(stdout)
        .or_else(|| parse_ajv_validation_errors_from_bytes(stderr))
}

fn parse_ajv_validation_errors_from_bytes(bytes: &[u8]) -> Option<Vec<AjvErrorEntry>> {
    let text = String::from_utf8_lossy(bytes);
    let parsed = parse_json_fragment(text.trim())?;
    normalize_ajv_error_payload(&parsed)
}

fn parse_json_fragment(text: &str) -> Option<Value> {
    if text.trim().is_empty() {
        return None;
    }

    if let Ok(value) = serde_json::from_str::<Value>(text.trim()) {
        return Some(value);
    }

    let trimmed = text.trim();
    for needle in ['[', '{'] {
        if let Some(index) = trimmed.find(needle) {
            if let Ok(value) = serde_json::from_str::<Value>(&trimmed[index..]) {
                return Some(value);
            }
        }
    }
    None
}

fn normalize_ajv_error_payload(value: &Value) -> Option<Vec<AjvErrorEntry>> {
    let raw_entries = match value {
        Value::Array(entries) => Some(entries.clone()),
        Value::Object(object) => object
            .get("errors")
            .and_then(Value::as_array)
            .cloned()
            .or_else(|| Some(vec![value.clone()])),
        _ => None,
    }?;

    let mut normalized = Vec::new();
    for entry in raw_entries {
        let object = entry.as_object()?;
        normalized.push(normalize_single_ajv_error(object)?);
    }
    Some(normalized)
}

fn normalize_single_ajv_error(entry: &Map<String, Value>) -> Option<AjvErrorEntry> {
    let instance_path = entry
        .get("instancePath")
        .or_else(|| entry.get("instance_path"))
        .or_else(|| entry.get("dataPath"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let schema_path = entry
        .get("schemaPath")
        .or_else(|| entry.get("schema_path"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let keyword = entry
        .get("keyword")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| schema_keyword_from_path(schema_path));
    let message = entry
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if message.is_empty() {
        return None;
    }

    Some(AjvErrorEntry {
        instance_path: normalize_instance_path(instance_path),
        schema_path: normalize_schema_path(schema_path),
        keyword,
        message: message.to_string(),
    })
}

fn normalize_instance_path(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    if path.starts_with('/') {
        return path.to_string();
    }
    if path.starts_with('#') {
        return path.trim_start_matches('#').to_string();
    }
    path.to_string()
}

fn normalize_schema_path(path: &str) -> String {
    path.trim_start_matches('#').to_string()
}

fn schema_keyword_from_path(path: &str) -> Option<String> {
    let normalized = normalize_schema_path(path);
    normalized
        .trim_start_matches('/')
        .split('/')
        .rfind(|segment| !segment.is_empty())
        .map(decode_pointer_token)
}

fn first_non_empty_line(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines().map(str::trim).find(|line| !line.is_empty())
}

fn value_at_pointer(root: &Value, pointer: &str) -> Value {
    if pointer.is_empty() {
        return root.clone();
    }
    root.pointer(pointer).cloned().unwrap_or(Value::Null)
}

fn row_path_from_json_pointer(row_index: usize, root: &Value, pointer: &str) -> String {
    let mut path = format!("$[{row_index}]");
    if pointer.is_empty() {
        return path;
    }

    let mut current = Some(root);
    for token in pointer.trim_start_matches('/').split('/') {
        let segment = decode_pointer_token(token);
        match current {
            Some(Value::Array(items)) => {
                if let Ok(index) = segment.parse::<usize>() {
                    path.push('[');
                    path.push_str(&index.to_string());
                    path.push(']');
                    current = items.get(index);
                } else {
                    push_object_key_segment(&mut path, &segment);
                    current = None;
                }
            }
            Some(Value::Object(map)) => {
                push_object_key_segment(&mut path, &segment);
                current = map.get(&segment);
            }
            _ => {
                push_object_key_segment(&mut path, &segment);
                current = None;
            }
        }
    }

    path
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn is_simple_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn push_object_key_segment(path: &mut String, segment: &str) {
    if is_simple_identifier(segment) {
        path.push('.');
        path.push_str(segment);
    } else {
        path.push('[');
        path.push_str(
            &serde_json::to_string(segment).unwrap_or_else(|_| "\"<invalid-segment>\"".to_string()),
        );
        path.push(']');
    }
}

fn sort_mismatches(mismatches: &mut [MismatchEntry]) {
    mismatches.sort_by(|left, right| {
        let left_key = (
            left.path.clone(),
            left.rule_kind.clone(),
            left.reason.clone(),
            stable_value_key(&left.actual),
            stable_value_key(&left.expected),
        );
        let right_key = (
            right.path.clone(),
            right.rule_kind.clone(),
            right.reason.clone(),
            stable_value_key(&right.actual),
            stable_value_key(&right.expected),
        );
        left_key.cmp(&right_key)
    });
}

fn stable_value_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<serialization-error>".to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;

    use super::{
        SchemaValidationEngine, parse_ajv_validation_errors_from_bytes, run_ajv_for_row_with_bin,
        validate_with_engine,
    };

    #[test]
    fn reports_schema_mismatches_with_normalized_shape() {
        let values = vec![json!({"id":"x","score":200})];
        let schema = json!({
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": {"type": "integer"},
                "score": {"type": "number", "maximum": 100}
            }
        });

        let report = validate_with_engine(&values, &schema, SchemaValidationEngine::Jsonschema)
            .expect("schema validation result");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 2);
        assert_eq!(report.mismatches[0].reason, "schema_mismatch");
        assert_eq!(report.mismatches[0].expected["engine"], json!("jsonschema"));
        assert!(report.mismatches[0].expected.get("schema_path").is_some());
        assert!(report.mismatches[0].expected.get("instance_path").is_some());
        assert!(report.mismatches[0].expected.get("message").is_some());
    }

    #[test]
    fn rejects_invalid_schema() {
        let values = vec![json!({"id": 1})];
        let schema = json!({"type": 123});

        let error = validate_with_engine(&values, &schema, SchemaValidationEngine::Jsonschema)
            .expect_err("schema should be invalid");
        assert!(error.to_string().contains("invalid schema"));
    }

    #[test]
    fn keeps_numeric_object_keys_unambiguous_in_paths() {
        let values = vec![json!({"0":"x"})];
        let schema = json!({
            "type": "object",
            "required": ["0"],
            "properties": {
                "0": {"type": "integer"}
            }
        });

        let report = validate_with_engine(&values, &schema, SchemaValidationEngine::Jsonschema)
            .expect("schema validation result");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 1);
        assert_eq!(report.mismatches[0].path, "$[0][\"0\"]");
    }

    #[test]
    fn parses_ajv_error_array_payload() {
        let payload = br##"[{"instancePath":"/id","schemaPath":"#/properties/id/type","keyword":"type","message":"must be integer"}]"##;
        let errors = parse_ajv_validation_errors_from_bytes(payload).expect("parse ajv errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].instance_path, "/id");
        assert_eq!(errors[0].schema_path, "/properties/id/type");
        assert_eq!(errors[0].keyword.as_deref(), Some("type"));
        assert_eq!(errors[0].message, "must be integer");
    }

    #[test]
    fn maps_missing_ajv_binary_to_usage_error() {
        let schema = json!({"type":"object"});
        let row = json!({"id": 1});
        let error = run_ajv_for_row_with_bin(&schema, &row, "/definitely-missing/ajv")
            .expect_err("missing binary should fail");
        assert!(error.to_string().contains("requires `ajv` in PATH"));
    }

    #[test]
    fn ajv_runner_collects_errors_from_external_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script = write_test_script(
            dir.path().join("fake-ajv"),
            r##"echo '[{"instancePath":"/id","schemaPath":"#/properties/id/type","keyword":"type","message":"must be integer"}]'
exit 1"##,
        );
        let schema = json!({"type":"object","properties":{"id":{"type":"integer"}}});
        let value = json!({"id":"x"});

        let ajv_errors = run_ajv_for_row_with_bin(&schema, &value, script.to_str().expect("utf8"))
            .expect("ajv rows");
        assert_eq!(ajv_errors.len(), 1);
        assert_eq!(ajv_errors[0].instance_path, "/id");
        assert_eq!(ajv_errors[0].schema_path, "/properties/id/type");
        assert_eq!(ajv_errors[0].keyword.as_deref(), Some("type"));
    }

    fn write_test_script(path: PathBuf, body: &str) -> PathBuf {
        let tmp_path = path.with_extension("tmp");
        let script = format!("#!/bin/sh\n{body}\n");
        fs::write(&tmp_path, script).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&tmp_path, permissions).expect("chmod");
        }
        fs::rename(&tmp_path, &path).expect("rename script");
        path
    }
}
