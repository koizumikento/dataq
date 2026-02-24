use std::fs;

use predicates::prelude::predicate;
use serde_json::Value;
use tempfile::tempdir;

#[test]
fn gate_schema_passes_when_input_matches_schema() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer"}
            }
        }"#,
    )
    .expect("write schema");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
        ])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(0)
        .stdout(predicate::str::contains("\"matched\":true"));
}

#[test]
fn gate_schema_reports_mismatch_with_exit_two() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer"}
            }
        }"#,
    )
    .expect("write schema");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
        ])
        .write_stdin(r#"[{"id":"oops"}]"#)
        .assert()
        .code(2)
        .stdout(predicate::str::contains("\"mismatch_count\":1"));
}

#[test]
fn gate_schema_maps_invalid_schema_to_exit_three() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(&schema_path, r#"{"type":123}"#).expect("write invalid schema");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
        ])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("invalid schema"));
}

#[test]
fn gate_schema_rejects_unknown_from_preset_with_explicit_error() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(&schema_path, r#"{"type":"array"}"#).expect("write schema");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
            "--from",
            "unknown-preset",
        ])
        .write_stdin("[]")
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "unsupported `--from` preset `unknown-preset`",
        ));
}

#[test]
fn gate_schema_emit_pipeline_uses_required_step_names() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(&schema_path, r#"{"type":"array"}"#).expect("write schema");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--emit-pipeline",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
        ])
        .write_stdin("[]")
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("gate.schema"));
    assert_eq!(
        stderr_json["steps"],
        Value::Array(vec![
            Value::from("gate_schema_ingest"),
            Value::from("gate_schema_validate"),
        ])
    );
}

fn parse_last_stderr_json(stderr: &[u8]) -> Value {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    let line = text
        .lines()
        .rev()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stderr line");
    serde_json::from_str(line).expect("stderr json")
}
