use std::fs;
use std::path::PathBuf;

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

#[test]
fn gate_schema_from_preset_accepts_file_input_without_extension() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("workflow");
    let schema_path = dir.path().join("schema.json");
    let yq_bin = dir.path().join("fake-yq");
    let jq_bin = dir.path().join("fake-jq");
    let mlr_bin = dir.path().join("fake-mlr");

    fs::write(
        &input_path,
        r#"
name: CI
on:
  push: {}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
    )
    .expect("write input");
    fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["job_id", "runs_on", "steps_count", "uses_unpinned_action"],
            "properties": {
                "job_id": {"type": "string"},
                "runs_on": {"type": "string"},
                "steps_count": {"type": "integer"},
                "uses_unpinned_action": {"type": "boolean"}
            }
        }"#,
    )
    .expect("write schema");

    write_exec_script(
        &yq_bin,
        r#"#!/bin/sh
printf '%s\n' '[{"job_id":"build","job":{"runs-on":"ubuntu-latest","steps":[{"uses":"actions/checkout@v4"}]}}]'
"#,
    );
    write_exec_script(
        &jq_bin,
        r#"#!/bin/sh
printf '%s\n' '[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":1,"uses_unpinned_action":false}]'
"#,
    );
    write_exec_script(
        &mlr_bin,
        r#"#!/bin/sh
printf '%s\n' '[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":1,"uses_unpinned_action":false}]'
"#,
    );

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--from",
            "github-actions-jobs",
        ])
        .assert()
        .code(0)
        .stdout(predicate::str::contains("\"matched\":true"));
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

fn write_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
