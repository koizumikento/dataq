use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use predicates::prelude::predicate;
use serde_json::Value;
use tempfile::tempdir;

#[test]
fn help_is_available() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("canon"))
        .stdout(predicate::str::contains("assert"))
        .stdout(predicate::str::contains("sdiff"))
        .stdout(predicate::str::contains("profile"));
}

#[test]
fn version_is_available() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn canon_command_runs_from_stdin_to_stdout() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--from", "json"])
        .write_stdin(r#"{"z":"2","a":"true"}"#)
        .assert()
        .code(0)
        .stdout(predicate::str::contains(r#"{"a":true,"z":2}"#));
}

#[test]
fn canon_command_without_emit_pipeline_keeps_stderr_unchanged() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--from", "json"])
        .write_stdin(r#"{"z":"2","a":"true"}"#)
        .assert()
        .code(0)
        .stdout(predicate::str::contains(r#"{"a":true,"z":2}"#))
        .stderr(predicate::str::is_empty());
}

#[test]
fn canon_command_allows_disabling_key_sorting() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "canon",
            "--from",
            "json",
            "--to",
            "json",
            "--sort-keys=false",
        ])
        .write_stdin(r#"{"z":"2","a":"true"}"#)
        .assert()
        .code(0)
        .stdout(predicate::str::contains(r#"{"z":2,"a":true}"#));
}

#[test]
fn assert_command_reports_validation_mismatch() {
    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.json");
    fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id"],
            "fields": {"id": {"type": "integer"}},
            "count": {"min": 1, "max": 1},
            "forbid_keys": []
        }"#,
    )
    .expect("write rules");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["assert", "--rules", rules_path.to_str().expect("utf8 path")])
        .write_stdin(r#"[{"id":"oops"}]"#)
        .assert()
        .code(2)
        .stdout(predicate::str::contains("\"mismatch_count\":1"));
}

#[test]
fn assert_command_normalize_github_actions_jobs_from_raw_yaml() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_normalize_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("temp dir");
    let workflow_path = dir.path().join("workflow.yml");
    fs::write(
        &workflow_path,
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
    .expect("write workflow");
    let rules_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/assert-rules/github-actions/jobs.rules.yaml");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "assert",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--normalize",
            "github-actions-jobs",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .assert()
        .code(0)
        .stdout(predicate::str::contains("\"matched\":true"));
    drop(tool_dir);
}

#[test]
fn assert_command_normalize_gitlab_ci_jobs_from_raw_yaml() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_normalize_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("temp dir");
    let workflow_path = dir.path().join(".gitlab-ci.yml");
    fs::write(
        &workflow_path,
        r#"
stages: [build]
build:
  stage: build
  script:
    - echo ok
  only:
    - main
"#,
    )
    .expect("write workflow");
    let rules_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/assert-rules/gitlab-ci/jobs.rules.yaml");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "assert",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--normalize",
            "gitlab-ci-jobs",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .assert()
        .code(2)
        .stdout(predicate::str::contains("\"reason\":\"enum_mismatch\""));
    drop(tool_dir);
}

#[test]
fn assert_rules_help_outputs_machine_readable_json() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["assert", "--rules-help"])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(stdout_json["schema"], Value::from("dataq.assert.rules.v1"));
    assert!(stdout_json["top_level_keys"]["fields"].is_string());
    assert!(stdout_json["example"]["fields"]["id"]["type"].is_string());
}

#[test]
fn assert_rules_help_conflicts_with_rules_source() {
    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.json");
    fs::write(&rules_path, r#"{"required_keys":[],"count":{}}"#).expect("write rules");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "assert",
            "--rules-help",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn assert_schema_help_outputs_machine_readable_json() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["assert", "--schema-help"])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(
        stdout_json["schema"],
        Value::from("dataq.assert.schema_help.v1")
    );
    assert_eq!(
        stdout_json["mismatch_shape"]["reason"],
        Value::from("schema_mismatch")
    );
    assert!(stdout_json["example_schema"].is_object());
}

#[test]
fn assert_rules_help_with_emit_pipeline_emits_help_stage() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["assert", "--rules-help", "--emit-pipeline"])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(stdout_json["schema"], Value::from("dataq.assert.rules.v1"));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("assert"));
    assert_eq!(
        stderr_json["steps"],
        Value::Array(vec![Value::from("emit_assert_rules_help")])
    );
}

#[test]
fn assert_schema_help_with_emit_pipeline_emits_help_stage() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["assert", "--schema-help", "--emit-pipeline"])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(
        stdout_json["schema"],
        Value::from("dataq.assert.schema_help.v1")
    );
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("assert"));
    assert_eq!(
        stderr_json["steps"],
        Value::Array(vec![Value::from("emit_assert_schema_help")])
    );
}

#[test]
fn assert_schema_help_conflicts_with_schema_source() {
    let dir = tempdir().expect("temp dir");
    let schema_path = dir.path().join("schema.json");
    fs::write(&schema_path, r#"{"type":"object"}"#).expect("write schema");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "assert",
            "--schema-help",
            "--schema",
            schema_path.to_str().expect("utf8 path"),
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn emit_pipeline_outputs_stderr_json_with_expected_schema() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["--emit-pipeline", "canon", "--from", "json"])
        .write_stdin(r#"{"z":"2","a":"true"}"#)
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("canon"));
    assert!(stderr_json["input"]["sources"].is_array());
    assert!(
        !stderr_json["steps"]
            .as_array()
            .expect("steps array")
            .is_empty()
    );
    assert!(stderr_json["external_tools"].is_array());
    assert!(
        !stderr_json["deterministic_guards"]
            .as_array()
            .expect("guards array")
            .is_empty()
    );
}

#[test]
fn emit_pipeline_preserves_assert_exit_code_contract() {
    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.json");
    fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id"],
            "fields": {"id": {"type": "integer"}},
            "count": {"min": 1, "max": 1},
            "forbid_keys": []
        }"#,
    )
    .expect("write rules");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "assert",
            "--emit-pipeline",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .write_stdin(r#"[{"id":"oops"}]"#)
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("\"mismatch_count\":1"));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("assert"));
    assert!(
        !stderr_json["steps"]
            .as_array()
            .expect("steps array")
            .is_empty()
    );
}

#[test]
fn sdiff_command_reports_counts_and_values() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"name":"alice"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"name":"bob"}]"#).expect("write right");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
        ])
        .assert()
        .code(0)
        .stdout(predicate::str::contains("\"counts\""))
        .stdout(predicate::str::contains("\"values\""));
}

#[test]
fn parser_errors_return_json_with_exit_code_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--to", "json"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
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

fn create_normalize_tool_shims() -> Option<(tempfile::TempDir, String, String)> {
    if Command::new("jq").arg("--version").output().is_err() {
        return None;
    }

    let dir = tempdir().expect("tempdir");
    let yq_path = dir.path().join("fake-yq");
    let mlr_path = dir.path().join("fake-mlr");

    write_exec_script(
        &yq_path,
        r#"#!/bin/sh
if [ "$1" = "eval" ]; then shift; fi
if [ "$1" = "-o=json" ]; then shift; fi
if [ "$1" = "-I=0" ]; then shift; fi
filter="$1"
exec jq -c "$filter"
"#,
    );
    write_exec_script(
        &mlr_path,
        r#"#!/bin/sh
key="job_id"
while [ $# -gt 0 ]; do
  if [ "$1" = "-f" ]; then
    key="$2"
    break
  fi
  shift
done
exec jq -c --arg key "$key" 'sort_by(.[$key] // "")'
"#,
    );

    Some((
        dir,
        yq_path.display().to_string(),
        mlr_path.display().to_string(),
    ))
}

fn write_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
