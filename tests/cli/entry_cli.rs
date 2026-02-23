use std::fs;

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
        .stdout(predicate::str::contains("sdiff"));
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
            "types": {"id": "integer"},
            "count": {"min": 1, "max": 1},
            "ranges": {}
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
            "types": {"id": "integer"},
            "count": {"min": 1, "max": 1},
            "ranges": {}
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
