use std::fs;

use predicates::prelude::predicate;
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
