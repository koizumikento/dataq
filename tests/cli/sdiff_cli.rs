use std::fs;

use dataq::cmd::sdiff;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn no_diff_report_is_empty_and_deterministic() {
    let left = vec![json!({"a": 1}), json!({"a": 2})];
    let right = vec![json!({"a": 1}), json!({"a": 2})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 2,
            "right": 2,
            "delta": 0,
            "equal": true
        },
        "keys": {
            "left_only": [],
            "right_only": [],
            "shared": ["$[\"a\"]"]
        },
        "ignored_paths": [],
        "values": {
            "total": 0,
            "truncated": false,
            "items": []
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn count_diff_is_reported() {
    let left = vec![json!({"a": 1})];
    let right = vec![json!({"a": 1}), json!({"a": 2})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 1,
            "right": 2,
            "delta": 1,
            "equal": false
        },
        "keys": {
            "left_only": [],
            "right_only": [],
            "shared": ["$[\"a\"]"]
        },
        "ignored_paths": [],
        "values": {
            "total": 0,
            "truncated": false,
            "items": []
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn key_diff_is_reported() {
    let left = vec![json!({"a": 1, "only_left": true})];
    let right = vec![json!({"a": 1, "only_right": true})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 1,
            "right": 1,
            "delta": 0,
            "equal": true
        },
        "keys": {
            "left_only": ["$[\"only_left\"]"],
            "right_only": ["$[\"only_right\"]"],
            "shared": ["$[\"a\"]"]
        },
        "ignored_paths": [],
        "values": {
            "total": 2,
            "truncated": false,
            "items": [
                {
                    "path": "$[0][\"only_left\"]",
                    "left": true,
                    "right": null
                },
                {
                    "path": "$[0][\"only_right\"]",
                    "left": null,
                    "right": true
                }
            ]
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn paths_escape_punctuation_and_quotes_deterministically() {
    let left = vec![json!({
        "a.b": 1,
        "quote\"key": "left",
        "bracket[": true,
        "bracket]": 1
    })];
    let right = vec![json!({
        "a": {"b": 1},
        "quote\"key": "right",
        "bracket[": true,
        "bracket]": 2
    })];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 1,
            "right": 1,
            "delta": 0,
            "equal": true
        },
        "keys": {
            "left_only": ["$[\"a.b\"]"],
            "right_only": ["$[\"a\"]", "$[\"a\"][\"b\"]"],
            "shared": ["$[\"bracket[\"]", "$[\"bracket]\"]", "$[\"quote\\\"key\"]"]
        },
        "ignored_paths": [],
        "values": {
            "total": 4,
            "truncated": false,
            "items": [
                {
                    "path": "$[0][\"a\"]",
                    "left": null,
                    "right": {"b": 1}
                },
                {
                    "path": "$[0][\"a.b\"]",
                    "left": 1,
                    "right": null
                },
                {
                    "path": "$[0][\"bracket]\"]",
                    "left": 1,
                    "right": 2
                },
                {
                    "path": "$[0][\"quote\\\"key\"]",
                    "left": "left",
                    "right": "right"
                }
            ]
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn key_mode_aligns_rows_by_canonical_path() {
    let left = vec![json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})];
    let right = vec![json!({"id": 2, "v": "b"}), json!({"id": 1, "v": "a"})];

    let by_index = sdiff::execute(&left, &right);
    assert_eq!(by_index.values.total, 4);

    let options = sdiff::parse_options(
        sdiff::DEFAULT_VALUE_DIFF_CAP,
        Some(r#"$["id"]"#),
        &Vec::<String>::new(),
    )
    .expect("parse options");
    let by_key = sdiff::execute_with_options(&left, &right, options).expect("compare by key");
    assert_eq!(by_key.values.total, 0);
}

#[test]
fn key_path_supports_escaped_keys() {
    let left = vec![
        json!({"a.b": 1, "v": "left"}),
        json!({"a.b": 2, "v": "same"}),
    ];
    let right = vec![
        json!({"a.b": 2, "v": "same"}),
        json!({"a.b": 1, "v": "right"}),
    ];

    let options = sdiff::parse_options(
        sdiff::DEFAULT_VALUE_DIFF_CAP,
        Some(r#"$["a.b"]"#),
        &Vec::<String>::new(),
    )
    .expect("parse options");
    let report = sdiff::execute_with_options(&left, &right, options).expect("compare by key");
    assert_eq!(report.values.total, 1);
    assert_eq!(report.values.items[0].path, "$[0][\"v\"]");
}

#[test]
fn ignore_path_reduces_value_diffs_and_reports_ignored_paths() {
    let left = vec![json!({"id": 1, "name": "alice", "meta": {"updated": "2025-01-01"}})];
    let right = vec![json!({"id": 1, "name": "bob", "meta": {"updated": "2025-02-01"}})];

    let ignore_paths = vec![r#"$["meta"]["updated"]"#.to_string()];
    let options = sdiff::parse_options(sdiff::DEFAULT_VALUE_DIFF_CAP, None, &ignore_paths)
        .expect("parse options");
    let report = sdiff::execute_with_options(&left, &right, options).expect("compare");
    let actual = serde_json::to_value(report).expect("serialize report");

    assert_eq!(actual["ignored_paths"], json!([r#"$["meta"]["updated"]"#]));
    assert_eq!(actual["values"]["total"], json!(1));
    assert_eq!(
        actual["values"]["items"][0]["path"],
        json!("$[0][\"name\"]")
    );
}

#[test]
fn duplicate_key_returns_exit_code_three() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"a"},{"id":1,"v":"b"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"c"}]"#).expect("write right");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--key",
            r#"$["id"]"#,
        ])
        .assert()
        .code(3)
        .stderr(predicates::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicates::str::contains("duplicate key value"));
}

#[test]
fn value_diff_cap_option_truncates_report_items() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"a"},{"id":2,"v":"b"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"x"},{"id":2,"v":"y"}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--value-diff-cap",
            "1",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let report = parse_stdout_json(&output.stdout);
    assert_eq!(report["values"]["total"], json!(2));
    assert_eq!(report["values"]["truncated"], json!(true));
    assert_eq!(report["values"]["items"].as_array().map(Vec::len), Some(1));
}

#[test]
fn fail_on_diff_is_opt_in_and_defaults_to_exit_zero() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"left"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"right"}]"#).expect("write right");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
        ])
        .assert()
        .code(0);
}

#[test]
fn fail_on_diff_returns_exit_two_when_value_diff_exists() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"left"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"right"}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--fail-on-diff",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    let report = parse_stdout_json(&output.stdout);
    assert_eq!(report["values"]["total"], json!(1));
}

#[test]
fn fail_on_diff_keeps_exit_zero_when_no_value_diffs() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"same"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"same"}]"#).expect("write right");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--fail-on-diff",
        ])
        .assert()
        .code(0);
}

#[test]
fn fail_on_diff_preserves_emit_pipeline_output() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"left"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"right"}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--fail-on-diff",
            "--emit-pipeline",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    let pipeline_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(pipeline_json["command"], json!("sdiff"));
    assert!(
        !pipeline_json["steps"]
            .as_array()
            .expect("steps array")
            .is_empty()
    );
}

#[test]
fn fail_on_diff_with_key_and_ignore_path_keeps_alignment_behavior() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(
        &left_path,
        r#"[{"id":2,"v":"left","updated":"2025-01-01"},{"id":1,"v":"same","updated":"2025-01-01"}]"#,
    )
    .expect("write left");
    fs::write(
        &right_path,
        r#"[{"id":1,"v":"same","updated":"2025-02-01"},{"id":2,"v":"right","updated":"2025-02-01"}]"#,
    )
    .expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left path"),
            "--right",
            right_path.to_str().expect("utf8 right path"),
            "--key",
            r#"$["id"]"#,
            "--ignore-path",
            r#"$["updated"]"#,
            "--fail-on-diff",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    let report = parse_stdout_json(&output.stdout);
    assert_eq!(report["ignored_paths"], json!([r#"$["updated"]"#]));
    assert_eq!(report["values"]["total"], json!(1));
    assert_eq!(report["values"]["items"][0]["path"], json!("$[1][\"v\"]"));
}

fn parse_stdout_json(stdout: &[u8]) -> Value {
    let text = String::from_utf8(stdout.to_vec()).expect("stdout utf8");
    let line = text
        .lines()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stdout line");
    serde_json::from_str(line).expect("stdout json")
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
