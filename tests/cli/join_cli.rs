use std::fs;
use std::path::PathBuf;

use predicates::prelude::predicate;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn join_command_inner_and_left_return_expected_json_array() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let left = dir.path().join("left.json");
    let right = dir.path().join("right.json");
    fs::write(&left, r#"[{"id":1,"l":"L1"},{"id":2,"l":"L2"}]"#).expect("write left");
    fs::write(&right, r#"[{"id":1,"r":"R1"}]"#).expect("write right");

    let inner_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "join",
            "--left",
            left.to_str().expect("utf8 left path"),
            "--right",
            right.to_str().expect("utf8 right path"),
            "--on",
            "id",
            "--how",
            "inner",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();
    let inner: Value = serde_json::from_slice(&inner_output).expect("parse join inner output");
    assert_eq!(inner, json!([{"id": 1, "l": "L1", "r": "R1"}]));

    let left_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "join",
            "--left",
            left.to_str().expect("utf8 left path"),
            "--right",
            right.to_str().expect("utf8 right path"),
            "--on",
            "id",
            "--how",
            "left",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();
    let left_join: Value = serde_json::from_slice(&left_output).expect("parse join left output");
    assert_eq!(
        left_join,
        json!([
            {"id": 1, "l": "L1", "r": "R1"},
            {"id": 2, "l": "L2", "r": Value::Null}
        ])
    );
}

#[test]
fn aggregate_command_count_sum_avg_are_deterministic() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let first_count = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "aggregate",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--group-by",
            "team",
            "--metric",
            "count",
            "--target",
            "price",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let second_count = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "aggregate",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--group-by",
            "team",
            "--metric",
            "count",
            "--target",
            "price",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    assert_eq!(first_count, second_count);

    let count_json: Value = serde_json::from_slice(&first_count).expect("parse count output");
    assert_eq!(
        count_json,
        json!([
            {"count": 2, "team": "a"},
            {"count": 1, "team": "b"}
        ])
    );

    let sum_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "aggregate",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--group-by",
            "team",
            "--metric",
            "sum",
            "--target",
            "price",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();
    let sum_json: Value = serde_json::from_slice(&sum_output).expect("parse sum output");
    assert_eq!(
        sum_json,
        json!([
            {"sum": 15.0, "team": "a"},
            {"sum": 7.0, "team": "b"}
        ])
    );

    let avg_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "aggregate",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--group-by",
            "team",
            "--metric",
            "avg",
            "--target",
            "price",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();
    let avg_json: Value = serde_json::from_slice(&avg_output).expect("parse avg output");
    assert_eq!(
        avg_json,
        json!([
            {"avg": 7.5, "team": "a"},
            {"avg": 7.0, "team": "b"}
        ])
    );
}

#[test]
fn aggregate_emit_pipeline_reports_stage_diagnostics_with_metrics() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "aggregate",
            "--emit-pipeline",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--group-by",
            "team",
            "--metric",
            "count",
            "--target",
            "price",
        ])
        .output()
        .expect("run aggregate command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("aggregate"));
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["step"],
        Value::from("aggregate_mlr_execute")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["tool"],
        Value::from("mlr")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["status"],
        Value::from("ok")
    );
    assert_stage_metrics_shape(&stderr_json["stage_diagnostics"][0]);
}

#[test]
fn aggregate_emit_pipeline_is_deterministic_for_identical_input() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let run_once = || {
        let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .env("DATAQ_MLR_BIN", &mlr_bin)
            .args([
                "aggregate",
                "--emit-pipeline",
                "--input",
                input.to_str().expect("utf8 input path"),
                "--group-by",
                "team",
                "--metric",
                "count",
                "--target",
                "price",
            ])
            .output()
            .expect("run aggregate command");
        assert_eq!(output.status.code(), Some(0));
        parse_last_stderr_json(&output.stderr)
    };

    let first = run_once();
    let second = run_once();
    assert_eq!(first, second);
    assert_eq!(first["stage_diagnostics"][0]["duration_ms"], Value::from(0));
}

#[test]
fn join_missing_key_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let left = dir.path().join("left.json");
    let right = dir.path().join("right.json");
    fs::write(&left, r#"[{"id":1},{"name":"missing-id"}]"#).expect("write left");
    fs::write(&right, r#"[{"id":1}]"#).expect("write right");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "join",
            "--left",
            left.to_str().expect("utf8 left path"),
            "--right",
            right.to_str().expect("utf8 right path"),
            "--on",
            "id",
            "--how",
            "inner",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("missing join key `id`"));
}

#[test]
fn join_validation_failure_does_not_mark_mlr_used() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let left = dir.path().join("left.json");
    let right = dir.path().join("right.json");
    fs::write(&left, r#"[{"id":1},{"name":"missing-id"}]"#).expect("write left");
    fs::write(&right, r#"[{"id":1}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "join",
            "--emit-pipeline",
            "--left",
            left.to_str().expect("utf8 left path"),
            "--right",
            right.to_str().expect("utf8 right path"),
            "--on",
            "id",
            "--how",
            "inner",
        ])
        .output()
        .expect("run join command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);

    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external tools array");
    let mlr_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("mlr"))
        .expect("mlr entry");
    assert_eq!(mlr_entry["used"], Value::Bool(false));

    let stage_steps: Vec<String> = stderr_json["stage_diagnostics"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|entry| entry["step"].as_str().map(ToString::to_string))
        .collect();
    assert!(!stage_steps.iter().any(|step| step == "join_mlr_execute"));
}

#[test]
fn join_emit_pipeline_reports_stage_diagnostics() {
    let dir = tempdir().expect("tempdir");
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let left = dir.path().join("left.json");
    let right = dir.path().join("right.json");
    fs::write(&left, r#"[{"id":1,"l":"L1"}]"#).expect("write left");
    fs::write(&right, r#"[{"id":1,"r":"R1"}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "join",
            "--emit-pipeline",
            "--left",
            left.to_str().expect("utf8 left path"),
            "--right",
            right.to_str().expect("utf8 right path"),
            "--on",
            "id",
            "--how",
            "inner",
        ])
        .output()
        .expect("run join command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("join"));
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["step"],
        Value::from("join_mlr_execute")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["tool"],
        Value::from("mlr")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["status"],
        Value::from("ok")
    );
    assert_stage_metrics_shape(&stderr_json["stage_diagnostics"][0]);

    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external tools array");
    let mlr_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("mlr"))
        .expect("mlr entry");
    assert_eq!(mlr_entry["used"], Value::Bool(true));
    assert_eq!(
        stderr_json["fingerprint"]["tool_versions"]["mlr"],
        Value::from("mlr-fake 1.0.0")
    );
}

fn assert_stage_metrics_shape(stage: &Value) {
    assert!(stage["input_bytes"].is_u64());
    assert!(stage["output_bytes"].is_u64());
    assert!(stage["duration_ms"].is_u64());
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

fn write_fake_mlr_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo 'mlr-fake 1.0.0'
  exit 0
fi

mode=""
action=""
left_file=""
capture_next_f=0
for arg in "$@"; do
  if [ "$capture_next_f" = "1" ]; then
    left_file="$arg"
    capture_next_f=0
    continue
  fi
  if [ "$arg" = "join" ]; then mode="join"; fi
  if [ "$arg" = "stats1" ]; then mode="stats1"; fi
  if [ "$arg" = "count" ] || [ "$arg" = "sum" ] || [ "$arg" = "mean" ]; then action="$arg"; fi
  if [ "$arg" = "-f" ]; then capture_next_f=1; fi
  if [ "$arg" = "--ul" ]; then left_join="1"; fi
done

if [ "$mode" = "join" ]; then
  if [ -z "$left_file" ]; then
    echo 'missing -f left file' 1>&2
    exit 9
  fi
  if ! grep -q '"l":"L' "$left_file"; then
    echo 'left file does not contain expected left records' 1>&2
    exit 9
  fi
  stdin_payload="$(cat)"
  if ! printf '%s' "$stdin_payload" | grep -q '"r":"R1"'; then
    echo 'stdin does not contain expected right records' 1>&2
    exit 9
  fi
  if [ -n "$left_join" ]; then
    printf '[{"id":1,"l":"L1","r":"R1"},{"id":2,"l":"L2","r":null}]'
  else
    printf '[{"id":1,"l":"L1","r":"R1"}]'
  fi
  exit 0
fi

if [ "$mode" = "stats1" ]; then
  if [ "$action" = "count" ]; then
    printf '[{"team":"a","price_count":"2"},{"team":"b","price_count":"1"}]'
    exit 0
  fi
  if [ "$action" = "sum" ]; then
    printf '[{"team":"a","price_sum":"15.0"},{"team":"b","price_sum":"7.0"}]'
    exit 0
  fi
  if [ "$action" = "mean" ]; then
    printf '[{"team":"a","price_mean":"7.5"},{"team":"b","price_mean":"7.0"}]'
    exit 0
  fi
fi

echo 'unexpected mlr args' 1>&2
exit 9
"#;

    fs::write(&path, script).expect("write fake mlr script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    path
}
