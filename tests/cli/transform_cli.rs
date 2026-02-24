use std::fs;
use std::path::PathBuf;

use predicates::prelude::predicate;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn transform_rowset_count_sum_avg_are_deterministic_json_arrays() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    for (metric, expected) in [
        (
            "count",
            json!([
                {"count": 1, "team": "b"},
                {"count": 2, "team": "a"}
            ]),
        ),
        (
            "sum",
            json!([
                {"sum": 15.0, "team": "a"},
                {"sum": 7.0, "team": "b"}
            ]),
        ),
        (
            "mean",
            json!([
                {"avg": 7.0, "team": "b"},
                {"avg": 7.5, "team": "a"}
            ]),
        ),
    ] {
        let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .env("DATAQ_JQ_BIN", &jq_bin)
            .env("DATAQ_MLR_BIN", &mlr_bin)
            .args([
                "transform",
                "rowset",
                "--input",
                input.to_str().expect("utf8 input path"),
                "--jq-filter",
                ".",
                "--mlr",
                "stats1",
                "-a",
                metric,
                "-f",
                "price",
                "-g",
                "team",
            ])
            .assert()
            .code(0)
            .get_output()
            .stdout
            .clone();

        let parsed: Value = serde_json::from_slice(&output).expect("parse transform rowset output");
        assert_eq!(parsed, expected);
    }
}

#[test]
fn transform_rowset_emit_pipeline_reports_stage_diagnostics_with_record_counts() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--emit-pipeline",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--jq-filter",
            ".",
            "--mlr",
            "stats1",
            "-a",
            "count",
            "-f",
            "price",
            "-g",
            "team",
        ])
        .output()
        .expect("run transform rowset");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("transform.rowset"));
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["step"],
        Value::from("transform_rowset_jq")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["input_records"],
        Value::from(3)
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["output_records"],
        Value::from(3)
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][1]["step"],
        Value::from("transform_rowset_mlr")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][1]["input_records"],
        Value::from(3)
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][1]["output_records"],
        Value::from(2)
    );
}

#[test]
fn transform_rowset_malformed_commands_return_exit_three() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(&input, r#"[{"team":"a","price":10.0}]"#).expect("write input");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--jq-filter",
            " ",
            "--mlr",
            "stats1",
            "-a",
            "count",
            "-f",
            "price",
            "-g",
            "team",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("`--jq-filter` cannot be empty"));

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--jq-filter",
            ".",
            "--mlr",
            "badverb",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "failed to transform rowset with mlr",
        ));
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

fn write_fake_jq_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'jq-1.7\n'
    exit 0
  fi
done

filter="$2"
if [ -z "$filter" ] || [ "$filter" = "bad_filter(" ]; then
  echo 'jq parse error' 1>&2
  exit 3
fi

cat
"#;

    fs::write(&path, script).expect("write jq script");
    set_executable(&path);
    path
}

fn write_fake_mlr_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'mlr 6.13.0\n'
    exit 0
  fi
done

mode=""
action=""
for arg in "$@"; do
  if [ "$arg" = "stats1" ]; then mode="stats1"; fi
  if [ "$arg" = "count" ] || [ "$arg" = "sum" ] || [ "$arg" = "mean" ]; then action="$arg"; fi
done

if [ "$mode" != "stats1" ]; then
  echo 'unsupported mlr mode' 1>&2
  exit 9
fi

if [ "$action" = "count" ]; then
  printf '[{"team":"b","count":1},{"team":"a","count":2}]'
  exit 0
fi
if [ "$action" = "sum" ]; then
  printf '[{"team":"b","sum":"7.000000"},{"team":"a","sum":"15.000000"}]'
  exit 0
fi
if [ "$action" = "mean" ]; then
  printf '[{"team":"b","avg":"7.000000"},{"team":"a","avg":"7.500000"}]'
  exit 0
fi

echo 'missing stats action' 1>&2
exit 9
"#;

    fs::write(&path, script).expect("write mlr script");
    set_executable(&path);
    path
}

fn set_executable(path: &PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
