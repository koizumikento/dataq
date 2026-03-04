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
                {"sum": "15.000000", "team": "a"},
                {"sum": "7.000000", "team": "b"}
            ]),
        ),
        (
            "mean",
            json!([
                {"avg": "7.000000", "team": "b"},
                {"avg": "7.500000", "team": "a"}
            ]),
        ),
    ] {
        let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .env("DATAQ_JQ_BIN", &jq_bin)
            .env("DATAQ_MLR_BIN", &mlr_bin)
            .args([
                "transform",
                "rowset",
                "--engine",
                "sqlite",
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
            "--engine",
            "sqlite",
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
        stderr_json["stage_diagnostics"][1]["tool"],
        Value::from("mlr")
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
fn transform_rowset_preserves_string_values_from_mlr_output() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(&input, r#"[{"team":"a","price":10.0}]"#).expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--engine",
            "sqlite",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--jq-filter",
            ".",
            "--mlr",
            "stats1",
            "-a",
            "literal",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let parsed: Value = serde_json::from_slice(&output).expect("parse transform rowset output");
    assert_eq!(
        parsed,
        json!([
            {"label": "exp", "value": "1e3"},
            {"label": "fixed", "value": "7.000000"}
        ])
    );
}

#[test]
fn transform_rowset_emit_pipeline_after_mlr_args_is_parsed_as_global_flag() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--engine",
            "sqlite",
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
            "--emit-pipeline",
        ])
        .output()
        .expect("run transform rowset");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("transform.rowset"));
    assert_eq!(
        stderr_json["stage_diagnostics"][1]["step"],
        Value::from("transform_rowset_mlr")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][1]["tool"],
        Value::from("mlr")
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
            "--engine",
            "sqlite",
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
            "--engine",
            "sqlite",
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

#[test]
fn transform_sql_duckdb_is_machine_readable_and_deterministic() {
    let dir = tempdir().expect("tempdir");
    let duckdb_bin = write_fake_duckdb_script(dir.path().join("fake-duckdb"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", &duckdb_bin)
        .args([
            "transform",
            "sql",
            "--engine",
            "duckdb",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--query",
            "select team, avg(price) as avg from input group by team",
        ])
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let parsed: Value = serde_json::from_slice(&output).expect("parse transform sql output");
    assert_eq!(
        parsed,
        json!([
            {"avg": 7.0, "team": "a"},
            {"avg": 7.5, "team": "z"}
        ])
    );
}

#[test]
fn transform_sql_emit_pipeline_reports_duckdb_stage() {
    let dir = tempdir().expect("tempdir");
    let duckdb_bin = write_fake_duckdb_script(dir.path().join("fake-duckdb"));

    let input = dir.path().join("input.json");
    fs::write(
        &input,
        r#"[{"team":"a","price":10.0},{"team":"b","price":7.0}]"#,
    )
    .expect("write input");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", &duckdb_bin)
        .args([
            "transform",
            "sql",
            "--emit-pipeline",
            "--engine",
            "duckdb",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--query",
            "select team, avg(price) as avg from input group by team",
        ])
        .output()
        .expect("run transform sql");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("transform.sql"));
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["step"],
        Value::from("transform_sql_duckdb")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["tool"],
        Value::from("duckdb")
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["input_records"],
        Value::from(2)
    );
    assert_eq!(
        stderr_json["stage_diagnostics"][0]["output_records"],
        Value::from(2)
    );

    let duckdb_tool = stderr_json["external_tools"]
        .as_array()
        .expect("external_tools")
        .iter()
        .find(|tool| tool["name"] == "duckdb")
        .expect("duckdb tool");
    assert_eq!(duckdb_tool["used"], Value::Bool(true));
}

#[test]
fn transform_sql_missing_duckdb_maps_to_exit_three() {
    let dir = tempdir().expect("tempdir");
    let input = dir.path().join("input.json");
    fs::write(&input, r#"[{"team":"a","price":10.0}]"#).expect("write input");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", "/definitely-missing/duckdb")
        .args([
            "transform",
            "sql",
            "--engine",
            "duckdb",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--query",
            "select * from input",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "failed to transform rowset with duckdb: `duckdb` is not available in PATH",
        ));
}

#[test]
fn transform_sql_bootstraps_input_relation_for_duckdb() {
    let dir = tempdir().expect("tempdir");
    let duckdb_bin = write_duckdb_bootstrap_asserting_script(dir.path().join("fake-duckdb"));

    let input = dir.path().join("input.json");
    fs::write(&input, r#"[{"team":"a","price":10.0}]"#).expect("write input");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", &duckdb_bin)
        .args([
            "transform",
            "sql",
            "--engine",
            "duckdb",
            "--input",
            input.to_str().expect("utf8 input path"),
            "--query",
            "select * from input",
        ])
        .assert()
        .code(0);
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
  if [ "$arg" = "count" ] || [ "$arg" = "sum" ] || [ "$arg" = "mean" ] || [ "$arg" = "literal" ]; then action="$arg"; fi
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
if [ "$action" = "literal" ]; then
  printf '[{"label":"exp","value":"1e3"},{"label":"fixed","value":"7.000000"}]'
  exit 0
fi

echo 'missing stats action' 1>&2
exit 9
"#;

    fs::write(&path, script).expect("write mlr script");
    set_executable(&path);
    path
}

fn write_fake_duckdb_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'duckdb v1.3.0\n'
    exit 0
  fi
done

if [ -t 0 ]; then
  :
else
  cat >/dev/null
fi

printf '[{"team":"z","avg":7.5},{"team":"a","avg":7.0}]'
"#;

    fs::write(&path, script).expect("write duckdb script");
    set_executable(&path);
    path
}

fn write_duckdb_bootstrap_asserting_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
if [ "$1" != "-json" ] || [ "$2" != "-batch" ] || [ "$3" != "-c" ]; then
  echo 'unexpected duckdb args' 1>&2
  exit 9
fi
case "$4" in
  *"CREATE OR REPLACE TEMP TABLE input AS SELECT * FROM read_json_auto("*"; select * from input"*)
    printf '[{"ok":true}]'
    exit 0
    ;;
  *)
    echo 'missing input bootstrap query' 1>&2
    exit 9
    ;;
esac
"#;

    fs::write(&path, script).expect("write duckdb script");
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
