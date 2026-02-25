use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn transform_rowset_stdin_flow_is_deterministic() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_fake_jq_script(dir.path().join("fake-jq"));
    let mlr_bin = write_fake_mlr_script(dir.path().join("fake-mlr"));

    let input = r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#;

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--input",
            "-",
            "--jq-filter",
            ".",
            "--mlr",
            "stats1",
            "-a",
            "mean",
            "-f",
            "price",
            "-g",
            "team",
        ])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "transform",
            "rowset",
            "--input",
            "-",
            "--jq-filter",
            ".",
            "--mlr",
            "stats1",
            "-a",
            "mean",
            "-f",
            "price",
            "-g",
            "team",
        ])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    assert_eq!(first, second);

    let actual: Value = serde_json::from_slice(&first).expect("stdout json");
    assert_eq!(
        actual,
        json!([
            {"avg": "7.000000", "team": "b"},
            {"avg": "7.500000", "team": "a"}
        ])
    );
}

fn write_fake_jq_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'jq-1.7\n'
    exit 0
  fi
done

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

if [ "$mode" = "stats1" ] && [ "$action" = "mean" ]; then
  printf '[{"team":"b","avg":"7.000000"},{"team":"a","avg":"7.500000"}]'
  exit 0
fi

echo 'unexpected mlr args' 1>&2
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
