use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn transform_sql_duckdb_flow_is_deterministic() {
    let dir = tempdir().expect("tempdir");
    let duckdb_bin = write_fake_duckdb_script(dir.path().join("fake-duckdb"));

    let input = r#"[{"team":"a","price":10.0},{"team":"a","price":5.0},{"team":"b","price":7.0}]"#;
    let query = "SELECT team, AVG(price) AS avg_price FROM input GROUP BY team ORDER BY team";

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", &duckdb_bin)
        .args([
            "transform",
            "sql",
            "--engine",
            "duckdb",
            "--input",
            "-",
            "--query",
            query,
        ])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_DUCKDB_BIN", &duckdb_bin)
        .args([
            "transform",
            "sql",
            "--engine",
            "duckdb",
            "--input",
            "-",
            "--query",
            query,
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
            {"team": "b", "avg_price": "7.000000"},
            {"team": "a", "avg_price": "7.500000"}
        ])
    );
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

printf '[{"team":"a","avg_price":"7.500000"},{"team":"b","avg_price":"7.000000"}]'
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
