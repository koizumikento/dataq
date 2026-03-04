use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn ingest_tabular_output_is_deterministic_for_identical_input() {
    let (_tool_dir, in2csv_bin, csvjson_bin) = create_fake_csvkit_shims();
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("rows.csv");
    fs::write(&input_path, "id,name\n1,alice\n2,bob\n").expect("write csv");

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_CSVKIT_IN2CSV_BIN", &in2csv_bin)
        .env("DATAQ_CSVKIT_CSVJSON_BIN", &csvjson_bin)
        .args([
            "ingest",
            "tabular",
            "--input",
            input_path.to_str().expect("utf8 input"),
        ])
        .output()
        .expect("run first ingest");
    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_CSVKIT_IN2CSV_BIN", &in2csv_bin)
        .env("DATAQ_CSVKIT_CSVJSON_BIN", &csvjson_bin)
        .args([
            "ingest",
            "tabular",
            "--input",
            input_path.to_str().expect("utf8 input"),
        ])
        .output()
        .expect("run second ingest");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&first.stdout).expect("stdout json");
    assert_eq!(
        payload,
        json!([
            {"id": "1", "name": "alice"},
            {"id": "2", "name": "bob"}
        ])
    );
}

#[test]
fn ingest_tabular_output_roundtrips_into_canon_from_stdin() {
    let (_tool_dir, in2csv_bin, csvjson_bin) = create_fake_csvkit_shims();
    let ingest_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_CSVKIT_IN2CSV_BIN", &in2csv_bin)
        .env("DATAQ_CSVKIT_CSVJSON_BIN", &csvjson_bin)
        .args(["ingest", "tabular", "--input", "-"])
        .write_stdin("id,name\n1,alice\n2,bob\n")
        .output()
        .expect("run ingest from stdin");

    assert_eq!(ingest_output.status.code(), Some(0));
    assert!(ingest_output.stderr.is_empty());

    let canon_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--from", "json"])
        .write_stdin(ingest_output.stdout)
        .output()
        .expect("run canon");

    assert_eq!(canon_output.status.code(), Some(0));
    let canon_payload: Value = serde_json::from_slice(&canon_output.stdout).expect("canon json");
    assert_eq!(
        canon_payload,
        json!([
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"}
        ])
    );
}

fn create_fake_csvkit_shims() -> (tempfile::TempDir, String, String) {
    let dir = tempdir().expect("tempdir");
    let in2csv_path = dir.path().join("fake-in2csv");
    let csvjson_path = dir.path().join("fake-csvjson");

    write_exec_script(
        in2csv_path.clone(),
        r#"if [ "$1" = "--version" ]; then
  echo "csvkit 2.1.0"
  exit 0
fi

if [ $# -gt 0 ]; then
  cat "$1"
else
  cat
fi"#,
    );

    write_exec_script(
        csvjson_path.clone(),
        r#"if [ "$1" = "--version" ]; then
  echo "csvkit 2.1.0"
  exit 0
fi

if [ "$1" = "--no-inference" ]; then
  shift
fi

cat >/dev/null
cat <<'JSON'
[{"name":"alice","id":"1"},{"name":"bob","id":"2"}]
JSON"#,
    );

    (
        dir,
        in2csv_path.display().to_string(),
        csvjson_path.display().to_string(),
    )
}

fn write_exec_script(path: PathBuf, body: &str) {
    let script = format!("#!/bin/sh\n{body}\n");
    fs::write(&path, script).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
