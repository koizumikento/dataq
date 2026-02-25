use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn ingest_notes_jsonl_is_deterministic_and_boundary_inclusive() {
    if Command::new("jq").arg("--version").output().is_err() {
        return;
    }

    let dir = tempdir().expect("tempdir");
    let nb_bin = write_fake_nb_script(dir.path().join("fake-nb"));

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_NB_BIN", &nb_bin)
        .args([
            "ingest",
            "notes",
            "--tag",
            "work",
            "--since",
            "2025-01-15T00:00:00Z",
            "--until",
            "2025-01-31T23:59:59Z",
            "--to",
            "jsonl",
        ])
        .output()
        .expect("run first ingest notes");
    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_NB_BIN", &nb_bin)
        .args([
            "ingest",
            "notes",
            "--tag",
            "work",
            "--since",
            "2025-01-15T00:00:00Z",
            "--until",
            "2025-01-31T23:59:59Z",
            "--to",
            "jsonl",
        ])
        .output()
        .expect("run second ingest notes");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());

    let stdout_text = String::from_utf8(first.stdout).expect("stdout utf8");
    let rows: Vec<Value> = stdout_text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl row"))
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], Value::from("n-100"));
    assert_eq!(rows[0]["created_at"], Value::from("2025-01-15T00:00:00Z"));
    assert_eq!(rows[1]["id"], Value::from("n-200"));
    assert_eq!(rows[1]["created_at"], Value::from("2025-01-31T23:59:59Z"));
}

fn write_fake_nb_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
if [ "$1" = "list" ] && [ "$2" = "--format" ] && [ "$3" = "json" ]; then
  cat <<'JSON'
[
  {
    "id": "n-200",
    "title": "Boundary until",
    "body": "u",
    "tags": ["work"],
    "created_at": "2025-01-31T23:59:59Z",
    "updated_at": null,
    "notebook": "ops",
    "path": "ops/boundary-until"
  },
  {
    "id": "n-100",
    "title": "Boundary since (offset)",
    "body": "s",
    "tags": ["work"],
    "created_at": "2025-01-15T09:00:00+09:00",
    "updated_at": null,
    "notebook": "ops",
    "path": "ops/boundary-since"
  },
  {
    "id": "n-300",
    "title": "Outside range",
    "body": "x",
    "tags": ["work"],
    "created_at": "2025-02-01T00:00:00Z",
    "updated_at": null,
    "notebook": "ops",
    "path": "ops/outside"
  }
]
JSON
  exit 0
fi

if [ "$1" = "export" ] && [ "$2" = "--format" ] && [ "$3" = "json" ]; then
  exec "$0" list --format json
fi

if [ "$1" = "--version" ]; then
  printf 'nb 7.0.0\n'
  exit 0
fi

echo 'unexpected nb args' 1>&2
exit 9
"#;

    fs::write(&path, script).expect("write fake nb script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    path
}
