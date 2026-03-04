use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn ingest_jc_is_deterministic_for_identical_input() {
    let dir = tempdir().expect("tempdir");
    let jc_bin = write_fake_jc_script(dir.path().join("fake-jc"));

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JC_BIN", &jc_bin)
        .args(["ingest", "jc", "--parser", "ifconfig", "--input", "-"])
        .write_stdin("eth0: up\n")
        .output()
        .expect("run first ingest jc");
    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JC_BIN", &jc_bin)
        .args(["ingest", "jc", "--parser", "ifconfig", "--input", "-"])
        .write_stdin("eth0: up\n")
        .output()
        .expect("run second ingest jc");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&first.stdout).expect("stdout json");
    assert_eq!(payload["source"], json!("jc"));
    assert_eq!(payload["parser"], json!("ifconfig"));
    assert_eq!(payload["record_count"], json!(2));
    assert_eq!(payload["records"][0], json!({"a": 1, "b": 2}));
}

#[test]
fn ingest_jc_missing_binary_returns_exit_three() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JC_BIN", "/definitely-missing/jc")
        .args(["ingest", "jc", "--parser", "ifconfig", "--input", "-"])
        .write_stdin("eth0: up\n")
        .output()
        .expect("run ingest jc");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], json!("input_usage_error"));
    assert_eq!(
        stderr_json["message"],
        json!("ingest jc requires `jc` in PATH")
    );
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

fn write_fake_jc_script(path: PathBuf) -> PathBuf {
    write_exec_script(
        &path,
        r#"#!/bin/sh
parser=""
while [ $# -gt 0 ]; do
  case "$1" in
    --quiet)
      shift
      ;;
    --*)
      parser="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

cat >/dev/null
if [ "$parser" = "--ifconfig" ]; then
  cat <<'JSON'
[{"b":2,"a":1},{"nested":{"d":4,"c":3}}]
JSON
  exit 0
fi

echo "unsupported parser: $parser" 1>&2
exit 7
"#,
    );
    path
}

fn write_exec_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
