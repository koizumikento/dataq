use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn ingest_api_success_emits_normalized_payload_and_pipeline() {
    let Some((tool_dir, xh_bin, jq_bin)) = create_ingest_tool_shims() else {
        return;
    };

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_XH_BIN", &xh_bin)
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "api",
            "--emit-pipeline",
            "--url",
            "https://example.test/items",
            "--method",
            "GET",
            "--header",
            "accept:application/json",
        ])
        .output()
        .expect("run ingest api");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(stdout_json["source"]["kind"], Value::from("api"));
    assert_eq!(
        stdout_json["source"]["url"],
        Value::from("https://example.test/items")
    );
    assert_eq!(stdout_json["status"], Value::from(200));
    assert_eq!(
        stdout_json["headers"]["content-type"],
        Value::from("application/json")
    );
    assert_eq!(stdout_json["headers"]["etag"], Value::from("W/\"abc\""));
    assert_eq!(
        stdout_json["fetched_at"],
        Value::from("2025-02-24T10:00:00Z")
    );
    assert_eq!(stdout_json["body"]["ok"], Value::Bool(true));

    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("ingest_api"));
    assert_eq!(
        stderr_json["steps"],
        Value::Array(vec![
            Value::from("ingest_api_xh_fetch"),
            Value::from("ingest_api_jq_normalize")
        ])
    );
    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external tools");
    let jq_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("jq"))
        .expect("jq entry");
    let xh_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("xh"))
        .expect("xh entry");
    assert_eq!(jq_entry["used"], Value::Bool(true));
    assert_eq!(xh_entry["used"], Value::Bool(true));

    drop(tool_dir);
}

#[test]
fn ingest_api_expect_status_mismatch_returns_exit_two_with_payload() {
    let Some((tool_dir, xh_bin, jq_bin)) = create_ingest_tool_shims() else {
        return;
    };

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_XH_BIN", &xh_bin)
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "api",
            "--url",
            "https://example.test/items",
            "--expect-status",
            "201",
        ])
        .output()
        .expect("run ingest api");

    assert_eq!(output.status.code(), Some(2));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(stdout_json["status"], Value::from(200));
    assert_eq!(
        stdout_json["fetched_at"],
        Value::from("2025-02-24T10:00:00Z")
    );

    drop(tool_dir);
}

#[test]
fn ingest_api_missing_xh_maps_to_exit_three() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_XH_BIN", "/definitely-missing/xh")
        .args(["ingest", "api", "--url", "https://example.test/items"])
        .output()
        .expect("run ingest api");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert_eq!(
        stderr_json["message"],
        Value::from("ingest api requires `xh` in PATH")
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

fn create_ingest_tool_shims() -> Option<(tempfile::TempDir, String, String)> {
    if Command::new("python3").arg("--version").output().is_err() {
        return None;
    }

    let dir = tempdir().expect("tempdir");
    let xh_path = dir.path().join("fake-xh");
    let jq_path = dir.path().join("fake-jq");

    write_exec_script(
        &xh_path,
        r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'xh 0.23.0\n'
    exit 0
  fi
done

cat <<'EOF'
HTTP/1.1 200 OK
Date: Mon, 24 Feb 2025 10:00:00 GMT
Content-Type: application/json
ETag: W/"abc"
X-Trace-Id: trace-123

{"ok":true,"n":1}
EOF
"#,
    );
    write_exec_script(
        &jq_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'jq-1.7\n'
  exit 0
fi
python3 -c '
import json
import sys

payload = json.load(sys.stdin)
allow = {"cache-control", "content-type", "date", "etag", "last-modified"}
headers = {}
for key, value in (payload.get("headers") or {}).items():
    lowered = key.lower()
    if lowered in allow:
        headers[lowered] = str(value)
body = payload.get("body")
if isinstance(body, str):
    try:
        body = json.loads(body)
    except Exception:
        pass
result = {
    "source": payload.get("source"),
    "status": int(payload.get("status", 0)),
    "headers": dict(sorted(headers.items())),
    "body": body,
    "fetched_at": payload.get("fetched_at"),
}
json.dump(result, sys.stdout, separators=(",", ":"))
'
"#,
    );

    Some((
        dir,
        xh_path.display().to_string(),
        jq_path.display().to_string(),
    ))
}

fn write_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
