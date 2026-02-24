use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn ingest_api_is_deterministic_for_identical_response() {
    let Some((tool_dir, xh_bin, jq_bin)) = create_ingest_tool_shims() else {
        return;
    };

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_XH_BIN", &xh_bin)
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args(["ingest", "api", "--url", "https://example.test/items"])
        .output()
        .expect("first run");
    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_XH_BIN", &xh_bin)
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args(["ingest", "api", "--url", "https://example.test/items"])
        .output()
        .expect("second run");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);

    let payload: Value = serde_json::from_slice(&first.stdout).expect("stdout json");
    assert_eq!(
        payload["headers"]["content-type"],
        Value::from("application/json")
    );
    assert_eq!(payload["fetched_at"], Value::from("2025-02-24T10:00:00Z"));

    drop(tool_dir);
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
ETag: W/"abc"
Content-Type: application/json
Date: Mon, 24 Feb 2025 10:00:00 GMT

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
