use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir, tempdir_in};

#[test]
fn scan_text_multiline_matches_are_sorted_and_relative() {
    let toolchain = FakeRgToolchain::new();
    let scan_root = tempdir_in(std::env::current_dir().expect("cwd")).expect("scan root");
    fs::create_dir_all(scan_root.path().join("sub")).expect("mkdir");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_RG_BIN", &toolchain.rg_bin)
        .args([
            "scan",
            "text",
            "--emit-pipeline",
            "--pattern",
            "multi",
            "--path",
            scan_root.path().to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run scan text");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let matches = payload["matches"].as_array().expect("matches array");
    assert_eq!(matches.len(), 2);

    let relative_root = scan_root
        .path()
        .strip_prefix(std::env::current_dir().expect("cwd"))
        .expect("relative scan root");
    let first_expected = format!("{}/a.txt", relative_root.display());
    let second_expected = format!("{}/sub/b.txt", relative_root.display());

    assert_eq!(matches[0]["path"], json!(first_expected));
    assert_eq!(matches[0]["line"], json!(2));
    assert_eq!(matches[0]["column"], json!(1));
    assert_eq!(matches[1]["path"], json!(second_expected));
    assert_eq!(matches[1]["line"], json!(3));
    assert_eq!(matches[1]["column"], json!(3));

    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], json!("scan"));
    assert_eq!(
        stderr_json["steps"],
        json!([
            "scan_text_rg_execute",
            "scan_text_parse",
            "scan_text_jq_project"
        ])
    );
}

#[test]
fn scan_text_no_match_returns_empty_payload() {
    let toolchain = FakeRgToolchain::new();
    let scan_root = tempdir().expect("scan root");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_RG_BIN", &toolchain.rg_bin)
        .args([
            "scan",
            "text",
            "--pattern",
            "none",
            "--path",
            scan_root.path().to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run scan text");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["matches"], json!([]));
    assert_eq!(payload["summary"]["total_matches"], json!(0));
    assert_eq!(payload["summary"]["returned_matches"], json!(0));
}

#[test]
fn scan_text_invalid_regex_maps_to_exit_three() {
    let toolchain = FakeRgToolchain::new();
    let scan_root = tempdir().expect("scan root");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_RG_BIN", &toolchain.rg_bin)
        .args([
            "scan",
            "text",
            "--pattern",
            "invalid-regex",
            "--path",
            scan_root.path().to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run scan text");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], json!("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("message")
            .contains("regex parse error")
    );
}

#[test]
fn scan_text_policy_mode_returns_exit_two_when_matches_found() {
    let toolchain = FakeRgToolchain::new();
    let scan_root = tempdir().expect("scan root");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_RG_BIN", &toolchain.rg_bin)
        .args([
            "scan",
            "text",
            "--pattern",
            "forbidden",
            "--policy-mode",
            "--path",
            scan_root.path().to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run scan text");

    assert_eq!(output.status.code(), Some(2));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["summary"]["policy_mode"], json!(true));
    assert_eq!(payload["summary"]["forbidden_matches"], json!(1));
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

struct FakeRgToolchain {
    _dir: TempDir,
    rg_bin: PathBuf,
}

impl FakeRgToolchain {
    fn new() -> Self {
        let dir = tempdir().expect("tempdir");
        let rg_bin = write_fake_rg_script(dir.path().join("rg"));
        Self { _dir: dir, rg_bin }
    }
}

fn write_fake_rg_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'ripgrep 14.1.1\n'
    exit 0
  fi
done

prev=""
last=""
for arg in "$@"; do
  prev="$last"
  last="$arg"
done
pattern="$prev"
root="$last"

if [ "$pattern" = "invalid-regex" ]; then
  echo 'regex parse error: unclosed group' 1>&2
  exit 2
fi

if [ "$pattern" = "none" ]; then
  exit 1
fi

if [ "$pattern" = "multi" ]; then
  printf '{"type":"begin","data":{"path":{"text":"%s/sub/b.txt"}}}\n' "$root"
  printf '{"type":"match","data":{"path":{"text":"%s/sub/b.txt"},"lines":{"text":"xxfoo\\nbar\\n"},"line_number":3,"submatches":[{"match":{"text":"foo\\nbar"},"start":2,"end":9}]}}\n' "$root"
  printf '{"type":"begin","data":{"path":{"text":"%s/a.txt"}}}\n' "$root"
  printf '{"type":"match","data":{"path":{"text":"%s/a.txt"},"lines":{"text":"foo\\nbar\\n"},"line_number":2,"submatches":[{"match":{"text":"foo\\nbar"},"start":0,"end":7}]}}\n' "$root"
  printf '{"type":"summary","data":{"stats":{"matches":2}}}\n'
  exit 0
fi

if [ "$pattern" = "forbidden" ]; then
  printf '{"type":"match","data":{"path":{"text":"%s/policy.txt"},"lines":{"text":"forbidden token\\n"},"line_number":1,"submatches":[{"match":{"text":"forbidden"},"start":0,"end":9}]}}\n' "$root"
  exit 0
fi

exit 1
"#;

    fs::write(&path, script).expect("write fake rg script");
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
