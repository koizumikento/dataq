use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn ingest_book_is_deterministic_for_unchanged_tree() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_passthrough_jq_script(dir.path().join("jq-pass"));
    let book_root = dir.path().join("book");
    let src_dir = book_root.join("src");
    fs::create_dir_all(src_dir.join("guide")).expect("create guide");

    fs::write(
        book_root.join("book.toml"),
        r#"[book]
title = "Deterministic Book"
src = "src"
"#,
    )
    .expect("write book.toml");
    fs::write(
        src_dir.join("SUMMARY.md"),
        r#"- [Intro](intro.md)
  - [Guide](guide/start.md)
"#,
    )
    .expect("write summary");
    fs::write(src_dir.join("intro.md"), "# Intro\n").expect("write intro");
    fs::write(src_dir.join("guide/start.md"), "# Start\n").expect("write guide");

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    assert_eq!(first, second);
    let payload: Value = serde_json::from_slice(&first).expect("stdout json");
    assert_eq!(payload["summary"]["chapter_count"], Value::from(2));
}

#[test]
fn ingest_book_missing_chapter_file_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_passthrough_jq_script(dir.path().join("jq-pass"));
    let book_root = dir.path().join("book");
    let src_dir = book_root.join("src");
    fs::create_dir_all(&src_dir).expect("create src");
    fs::write(book_root.join("book.toml"), "[book]\nsrc = \"src\"\n").expect("write book.toml");
    fs::write(src_dir.join("SUMMARY.md"), "- [Missing](missing.md)\n").expect("write summary");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
        ])
        .output()
        .expect("run ingest");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("message")
            .contains("missing chapter files")
    );
}

#[test]
fn ingest_book_malformed_summary_entry_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let book_root = dir.path().join("book");
    let src_dir = book_root.join("src");
    fs::create_dir_all(&src_dir).expect("create src");
    fs::write(book_root.join("book.toml"), "[book]\nsrc = \"src\"\n").expect("write book.toml");
    fs::write(src_dir.join("SUMMARY.md"), "- [Broken](broken.md\n").expect("write summary");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
        ])
        .output()
        .expect("run ingest");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("message")
            .contains("invalid SUMMARY.md line 1")
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

fn write_passthrough_jq_script(path: PathBuf) -> PathBuf {
    write_exec_script(&path, "#!/bin/sh\ncat\n");
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
