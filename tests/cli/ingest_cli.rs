use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use predicates::prelude::predicate;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn ingest_doc_extracts_expected_json_and_pipeline_steps() {
    if Command::new("jq").arg("--version").output().is_err() {
        return;
    }

    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let input_path = fixtures.join("input/ingest_doc.md");
    let md_ast = fixtures.join("input/ingest_doc_md_ast.json");
    let html_ast = fixtures.join("input/ingest_doc_html_ast.json");
    let expected: Value = serde_json::from_slice(
        &fs::read(fixtures.join("expected/ingest_doc.expected.json")).expect("read expected"),
    )
    .expect("expected json");

    let (tool_dir, pandoc_bin) = create_fake_pandoc_shim();
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_PANDOC_BIN", &pandoc_bin)
        .env("DATAQ_TEST_AST_MD", &md_ast)
        .env("DATAQ_TEST_AST_HTML", &html_ast)
        .args([
            "--emit-pipeline",
            "ingest",
            "doc",
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--from",
            "md",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));

    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(stdout_json, expected);

    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], Value::from("ingest.doc"));
    assert_eq!(
        stderr_json["steps"],
        Value::Array(vec![
            Value::from("ingest_doc_pandoc_ast"),
            Value::from("ingest_doc_jq_project"),
        ])
    );

    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external_tools array");
    let jq_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("jq"))
        .expect("jq entry");
    assert_eq!(jq_entry["used"], Value::Bool(true));
    let pandoc_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("pandoc"))
        .expect("pandoc entry");
    assert_eq!(pandoc_entry["used"], Value::Bool(true));
    assert_eq!(
        stderr_json["fingerprint"]["tool_versions"]["pandoc"],
        Value::from("fake-pandoc 9.9.9")
    );

    drop(tool_dir);
}

#[test]
fn ingest_doc_missing_pandoc_returns_exit_three() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let input_path = fixtures.join("input/ingest_doc.md");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_PANDOC_BIN", "/definitely-missing/pandoc")
        .args([
            "ingest",
            "doc",
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--from",
            "md",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert_eq!(
        stderr_json["message"],
        Value::from("ingest doc requires `pandoc` in PATH")
    );
}

#[test]
fn ingest_doc_unsupported_format_is_cli_input_error() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["ingest", "doc", "--input", "-", "--from", "txt"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn ingest_book_outputs_nested_tree_and_optional_file_metadata() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_passthrough_jq_script(dir.path().join("jq-pass"));
    let book_root = dir.path().join("book");
    let src_dir = book_root.join("src");
    fs::create_dir_all(src_dir.join("chapters")).expect("create chapters");

    fs::write(
        book_root.join("book.toml"),
        r#"[book]
title = "CLI Book"
authors = ["alice"]
src = "src"
"#,
    )
    .expect("write book.toml");
    fs::write(
        src_dir.join("SUMMARY.md"),
        r#"# Summary

- [Intro](intro.md)
  - [Nested](chapters/nested.md)
"#,
    )
    .expect("write summary");
    fs::write(src_dir.join("intro.md"), "# Intro\n").expect("write intro");
    fs::write(src_dir.join("chapters/nested.md"), "# Nested\n").expect("write nested");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
            "--include-files",
        ])
        .output()
        .expect("run ingest book");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["book"]["title"], json!("CLI Book"));
    assert_eq!(payload["summary"]["chapter_count"], json!(2));
    assert_eq!(
        payload["summary"]["order"][0]["path"],
        json!("src/intro.md")
    );
    assert_eq!(
        payload["summary"]["order"][1]["path"],
        json!("src/chapters/nested.md")
    );
    assert_eq!(payload["summary"]["chapters"][0]["index"], json!(1));
    assert_eq!(
        payload["summary"]["chapters"][0]["children"][0]["index"],
        json!(2)
    );
    assert!(payload["summary"]["chapters"][0]["file"]["size_bytes"].is_u64());
    assert_eq!(
        payload["summary"]["chapters"][0]["file"]["content_hash"]
            .as_str()
            .map(str::len),
        Some(16)
    );
}

#[test]
fn ingest_book_emit_pipeline_reports_expected_steps() {
    let dir = tempdir().expect("tempdir");
    let jq_bin = write_passthrough_jq_script(dir.path().join("jq-pass"));
    let book_root = dir.path().join("book");
    let src_dir = book_root.join("src");
    fs::create_dir_all(&src_dir).expect("create src");
    fs::write(book_root.join("book.toml"), "[book]\nsrc = \"src\"\n").expect("write book.toml");
    fs::write(src_dir.join("SUMMARY.md"), "- [Intro](intro.md)\n").expect("write summary");
    fs::write(src_dir.join("intro.md"), "# Intro\n").expect("write intro");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &jq_bin)
        .args([
            "--emit-pipeline",
            "ingest",
            "book",
            "--root",
            book_root.to_str().expect("utf8 root"),
        ])
        .output()
        .expect("run ingest book");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["command"], json!("ingest.book"));
    assert_eq!(
        stderr_json["steps"],
        json!([
            "ingest_book_summary_parse",
            "ingest_book_mdbook_meta",
            "ingest_book_jq_project"
        ])
    );

    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external_tools");
    let jq_entry = tools
        .iter()
        .find(|entry| entry["name"] == json!("jq"))
        .expect("jq entry");
    assert_eq!(jq_entry["used"], json!(true));
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

fn create_fake_pandoc_shim() -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("fake-pandoc");

    write_exec_script(
        &path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "fake-pandoc 9.9.9"
  exit 0
fi

from=""
while [ $# -gt 0 ]; do
  case "$1" in
    -f|--from)
      from="$2"
      shift 2
      ;;
    -t|--to)
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

case "$from" in
  markdown)
    cat "$DATAQ_TEST_AST_MD"
    ;;
  html)
    cat "$DATAQ_TEST_AST_HTML"
    ;;
  *)
    echo "unsupported format: $from" 1>&2
    exit 2
    ;;
esac
"#,
    );

    (dir, path.display().to_string())
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
