use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn ingest_doc_markdown_and_html_roundtrip_to_same_projection() {
    if Command::new("jq").arg("--version").output().is_err() {
        return;
    }

    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let md_input = fixtures.join("input/ingest_doc.md");
    let html_input = fixtures.join("input/ingest_doc.html");
    let md_ast = fixtures.join("input/ingest_doc_md_ast.json");
    let html_ast = fixtures.join("input/ingest_doc_html_ast.json");
    let expected: Value = serde_json::from_slice(
        &fs::read(fixtures.join("expected/ingest_doc.expected.json")).expect("read expected"),
    )
    .expect("expected json");

    let (tool_dir, pandoc_bin) = create_fake_pandoc_shim();

    let md_output = run_ingest_doc(
        md_input.as_path(),
        "md",
        pandoc_bin.as_str(),
        md_ast.as_path(),
        html_ast.as_path(),
    );
    let html_output = run_ingest_doc(
        html_input.as_path(),
        "html",
        pandoc_bin.as_str(),
        md_ast.as_path(),
        html_ast.as_path(),
    );

    assert_eq!(md_output, expected);
    assert_eq!(html_output, expected);
    assert_eq!(md_output, html_output);

    drop(tool_dir);
}

fn run_ingest_doc(
    input_path: &Path,
    from: &str,
    pandoc_bin: &str,
    md_ast: &Path,
    html_ast: &Path,
) -> Value {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_PANDOC_BIN", pandoc_bin)
        .env("DATAQ_TEST_AST_MD", md_ast)
        .env("DATAQ_TEST_AST_HTML", html_ast)
        .args([
            "ingest",
            "doc",
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--from",
            from,
        ])
        .output()
        .expect("run ingest doc");

    assert_eq!(output.status.code(), Some(0));
    serde_json::from_slice(&output.stdout).expect("stdout json")
}

fn create_fake_pandoc_shim() -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("fake-pandoc");

    write_exec_script(
        &path,
        r#"#!/bin/sh
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

fn write_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
