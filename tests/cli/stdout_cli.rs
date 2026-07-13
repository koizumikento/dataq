#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::process::{Command, Output, Stdio};

use serde_json::Value;
use tempfile::tempdir;

fn run_with_closed_stdout(args: &[&str], stdin: &[u8]) -> Output {
    let (read_end, write_end) = UnixStream::pair().expect("stdout socket pair");
    drop(read_end);
    let write_end = OwnedFd::from(write_end);

    let mut child = Command::new(assert_cmd::cargo::cargo_bin!("dataq"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn dataq");

    let mut child_stdin = child.stdin.take().expect("child stdin");
    child_stdin.write_all(stdin).expect("write child stdin");
    drop(child_stdin);

    child.wait_with_output().expect("wait for dataq")
}

fn assert_consumer_closed_success(output: &Output) {
    assert_eq!(output.status.code(), Some(0));
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn common_json_help_and_version_outputs_accept_a_closed_consumer() {
    for args in [
        ["contract", "--command", "canon"].as_slice(),
        ["--help"].as_slice(),
        ["--version"].as_slice(),
    ] {
        assert_consumer_closed_success(&run_with_closed_stdout(args, b""));
    }
}

#[test]
fn emit_pipeline_stays_on_stderr_when_stdout_consumer_is_closed() {
    let args = ["--emit-pipeline", "contract", "--command", "canon"];
    let first = run_with_closed_stdout(&args, b"");
    let second = run_with_closed_stdout(&args, b"");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stderr, second.stderr);

    let stderr = String::from_utf8(first.stderr).expect("stderr utf8");
    assert!(!stderr.contains("internal_error"));
    assert!(!stderr.contains("panicked at"));
    assert!(!stderr.contains("Broken pipe"));

    let lines = stderr
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let pipeline: Value = serde_json::from_str(lines[0]).expect("pipeline stderr json");
    assert_eq!(pipeline["command"], Value::from("contract"));
    assert!(pipeline["steps"].is_array());
    assert!(pipeline["fingerprint"].is_object());
}

#[test]
fn canon_jsonl_and_mcp_outputs_accept_a_closed_consumer() {
    let canon = run_with_closed_stdout(
        &["canon", "--from", "jsonl", "--to", "jsonl"],
        b"{\"id\":1}\n{\"id\":2}\n",
    );
    assert_consumer_closed_success(&canon);

    let mcp = run_with_closed_stdout(
        &["mcp"],
        br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
    );
    assert_consumer_closed_success(&mcp);
}

#[test]
fn recipe_lock_byte_output_accepts_a_closed_consumer() {
    let dir = tempdir().expect("temp dir");
    let recipe = dir.path().join("recipe.json");
    fs::write(&recipe, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let output = run_with_closed_stdout(
        &[
            "recipe",
            "lock",
            "--file",
            recipe.to_str().expect("recipe path utf8"),
        ],
        b"",
    );
    assert_consumer_closed_success(&output);
}

#[test]
fn direct_diff_outputs_override_validation_exit_two_for_a_closed_consumer() {
    let dir = tempdir().expect("temp dir");
    let left = dir.path().join("left.json");
    let right = dir.path().join("right.json");
    fs::write(&left, br#"[{"id":1}]"#).expect("write left");
    fs::write(&right, br#"[{"id":2}]"#).expect("write right");
    let left = left.to_str().expect("left path utf8");
    let right = right.to_str().expect("right path utf8");

    let sdiff = run_with_closed_stdout(
        &["sdiff", "--left", left, "--right", right, "--fail-on-diff"],
        b"",
    );
    assert_consumer_closed_success(&sdiff);

    let diff_source = run_with_closed_stdout(
        &[
            "diff",
            "source",
            "--left",
            left,
            "--right",
            right,
            "--fail-on-diff",
        ],
        b"",
    );
    assert_consumer_closed_success(&diff_source);
}

#[test]
fn parse_errors_keep_the_input_usage_contract_when_stdout_is_closed() {
    let output = run_with_closed_stdout(&["--definitely-unknown"], b"");

    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8(output.stderr).expect("stderr utf8");
    assert!(stderr.contains(r#""error":"input_usage_error""#));
    assert!(!stderr.contains("panicked at"));
    assert!(!stderr.contains("Broken pipe"));
}
