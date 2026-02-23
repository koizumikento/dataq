use std::io::{BufRead, BufReader, Cursor, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dataq::cmd::canon::{CanonCommandOptions, run};
use dataq::io::Format;
use predicates::prelude::predicate;

#[test]
fn canon_flow_jsonl_to_jsonl_success() {
    let input = br#"{"z":"3","a":"true"}
{"z":"2","a":"false"}
"#;
    let mut output = Vec::new();
    run(
        Cursor::new(input),
        &mut output,
        Format::Jsonl,
        Format::Jsonl,
        CanonCommandOptions::default(),
    )
    .expect("canon flow should succeed");

    let out = String::from_utf8(output).expect("valid utf8");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], r#"{"a":true,"z":3}"#);
    assert_eq!(lines[1], r#"{"a":false,"z":2}"#);
}

#[test]
fn canon_flow_is_idempotent() {
    let input = br#"{"z":"3","a":"true"}"#;
    let options = CanonCommandOptions {
        sort_keys: true,
        normalize_time: true,
    };

    let mut first = Vec::new();
    run(
        Cursor::new(input),
        &mut first,
        Format::Json,
        Format::Json,
        options,
    )
    .expect("first flow should succeed");

    let mut second = Vec::new();
    run(
        Cursor::new(first.clone()),
        &mut second,
        Format::Json,
        Format::Json,
        options,
    )
    .expect("second flow should succeed");

    assert_eq!(first, second);
}

#[test]
fn canon_flow_preserves_fractional_seconds_and_precision_sensitive_numbers() {
    let input = br#"{"ts":"2026-02-23T20:15:30.123456+09:00","safe":"3.5","precise":"0.10000000000000001","large":"18446744073709551616"}
"#;
    let mut output = Vec::new();
    run(
        Cursor::new(input),
        &mut output,
        Format::Jsonl,
        Format::Jsonl,
        CanonCommandOptions {
            sort_keys: true,
            normalize_time: true,
        },
    )
    .expect("canon flow should succeed");

    let out = String::from_utf8(output).expect("valid utf8");
    let mut lines = out.lines();
    let first = lines.next().expect("one output line expected");
    assert!(lines.next().is_none(), "expected exactly one output line");

    let parsed: serde_json::Value = serde_json::from_str(first).expect("line should be valid json");
    assert_eq!(
        parsed,
        serde_json::json!({
            "large": "18446744073709551616",
            "precise": "0.10000000000000001",
            "safe": 3.5,
            "ts": "2026-02-23T11:15:30.123456Z"
        })
    );
}

#[test]
fn canon_cli_autodetects_stdin_jsonl_when_from_omitted() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--to", "jsonl"])
        .write_stdin(
            r#"{"z":"3","a":"true"}
{"z":"2","a":"false"}
"#,
        )
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines, vec![r#"{"a":true,"z":3}"#, r#"{"a":false,"z":2}"#]);
}

#[test]
fn canon_cli_autodetect_jsonl_streams_before_stdin_close() {
    let bin = assert_cmd::cargo::cargo_bin!("dataq");
    let mut child = Command::new(bin)
        .args(["canon", "--to", "jsonl"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn dataq");
    let mut child_stdin = child.stdin.take().expect("child stdin");
    let child_stdout = child.stdout.take().expect("child stdout");

    let (continue_tx, continue_rx) = mpsc::channel::<()>();
    let writer = thread::spawn(move || {
        child_stdin
            .write_all(br#"{"z":"3","a":"true"}"#)
            .expect("write first record");
        child_stdin.write_all(b"\n").expect("write newline");
        child_stdin
            .write_all(br#"{"z":"2","a":"false"}"#)
            .expect("write second record");
        child_stdin.write_all(b"\n").expect("write newline");
        child_stdin.flush().expect("flush streamed records");
        continue_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("wait for continue signal");
    });

    let (line_tx, line_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut reader = BufReader::new(child_stdout);
        let mut first_line = String::new();
        let first_read = reader.read_line(&mut first_line).expect("read first line");
        line_tx
            .send((first_read, first_line))
            .expect("send first line");
        let mut second_line = String::new();
        let second_read = reader
            .read_line(&mut second_line)
            .expect("read second line");
        (second_read, second_line)
    });

    let (first_read, first_line) = line_rx
        .recv_timeout(Duration::from_millis(600))
        .expect("first output line should be streamed before stdin closes");
    assert!(first_read > 0);
    assert_eq!(first_line.trim_end(), r#"{"a":true,"z":3}"#);

    continue_tx.send(()).expect("resume stdin writer");
    writer.join().expect("writer thread join");

    let (second_read, second_line) = reader.join().expect("reader thread join");
    assert!(second_read > 0);
    assert_eq!(second_line.trim_end(), r#"{"a":false,"z":2}"#);

    let status = child.wait().expect("wait child");
    assert!(
        status.success(),
        "canon command should succeed, got status {status}"
    );
}

#[test]
fn canon_cli_stdin_autodetect_failure_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["canon", "--to", "jsonl"])
        .write_stdin(vec![0xff, 0xfe, 0xfd])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains(
            "could not autodetect stdin input format",
        ));
}
