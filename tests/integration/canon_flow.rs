use std::io::Cursor;

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
