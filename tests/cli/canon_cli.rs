use std::io::Cursor;

use dataq::cmd::canon::{CanonCommandOptions, run};
use dataq::io::Format;
use serde_json::json;

#[test]
fn canon_command_pipeline_success_path() {
    let input = br#"{"z":"false","a":{"time":"2026-02-23T20:15:30+09:00","n":"10"}}"#;
    let mut output = Vec::new();
    run(
        Cursor::new(input),
        &mut output,
        Format::Json,
        Format::Json,
        CanonCommandOptions {
            sort_keys: true,
            normalize_time: true,
        },
    )
    .expect("canon command should succeed");

    let out: serde_json::Value = serde_json::from_slice(&output).expect("output should be json");
    assert_eq!(
        out,
        json!({
            "a": {
                "n": 10,
                "time": "2026-02-23T11:15:30Z"
            },
            "z": false
        })
    );
}

#[test]
fn canon_command_is_deterministic() {
    let input = br#"{"b":{"z":"2","a":"1"},"a":[{"b":"false","a":"true"}]}"#;
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
    .expect("first run should succeed");

    let mut second = Vec::new();
    run(
        Cursor::new(input),
        &mut second,
        Format::Json,
        Format::Json,
        options,
    )
    .expect("second run should succeed");

    assert_eq!(first, second);
}

#[test]
fn canon_command_preserves_fractional_seconds_and_numeric_precision() {
    let input =
        br#"{"ts":"2026-02-23T20:15:30.123456+09:00","safe":"3.5","precise":"0.10000000000000001","large":"18446744073709551616"}"#;
    let mut output = Vec::new();
    run(
        Cursor::new(input),
        &mut output,
        Format::Json,
        Format::Json,
        CanonCommandOptions {
            sort_keys: true,
            normalize_time: true,
        },
    )
    .expect("canon command should succeed");

    let out: serde_json::Value = serde_json::from_slice(&output).expect("output should be json");
    assert_eq!(
        out,
        json!({
            "large": "18446744073709551616",
            "precise": "0.10000000000000001",
            "safe": 3.5,
            "ts": "2026-02-23T11:15:30.123456Z"
        })
    );
}
