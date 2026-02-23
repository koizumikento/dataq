use std::io::Cursor;

use dataq::cmd::canon::{CanonCommandOptions, run};
use dataq::io::Format;

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
