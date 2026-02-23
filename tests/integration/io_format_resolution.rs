use std::path::Path;

use dataq::io::{
    Format, IoError, autodetect_stdin_input_format, resolve_input_format, resolve_output_format,
};

#[test]
fn explicit_format_takes_priority() {
    let input =
        resolve_input_format(Some(Format::Yaml), Some(Path::new("in.json"))).expect("input");
    let output =
        resolve_output_format(Some(Format::Jsonl), Some(Path::new("out.csv"))).expect("output");
    assert_eq!(input, Format::Yaml);
    assert_eq!(output, Format::Jsonl);
}

#[test]
fn extension_fallback_works() {
    let input = resolve_input_format(None, Some(Path::new("in.yml"))).expect("input");
    let output = resolve_output_format(None, Some(Path::new("out.ndjson"))).expect("output");
    assert_eq!(input, Format::Yaml);
    assert_eq!(output, Format::Jsonl);
}

#[test]
fn unknown_extension_is_error() {
    let err = resolve_input_format(None, Some(Path::new("in.unknown"))).expect_err("must fail");
    match err {
        IoError::UnsupportedPathExtension { kind, .. } => assert_eq!(kind, "input"),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn missing_path_and_explicit_format_is_error() {
    let err = resolve_output_format(None, None).expect_err("must fail");
    match err {
        IoError::UnresolvedFormat { kind } => assert_eq!(kind, "output"),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn autodetect_stdin_prefers_jsonl_before_json() {
    let input = br#"{"id":"1","enabled":"true"}
{"id":"2","enabled":"false"}"#;
    let format = autodetect_stdin_input_format(input).expect("autodetect should succeed");
    assert_eq!(format, Format::Jsonl);
}

#[test]
fn autodetect_stdin_uses_json_for_single_compact_json_value() {
    let input = br#"{"id":"1","enabled":"true"}"#;
    let format = autodetect_stdin_input_format(input).expect("autodetect should succeed");
    assert_eq!(format, Format::Json);
}

#[test]
fn autodetect_stdin_falls_back_to_json_then_yaml_then_csv() {
    let json_input = br#"{
  "items": [{"id":"1"}]
}"#;
    let yaml_input = br#"items:
  - id: "1"
"#;
    let csv_input = br#"id,enabled
1,true
"#;

    assert_eq!(
        autodetect_stdin_input_format(json_input).expect("json autodetect"),
        Format::Json
    );
    assert_eq!(
        autodetect_stdin_input_format(yaml_input).expect("yaml autodetect"),
        Format::Yaml
    );
    assert_eq!(
        autodetect_stdin_input_format(csv_input).expect("csv autodetect"),
        Format::Csv
    );
}

#[test]
fn autodetect_stdin_error_when_no_format_matches() {
    let err = autodetect_stdin_input_format(&[0xff, 0xfe, 0xfd]).expect_err("autodetect must fail");
    match err {
        IoError::StdinAutodetectFailed => {}
        other => panic!("unexpected error: {other}"),
    }
}
