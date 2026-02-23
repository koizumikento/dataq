use std::path::Path;

use dataq::io::{Format, IoError, resolve_input_format, resolve_output_format};

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
