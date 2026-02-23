use std::io::Cursor;

use dataq::cmd::r#assert::{AssertCommandArgs, run_with_stdin};
use dataq::io::Format;
use serde_json::Value;
use tempfile::tempdir;

#[test]
fn assert_api_success_with_stdin_input() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.yaml");
    std::fs::write(
        &rules_path,
        r#"
required_keys: [id, score]
types:
  id: integer
  score: number
count:
  min: 1
  max: 2
ranges:
  score:
    min: 0
    max: 100
"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: rules_path,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1,"score":10.5}]"#));
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
    assert_eq!(response.payload["mismatch_count"], Value::from(0));
}

#[test]
fn assert_api_reports_mismatch_shape() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id", "score"],
            "types": {"id": "integer", "score": "number"},
            "count": {"min": 1, "max": 1},
            "ranges": {"score": {"min": 0.0, "max": 1.0}}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: rules_path,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"x","score":4}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert_eq!(response.payload["mismatch_count"], Value::from(2));

    let mismatches = response.payload["mismatches"]
        .as_array()
        .expect("mismatches array");
    assert!(!mismatches.is_empty());
    for mismatch in mismatches {
        let obj = mismatch.as_object().expect("mismatch object");
        assert!(obj.contains_key("path"));
        assert!(obj.contains_key("reason"));
        assert!(obj.contains_key("actual"));
        assert!(obj.contains_key("expected"));
    }
}

#[test]
fn assert_api_reports_input_usage_errors() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "types": {},
            "count": {},
            "ranges": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: None,
        rules: rules_path,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}
