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
        rules: Some(rules_path),
        schema: None,
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
        rules: Some(rules_path),
        schema: None,
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
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_rejects_unknown_rule_keys() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "types": {},
            "count": {"min": 0, "max": 1, "oops": 2},
            "ranges": {},
            "unexpected": true
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    let message = response.payload["message"]
        .as_str()
        .expect("input usage message");
    assert!(message.contains("unknown field"));
}

#[test]
fn assert_api_compares_large_integer_ranges_exactly() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "types": {},
            "count": {},
            "ranges": {"value": {"max": 9007199254740992}}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"value":9007199254740993}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["mismatch_count"], Value::from(1));
    assert_eq!(
        response.payload["mismatches"][0]["path"],
        Value::from("$[0].value")
    );
    assert_eq!(
        response.payload["mismatches"][0]["reason"],
        Value::from("above_max")
    );
}

#[test]
fn assert_api_supports_jsonschema_mode() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": {"type": "integer"},
                "score": {"type": "number", "maximum": 10}
            }
        }"#,
    )
    .expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"x","score":12}]"#));
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
        assert_eq!(
            obj.get("reason"),
            Some(&Value::String("schema_mismatch".to_string()))
        );
    }
}

#[test]
fn assert_api_rejects_rules_and_schema_together() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(
        &rules_path,
        r#"{"required_keys":[],"types":{},"count":{},"ranges":{}}"#,
    )
    .expect("write rules");
    std::fs::write(&schema_path, r#"{"type":"object"}"#).expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_maps_schema_parse_errors_to_exit_three() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(&schema_path, r#"{"type":"object","properties":{"id":}"#)
        .expect("write invalid schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1}]"#));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_schema_mode_keeps_numeric_object_key_paths_unambiguous() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["0"],
            "properties": {
                "0": {"type": "integer"}
            }
        }"#,
    )
    .expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"0":"x"}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(
        response.payload["mismatches"][0]["path"],
        Value::from("$[0][\"0\"]")
    );
}
