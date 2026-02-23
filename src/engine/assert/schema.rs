use jsonschema::validator_for;
use serde_json::{Value, json};

use crate::domain::rules::{AssertReport, MismatchEntry};

use super::AssertValidationError;

pub fn validate(values: &[Value], schema: &Value) -> Result<AssertReport, AssertValidationError> {
    let validator = validator_for(schema)
        .map_err(|error| AssertValidationError::InputUsage(format!("invalid schema: {error}")))?;

    let mut mismatches = Vec::new();

    for (row_index, value) in values.iter().enumerate() {
        for error in validator.iter_errors(value) {
            let instance_pointer = error.instance_path().as_str().to_string();
            let schema_path = error.schema_path().as_str().to_string();
            let message = error.to_string();

            mismatches.push(MismatchEntry {
                path: row_path_from_json_pointer(row_index, &instance_pointer),
                reason: "schema_mismatch".to_string(),
                actual: value_at_pointer(value, &instance_pointer),
                expected: json!({
                    "schema_path": schema_path,
                    "message": message
                }),
            });
        }
    }

    sort_mismatches(&mut mismatches);

    Ok(AssertReport {
        matched: mismatches.is_empty(),
        mismatch_count: mismatches.len(),
        mismatches,
    })
}

fn value_at_pointer(root: &Value, pointer: &str) -> Value {
    if pointer.is_empty() {
        return root.clone();
    }
    root.pointer(pointer).cloned().unwrap_or(Value::Null)
}

fn row_path_from_json_pointer(row_index: usize, pointer: &str) -> String {
    let mut path = format!("$[{row_index}]");
    if pointer.is_empty() {
        return path;
    }

    for token in pointer.trim_start_matches('/').split('/') {
        let segment = decode_pointer_token(token);
        if is_simple_identifier(&segment) {
            path.push('.');
            path.push_str(&segment);
        } else if is_array_index(&segment) {
            path.push('[');
            path.push_str(&segment);
            path.push(']');
        } else {
            path.push('[');
            path.push_str(
                &serde_json::to_string(&segment)
                    .unwrap_or_else(|_| "\"<invalid-segment>\"".to_string()),
            );
            path.push(']');
        }
    }

    path
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn is_simple_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_array_index(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn sort_mismatches(mismatches: &mut [MismatchEntry]) {
    mismatches.sort_by(|left, right| {
        let left_key = (
            left.path.clone(),
            left.reason.clone(),
            stable_value_key(&left.actual),
            stable_value_key(&left.expected),
        );
        let right_key = (
            right.path.clone(),
            right.reason.clone(),
            stable_value_key(&right.actual),
            stable_value_key(&right.expected),
        );
        left_key.cmp(&right_key)
    });
}

fn stable_value_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<serialization-error>".to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::validate;

    #[test]
    fn reports_schema_mismatches_with_normalized_shape() {
        let values = vec![json!({"id":"x","score":200})];
        let schema = json!({
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": {"type": "integer"},
                "score": {"type": "number", "maximum": 100}
            }
        });

        let report = validate(&values, &schema).expect("schema validation result");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 2);
        assert_eq!(report.mismatches[0].reason, "schema_mismatch");
        assert!(report.mismatches[0].expected.get("schema_path").is_some());
        assert!(report.mismatches[0].expected.get("message").is_some());
    }

    #[test]
    fn rejects_invalid_schema() {
        let values = vec![json!({"id": 1})];
        let schema = json!({"type": 123});

        let error = validate(&values, &schema).expect_err("schema should be invalid");
        assert!(error.to_string().contains("invalid schema"));
    }
}
