use jsonschema::validator_for;
use serde_json::{Value, json};

use crate::adapters::check_jsonschema::{self, CheckJsonschemaMismatch, CheckJsonschemaValidation};
use crate::domain::rules::{AssertReport, MismatchEntry};

use super::AssertValidationError;

pub fn validate(values: &[Value], schema: &Value) -> Result<AssertReport, AssertValidationError> {
    validator_for(schema)
        .map_err(|error| AssertValidationError::InputUsage(format!("invalid schema: {error}")))?;

    match check_jsonschema::validate_rows(values, schema) {
        Ok(CheckJsonschemaValidation::Matched) => Ok(AssertReport {
            matched: true,
            mismatch_count: 0,
            mismatches: Vec::new(),
        }),
        Ok(CheckJsonschemaValidation::Mismatched(errors)) => {
            let mut mismatches = errors
                .into_iter()
                .map(|error| mismatch_from_check_jsonschema(values, error))
                .collect::<Vec<_>>();
            sort_mismatches(&mut mismatches);
            Ok(AssertReport {
                matched: mismatches.is_empty(),
                mismatch_count: mismatches.len(),
                mismatches,
            })
        }
        Err(error) => Err(AssertValidationError::InputUsage(error.to_string())),
    }
}

fn mismatch_from_check_jsonschema(
    values: &[Value],
    error: CheckJsonschemaMismatch,
) -> MismatchEntry {
    let row_value = values.get(error.row_index).cloned().unwrap_or(Value::Null);
    let tokens = parse_json_path(error.path.as_str());

    MismatchEntry {
        path: row_path_from_json_path(error.row_index, tokens.as_deref()),
        rule_kind: "schema".to_string(),
        reason: "schema_mismatch".to_string(),
        actual: value_at_json_path(&row_value, error.path.as_str(), tokens.as_deref()),
        expected: json!({
            "schema_path": Value::Null,
            "message": error.message,
            "check_jsonschema_path": error.path
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathToken {
    Key(String),
    Index(usize),
}

fn value_at_json_path(root: &Value, path: &str, tokens: Option<&[JsonPathToken]>) -> Value {
    let Some(tokens) = tokens else {
        if path == "$" {
            return root.clone();
        }
        return Value::Null;
    };

    let mut current = root;
    for token in tokens {
        match token {
            JsonPathToken::Key(key) => match current {
                Value::Object(map) => {
                    let Some(next) = map.get(key) else {
                        return Value::Null;
                    };
                    current = next;
                }
                _ => return Value::Null,
            },
            JsonPathToken::Index(index) => match current {
                Value::Array(items) => {
                    let Some(next) = items.get(*index) else {
                        return Value::Null;
                    };
                    current = next;
                }
                _ => return Value::Null,
            },
        }
    }
    current.clone()
}

fn row_path_from_json_path(row_index: usize, tokens: Option<&[JsonPathToken]>) -> String {
    let mut path = format!("$[{row_index}]");
    let Some(tokens) = tokens else {
        return path;
    };
    for token in tokens {
        match token {
            JsonPathToken::Key(key) => push_object_key_segment(&mut path, key),
            JsonPathToken::Index(index) => {
                path.push('[');
                path.push_str(&index.to_string());
                path.push(']');
            }
        }
    }
    path
}

fn parse_json_path(path: &str) -> Option<Vec<JsonPathToken>> {
    if !path.starts_with('$') {
        return None;
    }

    let bytes = path.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 1usize;
    while index < bytes.len() {
        match bytes[index] {
            b'.' => {
                index += 1;
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                if start == index {
                    return None;
                }
                tokens.push(JsonPathToken::Key(path[start..index].to_string()));
            }
            b'[' => {
                index += 1;
                if index >= bytes.len() {
                    return None;
                }
                if bytes[index] == b'\'' || bytes[index] == b'"' {
                    let quote = bytes[index];
                    index += 1;
                    let mut segment = String::new();
                    while index < bytes.len() {
                        let current = bytes[index];
                        if current == b'\\' {
                            index += 1;
                            if index >= bytes.len() {
                                return None;
                            }
                            segment.push(bytes[index] as char);
                            index += 1;
                            continue;
                        }
                        if current == quote {
                            break;
                        }
                        segment.push(current as char);
                        index += 1;
                    }
                    if index >= bytes.len() || bytes[index] != quote {
                        return None;
                    }
                    index += 1;
                    if index >= bytes.len() || bytes[index] != b']' {
                        return None;
                    }
                    index += 1;
                    tokens.push(JsonPathToken::Key(segment));
                } else {
                    let start = index;
                    while index < bytes.len() && bytes[index].is_ascii_digit() {
                        index += 1;
                    }
                    if start == index || index >= bytes.len() || bytes[index] != b']' {
                        return None;
                    }
                    let parsed = path[start..index].parse::<usize>().ok()?;
                    index += 1;
                    tokens.push(JsonPathToken::Index(parsed));
                }
            }
            _ => return None,
        }
    }

    Some(tokens)
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

fn push_object_key_segment(path: &mut String, segment: &str) {
    if is_simple_identifier(segment) {
        path.push('.');
        path.push_str(segment);
    } else {
        path.push('[');
        path.push_str(
            &serde_json::to_string(segment).unwrap_or_else(|_| "\"<invalid-segment>\"".to_string()),
        );
        path.push(']');
    }
}

fn sort_mismatches(mismatches: &mut [MismatchEntry]) {
    mismatches.sort_by(|left, right| {
        let left_key = (
            left.path.clone(),
            left.rule_kind.clone(),
            left.reason.clone(),
            stable_value_key(&left.actual),
            stable_value_key(&left.expected),
        );
        let right_key = (
            right.path.clone(),
            right.rule_kind.clone(),
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

    use super::{parse_json_path, row_path_from_json_path, value_at_json_path};

    #[test]
    fn converts_numeric_object_keys_to_unambiguous_row_path() {
        let row_value = json!({"0":"x"});
        let tokens = parse_json_path("$['0']").expect("tokens");
        let path = row_path_from_json_path(0, Some(tokens.as_slice()));
        let actual = value_at_json_path(&row_value, "$['0']", Some(tokens.as_slice()));

        assert_eq!(path, "$[0][\"0\"]");
        assert_eq!(actual, json!("x"));
    }

    #[test]
    fn returns_null_for_unparseable_json_path_lookup() {
        let row_value = json!({"id":1});
        let actual = value_at_json_path(&row_value, "$.['broken'", None);
        assert_eq!(actual, json!(null));
    }
}
