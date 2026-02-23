use serde_json::{Map, Value};

/// Recursively normalize JSON values for deterministic output.
pub fn normalize_value(value: Value, sort_keys: bool) -> Value {
    match value {
        Value::Object(map) => normalize_object(map, sort_keys),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| normalize_value(item, sort_keys))
                .collect(),
        ),
        primitive => primitive,
    }
}

fn normalize_object(map: Map<String, Value>, sort_keys: bool) -> Value {
    let mut entries: Vec<(String, Value)> = map
        .into_iter()
        .map(|(key, value)| (key, normalize_value(value, sort_keys)))
        .collect();

    if sort_keys {
        entries.sort_by(|left, right| left.0.cmp(&right.0));
    }

    let mut normalized = Map::new();
    for (key, value) in entries {
        normalized.insert(key, value);
    }
    Value::Object(normalized)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalize_value;

    #[test]
    fn sorts_keys_recursively_when_enabled() {
        let input = json!({
            "z": {"d": 4, "a": 1, "b": {"d": 0, "c": 1}},
            "a": [{"c": 3, "a": 1}]
        });
        let actual = normalize_value(input, true);
        let as_json = serde_json::to_string(&actual).expect("serialize normalized json");
        assert_eq!(
            as_json,
            r#"{"a":[{"a":1,"c":3}],"z":{"a":1,"b":{"c":1,"d":0},"d":4}}"#
        );
    }

    #[test]
    fn normalization_is_idempotent() {
        let input = json!({"b":{"z":2,"a":1},"a":[{"d":2,"c":1}]});
        let once = normalize_value(input, true);
        let twice = normalize_value(once.clone(), true);
        assert_eq!(once, twice);
    }
}
