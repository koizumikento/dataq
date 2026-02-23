use std::collections::BTreeMap;

use serde_json::{Map, Value};

pub fn sort_value_keys(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (key, child) in map {
                sorted.insert(key.clone(), sort_value_keys(child));
            }
            let mut out = Map::new();
            for (key, child) in sorted {
                out.insert(key, child);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(sort_value_keys).collect()),
        _ => value.clone(),
    }
}
