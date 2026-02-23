use serde_json::{Map, Value};

use crate::util::sort::sort_value_keys;

/// Deterministic merge policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePolicy {
    /// Shallow object merge where conflicting keys are replaced by overlay values.
    LastWins,
    /// Recursive object merge; arrays merge by index.
    DeepMerge,
    /// Recursive object merge; arrays are fully replaced by overlay values.
    ArrayReplace,
}

/// Merges `base` with overlays from left to right under `policy`.
///
/// Overlay order is significant and deterministic.
pub fn merge_with_policy(base: &Value, overlays: &[Value], policy: MergePolicy) -> Value {
    let merged = overlays.iter().fold(base.clone(), |acc, overlay| {
        merge_value_pair(&acc, overlay, policy)
    });
    sort_value_keys(&merged)
}

fn merge_value_pair(base: &Value, overlay: &Value, policy: MergePolicy) -> Value {
    match policy {
        MergePolicy::LastWins => merge_last_wins(base, overlay),
        MergePolicy::DeepMerge => merge_deep(base, overlay, false),
        MergePolicy::ArrayReplace => merge_deep(base, overlay, true),
    }
}

fn merge_last_wins(base: &Value, overlay: &Value) -> Value {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            let mut merged: Map<String, Value> = base_map.clone();
            for (key, value) in overlay_map {
                merged.insert(key.clone(), value.clone());
            }
            Value::Object(merged)
        }
        _ => overlay.clone(),
    }
}

fn merge_deep(base: &Value, overlay: &Value, replace_arrays: bool) -> Value {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            let mut merged: Map<String, Value> = base_map.clone();
            for (key, overlay_value) in overlay_map {
                if let Some(base_value) = merged.remove(key) {
                    let next = merge_deep(&base_value, overlay_value, replace_arrays);
                    merged.insert(key.clone(), next);
                } else {
                    merged.insert(key.clone(), overlay_value.clone());
                }
            }
            Value::Object(merged)
        }
        (Value::Array(base_items), Value::Array(overlay_items)) => {
            if replace_arrays {
                Value::Array(overlay_items.clone())
            } else {
                let max_len = base_items.len().max(overlay_items.len());
                let mut merged = Vec::with_capacity(max_len);
                for index in 0..max_len {
                    match (base_items.get(index), overlay_items.get(index)) {
                        (Some(base_item), Some(overlay_item)) => {
                            merged.push(merge_deep(base_item, overlay_item, replace_arrays));
                        }
                        (Some(base_item), None) => merged.push(base_item.clone()),
                        (None, Some(overlay_item)) => merged.push(overlay_item.clone()),
                        (None, None) => {}
                    }
                }
                Value::Array(merged)
            }
        }
        _ => overlay.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MergePolicy, merge_with_policy};

    #[test]
    fn last_wins_replaces_conflicting_keys_without_recursive_object_merge() {
        let base = json!({
            "keep": true,
            "user": {
                "name": "alice",
                "role": "admin"
            }
        });
        let overlays = vec![json!({
            "added": 1,
            "user": {
                "name": "bob"
            }
        })];

        let actual = merge_with_policy(&base, &overlays, MergePolicy::LastWins);

        assert_eq!(
            actual,
            json!({
                "added": 1,
                "keep": true,
                "user": {
                    "name": "bob"
                }
            })
        );
    }

    #[test]
    fn deep_merge_recursively_merges_objects_and_arrays_by_index() {
        let base = json!({
            "cfg": {
                "keep": "x",
                "retry": 1,
                "tags": ["a", {"k": 1}]
            }
        });
        let overlays = vec![json!({
            "cfg": {
                "new": true,
                "retry": 2,
                "tags": ["b", {"m": 2}, 3]
            }
        })];

        let actual = merge_with_policy(&base, &overlays, MergePolicy::DeepMerge);

        assert_eq!(
            actual,
            json!({
                "cfg": {
                    "keep": "x",
                    "new": true,
                    "retry": 2,
                    "tags": ["b", {"k": 1, "m": 2}, 3]
                }
            })
        );
    }

    #[test]
    fn array_replace_replaces_arrays_while_merging_objects() {
        let base = json!({"cfg": {"retry": 1, "tags": ["a", "b"]}});
        let overlays = vec![json!({"cfg": {"retry": 2, "tags": ["z"]}})];

        let actual = merge_with_policy(&base, &overlays, MergePolicy::ArrayReplace);

        assert_eq!(actual, json!({"cfg": {"retry": 2, "tags": ["z"]}}));
    }

    #[test]
    fn merge_is_deterministic_for_same_input() {
        let base = json!({"b": {"x": 1, "y": [1, 2]}, "a": true});
        let overlays = vec![json!({"b": {"y": [3], "z": "ok"}}), json!({"c": 9})];

        let first = merge_with_policy(&base, &overlays, MergePolicy::DeepMerge);
        let second = merge_with_policy(&base, &overlays, MergePolicy::DeepMerge);

        assert_eq!(first, second);
    }
}
