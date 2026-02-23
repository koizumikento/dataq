use serde_json::{Map, Value};

use crate::domain::value_path::{PathSegment, ValuePath};
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

impl MergePolicy {
    /// Parses a CLI literal (`last-wins`, `deep-merge`, `array-replace`) into policy.
    pub fn parse_cli_name(input: &str) -> Option<Self> {
        match input {
            "last-wins" => Some(Self::LastWins),
            "deep-merge" => Some(Self::DeepMerge),
            "array-replace" => Some(Self::ArrayReplace),
            _ => None,
        }
    }
}

/// Merge policy bound to a canonical subtree path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathMergePolicy {
    pub path: ValuePath,
    pub policy: MergePolicy,
}

/// Merges `base` with overlays from left to right under `policy`.
///
/// Overlay order is significant and deterministic.
pub fn merge_with_policy(base: &Value, overlays: &[Value], policy: MergePolicy) -> Value {
    merge_with_path_policies(base, overlays, policy, &[])
}

/// Merges with optional path-scoped policies.
///
/// For each merge point, the longest matching `path` policy is selected.
/// If no path policy matches, `default_policy` is used.
pub fn merge_with_path_policies(
    base: &Value,
    overlays: &[Value],
    default_policy: MergePolicy,
    path_policies: &[PathMergePolicy],
) -> Value {
    let mut path = Vec::new();
    let merged = overlays.iter().fold(base.clone(), |acc, overlay| {
        merge_value_pair(&acc, overlay, default_policy, path_policies, &mut path)
    });
    sort_value_keys(&merged)
}

fn merge_value_pair(
    base: &Value,
    overlay: &Value,
    default_policy: MergePolicy,
    path_policies: &[PathMergePolicy],
    path: &mut Vec<PathSegment>,
) -> Value {
    let resolved_policy = resolve_policy(path, default_policy, path_policies);
    match resolved_policy {
        MergePolicy::LastWins => merge_last_wins(base, overlay),
        MergePolicy::DeepMerge => {
            merge_deep(base, overlay, false, default_policy, path_policies, path)
        }
        MergePolicy::ArrayReplace => {
            merge_deep(base, overlay, true, default_policy, path_policies, path)
        }
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

fn merge_deep(
    base: &Value,
    overlay: &Value,
    replace_arrays: bool,
    default_policy: MergePolicy,
    path_policies: &[PathMergePolicy],
    path: &mut Vec<PathSegment>,
) -> Value {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            let mut merged: Map<String, Value> = base_map.clone();
            for (key, overlay_value) in overlay_map {
                if let Some(base_value) = merged.remove(key) {
                    path.push(PathSegment::Key(key.clone()));
                    let next = merge_value_pair(
                        &base_value,
                        overlay_value,
                        default_policy,
                        path_policies,
                        path,
                    );
                    path.pop();
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
                            path.push(PathSegment::Index(index));
                            merged.push(merge_value_pair(
                                base_item,
                                overlay_item,
                                default_policy,
                                path_policies,
                                path,
                            ));
                            path.pop();
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

fn resolve_policy(
    path: &[PathSegment],
    default_policy: MergePolicy,
    path_policies: &[PathMergePolicy],
) -> MergePolicy {
    let mut selected: Option<(usize, usize, MergePolicy)> = None;
    for (index, path_policy) in path_policies.iter().enumerate() {
        let candidate_path = path_policy.path.segments();
        if !has_path_prefix(path, candidate_path) {
            continue;
        }
        let candidate_depth = candidate_path.len();
        match selected {
            None => selected = Some((candidate_depth, index, path_policy.policy)),
            Some((best_depth, best_index, _)) => {
                if candidate_depth > best_depth
                    || (candidate_depth == best_depth && index >= best_index)
                {
                    selected = Some((candidate_depth, index, path_policy.policy));
                }
            }
        }
    }

    selected
        .map(|(_, _, policy)| policy)
        .unwrap_or(default_policy)
}

fn has_path_prefix(path: &[PathSegment], prefix: &[PathSegment]) -> bool {
    if prefix.len() > path.len() {
        return false;
    }
    path.iter().zip(prefix.iter()).all(|(lhs, rhs)| lhs == rhs)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MergePolicy, PathMergePolicy, merge_with_path_policies, merge_with_policy};
    use crate::domain::value_path::ValuePath;

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

    #[test]
    fn empty_path_policy_rules_preserve_existing_behavior() {
        let base = json!({
            "cfg": {
                "arr": [{"a": 1}, 2],
                "obj": {"left": 1}
            },
            "keep": true
        });
        let overlays = vec![json!({
            "cfg": {
                "arr": [{"b": 2}],
                "obj": {"right": 2}
            },
            "add": true
        })];

        let legacy = merge_with_policy(&base, &overlays, MergePolicy::DeepMerge);
        let scoped = merge_with_path_policies(&base, &overlays, MergePolicy::DeepMerge, &[]);

        assert_eq!(legacy, scoped);
    }

    #[test]
    fn subtree_policy_applies_only_to_matching_subtree() {
        let base = json!({
            "cfg": {
                "items": [{"left": 1}, 2],
                "obj": {"left": 1}
            }
        });
        let overlays = vec![json!({
            "cfg": {
                "items": [{"right": 2}],
                "obj": {"right": 2}
            }
        })];
        let path_policies = vec![PathMergePolicy {
            path: ValuePath::parse_canonical(r#"$["cfg"]["items"]"#).expect("parse path"),
            policy: MergePolicy::ArrayReplace,
        }];

        let actual =
            merge_with_path_policies(&base, &overlays, MergePolicy::DeepMerge, &path_policies);

        assert_eq!(
            actual,
            json!({
                "cfg": {
                    "items": [{"right": 2}],
                    "obj": {"left": 1, "right": 2}
                }
            })
        );
    }

    #[test]
    fn longest_matching_path_policy_takes_precedence() {
        let base = json!({
            "cfg": {
                "items": [{"left": 1}, 2]
            }
        });
        let overlays = vec![json!({
            "cfg": {
                "items": [{"right": 2}]
            }
        })];
        let path_policies = vec![
            PathMergePolicy {
                path: ValuePath::parse_canonical(r#"$["cfg"]"#).expect("parse parent path"),
                policy: MergePolicy::ArrayReplace,
            },
            PathMergePolicy {
                path: ValuePath::parse_canonical(r#"$["cfg"]["items"]"#).expect("parse child path"),
                policy: MergePolicy::DeepMerge,
            },
        ];

        let actual =
            merge_with_path_policies(&base, &overlays, MergePolicy::DeepMerge, &path_policies);

        assert_eq!(
            actual,
            json!({
                "cfg": {
                    "items": [{"left": 1, "right": 2}, 2]
                }
            })
        );
    }

    #[test]
    fn equal_depth_path_policy_uses_last_definition() {
        let base = json!({
            "cfg": {
                "items": [{"left": 1}, 2]
            }
        });
        let overlays = vec![json!({
            "cfg": {
                "items": [{"right": 2}]
            }
        })];
        let path_policies = vec![
            PathMergePolicy {
                path: ValuePath::parse_canonical(r#"$["cfg"]["items"]"#).expect("parse path"),
                policy: MergePolicy::ArrayReplace,
            },
            PathMergePolicy {
                path: ValuePath::parse_canonical(r#"$["cfg"]["items"]"#)
                    .expect("parse same-depth path"),
                policy: MergePolicy::DeepMerge,
            },
        ];

        let actual =
            merge_with_path_policies(&base, &overlays, MergePolicy::DeepMerge, &path_policies);

        assert_eq!(
            actual,
            json!({
                "cfg": {
                    "items": [{"left": 1, "right": 2}, 2]
                }
            })
        );
    }
}
