use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::engine::sdiff::{
    CountDiff, KeyDiff, SdiffOptions, SdiffReport, ValueDiffItem, ValueDiffSection,
};
use crate::util::sort::sort_value_keys;

pub(crate) fn compare_datasets(
    left: &[Value],
    right: &[Value],
    options: SdiffOptions,
) -> SdiffReport {
    let left_count = left.len();
    let right_count = right.len();

    let left_keys = collect_dataset_key_paths(left);
    let right_keys = collect_dataset_key_paths(right);
    let shared_keys: Vec<String> = left_keys.intersection(&right_keys).cloned().collect();
    let left_only_keys: Vec<String> = left_keys.difference(&right_keys).cloned().collect();
    let right_only_keys: Vec<String> = right_keys.difference(&left_keys).cloned().collect();

    let values = compare_value_sections(left, right, options.value_diff_cap);

    SdiffReport {
        counts: CountDiff {
            left: left_count,
            right: right_count,
            delta: right_count as i64 - left_count as i64,
            equal: left_count == right_count,
        },
        keys: KeyDiff {
            left_only: left_only_keys,
            right_only: right_only_keys,
            shared: shared_keys,
        },
        values,
    }
}

fn collect_dataset_key_paths(dataset: &[Value]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for value in dataset {
        collect_value_key_paths(value, "$", &mut out);
    }
    out
}

fn collect_value_key_paths(value: &Value, path: &str, out: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
            keys.sort_unstable();
            for key in keys {
                let next_path = append_object_key_path(path, key);
                out.insert(next_path.clone());
                if let Some(child) = map.get(key) {
                    collect_value_key_paths(child, &next_path, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_value_key_paths(item, path, out);
            }
        }
        _ => {}
    }
}

fn compare_value_sections(
    left: &[Value],
    right: &[Value],
    value_diff_cap: usize,
) -> ValueDiffSection {
    let mut collector = ValueDiffCollector::new(value_diff_cap);
    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        let row_path = append_array_index_path("$", index);
        compare_value_pair(left_value, right_value, &row_path, &mut collector);
    }
    collector.finish()
}

fn compare_value_pair(left: &Value, right: &Value, path: &str, collector: &mut ValueDiffCollector) {
    if left == right {
        return;
    }
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            compare_object_values(left_map, right_map, path, collector);
        }
        (Value::Array(left_items), Value::Array(right_items)) => {
            compare_array_values(left_items, right_items, path, collector);
        }
        _ => collector.push(path.to_string(), left.clone(), right.clone()),
    }
}

fn compare_object_values(
    left_map: &Map<String, Value>,
    right_map: &Map<String, Value>,
    path: &str,
    collector: &mut ValueDiffCollector,
) {
    let mut keys = BTreeSet::new();
    keys.extend(left_map.keys().map(String::as_str));
    keys.extend(right_map.keys().map(String::as_str));

    for key in keys {
        let next_path = append_object_key_path(path, key);
        match (left_map.get(key), right_map.get(key)) {
            (Some(left_value), Some(right_value)) => {
                compare_value_pair(left_value, right_value, &next_path, collector);
            }
            (Some(left_value), None) => collector.push(next_path, left_value.clone(), Value::Null),
            (None, Some(right_value)) => {
                collector.push(next_path, Value::Null, right_value.clone())
            }
            (None, None) => {}
        }
    }
}

fn compare_array_values(
    left_items: &[Value],
    right_items: &[Value],
    path: &str,
    collector: &mut ValueDiffCollector,
) {
    for (index, (left_item, right_item)) in left_items.iter().zip(right_items.iter()).enumerate() {
        let next_path = append_array_index_path(path, index);
        compare_value_pair(left_item, right_item, &next_path, collector);
    }

    if left_items.len() > right_items.len() {
        for (index, left_item) in left_items.iter().enumerate().skip(right_items.len()) {
            let next_path = append_array_index_path(path, index);
            collector.push(next_path, left_item.clone(), Value::Null);
        }
    } else if right_items.len() > left_items.len() {
        for (index, right_item) in right_items.iter().enumerate().skip(left_items.len()) {
            let next_path = append_array_index_path(path, index);
            collector.push(next_path, Value::Null, right_item.clone());
        }
    }
}

fn append_object_key_path(path: &str, key: &str) -> String {
    let encoded_key = serde_json::to_string(key).expect("serializing object key cannot fail");
    format!("{path}[{encoded_key}]")
}

fn append_array_index_path(path: &str, index: usize) -> String {
    format!("{path}[{index}]")
}

struct ValueDiffCollector {
    total: usize,
    cap: usize,
    truncated: bool,
    items: Vec<ValueDiffItem>,
}

impl ValueDiffCollector {
    fn new(cap: usize) -> Self {
        Self {
            total: 0,
            cap,
            truncated: false,
            items: Vec::new(),
        }
    }

    fn push(&mut self, path: String, left: Value, right: Value) {
        self.total += 1;
        if self.items.len() < self.cap {
            self.items.push(ValueDiffItem {
                path,
                left: sort_value_keys(&left),
                right: sort_value_keys(&right),
            });
            return;
        }
        self.truncated = true;
    }

    fn finish(self) -> ValueDiffSection {
        ValueDiffSection {
            total: self.total,
            truncated: self.truncated,
            items: self.items,
        }
    }
}
