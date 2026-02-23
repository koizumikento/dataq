use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::domain::value_path::{PathSegment, ValuePath};
use crate::engine::sdiff::{
    CountDiff, KeyDiff, SdiffError, SdiffOptions, SdiffReport, ValueDiffItem, ValueDiffSection,
};
use crate::util::sort::sort_value_keys;

pub(crate) fn compare_datasets(
    left: &[Value],
    right: &[Value],
    options: SdiffOptions,
) -> Result<SdiffReport, SdiffError> {
    let left_count = left.len();
    let right_count = right.len();

    let left_keys = collect_dataset_key_paths(left);
    let right_keys = collect_dataset_key_paths(right);
    let shared_keys: Vec<String> = left_keys.intersection(&right_keys).cloned().collect();
    let left_only_keys: Vec<String> = left_keys.difference(&right_keys).cloned().collect();
    let right_only_keys: Vec<String> = right_keys.difference(&left_keys).cloned().collect();

    let values = compare_value_sections(left, right, &options)?;
    let ignored_paths = options
        .ignore_paths
        .iter()
        .map(ToString::to_string)
        .collect();

    Ok(SdiffReport {
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
        ignored_paths,
        values,
    })
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

fn append_object_key_path(path: &str, key: &str) -> String {
    let encoded_key = serde_json::to_string(key).expect("serializing object key cannot fail");
    format!("{path}[{encoded_key}]")
}

fn compare_value_sections(
    left: &[Value],
    right: &[Value],
    options: &SdiffOptions,
) -> Result<ValueDiffSection, SdiffError> {
    match options.key_path.as_ref() {
        Some(key_path) => compare_value_sections_by_key(
            left,
            right,
            key_path,
            &options.ignore_paths,
            options.value_diff_cap,
        ),
        None => Ok(compare_value_sections_by_index(
            left,
            right,
            &options.ignore_paths,
            options.value_diff_cap,
        )),
    }
}

fn compare_value_sections_by_index(
    left: &[Value],
    right: &[Value],
    ignore_paths: &[ValuePath],
    value_diff_cap: usize,
) -> ValueDiffSection {
    let mut collector = ValueDiffCollector::new(value_diff_cap, ignore_paths);
    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        let mut row_segments = vec![PathSegment::Index(index)];
        compare_value_pair(left_value, right_value, &mut row_segments, &mut collector);
    }
    collector.finish()
}

fn compare_value_sections_by_key(
    left: &[Value],
    right: &[Value],
    key_path: &ValuePath,
    ignore_paths: &[ValuePath],
    value_diff_cap: usize,
) -> Result<ValueDiffSection, SdiffError> {
    let left_rows = index_rows_by_key(left, key_path, "left")?;
    let right_rows = index_rows_by_key(right, key_path, "right")?;

    let mut all_keys = BTreeSet::new();
    all_keys.extend(left_rows.keys().cloned());
    all_keys.extend(right_rows.keys().cloned());

    let mut collector = ValueDiffCollector::new(value_diff_cap, ignore_paths);
    let null = Value::Null;
    for (aligned_index, key) in all_keys.into_iter().enumerate() {
        let mut row_segments = vec![PathSegment::Index(aligned_index)];
        match (left_rows.get(&key), right_rows.get(&key)) {
            (Some(left_row), Some(right_row)) => compare_value_pair(
                left_row.value,
                right_row.value,
                &mut row_segments,
                &mut collector,
            ),
            (Some(left_row), None) => {
                compare_value_pair(left_row.value, &null, &mut row_segments, &mut collector)
            }
            (None, Some(right_row)) => {
                compare_value_pair(&null, right_row.value, &mut row_segments, &mut collector)
            }
            (None, None) => {}
        }
    }

    Ok(collector.finish())
}

fn compare_value_pair(
    left: &Value,
    right: &Value,
    path_segments: &mut Vec<PathSegment>,
    collector: &mut ValueDiffCollector<'_>,
) {
    if left == right {
        return;
    }
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            compare_object_values(left_map, right_map, path_segments, collector);
        }
        (Value::Array(left_items), Value::Array(right_items)) => {
            compare_array_values(left_items, right_items, path_segments, collector);
        }
        _ => collector.push(path_segments, left.clone(), right.clone()),
    }
}

fn compare_object_values(
    left_map: &Map<String, Value>,
    right_map: &Map<String, Value>,
    path_segments: &mut Vec<PathSegment>,
    collector: &mut ValueDiffCollector<'_>,
) {
    let mut keys = BTreeSet::new();
    keys.extend(left_map.keys().map(String::as_str));
    keys.extend(right_map.keys().map(String::as_str));

    for key in keys {
        path_segments.push(PathSegment::Key(key.to_string()));
        match (left_map.get(key), right_map.get(key)) {
            (Some(left_value), Some(right_value)) => {
                compare_value_pair(left_value, right_value, path_segments, collector);
            }
            (Some(left_value), None) => {
                collector.push(path_segments, left_value.clone(), Value::Null)
            }
            (None, Some(right_value)) => {
                collector.push(path_segments, Value::Null, right_value.clone())
            }
            (None, None) => {}
        }
        path_segments.pop();
    }
}

fn compare_array_values(
    left_items: &[Value],
    right_items: &[Value],
    path_segments: &mut Vec<PathSegment>,
    collector: &mut ValueDiffCollector<'_>,
) {
    for (index, (left_item, right_item)) in left_items.iter().zip(right_items.iter()).enumerate() {
        path_segments.push(PathSegment::Index(index));
        compare_value_pair(left_item, right_item, path_segments, collector);
        path_segments.pop();
    }

    if left_items.len() > right_items.len() {
        for (index, left_item) in left_items.iter().enumerate().skip(right_items.len()) {
            path_segments.push(PathSegment::Index(index));
            collector.push(path_segments, left_item.clone(), Value::Null);
            path_segments.pop();
        }
    } else if right_items.len() > left_items.len() {
        for (index, right_item) in right_items.iter().enumerate().skip(left_items.len()) {
            path_segments.push(PathSegment::Index(index));
            collector.push(path_segments, Value::Null, right_item.clone());
            path_segments.pop();
        }
    }
}

fn render_path(path_segments: &[PathSegment]) -> String {
    let mut out = String::from("$");
    for segment in path_segments {
        match segment {
            PathSegment::Key(key) => {
                let encoded =
                    serde_json::to_string(key).expect("serializing object key cannot fail");
                out.push('[');
                out.push_str(&encoded);
                out.push(']');
            }
            PathSegment::Index(index) => {
                out.push('[');
                out.push_str(&index.to_string());
                out.push(']');
            }
        }
    }
    out
}

fn path_prefix_matches(path_segments: &[PathSegment], prefix: &[PathSegment]) -> bool {
    path_segments.len() >= prefix.len() && path_segments[..prefix.len()] == *prefix
}

fn should_ignore_path(path_segments: &[PathSegment], ignore_paths: &[ValuePath]) -> bool {
    ignore_paths.iter().any(|ignore_path| {
        let ignore_segments = ignore_path.segments();
        path_prefix_matches(path_segments, ignore_segments)
            || (matches!(path_segments.first(), Some(PathSegment::Index(_)))
                && !matches!(ignore_segments.first(), Some(PathSegment::Index(_)))
                && path_prefix_matches(&path_segments[1..], ignore_segments))
    })
}

fn get_value_at_path<'a>(root: &'a Value, path: &ValuePath) -> Option<&'a Value> {
    let mut current = root;
    for segment in path.segments() {
        current = match segment {
            PathSegment::Key(key) => current.as_object()?.get(key)?,
            PathSegment::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

struct KeyedRow<'a> {
    row_index: usize,
    value: &'a Value,
}

fn index_rows_by_key<'a>(
    dataset: &'a [Value],
    key_path: &ValuePath,
    side: &'static str,
) -> Result<BTreeMap<String, KeyedRow<'a>>, SdiffError> {
    let mut out: BTreeMap<String, KeyedRow<'a>> = BTreeMap::new();
    for (row_index, row) in dataset.iter().enumerate() {
        let key_value =
            get_value_at_path(row, key_path).ok_or_else(|| SdiffError::MissingKeyValue {
                side,
                row_index,
                key_path: key_path.to_string(),
            })?;
        let normalized_key = sort_value_keys(key_value);
        let normalized_key_literal =
            serde_json::to_string(&normalized_key).expect("serializing key value cannot fail");

        if let Some(existing) = out.get(&normalized_key_literal) {
            return Err(SdiffError::DuplicateKeyValue {
                side,
                key_path: key_path.to_string(),
                key_literal: normalized_key_literal,
                first_index: existing.row_index,
                second_index: row_index,
            });
        }

        out.insert(
            normalized_key_literal,
            KeyedRow {
                row_index,
                value: row,
            },
        );
    }
    Ok(out)
}

struct ValueDiffCollector<'a> {
    total: usize,
    cap: usize,
    truncated: bool,
    ignore_paths: &'a [ValuePath],
    items: Vec<ValueDiffItem>,
}

impl<'a> ValueDiffCollector<'a> {
    fn new(cap: usize, ignore_paths: &'a [ValuePath]) -> Self {
        Self {
            total: 0,
            cap,
            truncated: false,
            ignore_paths,
            items: Vec::new(),
        }
    }

    fn push(&mut self, path_segments: &[PathSegment], left: Value, right: Value) {
        if should_ignore_path(path_segments, self.ignore_paths) {
            return;
        }

        self.total += 1;
        if self.items.len() < self.cap {
            self.items.push(ValueDiffItem {
                path: render_path(path_segments),
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
