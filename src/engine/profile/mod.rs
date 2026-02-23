use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::domain::report::{ProfileFieldReport, ProfileReport, ProfileTypeDistribution};
use crate::util::sort::sort_value_keys;

/// Builds deterministic profile statistics for a dataset.
pub fn profile_values(values: &[Value]) -> ProfileReport {
    let mut per_record_samples = Vec::with_capacity(values.len());
    let mut all_paths = BTreeSet::new();

    for value in values {
        let mut record_samples = BTreeMap::new();
        collect_record_samples(value, "$", &mut record_samples);
        all_paths.extend(record_samples.keys().cloned());
        per_record_samples.push(record_samples);
    }

    let mut fields = BTreeMap::new();
    for path in all_paths {
        let field_report = summarize_field_path(&per_record_samples, &path);
        fields.insert(path, field_report);
    }

    ProfileReport {
        record_count: values.len(),
        field_count: fields.len(),
        fields,
    }
}

fn collect_record_samples(value: &Value, path: &str, out: &mut BTreeMap<String, Vec<Value>>) {
    match value {
        Value::Object(map) => collect_object_samples(map, path, out),
        Value::Array(items) => {
            for item in items {
                collect_record_samples(item, path, out);
            }
        }
        _ => {}
    }
}

fn collect_object_samples(
    map: &Map<String, Value>,
    path: &str,
    out: &mut BTreeMap<String, Vec<Value>>,
) {
    let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
    keys.sort_unstable();

    for key in keys {
        let next_path = append_object_key_path(path, key);
        if let Some(child) = map.get(key) {
            out.entry(next_path.clone())
                .or_default()
                .push(child.clone());
            collect_record_samples(child, &next_path, out);
        }
    }
}

fn summarize_field_path(
    per_record_samples: &[BTreeMap<String, Vec<Value>>],
    path: &str,
) -> ProfileFieldReport {
    let mut observed = 0usize;
    let mut null_count = 0usize;
    let mut unique_values = BTreeSet::new();
    let mut type_distribution = ProfileTypeDistribution::default();

    for samples in per_record_samples {
        if let Some(values) = samples.get(path) {
            for value in values {
                observe_value(
                    value,
                    &mut observed,
                    &mut null_count,
                    &mut unique_values,
                    &mut type_distribution,
                );
            }
        } else {
            observe_value(
                &Value::Null,
                &mut observed,
                &mut null_count,
                &mut unique_values,
                &mut type_distribution,
            );
        }
    }

    let null_ratio = if observed == 0 {
        0.0
    } else {
        null_count as f64 / observed as f64
    };

    ProfileFieldReport {
        null_ratio,
        unique_count: unique_values.len(),
        type_distribution,
    }
}

fn observe_value(
    value: &Value,
    observed: &mut usize,
    null_count: &mut usize,
    unique_values: &mut BTreeSet<String>,
    type_distribution: &mut ProfileTypeDistribution,
) {
    *observed += 1;

    match value {
        Value::Null => {
            type_distribution.null += 1;
            *null_count += 1;
        }
        Value::Bool(_) => type_distribution.boolean += 1,
        Value::Number(_) => type_distribution.number += 1,
        Value::String(_) => type_distribution.string += 1,
        Value::Array(_) => type_distribution.array += 1,
        Value::Object(_) => type_distribution.object += 1,
    }

    let normalized = sort_value_keys(value);
    let signature =
        serde_json::to_string(&normalized).expect("serializing normalized value should succeed");
    unique_values.insert(signature);
}

fn append_object_key_path(path: &str, key: &str) -> String {
    let encoded_key = serde_json::to_string(key).expect("serializing object key cannot fail");
    format!("{path}[{encoded_key}]")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::profile_values;

    #[test]
    fn profiles_flat_fields_with_deterministic_counts() {
        let values = vec![
            json!({"id": 1, "active": true}),
            json!({"id": 1}),
            json!({"id": null, "active": false}),
        ];

        let report = profile_values(&values);
        assert_eq!(report.record_count, 3);
        assert_eq!(report.field_count, 2);

        let id = report.fields.get("$[\"id\"]").expect("id profile");
        assert_eq!(id.null_ratio, 1.0 / 3.0);
        assert_eq!(id.unique_count, 2);
        assert_eq!(id.type_distribution.null, 1);
        assert_eq!(id.type_distribution.number, 2);

        let active = report.fields.get("$[\"active\"]").expect("active profile");
        assert_eq!(active.null_ratio, 1.0 / 3.0);
        assert_eq!(active.unique_count, 3);
        assert_eq!(active.type_distribution.null, 1);
        assert_eq!(active.type_distribution.boolean, 2);
    }

    #[test]
    fn profiles_nested_paths_with_sdiff_compatible_canonical_paths() {
        let values = vec![
            json!({"meta": {"name": "a"}, "tags": [{"v": 1}, {"v": null}]}),
            json!({"meta": {"name": "b"}, "tags": [{"v": 1}]}),
        ];

        let report = profile_values(&values);

        assert!(report.fields.contains_key("$[\"meta\"]"));
        assert!(report.fields.contains_key("$[\"meta\"][\"name\"]"));
        assert!(report.fields.contains_key("$[\"tags\"]"));
        assert!(report.fields.contains_key("$[\"tags\"][\"v\"]"));

        let tags_value = report
            .fields
            .get("$[\"tags\"][\"v\"]")
            .expect("tags.v profile");
        assert_eq!(tags_value.type_distribution.number, 2);
        assert_eq!(tags_value.type_distribution.null, 1);
    }

    #[test]
    fn profile_output_is_stable_for_identical_input() {
        let values = vec![
            json!({"z": "x", "a": 1}),
            json!({"a": 1, "z": "x"}),
            json!({"a": null}),
        ];

        let first = profile_values(&values);
        let second = profile_values(&values);

        let first_json = serde_json::to_string(&first).expect("serialize first");
        let second_json = serde_json::to_string(&second).expect("serialize second");
        assert_eq!(first_json, second_json);
    }
}
