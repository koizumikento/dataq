use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::domain::report::{
    ProfileBriefFieldReport, ProfileBriefReport, ProfileDominantType, ProfileFieldReport,
    ProfileNumericStats, ProfileReport, ProfileTypeDistribution,
};
use crate::util::sort::sort_value_keys;

const NUMERIC_STAT_SCALE: f64 = 1_000_000.0;

/// Sort mode for compact profile fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileBriefSortFields {
    Path,
    UniqueCount,
    NullRatio,
}

#[derive(Debug, Clone)]
struct QsvColumns {
    field: String,
    value_type: String,
    unique_count: String,
    null_count: Option<String>,
    null_ratio: Option<String>,
    record_count: Option<String>,
    count: Option<String>,
    n_negative: Option<String>,
    n_zero: Option<String>,
    n_positive: Option<String>,
    min: Option<String>,
    max: Option<String>,
    mean: Option<String>,
    p50: Option<String>,
    p95: Option<String>,
    percentiles: Option<String>,
}

impl QsvColumns {
    fn detect(header_row: &Map<String, Value>) -> Option<Self> {
        let header_lookup: BTreeMap<String, String> = header_row
            .keys()
            .map(|key| (key.to_ascii_lowercase(), key.clone()))
            .collect();

        let field = lookup_column(&header_lookup, &["field", "column", "column_name", "name"])?;
        let value_type = lookup_column(&header_lookup, &["type", "value_type", "inferred_type"])?;
        let unique_count = lookup_column(
            &header_lookup,
            &["cardinality", "unique_count", "distinct_count"],
        )?;

        let null_count = lookup_column(&header_lookup, &["nullcount", "null_count"]);
        let null_ratio = lookup_column(&header_lookup, &["null_ratio", "nullratio", "sparsity"]);
        if null_count.is_none() && null_ratio.is_none() {
            return None;
        }

        let record_count = lookup_column(
            &header_lookup,
            &["record_count", "records", "rows", "row_count", "total_rows"],
        );
        let count = lookup_column(&header_lookup, &["count", "nonnull_count"]);
        let n_negative = lookup_column(&header_lookup, &["n_negative", "negative_count"]);
        let n_zero = lookup_column(&header_lookup, &["n_zero", "zero_count"]);
        let n_positive = lookup_column(&header_lookup, &["n_positive", "positive_count"]);
        let min = lookup_column(&header_lookup, &["min"]);
        let max = lookup_column(&header_lookup, &["max"]);
        let mean = lookup_column(&header_lookup, &["mean"]);
        let p50 = lookup_column(&header_lookup, &["p50", "q2_median", "median"]);
        let p95 = lookup_column(&header_lookup, &["p95"]);
        let percentiles = lookup_column(&header_lookup, &["percentiles", "quantiles"]);

        let has_qsv_marker = [
            record_count.as_ref(),
            n_negative.as_ref(),
            n_zero.as_ref(),
            n_positive.as_ref(),
            percentiles.as_ref(),
            p50.as_ref(),
            p95.as_ref(),
        ]
        .iter()
        .any(Option::is_some);
        if !has_qsv_marker {
            return None;
        }

        Some(Self {
            field,
            value_type,
            unique_count,
            null_count,
            null_ratio,
            record_count,
            count,
            n_negative,
            n_zero,
            n_positive,
            min,
            max,
            mean,
            p50,
            p95,
            percentiles,
        })
    }
}

/// Normalize qsv-style CSV profile rows into `ProfileReport`.
///
/// Returns `Ok(None)` when rows do not match qsv-profile shape.
pub fn normalize_qsv_profile_rows(values: &[Value]) -> Result<Option<ProfileReport>, String> {
    if values.is_empty() {
        return Ok(None);
    }

    let Some(first_row) = values.first().and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(columns) = QsvColumns::detect(first_row) else {
        return Ok(None);
    };

    let mut row_maps = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        let Some(map) = value.as_object() else {
            return Err(format!("qsv profile row {index} must be an object"));
        };
        row_maps.push(map);
    }

    let record_count = derive_qsv_record_count(&row_maps, &columns).ok_or_else(|| {
        "failed to derive `record_count` from qsv profile rows; include `record_count` or qsv counters".to_string()
    })?;

    let mut fields = BTreeMap::new();
    for (index, row) in row_maps.iter().enumerate() {
        let field_name = required_row_cell(row, &columns.field, index)?;
        let type_name = required_row_cell(row, &columns.value_type, index)?;
        let unique_count = parse_required_usize(
            row_cell(row, &columns.unique_count),
            &columns.unique_count,
            index,
        )?;

        let row_record_count = row_record_count_candidate(row, &columns).unwrap_or(record_count);
        let mut null_count = parse_optional_usize(row_cell_opt(row, columns.null_count.as_deref()))
            .map_err(|message| format!("qsv profile row {index} {message}"))?
            .unwrap_or(0);
        if null_count == 0 {
            if let Some(ratio_text) = row_cell_opt(row, columns.null_ratio.as_deref()) {
                if let Ok(ratio) = parse_float(ratio_text) {
                    if ratio > 0.0 {
                        null_count = ((row_record_count as f64) * ratio).round() as usize;
                    }
                }
            }
        }
        null_count = null_count.min(row_record_count);
        let non_null_count = row_record_count.saturating_sub(null_count);

        let mut type_distribution = ProfileTypeDistribution {
            null: null_count,
            ..ProfileTypeDistribution::default()
        };
        match normalized_type_bucket(type_name) {
            QsvTypeBucket::Null => {
                type_distribution.null = row_record_count;
            }
            QsvTypeBucket::Boolean => type_distribution.boolean = non_null_count,
            QsvTypeBucket::Number => type_distribution.number = non_null_count,
            QsvTypeBucket::String => type_distribution.string = non_null_count,
        }

        let numeric_stats = if matches!(normalized_type_bucket(type_name), QsvTypeBucket::Number) {
            parse_qsv_numeric_stats(row, &columns, non_null_count)
        } else {
            None
        };

        let field_path = append_object_key_path("$", field_name);
        let null_ratio = if row_record_count == 0 {
            0.0
        } else {
            null_count as f64 / row_record_count as f64
        };
        fields.insert(
            field_path,
            ProfileFieldReport {
                null_ratio,
                unique_count,
                type_distribution,
                numeric_stats,
            },
        );
    }

    Ok(Some(ProfileReport {
        record_count,
        field_count: fields.len(),
        returned_field_count: None,
        fields,
        missing_fields: None,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QsvTypeBucket {
    Null,
    Boolean,
    Number,
    String,
}

fn normalized_type_bucket(type_name: &str) -> QsvTypeBucket {
    let normalized = type_name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return QsvTypeBucket::String;
    }
    if matches!(normalized.as_str(), "null") {
        return QsvTypeBucket::Null;
    }
    if matches!(
        normalized.as_str(),
        "bool" | "boolean" | "logical" | "binary"
    ) {
        return QsvTypeBucket::Boolean;
    }
    if matches!(
        normalized.as_str(),
        "int" | "integer" | "float" | "double" | "number" | "numeric" | "decimal" | "currency"
    ) {
        return QsvTypeBucket::Number;
    }
    QsvTypeBucket::String
}

fn parse_qsv_numeric_stats(
    row: &Map<String, Value>,
    columns: &QsvColumns,
    non_null_count: usize,
) -> Option<ProfileNumericStats> {
    if non_null_count == 0 {
        return None;
    }

    let min = parse_float(row_cell_opt(row, columns.min.as_deref())?).ok()?;
    let max = parse_float(row_cell_opt(row, columns.max.as_deref())?).ok()?;
    let mean = parse_float(row_cell_opt(row, columns.mean.as_deref())?).ok()?;
    let p50 = if let Some(value) = row_cell_opt(row, columns.p50.as_deref()) {
        parse_float(value).ok()?
    } else {
        let percentiles = row_cell_opt(row, columns.percentiles.as_deref())?;
        parse_percentile_value(percentiles, 50)?
    };
    let p95 = if let Some(value) = row_cell_opt(row, columns.p95.as_deref()) {
        parse_float(value).ok()?
    } else {
        let percentiles = row_cell_opt(row, columns.percentiles.as_deref())?;
        parse_percentile_value(percentiles, 95)?
    };

    Some(ProfileNumericStats {
        count: non_null_count,
        min: round_numeric_stat(min),
        max: round_numeric_stat(max),
        mean: round_numeric_stat(mean),
        p50: round_numeric_stat(p50),
        p95: round_numeric_stat(p95),
    })
}

fn parse_percentile_value(text: &str, percentile: usize) -> Option<f64> {
    for chunk in text.split('|') {
        let Some((label, raw_value)) = chunk.split_once(':') else {
            continue;
        };
        let Ok(parsed_label) = label.trim().trim_end_matches('%').parse::<usize>() else {
            continue;
        };
        if parsed_label == percentile {
            return parse_float(raw_value.trim()).ok();
        }
    }
    None
}

fn derive_qsv_record_count(rows: &[&Map<String, Value>], columns: &QsvColumns) -> Option<usize> {
    let mut max_candidate = 0usize;
    let mut has_candidate = false;
    for row in rows {
        if let Some(candidate) = row_record_count_candidate(row, columns) {
            max_candidate = max_candidate.max(candidate);
            has_candidate = true;
        }
    }
    has_candidate.then_some(max_candidate)
}

fn row_record_count_candidate(row: &Map<String, Value>, columns: &QsvColumns) -> Option<usize> {
    if let Some(record_count) = columns
        .record_count
        .as_deref()
        .and_then(|column| parse_optional_usize(row_cell(row, column)).ok().flatten())
    {
        return Some(record_count);
    }

    let null_count = columns
        .null_count
        .as_deref()
        .and_then(|column| parse_optional_usize(row_cell(row, column)).ok().flatten())
        .unwrap_or(0);

    let mut signed_count_total = 0usize;
    let mut has_signed_counters = false;
    for column in [
        columns.n_negative.as_deref(),
        columns.n_zero.as_deref(),
        columns.n_positive.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        has_signed_counters = true;
        let count = parse_optional_usize(row_cell(row, column))
            .ok()
            .flatten()
            .unwrap_or(0);
        signed_count_total = signed_count_total.saturating_add(count);
    }
    if has_signed_counters {
        return Some(null_count.saturating_add(signed_count_total));
    }

    if let Some(ratio_column) = columns.null_ratio.as_deref() {
        if let Some(ratio_text) = row_cell(row, ratio_column) {
            if let Ok(ratio) = parse_float(ratio_text) {
                if ratio > 0.0 && null_count > 0 {
                    let estimated = (null_count as f64 / ratio).round();
                    if estimated.is_finite() && estimated >= null_count as f64 {
                        return Some(estimated as usize);
                    }
                }
            }
        }
    }

    if let Some(column) = columns.count.as_deref() {
        if let Some(count) = parse_optional_usize(row_cell(row, column)).ok().flatten() {
            return Some(count.max(null_count));
        }
    }

    None
}

fn lookup_column(header_lookup: &BTreeMap<String, String>, candidates: &[&str]) -> Option<String> {
    candidates
        .iter()
        .find_map(|candidate| header_lookup.get(*candidate))
        .cloned()
}

fn row_cell<'a>(row: &'a Map<String, Value>, column: &str) -> Option<&'a str> {
    row.get(column).and_then(Value::as_str).map(str::trim)
}

fn row_cell_opt<'a>(row: &'a Map<String, Value>, column: Option<&str>) -> Option<&'a str> {
    column.and_then(|column| row_cell(row, column))
}

fn required_row_cell<'a>(
    row: &'a Map<String, Value>,
    column: &str,
    index: usize,
) -> Result<&'a str, String> {
    let value = row_cell(row, column)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("qsv profile row {index} missing `{column}`"))?;
    Ok(value)
}

fn parse_required_usize(value: Option<&str>, column: &str, index: usize) -> Result<usize, String> {
    parse_optional_usize(value)
        .map_err(|message| format!("qsv profile row {index} {message}"))?
        .ok_or_else(|| format!("qsv profile row {index} missing `{column}`"))
}

fn parse_optional_usize(value: Option<&str>) -> Result<Option<usize>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }

    if let Ok(parsed) = value.parse::<usize>() {
        return Ok(Some(parsed));
    }
    if let Ok(parsed) = value.parse::<f64>() {
        if parsed.is_finite() && parsed >= 0.0 {
            let rounded = parsed.round();
            if (parsed - rounded).abs() < f64::EPSILON {
                return Ok(Some(rounded as usize));
            }
        }
    }

    Err(format!("has invalid integer value `{value}`"))
}

fn parse_float(value: &str) -> Result<f64, String> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("has invalid number value `{value}`"))
}

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
        returned_field_count: None,
        fields,
        missing_fields: None,
    }
}

/// Projects a completed profile report to a deterministic set of canonical field paths.
pub fn project_report(
    mut report: ProfileReport,
    requested_fields: &[String],
    allow_missing_fields: bool,
) -> Result<ProfileReport, Vec<String>> {
    let requested: BTreeSet<String> = requested_fields.iter().cloned().collect();
    let missing_fields: Vec<String> = requested
        .iter()
        .filter(|field| !report.fields.contains_key(*field))
        .cloned()
        .collect();

    if !allow_missing_fields && !missing_fields.is_empty() {
        return Err(missing_fields);
    }

    report.fields.retain(|field, _| requested.contains(field));
    report.returned_field_count = Some(report.fields.len());
    report.missing_fields = if allow_missing_fields {
        Some(missing_fields)
    } else {
        None
    };
    Ok(report)
}

/// Converts a full profile report into compact LLM-oriented output.
pub fn brief_report(
    report: ProfileReport,
    max_fields: Option<usize>,
    sort_fields: ProfileBriefSortFields,
) -> ProfileBriefReport {
    let mut fields: Vec<ProfileBriefFieldReport> = report
        .fields
        .into_iter()
        .map(|(path, field)| ProfileBriefFieldReport {
            path,
            null_ratio: field.null_ratio,
            unique_count: field.unique_count,
            dominant_type: dominant_type(&field.type_distribution),
            numeric: field.numeric_stats,
        })
        .collect();

    sort_brief_fields(&mut fields, sort_fields);

    let available_field_count = fields.len();
    if let Some(max_fields) = max_fields {
        fields.truncate(max_fields);
    }

    let truncated = available_field_count > fields.len();

    ProfileBriefReport {
        record_count: report.record_count,
        field_count: report.field_count,
        truncated,
        fields,
        missing_fields: report.missing_fields,
    }
}

fn sort_brief_fields(fields: &mut [ProfileBriefFieldReport], sort_fields: ProfileBriefSortFields) {
    match sort_fields {
        ProfileBriefSortFields::Path => fields.sort_by(|left, right| left.path.cmp(&right.path)),
        ProfileBriefSortFields::UniqueCount => fields.sort_by(|left, right| {
            right
                .unique_count
                .cmp(&left.unique_count)
                .then_with(|| left.path.cmp(&right.path))
        }),
        ProfileBriefSortFields::NullRatio => fields.sort_by(|left, right| {
            right
                .null_ratio
                .total_cmp(&left.null_ratio)
                .then_with(|| left.path.cmp(&right.path))
        }),
    }
}

fn dominant_type(distribution: &ProfileTypeDistribution) -> ProfileDominantType {
    [
        (ProfileDominantType::Boolean, distribution.boolean),
        (ProfileDominantType::Number, distribution.number),
        (ProfileDominantType::String, distribution.string),
        (ProfileDominantType::Array, distribution.array),
        (ProfileDominantType::Object, distribution.object),
    ]
    .into_iter()
    .max_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| dominant_type_priority(left.0).cmp(&dominant_type_priority(right.0)))
    })
    .and_then(|(dominant_type, count)| (count > 0).then_some(dominant_type))
    .unwrap_or(ProfileDominantType::Null)
}

fn dominant_type_priority(dominant_type: ProfileDominantType) -> usize {
    match dominant_type {
        ProfileDominantType::Boolean => 5,
        ProfileDominantType::Number => 4,
        ProfileDominantType::String => 3,
        ProfileDominantType::Array => 2,
        ProfileDominantType::Object => 1,
        ProfileDominantType::Null => 0,
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
    let mut numeric_samples = Vec::new();

    for samples in per_record_samples {
        if let Some(values) = samples.get(path) {
            for value in values {
                observe_value(
                    value,
                    &mut observed,
                    &mut null_count,
                    &mut unique_values,
                    &mut type_distribution,
                    &mut numeric_samples,
                );
            }
        } else {
            observe_value(
                &Value::Null,
                &mut observed,
                &mut null_count,
                &mut unique_values,
                &mut type_distribution,
                &mut numeric_samples,
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
        numeric_stats: compute_numeric_stats(&numeric_samples),
    }
}

fn observe_value(
    value: &Value,
    observed: &mut usize,
    null_count: &mut usize,
    unique_values: &mut BTreeSet<String>,
    type_distribution: &mut ProfileTypeDistribution,
    numeric_samples: &mut Vec<f64>,
) {
    *observed += 1;

    match value {
        Value::Null => {
            type_distribution.null += 1;
            *null_count += 1;
        }
        Value::Bool(_) => type_distribution.boolean += 1,
        Value::Number(number) => {
            type_distribution.number += 1;
            if let Some(sample) = number.as_f64() {
                numeric_samples.push(sample);
            }
        }
        Value::String(_) => type_distribution.string += 1,
        Value::Array(_) => type_distribution.array += 1,
        Value::Object(_) => type_distribution.object += 1,
    }

    let normalized = sort_value_keys(value);
    let signature =
        serde_json::to_string(&normalized).expect("serializing normalized value should succeed");
    unique_values.insert(signature);
}

fn compute_numeric_stats(samples: &[f64]) -> Option<ProfileNumericStats> {
    if samples.is_empty() {
        return None;
    }

    let mut sorted_samples = samples.to_vec();
    sorted_samples.sort_by(f64::total_cmp);

    let count = sorted_samples.len();
    let mean = numeric_mean(&sorted_samples);
    let p50 = nearest_rank_percentile(&sorted_samples, 50);
    let p95 = nearest_rank_percentile(&sorted_samples, 95);

    Some(ProfileNumericStats {
        count,
        min: round_native_numeric_stat(sorted_samples[0]),
        max: round_native_numeric_stat(sorted_samples[count - 1]),
        mean: round_native_numeric_stat(mean),
        p50: round_native_numeric_stat(p50),
        p95: round_native_numeric_stat(p95),
    })
}

fn numeric_mean(samples: &[f64]) -> f64 {
    let direct_sum = samples.iter().sum::<f64>();
    if direct_sum.is_finite() {
        return direct_sum / samples.len() as f64;
    }

    let max_abs = samples
        .iter()
        .map(|sample| sample.abs())
        .fold(0.0, f64::max);
    if max_abs == 0.0 {
        return 0.0;
    }

    let normalized_sum = samples.iter().map(|sample| sample / max_abs).sum::<f64>();
    let normalized_mean = (normalized_sum / samples.len() as f64).clamp(-1.0, 1.0);
    normalized_mean * max_abs
}

fn nearest_rank_percentile(sorted_samples: &[f64], percentile: usize) -> f64 {
    let len = sorted_samples.len();
    let rank = ((percentile as f64 / 100.0) * len as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(len - 1);
    sorted_samples[index]
}

fn round_numeric_stat(value: f64) -> f64 {
    let rounded = (value * NUMERIC_STAT_SCALE).round() / NUMERIC_STAT_SCALE;
    if rounded == 0.0 { 0.0 } else { rounded }
}

fn round_native_numeric_stat(value: f64) -> f64 {
    let scaled = value * NUMERIC_STAT_SCALE;
    let rounded = if scaled.is_finite() {
        scaled.round() / NUMERIC_STAT_SCALE
    } else {
        value
    };
    if rounded == 0.0 { 0.0 } else { rounded }
}

fn append_object_key_path(path: &str, key: &str) -> String {
    let encoded_key = serde_json::to_string(key).expect("serializing object key cannot fail");
    format!("{path}[{encoded_key}]")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{normalize_qsv_profile_rows, profile_values};

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
        assert_eq!(id.numeric_stats.as_ref().expect("numeric stats").count, 2);
        assert_eq!(id.numeric_stats.as_ref().expect("numeric stats").mean, 1.0);

        let active = report.fields.get("$[\"active\"]").expect("active profile");
        assert_eq!(active.null_ratio, 1.0 / 3.0);
        assert_eq!(active.unique_count, 3);
        assert_eq!(active.type_distribution.null, 1);
        assert_eq!(active.type_distribution.boolean, 2);
        assert!(active.numeric_stats.is_none());
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

    #[test]
    fn profiles_numeric_stats_with_nearest_rank_percentiles_and_rounding() {
        let values = vec![
            json!({"score": 1.0}),
            json!({"score": 2.0}),
            json!({"score": 3.3333339}),
            json!({"score": 100.0}),
            json!({"score": null}),
        ];

        let report = profile_values(&values);
        let score = report.fields.get("$[\"score\"]").expect("score profile");
        let numeric = score.numeric_stats.as_ref().expect("numeric stats");

        assert_eq!(numeric.count, 4);
        assert_eq!(numeric.min, 1.0);
        assert_eq!(numeric.max, 100.0);
        assert_eq!(numeric.mean, 26.583333);
        assert_eq!(numeric.p50, 2.0);
        assert_eq!(numeric.p95, 100.0);
    }

    #[test]
    fn profiles_large_finite_numeric_stats_without_intermediate_overflow() {
        let cases = [
            (vec![1e308], 1e308, 1e308, 1e308),
            (vec![1e308, 1e308], 1e308, 1e308, 1e308),
            (vec![-1e308, -1e308], -1e308, -1e308, -1e308),
            (vec![-1e308, -1e308, 1e308, 1e308], -1e308, 1e308, 0.0),
        ];

        for (samples, expected_min, expected_max, expected_mean) in cases {
            let values: Vec<_> = samples
                .into_iter()
                .map(|score| json!({"score": score}))
                .collect();
            let report = profile_values(&values);
            let numeric = report.fields["$[\"score\"]"]
                .numeric_stats
                .as_ref()
                .expect("numeric stats");

            assert_eq!(numeric.min, expected_min);
            assert_eq!(numeric.max, expected_max);
            assert_eq!(numeric.mean, expected_mean);
            assert!(numeric.min.is_finite());
            assert!(numeric.max.is_finite());
            assert!(numeric.mean.is_finite());
            assert!(numeric.p50.is_finite());
            assert!(numeric.p95.is_finite());
        }
    }

    #[test]
    fn native_numeric_rounding_normalizes_negative_zero() {
        let report = profile_values(&[json!({"score": -0.0000001})]);
        let numeric = report.fields["$[\"score\"]"]
            .numeric_stats
            .as_ref()
            .expect("numeric stats");

        assert_eq!(numeric.mean, 0.0);
        assert!(numeric.mean.is_sign_positive());
    }

    #[test]
    fn normalizes_qsv_profile_rows_to_profile_report() {
        let values = vec![
            json!({
                "field": "id",
                "type": "Integer",
                "nullcount": "1",
                "cardinality": "3",
                "record_count": "4",
                "min": "1",
                "max": "4",
                "mean": "2.3333333",
                "q2_median": "2",
                "p95": "4"
            }),
            json!({
                "field": "flag",
                "type": "String",
                "nullcount": "2",
                "cardinality": "2",
                "record_count": "4",
                "min": "",
                "max": "",
                "mean": "",
                "q2_median": "",
                "p95": ""
            }),
        ];

        let report = normalize_qsv_profile_rows(&values)
            .expect("qsv normalization")
            .expect("qsv profile rows should be detected");
        assert_eq!(report.record_count, 4);
        assert_eq!(report.field_count, 2);

        let id = report.fields.get("$[\"id\"]").expect("id field");
        assert_eq!(id.null_ratio, 0.25);
        assert_eq!(id.unique_count, 3);
        assert_eq!(id.type_distribution.null, 1);
        assert_eq!(id.type_distribution.number, 3);
        let numeric = id.numeric_stats.as_ref().expect("numeric stats");
        assert_eq!(numeric.count, 3);
        assert_eq!(numeric.min, 1.0);
        assert_eq!(numeric.max, 4.0);
        assert_eq!(numeric.mean, 2.333333);
        assert_eq!(numeric.p50, 2.0);
        assert_eq!(numeric.p95, 4.0);

        let flag = report.fields.get("$[\"flag\"]").expect("flag field");
        assert_eq!(flag.null_ratio, 0.5);
        assert_eq!(flag.unique_count, 2);
        assert_eq!(flag.type_distribution.null, 2);
        assert_eq!(flag.type_distribution.string, 2);
        assert!(flag.numeric_stats.is_none());
    }

    #[test]
    fn qsv_normalization_ignores_non_qsv_rows() {
        let values = vec![json!({"id": "1", "flag": "true"})];
        let normalized = normalize_qsv_profile_rows(&values).expect("normalization result");
        assert!(normalized.is_none());
    }

    #[test]
    fn qsv_normalization_reports_invalid_count_cells() {
        let values = vec![json!({
            "field": "id",
            "type": "Integer",
            "nullcount": "0",
            "cardinality": "x",
            "record_count": "2",
            "min": "1",
            "max": "2",
            "mean": "1.5",
            "q2_median": "1.5",
            "p95": "2"
        })];

        let error = normalize_qsv_profile_rows(&values).expect_err("invalid qsv rows");
        assert!(error.contains("invalid integer value"));
    }
}
