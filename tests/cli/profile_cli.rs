use predicates::prelude::predicate;
use serde_json::json;

const QSV_20_1_EVERYTHING_MIXED: &str =
    include_str!("../fixtures/input/profile_qsv_20_1_everything_mixed.csv");
const QSV_20_1_EVERYTHING_ALL_STRING: &str =
    include_str!("../fixtures/input/profile_qsv_20_1_everything_all_string.csv");
const QSV_20_1_EVERYTHING_NAN_SINGLE: &str =
    include_str!("../fixtures/input/profile_qsv_20_1_everything_nan_single.csv");
const QSV_20_1_EVERYTHING_NAN_MIXED: &str =
    include_str!("../fixtures/input/profile_qsv_20_1_everything_nan_mixed.csv");

#[test]
fn profile_command_returns_expected_json_for_json_input() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin(r#"[{"id":1,"active":true},{"id":null}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    let expected = json!({
        "record_count": 2,
        "field_count": 2,
        "fields": {
            "$[\"active\"]": {
                "null_ratio": 0.5,
                "unique_count": 2,
                "type_distribution": {
                    "null": 1,
                    "boolean": 1,
                    "number": 0,
                    "string": 0,
                    "array": 0,
                    "object": 0
                },
                "numeric_stats": null
            },
            "$[\"id\"]": {
                "null_ratio": 0.5,
                "unique_count": 2,
                "type_distribution": {
                    "null": 1,
                    "boolean": 0,
                    "number": 1,
                    "string": 0,
                    "array": 0,
                    "object": 0
                },
                "numeric_stats": {
                    "count": 1,
                    "min": 1.0,
                    "max": 1.0,
                    "mean": 1.0,
                    "p50": 1.0,
                    "p95": 1.0
                }
            }
        }
    });
    let mut expected = expected;
    expected["fields"]["$[\"active\"]"]
        .as_object_mut()
        .expect("active field object")
        .remove("numeric_stats");
    assert_eq!(actual, expected);
}

#[test]
fn profile_command_numeric_stats_respect_null_mixing_and_non_numeric_fields() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin(r#"[{"score":1,"label":"a"},{"score":null,"label":"b"},{"label":null}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");

    assert_eq!(
        actual["fields"]["$[\"score\"]"]["type_distribution"]["number"],
        json!(1)
    );
    assert_eq!(
        actual["fields"]["$[\"score\"]"]["type_distribution"]["null"],
        json!(2)
    );
    assert_eq!(
        actual["fields"]["$[\"score\"]"]["null_ratio"],
        json!(2.0 / 3.0)
    );
    assert_eq!(
        actual["fields"]["$[\"score\"]"]["numeric_stats"]["count"],
        json!(1)
    );

    assert_eq!(
        actual["fields"]["$[\"label\"]"]["type_distribution"]["string"],
        json!(2)
    );
    assert_eq!(
        actual["fields"]["$[\"label\"]"]["type_distribution"]["null"],
        json!(1)
    );
    assert_eq!(
        actual["fields"]["$[\"label\"]"]["numeric_stats"],
        json!(null)
    );
}

#[test]
fn profile_command_keeps_large_finite_numeric_stats_as_json_numbers() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin(r#"[{"score":1e308},{"score":1e308}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    let numeric = &actual["fields"]["$[\"score\"]"]["numeric_stats"];
    for name in ["min", "max", "mean", "p50", "p95"] {
        assert_eq!(numeric[name].as_f64(), Some(1e308), "statistic {name}");
    }
}

#[test]
fn profile_command_rejects_unrepresentable_json_numbers() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin(r#"[{"score":1e400}]"#)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("number out of range"));
}

#[test]
fn profile_command_csv_type_distribution_is_stable() {
    let input = "id,flag\n1,true\n2,\n3,false\n";
    let first_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let second_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    assert_eq!(first_output, second_output);

    let actual: serde_json::Value =
        serde_json::from_slice(&first_output).expect("parse profile output");
    assert_eq!(
        actual["fields"]["$[\"flag\"]"]["type_distribution"]["string"],
        json!(3)
    );
    assert_eq!(
        actual["fields"]["$[\"id\"]"]["type_distribution"]["string"],
        json!(3)
    );
}

#[test]
fn profile_command_normalizes_qsv_csv_rows() {
    let qsv_rows = "field,type,nullcount,cardinality,record_count,min,max,mean,q2_median,p95\n\
id,Integer,1,3,4,1,4,2.333333,2,4\n\
flag,String,2,2,4,,,,,\n";

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(qsv_rows)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["record_count"], json!(4));
    assert_eq!(actual["field_count"], json!(2));
    assert_eq!(actual["fields"]["$[\"id\"]"]["null_ratio"], json!(0.25));
    assert_eq!(
        actual["fields"]["$[\"id\"]"]["type_distribution"]["number"],
        json!(3)
    );
    assert_eq!(
        actual["fields"]["$[\"id\"]"]["numeric_stats"]["mean"],
        json!(2.333333)
    );
    assert_eq!(actual["fields"]["$[\"flag\"]"]["null_ratio"], json!(0.5));
    assert_eq!(
        actual["fields"]["$[\"flag\"]"]["type_distribution"]["string"],
        json!(2)
    );
}

#[test]
fn profile_command_normalizes_real_qsv_20_1_mixed_rows() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(QSV_20_1_EVERYTHING_MIXED)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["record_count"], json!(4));
    assert_eq!(actual["field_count"], json!(4));
    assert_eq!(actual["fields"]["$[\"name\"]"]["null_ratio"], json!(0.25));
    assert_eq!(
        actual["fields"]["$[\"name\"]"]["type_distribution"]["string"],
        json!(3)
    );
    assert_eq!(
        actual["fields"]["$[\"name\"]"]["type_distribution"]["null"],
        json!(1)
    );
    assert_eq!(
        actual["fields"]["$[\"id\"]"]["numeric_stats"]["count"],
        json!(4)
    );
    assert_eq!(
        actual["fields"]["$[\"score\"]"]["numeric_stats"]["count"],
        json!(3)
    );
    assert_eq!(
        actual["fields"]["$[\"status\"]"]["type_distribution"]["string"],
        json!(4)
    );
    assert_eq!(
        actual["fields"]["$[\"status\"]"]["type_distribution"]["null"],
        json!(0)
    );
    assert_eq!(actual["fields"]["$[\"status\"]"]["null_ratio"], json!(0.0));
}

#[test]
fn profile_command_rejects_ambiguous_real_qsv_20_1_all_string_rows() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(QSV_20_1_EVERYTHING_ALL_STRING)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("exact dataset `record_count`"))
        .stderr(predicate::str::contains(
            "`record_count`, `records`, `rows`, `row_count`, or `total_rows`",
        ));
}

#[test]
fn profile_command_rejects_real_qsv_float_nan_signed_only_count() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(QSV_20_1_EVERYTHING_NAN_SINGLE)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("Float signed counters"));
}

#[test]
fn profile_command_uses_integer_count_for_real_qsv_float_nan_field() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv"])
        .write_stdin(QSV_20_1_EVERYTHING_NAN_MIXED)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["record_count"], json!(4));
    assert_eq!(actual["field_count"], json!(2));
    assert_eq!(
        actual["fields"]["$[\"value\"]"]["type_distribution"]["number"],
        json!(4)
    );
    assert_eq!(
        actual["fields"]["$[\"value\"]"]["type_distribution"]["null"],
        json!(0)
    );
    assert_eq!(actual["fields"]["$[\"value\"]"]["null_ratio"], json!(0.0));
}

#[test]
fn profile_command_projects_requested_fields_in_canonical_order() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--field",
            "name",
            "--field",
            "$[\"nested\"][\"score\"]",
            "--field",
            "name",
        ])
        .write_stdin(
            r#"[{"id":1,"name":"a","nested":{"score":2}},{"id":2,"name":"b","nested":{"score":4}}]"#,
        )
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["field_count"], json!(4));
    assert_eq!(actual["returned_field_count"], json!(2));
    assert_eq!(
        actual["fields"]
            .as_object()
            .expect("fields object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["$[\"name\"]", "$[\"nested\"][\"score\"]"]
    );
    assert!(actual.get("missing_fields").is_none());
}

#[test]
fn profile_command_missing_projection_field_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json", "--field", "missing"])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("$[\\\"missing\\\"]"));
}

#[test]
fn profile_command_allow_missing_projection_returns_present_fields_and_missing_list() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--field",
            "missing",
            "--field",
            "id",
            "--field",
            "$[\"missing\"]",
            "--allow-missing-fields",
        ])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["field_count"], json!(1));
    assert_eq!(actual["returned_field_count"], json!(1));
    assert_eq!(actual["missing_fields"], json!(["$[\"missing\"]"]));
    assert_eq!(
        actual["fields"]
            .as_object()
            .expect("fields object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["$[\"id\"]"]
    );
}

#[test]
fn profile_command_rejects_empty_or_invalid_projection_field() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--field",
            "",
            "--allow-missing-fields",
        ])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--field",
            "$.id",
            "--allow-missing-fields",
        ])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn profile_command_projects_qsv_normalized_profile_rows() {
    let qsv_rows = "field,type,nullcount,cardinality,record_count,min,max,mean,q2_median,p95\n\
id,Integer,1,3,4,1,4,2.333333,2,4\n\
flag,String,2,2,4,,,,,\n";

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv", "--field", "flag"])
        .write_stdin(qsv_rows)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["field_count"], json!(2));
    assert_eq!(actual["returned_field_count"], json!(1));
    assert_eq!(actual["fields"]["$[\"flag\"]"]["null_ratio"], json!(0.5));
    assert!(actual["fields"].get("$[\"id\"]").is_none());
}

#[test]
fn profile_command_preserves_projection_and_brief_for_real_qsv_rows() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "csv", "--field", "name", "--brief"])
        .write_stdin(QSV_20_1_EVERYTHING_MIXED)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["record_count"], json!(4));
    assert_eq!(actual["field_count"], json!(4));
    assert_eq!(actual["fields"].as_array().expect("fields array").len(), 1);
    assert_eq!(actual["fields"][0]["path"], json!("$[\"name\"]"));
    assert_eq!(actual["fields"][0]["null_ratio"], json!(0.25));
    assert_eq!(actual["fields"][0]["dominant_type"], json!("string"));
    assert_eq!(actual["fields"][0]["numeric"], json!(null));
}

#[test]
fn profile_command_brief_returns_compact_field_array() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json", "--brief"])
        .write_stdin(r#"[{"id":1,"active":true},{"id":2,"active":null}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["record_count"], json!(2));
    assert_eq!(actual["field_count"], json!(2));
    assert_eq!(actual["truncated"], json!(false));
    assert!(actual["fields"].as_array().expect("fields array").len() == 2);
    for field in actual["fields"].as_array().expect("fields array") {
        assert!(field.get("path").is_some());
        assert!(field.get("null_ratio").is_some());
        assert!(field.get("unique_count").is_some());
        assert!(field.get("dominant_type").is_some());
        assert!(field.get("numeric").is_some());
        assert!(field.get("type_distribution").is_none());
        assert!(field.get("numeric_stats").is_none());
    }
    assert_eq!(actual["fields"][0]["path"], json!("$[\"active\"]"));
    assert_eq!(actual["fields"][0]["dominant_type"], json!("boolean"));
    assert_eq!(actual["fields"][0]["numeric"], json!(null));
    assert_eq!(actual["fields"][1]["path"], json!("$[\"id\"]"));
    assert_eq!(actual["fields"][1]["dominant_type"], json!("number"));
    assert_eq!(actual["fields"][1]["numeric"]["count"], json!(2));
}

#[test]
fn profile_command_brief_default_path_order_is_deterministic() {
    let input = r#"[{"z":1,"a":1,"nested":{"b":1}}]"#;
    let first_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json", "--brief"])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();
    let second_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json", "--brief"])
        .write_stdin(input)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    assert_eq!(first_output, second_output);
    let actual: serde_json::Value =
        serde_json::from_slice(&first_output).expect("parse profile output");
    let paths = brief_paths(&actual);
    assert_eq!(
        paths,
        vec![
            "$[\"a\"]",
            "$[\"nested\"]",
            "$[\"nested\"][\"b\"]",
            "$[\"z\"]"
        ]
    );
}

#[test]
fn profile_command_brief_unique_count_sort_and_max_fields_truncate() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--brief",
            "--sort-fields",
            "unique_count",
            "--max-fields",
            "1",
        ])
        .write_stdin(r#"[{"a":1,"b":1},{"a":2,"b":1},{"a":3,"b":null}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["truncated"], json!(true));
    assert_eq!(brief_paths(&actual), vec!["$[\"a\"]"]);
    assert_eq!(actual["fields"][0]["unique_count"], json!(3));
}

#[test]
fn profile_command_brief_null_ratio_sort_ties_by_path() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--brief",
            "--sort-fields",
            "null_ratio",
        ])
        .write_stdin(r#"[{"a":null,"b":1,"c":null},{"a":1,"b":null}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(
        brief_paths(&actual),
        vec!["$[\"c\"]", "$[\"a\"]", "$[\"b\"]"]
    );
    assert_eq!(actual["fields"][0]["null_ratio"], json!(1.0));
    assert_eq!(actual["fields"][0]["dominant_type"], json!("null"));
}

#[test]
fn profile_command_brief_max_fields_zero_returns_empty_fields_and_truncated() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json", "--brief", "--max-fields", "0"])
        .write_stdin(r#"[{"id":1}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["field_count"], json!(1));
    assert_eq!(actual["truncated"], json!(true));
    assert_eq!(actual["fields"], json!([]));
}

#[test]
fn profile_command_brief_applies_projection_before_sort_and_preserves_missing_metadata() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "profile",
            "--from",
            "json",
            "--brief",
            "--field",
            "missing",
            "--field",
            "id",
            "--allow-missing-fields",
        ])
        .write_stdin(r#"[{"id":1,"name":"a"}]"#)
        .assert()
        .code(0)
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&output).expect("parse profile output");
    assert_eq!(actual["field_count"], json!(2));
    assert_eq!(actual["missing_fields"], json!(["$[\"missing\"]"]));
    assert_eq!(brief_paths(&actual), vec!["$[\"id\"]"]);
}

#[test]
fn profile_command_emit_pipeline_reports_qsv_stage_diagnostics() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--emit-pipeline", "--from", "csv"])
        .write_stdin(QSV_20_1_EVERYTHING_MIXED)
        .output()
        .expect("run profile");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    let qsv_tool = stderr_json["external_tools"]
        .as_array()
        .expect("external tools")
        .iter()
        .find(|entry| entry["name"] == json!("qsv"))
        .expect("qsv tool entry");
    assert_eq!(qsv_tool["used"], json!(true));

    let stage = stderr_json["stage_diagnostics"]
        .as_array()
        .expect("stage diagnostics")
        .first()
        .expect("qsv stage");
    assert_eq!(stage["step"], json!("profile_qsv_normalize"));
    assert_eq!(stage["tool"], json!("qsv"));
    assert_eq!(stage["status"], json!("ok"));
}

#[test]
fn profile_command_invalid_input_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin("{")
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

fn parse_last_stderr_json(stderr: &[u8]) -> serde_json::Value {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    let line = text
        .lines()
        .rev()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stderr line");
    serde_json::from_str(line).expect("stderr json")
}

fn brief_paths(value: &serde_json::Value) -> Vec<&str> {
    value["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|field| field["path"].as_str().expect("path string"))
        .collect()
}
