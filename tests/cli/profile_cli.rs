use predicates::prelude::predicate;
use serde_json::json;

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
fn profile_command_emit_pipeline_reports_qsv_stage_diagnostics() {
    let qsv_rows = "field,type,nullcount,cardinality,record_count,min,max,mean,q2_median,p95\n\
id,Integer,1,3,4,1,4,2.333333,2,4\n\
flag,String,2,2,4,,,,,\n";

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--emit-pipeline", "--from", "csv"])
        .write_stdin(qsv_rows)
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
