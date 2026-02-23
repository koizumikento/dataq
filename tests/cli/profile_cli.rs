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
                }
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
                }
            }
        }
    });
    assert_eq!(actual, expected);
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
fn profile_command_invalid_input_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["profile", "--from", "json"])
        .write_stdin("{")
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}
