use predicates::prelude::predicate;

#[test]
fn schema_help_lists_infer_subcommand() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["schema", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("infer"));
}

#[test]
fn schema_infer_missing_input_path_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "schema",
            "infer",
            "--input",
            "/definitely-missing/dataq-schema-input.csv",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}
