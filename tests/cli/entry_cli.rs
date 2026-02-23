use predicates::prelude::predicate;

#[test]
fn help_is_available() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--input"))
        .stdout(predicate::str::contains("--normalize-time"));
}

#[test]
fn version_is_available() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn returns_unimplemented_with_explicit_formats() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["--from", "json", "--to", "jsonl"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "\"error\":\"command_not_implemented\"",
        ))
        .stderr(predicate::str::contains(
            "\"resolved_input_format\":\"json\"",
        ))
        .stderr(predicate::str::contains(
            "\"resolved_output_format\":\"jsonl\"",
        ));
}

#[test]
fn resolves_formats_from_extension_when_not_explicit() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["--input", "sample.json", "--output", "sample.ndjson"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "\"error\":\"command_not_implemented\"",
        ))
        .stderr(predicate::str::contains(
            "\"resolved_input_format\":\"json\"",
        ))
        .stderr(predicate::str::contains(
            "\"resolved_output_format\":\"jsonl\"",
        ));
}

#[test]
fn reports_format_resolution_error_for_unknown_extension() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["--input", "sample.unknown", "--to", "json"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "\"error\":\"format_resolution_error\"",
        ))
        .stderr(predicate::str::contains("\"kind\":\"input\""));
}
