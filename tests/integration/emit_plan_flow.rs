use serde_json::Value;

#[test]
fn emit_plan_resolves_assert_normalize_stages_from_args_json() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "emit",
            "plan",
            "--command",
            "assert",
            "--args",
            r#"["--normalize","github-actions-jobs"]"#,
        ])
        .output()
        .expect("run emit plan");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let steps: Vec<String> = payload["stages"]
        .as_array()
        .expect("stages array")
        .iter()
        .map(|stage| stage["step"].as_str().expect("step string").to_string())
        .collect();

    assert_eq!(
        steps,
        vec![
            "load_rules",
            "resolve_input_format",
            "read_input_values",
            "normalize_assert_input",
            "validate_assert_rules",
        ]
    );
    assert_eq!(payload["tools"][0]["expected"], Value::from(true));
    assert_eq!(payload["tools"][1]["expected"], Value::from(true));
    assert_eq!(payload["tools"][2]["expected"], Value::from(true));
}

#[test]
fn emit_plan_rejects_unknown_command() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["emit", "plan", "--command", "not-a-command"])
        .output()
        .expect("run emit plan");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("message")
            .contains("unsupported emit plan command")
    );
}

#[test]
fn emit_plan_rejects_invalid_args_schema() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "emit",
            "plan",
            "--command",
            "canon",
            "--args",
            r#"{"not":"array"}"#,
        ])
        .output()
        .expect("run emit plan");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert_eq!(
        stderr_json["message"],
        Value::from("`--args` must be a JSON array of strings")
    );
}

#[test]
fn emit_plan_rejects_assigned_assert_help_values() {
    for invalid_arg in ["--rules-help=true", "--schema-help=true"] {
        let args_json = format!(r#"["{invalid_arg}"]"#);
        let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .args([
                "emit",
                "plan",
                "--command",
                "assert",
                "--args",
                args_json.as_str(),
            ])
            .output()
            .expect("run emit plan");

        assert_eq!(output.status.code(), Some(3));
        assert!(output.stdout.is_empty());
        let stderr_json = parse_last_stderr_json(&output.stderr);
        assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
        assert!(
            stderr_json["message"]
                .as_str()
                .expect("message")
                .contains("does not take a value")
        );
        assert!(
            stderr_json["message"]
                .as_str()
                .expect("message")
                .contains(invalid_arg)
        );
    }
}

fn parse_last_stderr_json(stderr: &[u8]) -> Value {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    let line = text
        .lines()
        .rev()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stderr line");
    serde_json::from_str(line).expect("stderr json")
}
