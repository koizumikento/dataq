use serde_json::Value;

#[test]
fn emit_plan_known_command_returns_stage_plan() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["emit", "plan", "--command", "join"])
        .output()
        .expect("run emit plan");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], Value::from("join"));
    assert_eq!(
        payload["stages"][0]["step"],
        Value::from("resolve_join_inputs")
    );
    assert_eq!(payload["stages"][0]["depends_on"], Value::Array(Vec::new()));
    assert_eq!(
        payload["stages"][3]["step"],
        Value::from("execute_join_with_mlr")
    );
    assert_eq!(payload["stages"][3]["tool"], Value::from("mlr"));
    assert_eq!(
        payload["stages"][3]["depends_on"],
        Value::Array(vec![Value::from("validate_join_keys")])
    );
    assert_eq!(payload["tools"][2]["name"], Value::from("mlr"));
    assert_eq!(payload["tools"][2]["expected"], Value::from(true));
}

#[test]
fn emit_plan_unknown_command_returns_exit_three() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["emit", "plan", "--command", "unknown"])
        .output()
        .expect("run emit plan");

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());

    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("error message")
            .contains("unsupported emit plan command")
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
                .expect("error message")
                .contains("does not take a value")
        );
        assert!(
            stderr_json["message"]
                .as_str()
                .expect("error message")
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
