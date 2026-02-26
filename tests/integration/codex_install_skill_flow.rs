use std::fs;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn codex_install_skill_resolves_default_root_from_codex_home_and_emits_stable_payload() {
    let codex_home = tempdir().expect("codex home tempdir");
    let home = tempdir().expect("home tempdir");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("CODEX_HOME", codex_home.path())
        .env("HOME", home.path())
        .args(["--emit-pipeline", "codex", "install-skill"])
        .output()
        .expect("run codex install-skill");

    assert_eq!(output.status.code(), Some(0));
    let destination = codex_home.path().join("skills").join("dataq");
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(
        payload,
        json!({
            "schema": "dataq.codex.install_skill.output.v1",
            "skill_name": "dataq",
            "destination": destination.display().to_string(),
            "copied_files": ["SKILL.md", "agents/openai.yaml"],
            "overwrite": false,
        })
    );

    assert_eq!(
        fs::read_to_string(destination.join("SKILL.md")).expect("read SKILL.md"),
        include_str!("../../.agents/skills/dataq/SKILL.md")
    );
    assert_eq!(
        fs::read_to_string(destination.join("agents/openai.yaml")).expect("read openai.yaml"),
        include_str!("../../.agents/skills/dataq/agents/openai.yaml")
    );

    let pipeline = parse_last_stderr_json(&output.stderr);
    assert_eq!(pipeline["command"], Value::from("codex.install-skill"));
    assert_eq!(
        pipeline["steps"],
        json!([
            "resolve_codex_skill_root",
            "prepare_codex_skill_destination",
            "write_embedded_codex_skill_files",
            "emit_codex_install_skill_output",
        ])
    );
    assert_eq!(
        pipeline["deterministic_guards"],
        json!([
            "rust_native_fs_execution",
            "compile_time_embedded_skill_assets",
            "fixed_embedded_asset_write_order",
        ])
    );
    assert_eq!(
        pipeline["input"]["sources"][0]["path"],
        Value::from(codex_home.path().join("skills").display().to_string())
    );
    assert_eq!(
        pipeline["input"]["sources"][1]["path"],
        Value::from(destination.display().to_string())
    );
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
