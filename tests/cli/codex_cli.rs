use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tempfile::tempdir;

const EXPECTED_SKILL_MD: &str = include_str!("../../.agents/skills/dataq/SKILL.md");
const EXPECTED_OPENAI_YAML: &str = include_str!("../../.agents/skills/dataq/agents/openai.yaml");

#[test]
fn codex_install_skill_succeeds_with_explicit_destination() {
    let dir = tempdir().expect("tempdir");
    let destination_root = dir.path().join("skills-root");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "codex",
            "install-skill",
            "--dest",
            destination_root.to_str().expect("utf8 destination root"),
        ])
        .output()
        .expect("run codex install-skill");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let destination_path = destination_root.join("dataq");
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(
        payload,
        json!({
            "schema": "dataq.codex.install_skill.output.v1",
            "skill_name": "dataq",
            "destination": destination_path.display().to_string(),
            "copied_files": ["SKILL.md", "agents/openai.yaml"],
            "overwrite": false,
        })
    );

    assert_eq!(
        collect_relative_files(destination_path.as_path()),
        vec!["SKILL.md".to_string(), "agents/openai.yaml".to_string()]
    );
    assert_eq!(
        fs::read_to_string(destination_path.join("SKILL.md")).expect("read SKILL.md"),
        EXPECTED_SKILL_MD
    );
    assert_eq!(
        fs::read_to_string(destination_path.join("agents/openai.yaml"))
            .expect("read agents/openai.yaml"),
        EXPECTED_OPENAI_YAML
    );
}

#[test]
fn codex_install_skill_existing_directory_without_force_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let destination_path = dir.path().join("skills").join("dataq");
    fs::create_dir_all(destination_path.as_path()).expect("create existing skill directory");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "codex",
            "install-skill",
            "--dest",
            dir.path()
                .join("skills")
                .to_str()
                .expect("utf8 destination root"),
        ])
        .output()
        .expect("run codex install-skill");

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());

    let stderr_payload = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_payload["error"], Value::from("input_usage_error"));
    assert!(
        stderr_payload["message"]
            .as_str()
            .expect("error message")
            .contains("already exists")
    );
}

#[test]
fn codex_install_skill_force_overwrites_existing_directory() {
    let dir = tempdir().expect("tempdir");
    let destination_root = dir.path().join("skills");
    let destination_path = destination_root.join("dataq");
    fs::create_dir_all(destination_path.join("agents")).expect("create agents directory");
    fs::write(destination_path.join("SKILL.md"), "stale skill").expect("write stale skill");
    fs::write(destination_path.join("agents/openai.yaml"), "stale config")
        .expect("write stale config");
    fs::write(destination_path.join("stale.txt"), "stale file").expect("write stale file");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "codex",
            "install-skill",
            "--dest",
            destination_root.to_str().expect("utf8 destination root"),
            "--force",
        ])
        .output()
        .expect("run codex install-skill");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["overwrite"], Value::from(true));
    assert!(!destination_path.join("stale.txt").exists());
    assert_eq!(
        fs::read_to_string(destination_path.join("SKILL.md")).expect("read SKILL.md"),
        EXPECTED_SKILL_MD
    );
    assert_eq!(
        fs::read_to_string(destination_path.join("agents/openai.yaml"))
            .expect("read agents/openai.yaml"),
        EXPECTED_OPENAI_YAML
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

fn collect_relative_files(root: &Path) -> Vec<String> {
    let mut collected = Vec::new();
    collect_relative_files_rec(root, root, &mut collected);
    collected.sort();
    collected
}

fn collect_relative_files_rec(root: &Path, directory: &Path, out: &mut Vec<String>) {
    let mut entries: Vec<PathBuf> = fs::read_dir(directory)
        .expect("read directory")
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            collect_relative_files_rec(root, path.as_path(), out);
        } else {
            let relative = path
                .strip_prefix(root)
                .expect("strip root prefix")
                .components()
                .map(|component| component.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<String>>()
                .join("/");
            out.push(relative);
        }
    }
}
