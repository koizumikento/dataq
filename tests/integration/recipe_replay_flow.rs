use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use dataq::util::hash::DeterministicHasher;
use dataq::util::sort::sort_value_keys;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

#[test]
fn recipe_replay_non_strict_keeps_lock_mismatch_and_returns_step_validation_exit_two() {
    let dir = tempdir().expect("tempdir");
    let toolchain = FakeToolchain::new("jq-1.7", "yq 4.35.2", "mlr 6.13.0");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.json");
    let lock_path = dir.path().join("recipe.lock.json");

    fs::write(&input_path, r#"[{"id":"oops"}]"#).expect("write input");
    let recipe_value = json!({
      "version": "dataq.recipe.v1",
      "steps": [
        {
          "kind": "canon",
          "args": {
            "input": input_path.display().to_string(),
            "from": "json"
          }
        },
        {
          "kind": "assert",
          "args": {
            "rules": {
              "required_keys": ["id"],
              "fields": {
                "id": {"type": "integer"}
              }
            }
          }
        }
      ]
    });
    fs::write(
        &recipe_path,
        serde_json::to_vec(&recipe_value).expect("serialize recipe"),
    )
    .expect("write recipe");
    write_recipe_lock(
        &lock_path,
        &recipe_value,
        BTreeMap::from([
            ("jq".to_string(), "jq-1.7".to_string()),
            ("mlr".to_string(), "mlr 6.13.0".to_string()),
            ("yq".to_string(), "yq 0.0.0".to_string()),
        ]),
    );

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", &toolchain.jq_bin)
        .env("DATAQ_YQ_BIN", &toolchain.yq_bin)
        .env("DATAQ_MLR_BIN", &toolchain.mlr_bin)
        .args([
            "recipe",
            "replay",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
            "--lock",
            lock_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run replay");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let summary: Value = serde_json::from_slice(&output.stdout).expect("summary json");
    assert_eq!(summary["lock_check"]["matched"], Value::Bool(false));
    assert_eq!(summary["exit_code"], Value::from(2));
    assert_eq!(summary["steps"][1]["kind"], Value::from("assert"));
    assert_eq!(summary["steps"][1]["matched"], Value::Bool(false));
}

fn write_recipe_lock(lock_path: &PathBuf, recipe: &Value, tool_versions: BTreeMap<String, String>) {
    let lock_value = json!({
      "version": "dataq.recipe.lock.v1",
      "command_graph_hash": hash_recipe_command_graph(recipe),
      "args_hash": hash_recipe_args(recipe),
      "tool_versions": tool_versions,
      "dataq_version": env!("CARGO_PKG_VERSION")
    });
    fs::write(
        lock_path,
        serde_json::to_vec(&lock_value).expect("serialize lock"),
    )
    .expect("write lock");
}

fn hash_recipe_command_graph(recipe: &Value) -> String {
    let version = recipe["version"].as_str().expect("recipe version");
    let steps = recipe["steps"].as_array().expect("recipe steps array");

    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.recipe.lock.command_graph.v1");
    hasher.update_len_prefixed(version.as_bytes());
    for (index, step) in steps.iter().enumerate() {
        let kind = step["kind"].as_str().expect("step kind");
        hasher.update_len_prefixed(index.to_string().as_bytes());
        hasher.update_len_prefixed(kind.as_bytes());
    }
    hasher.finish_hex()
}

fn hash_recipe_args(recipe: &Value) -> String {
    let steps = recipe["steps"].as_array().expect("recipe steps array");

    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.recipe.lock.args.v1");
    for (index, step) in steps.iter().enumerate() {
        let kind = step["kind"].as_str().expect("step kind");
        let args = step["args"].as_object().expect("step args object").clone();
        let sorted_args = sort_value_keys(&Value::Object(args));
        let serialized = serde_json::to_vec(&sorted_args).expect("serialize args");
        hasher.update_len_prefixed(index.to_string().as_bytes());
        hasher.update_len_prefixed(kind.as_bytes());
        hasher.update_len_prefixed(serialized.as_slice());
    }
    hasher.finish_hex()
}

struct FakeToolchain {
    _dir: TempDir,
    jq_bin: PathBuf,
    yq_bin: PathBuf,
    mlr_bin: PathBuf,
}

impl FakeToolchain {
    fn new(jq_version: &str, yq_version: &str, mlr_version: &str) -> Self {
        let dir = tempdir().expect("tempdir");
        let jq_bin = write_fake_version_script(dir.path().join("jq"), jq_version);
        let yq_bin = write_fake_version_script(dir.path().join("yq"), yq_version);
        let mlr_bin = write_fake_version_script(dir.path().join("mlr"), mlr_version);
        Self {
            _dir: dir,
            jq_bin,
            yq_bin,
            mlr_bin,
        }
    }
}

fn write_fake_version_script(path: PathBuf, version: &str) -> PathBuf {
    fs::write(
        &path,
        format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\n",
            version.replace('\'', "")
        ),
    )
    .expect("write version script");
    set_executable(&path);
    path
}

fn set_executable(path: &PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("set permissions");
    }
}
