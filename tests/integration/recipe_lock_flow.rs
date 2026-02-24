use std::fs;
use std::path::Path;

use tempfile::tempdir;

#[test]
fn recipe_lock_flow_is_byte_identical_for_repeated_runs() {
    let dir = tempdir().expect("tempdir");
    let bin_dir = dir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin dir");
    write_exec_script(&bin_dir.join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&bin_dir.join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");
    write_exec_script(&bin_dir.join("mlr"), "#!/bin/sh\necho 'mlr-6.13.0'\n");

    let recipe_path = dir.path().join("recipe.json");
    let lock_path = dir.path().join("recipe.lock.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 recipe path"),
            "--out",
            lock_path.to_str().expect("utf8 lock path"),
        ])
        .output()
        .expect("run first lock");
    let first_bytes = fs::read(&lock_path).expect("read first lock");

    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 recipe path"),
            "--out",
            lock_path.to_str().expect("utf8 lock path"),
        ])
        .output()
        .expect("run second lock");
    let second_bytes = fs::read(&lock_path).expect("read second lock");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert!(first.stdout.is_empty());
    assert!(second.stdout.is_empty());
    assert_eq!(first_bytes, second_bytes);
}

fn write_exec_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
