use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir, tempdir_in};

#[test]
fn scan_text_is_deterministic_for_unchanged_inputs() {
    let toolchain = FakeRgToolchain::new();
    let scan_root = tempdir_in(std::env::current_dir().expect("cwd")).expect("scan root");
    fs::create_dir_all(scan_root.path().join("sub")).expect("mkdir");

    let run_once = || {
        assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .env("DATAQ_RG_BIN", &toolchain.rg_bin)
            .args([
                "scan",
                "text",
                "--pattern",
                "stable",
                "--path",
                scan_root.path().to_str().expect("utf8 path"),
            ])
            .output()
            .expect("run scan")
    };

    let first = run_once();
    let second = run_once();

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);

    let payload: Value = serde_json::from_slice(&first.stdout).expect("stdout json");
    assert_eq!(payload["summary"]["total_matches"], json!(2));
    let matches = payload["matches"].as_array().expect("matches array");
    assert_eq!(matches[0]["line"], json!(2));
    assert_eq!(matches[1]["line"], json!(3));
}

struct FakeRgToolchain {
    _dir: TempDir,
    rg_bin: PathBuf,
}

impl FakeRgToolchain {
    fn new() -> Self {
        let dir = tempdir().expect("tempdir");
        let rg_bin = write_fake_rg_script(dir.path().join("rg"));
        Self { _dir: dir, rg_bin }
    }
}

fn write_fake_rg_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'ripgrep 14.1.1\n'
    exit 0
  fi
done

pattern=""
root=""
capture_pattern=0
capture_path=0
for arg in "$@"; do
  if [ "$capture_pattern" = "1" ]; then
    pattern="$arg"
    capture_pattern=0
    continue
  fi
  if [ "$capture_path" = "1" ]; then
    root="$arg"
    capture_path=0
    continue
  fi
  if [ "$arg" = "-e" ]; then
    capture_pattern=1
    continue
  fi
  if [ "$arg" = "--" ]; then
    capture_path=1
    continue
  fi
done

if [ -z "$pattern" ] || [ -z "$root" ]; then
  prev=""
  last=""
  for arg in "$@"; do
    prev="$last"
    last="$arg"
  done
  if [ -z "$pattern" ]; then
    pattern="$prev"
  fi
  if [ -z "$root" ]; then
    root="$last"
  fi
fi

if [ "$pattern" = "stable" ]; then
  printf '{"type":"match","data":{"path":{"text":"%s/sub/b.txt"},"lines":{"text":"beta\\n"},"line_number":3,"submatches":[{"match":{"text":"beta"},"start":0,"end":4}]}}\n' "$root"
  printf '{"type":"match","data":{"path":{"text":"%s/a.txt"},"lines":{"text":"alpha\\n"},"line_number":2,"submatches":[{"match":{"text":"alpha"},"start":0,"end":5}]}}\n' "$root"
  exit 0
fi

exit 1
"#;

    fs::write(&path, script).expect("write fake rg script");
    set_executable(&path);
    path
}

fn set_executable(path: &PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
