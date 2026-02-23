use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;
use serde_json::{Value, json};

#[cfg(windows)]
use std::ffi::OsString;

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DoctorCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details from doctor probe execution used by pipeline fingerprinting.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DoctorPipelineTrace {
    pub tool_versions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DoctorToolReport {
    name: String,
    found: bool,
    version: Option<String>,
    executable: bool,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolSpec {
    name: &'static str,
    install_hint: &'static str,
}

const TOOL_SPECS: [ToolSpec; 3] = [
    ToolSpec {
        name: "jq",
        install_hint: "Install `jq` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "yq",
        install_hint: "Install `yq` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "mlr",
        install_hint: "Install `mlr` and ensure it is available in PATH.",
    },
];

pub fn run() -> DoctorCommandResponse {
    run_with_trace().0
}

pub fn run_with_trace() -> (DoctorCommandResponse, DoctorPipelineTrace) {
    let reports: Vec<DoctorToolReport> = TOOL_SPECS.iter().map(diagnose_tool).collect();
    let all_executable = reports.iter().all(|report| report.executable);
    let tool_versions = reports
        .iter()
        .filter_map(|report| {
            report
                .version
                .as_ref()
                .map(|version| (report.name.clone(), version.clone()))
        })
        .collect();

    (
        DoctorCommandResponse {
            exit_code: if all_executable { 0 } else { 3 },
            payload: json!({
                "tools": reports,
            }),
        },
        DoctorPipelineTrace { tool_versions },
    )
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "doctor_probe_jq".to_string(),
        "doctor_probe_yq".to_string(),
        "doctor_probe_mlr".to_string(),
    ]
}

/// Determinism guards planned for the `doctor` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "fixed_tool_probe_order_jq_yq_mlr".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ]
}

fn diagnose_tool(spec: &ToolSpec) -> DoctorToolReport {
    let found_path = find_in_path(OsStr::new(spec.name));
    if found_path.is_none() {
        return DoctorToolReport {
            name: spec.name.to_string(),
            found: false,
            version: None,
            executable: false,
            message: format!(
                "`{}` was not found in PATH. {}",
                spec.name, spec.install_hint
            ),
        };
    }

    match Command::new(spec.name).arg("--version").output() {
        Ok(output) => {
            if output.status.success() {
                let version = capture_version(&output.stdout, &output.stderr);
                DoctorToolReport {
                    name: spec.name.to_string(),
                    found: true,
                    version,
                    executable: true,
                    message: "ok".to_string(),
                }
            } else {
                DoctorToolReport {
                    name: spec.name.to_string(),
                    found: true,
                    version: capture_version(&output.stdout, &output.stderr),
                    executable: false,
                    message: format!(
                        "`{} --version` returned non-zero status ({}). Reinstall or repair the tool. {}",
                        spec.name,
                        status_label(output.status.code()),
                        spec.install_hint,
                    ),
                }
            }
        }
        Err(error) => DoctorToolReport {
            name: spec.name.to_string(),
            found: true,
            version: None,
            executable: false,
            message: match error.kind() {
                std::io::ErrorKind::PermissionDenied => format!(
                    "`{}` exists in PATH but is not executable. Fix file permissions. {}",
                    spec.name, spec.install_hint
                ),
                _ => format!(
                    "failed to execute `{}`: {}. {}",
                    spec.name, error, spec.install_hint
                ),
            },
        },
    }
}

fn capture_version(stdout: &[u8], stderr: &[u8]) -> Option<String> {
    let value = first_non_empty_line(stdout).or_else(|| first_non_empty_line(stderr));
    value.map(ToOwned::to_owned)
}

fn first_non_empty_line(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines().find(|line| !line.trim().is_empty())
}

fn status_label(code: Option<i32>) -> String {
    code.map(|value| value.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string())
}

fn find_in_path(tool: &OsStr) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    env::split_paths(&path_var)
        .flat_map(|directory| candidate_paths(&directory, tool))
        .find(|candidate| candidate.is_file())
}

fn candidate_paths(directory: &Path, tool: &OsStr) -> Vec<PathBuf> {
    #[cfg(windows)]
    let mut candidates = vec![directory.join(tool)];
    #[cfg(not(windows))]
    let candidates = vec![directory.join(tool)];

    #[cfg(windows)]
    {
        if Path::new(tool).extension().is_none() {
            let pathext =
                env::var_os("PATHEXT").unwrap_or_else(|| OsString::from(".COM;.EXE;.BAT;.CMD"));
            for ext in env::split_paths(&pathext) {
                let ext_str = ext.to_string_lossy();
                if ext_str.is_empty() {
                    continue;
                }
                let mut name = OsString::from(tool);
                name.push(ext_str.as_ref());
                candidates.push(directory.join(name));
            }
        }
    }

    candidates
}
