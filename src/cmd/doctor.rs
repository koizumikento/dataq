use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

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

/// Command arguments for `doctor`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DoctorCommandArgs {
    pub capabilities: bool,
    pub profile: Option<String>,
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DoctorCapabilityReport {
    name: String,
    tool: String,
    available: bool,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolSpec {
    name: &'static str,
    install_hint: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CapabilitySpec {
    name: &'static str,
    tool: &'static str,
    probe_args: &'static [&'static str],
    probe_stdin: Option<&'static str>,
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

const CAPABILITY_SPECS: [CapabilitySpec; 3] = [
    CapabilitySpec {
        name: "jq.null_input_eval",
        tool: "jq",
        probe_args: &["-n", "."],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "yq.null_input_eval",
        tool: "yq",
        probe_args: &["--null-input", "."],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "mlr.help_command",
        tool: "mlr",
        probe_args: &["--help"],
        probe_stdin: None,
    },
];

pub fn run() -> DoctorCommandResponse {
    run_with_trace().0
}

pub fn run_with_trace() -> (DoctorCommandResponse, DoctorPipelineTrace) {
    run_with_args_and_trace(&DoctorCommandArgs::default())
}

pub fn run_with_args(args: &DoctorCommandArgs) -> DoctorCommandResponse {
    run_with_args_and_trace(args).0
}

pub fn run_with_args_and_trace(
    args: &DoctorCommandArgs,
) -> (DoctorCommandResponse, DoctorPipelineTrace) {
    let reports: Vec<DoctorToolReport> = TOOL_SPECS.iter().map(diagnose_tool).collect();
    let all_executable = reports.iter().all(|report| report.executable);
    let capabilities = diagnose_capabilities(&reports);
    let missing_profile_capabilities =
        missing_required_capabilities(args.profile.as_deref(), &capabilities);
    let tool_versions = reports
        .iter()
        .filter_map(|report| {
            report
                .version
                .as_ref()
                .map(|version| (report.name.clone(), version.clone()))
        })
        .collect();

    let mut payload = json!({
        "tools": reports,
    });
    if args.capabilities || args.profile.is_some() {
        payload["capabilities"] = json!(capabilities);
    }

    let exit_code = if all_executable && missing_profile_capabilities.is_empty() {
        0
    } else {
        3
    };

    (
        DoctorCommandResponse { exit_code, payload },
        DoctorPipelineTrace { tool_versions },
    )
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "doctor_probe_tools".to_string(),
        "doctor_probe_capabilities".to_string(),
    ]
}

/// Determinism guards planned for the `doctor` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "fixed_tool_probe_order_jq_yq_mlr".to_string(),
        "fixed_capability_probe_order_jq_yq_mlr".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ]
}

fn diagnose_capabilities(reports: &[DoctorToolReport]) -> Vec<DoctorCapabilityReport> {
    CAPABILITY_SPECS
        .iter()
        .map(|spec| diagnose_capability(spec, reports))
        .collect()
}

fn diagnose_capability(
    spec: &CapabilitySpec,
    reports: &[DoctorToolReport],
) -> DoctorCapabilityReport {
    let tool = reports
        .iter()
        .find(|report| report.name == spec.tool)
        .expect("capability tool must exist in tool reports");
    if !tool.executable {
        return DoctorCapabilityReport {
            name: spec.name.to_string(),
            tool: spec.tool.to_string(),
            available: false,
            message: format!(
                "requires executable `{}`. fix tool availability first.",
                spec.tool
            ),
        };
    }

    let command = capability_command_label(spec);
    match run_capability_probe(spec) {
        Ok(output) => {
            if output.status.success() {
                DoctorCapabilityReport {
                    name: spec.name.to_string(),
                    tool: spec.tool.to_string(),
                    available: true,
                    message: "ok".to_string(),
                }
            } else {
                DoctorCapabilityReport {
                    name: spec.name.to_string(),
                    tool: spec.tool.to_string(),
                    available: false,
                    message: format!(
                        "`{}` probe command `{}` returned non-zero status ({}).",
                        spec.tool,
                        command,
                        status_label(output.status.code()),
                    ),
                }
            }
        }
        Err(error) => DoctorCapabilityReport {
            name: spec.name.to_string(),
            tool: spec.tool.to_string(),
            available: false,
            message: format!(
                "failed to execute `{}` probe `{}`: {}.",
                spec.tool, command, error
            ),
        },
    }
}

fn missing_required_capabilities(
    profile: Option<&str>,
    capabilities: &[DoctorCapabilityReport],
) -> Vec<String> {
    let Some(profile_name) = profile else {
        return Vec::new();
    };

    let availability: BTreeMap<&str, bool> = capabilities
        .iter()
        .map(|capability| (capability.name.as_str(), capability.available))
        .collect();

    // Until explicit profile definitions are added, profile requests gate on all known
    // capabilities in fixed order.
    required_capabilities_for_profile(profile_name)
        .into_iter()
        .filter(|name| !availability.get(*name).copied().unwrap_or(false))
        .map(ToOwned::to_owned)
        .collect()
}

fn required_capabilities_for_profile(_profile: &str) -> Vec<&'static str> {
    CAPABILITY_SPECS.iter().map(|spec| spec.name).collect()
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

fn capability_command_label(spec: &CapabilitySpec) -> String {
    if spec.probe_args.is_empty() {
        spec.tool.to_string()
    } else {
        format!("{} {}", spec.tool, spec.probe_args.join(" "))
    }
}

fn run_capability_probe(spec: &CapabilitySpec) -> std::io::Result<Output> {
    if let Some(stdin) = spec.probe_stdin {
        let mut child = Command::new(spec.tool)
            .args(spec.probe_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(mut child_stdin) = child.stdin.take() {
            child_stdin.write_all(stdin.as_bytes())?;
        }
        child.wait_with_output()
    } else {
        Command::new(spec.tool).args(spec.probe_args).output()
    }
}
