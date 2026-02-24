use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::str::FromStr;

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

/// Static profile identifiers accepted by `doctor --profile`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorProfile {
    Core,
    CiJobs,
    Doc,
    Api,
    Notes,
    Book,
    Scan,
}

impl DoctorProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::CiJobs => "ci-jobs",
            Self::Doc => "doc",
            Self::Api => "api",
            Self::Notes => "notes",
            Self::Book => "book",
            Self::Scan => "scan",
        }
    }
}

impl FromStr for DoctorProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "core" => Ok(Self::Core),
            "ci-jobs" => Ok(Self::CiJobs),
            "doc" => Ok(Self::Doc),
            "api" => Ok(Self::Api),
            "notes" => Ok(Self::Notes),
            "book" => Ok(Self::Book),
            "scan" => Ok(Self::Scan),
            _ => Err(
                "profile must be one of `core`, `ci-jobs`, `doc`, `api`, `notes`, `book`, `scan`"
                    .to_string(),
            ),
        }
    }
}

/// CLI/MCP input for doctor command execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DoctorCommandInput {
    pub capabilities: bool,
    pub profile: Option<DoctorProfile>,
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DoctorProfileReport {
    version: String,
    name: String,
    description: String,
    satisfied: bool,
    requirements: Vec<DoctorProfileRequirementReport>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DoctorProfileRequirementReport {
    capability: String,
    tool: String,
    reason: String,
    satisfied: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileRequirementSpec {
    capability: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileSpec {
    profile: DoctorProfile,
    description: &'static str,
    requirements: &'static [ProfileRequirementSpec],
}

const PROFILE_TABLE_VERSION: &str = "dataq.doctor.profile.requirements.v1";

const BASE_TOOL_SPECS: [ToolSpec; 3] = [
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

const PROFILE_PROBE_TOOL_SPECS: [ToolSpec; 8] = [
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
    ToolSpec {
        name: "pandoc",
        install_hint: "Install `pandoc` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "xh",
        install_hint: "Install `xh` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "nb",
        install_hint: "Install `nb` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "mdbook",
        install_hint: "Install `mdbook` and ensure it is available in PATH.",
    },
    ToolSpec {
        name: "rg",
        install_hint: "Install `rg` (ripgrep) and ensure it is available in PATH.",
    },
];

const DEFAULT_CAPABILITY_SPECS: [CapabilitySpec; 3] = [
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

const CAPABILITY_SPECS: [CapabilitySpec; 8] = [
    CapabilitySpec {
        name: "jq.available",
        tool: "jq",
        probe_args: &["-n", "."],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "yq.available",
        tool: "yq",
        probe_args: &["--null-input", "."],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "mlr.available",
        tool: "mlr",
        probe_args: &["--help"],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "pandoc.available",
        tool: "pandoc",
        probe_args: &[],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "xh.available",
        tool: "xh",
        probe_args: &[],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "nb.available",
        tool: "nb",
        probe_args: &[],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "mdbook.available",
        tool: "mdbook",
        probe_args: &[],
        probe_stdin: None,
    },
    CapabilitySpec {
        name: "rg.available",
        tool: "rg",
        probe_args: &[],
        probe_stdin: None,
    },
];

const CORE_REQUIREMENTS: [ProfileRequirementSpec; 3] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "yq.available",
        reason: "requires deterministic YAML extraction via `yq`",
    },
    ProfileRequirementSpec {
        capability: "mlr.available",
        reason: "requires deterministic row shaping via `mlr`",
    },
];

const DOC_REQUIREMENTS: [ProfileRequirementSpec; 2] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "pandoc.available",
        reason: "requires document AST extraction via `pandoc`",
    },
];

const API_REQUIREMENTS: [ProfileRequirementSpec; 2] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "xh.available",
        reason: "requires HTTP fetch execution via `xh`",
    },
];

const NOTES_REQUIREMENTS: [ProfileRequirementSpec; 2] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "nb.available",
        reason: "requires note export execution via `nb`",
    },
];

const BOOK_REQUIREMENTS: [ProfileRequirementSpec; 2] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "mdbook.available",
        reason: "requires mdBook metadata access via `mdbook`",
    },
];

const SCAN_REQUIREMENTS: [ProfileRequirementSpec; 2] = [
    ProfileRequirementSpec {
        capability: "jq.available",
        reason: "requires deterministic JSON projection via `jq`",
    },
    ProfileRequirementSpec {
        capability: "rg.available",
        reason: "requires text scanning via `rg`",
    },
];

const PROFILE_SPECS: [ProfileSpec; 7] = [
    ProfileSpec {
        profile: DoctorProfile::Core,
        description: "base deterministic workflows powered by jq/yq/mlr",
        requirements: &CORE_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::CiJobs,
        description: "CI job extraction workflows via yq -> jq -> mlr",
        requirements: &CORE_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::Doc,
        description: "document ingestion workflows via pandoc + jq",
        requirements: &DOC_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::Api,
        description: "API ingestion workflows via xh + jq",
        requirements: &API_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::Notes,
        description: "notes ingestion workflows via nb + jq",
        requirements: &NOTES_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::Book,
        description: "book ingestion workflows via mdbook + jq",
        requirements: &BOOK_REQUIREMENTS,
    },
    ProfileSpec {
        profile: DoctorProfile::Scan,
        description: "text scan workflows via rg + jq",
        requirements: &SCAN_REQUIREMENTS,
    },
];

pub fn run() -> DoctorCommandResponse {
    run_with_trace().0
}

pub fn run_with_trace() -> (DoctorCommandResponse, DoctorPipelineTrace) {
    run_with_input_and_trace(DoctorCommandInput::default())
}

pub fn run_with_input(input: DoctorCommandInput) -> DoctorCommandResponse {
    run_with_input_and_trace(input).0
}

pub fn run_with_input_and_trace(
    input: DoctorCommandInput,
) -> (DoctorCommandResponse, DoctorPipelineTrace) {
    if let Some(profile) = input.profile {
        return run_with_profile(profile);
    }

    let reports: Vec<DoctorToolReport> = BASE_TOOL_SPECS.iter().map(diagnose_tool).collect();
    let all_executable = reports.iter().all(|report| report.executable);
    let tool_versions = collect_tool_versions(&reports);
    let tool_lookup = tool_lookup(&reports);
    let mut payload = json!({
        "tools": reports,
    });
    if input.capabilities {
        let capabilities = capability_reports(&tool_lookup, &DEFAULT_CAPABILITY_SPECS);
        payload["capabilities"] = json!(capabilities);
    }

    (
        DoctorCommandResponse {
            exit_code: if all_executable { 0 } else { 3 },
            payload,
        },
        DoctorPipelineTrace { tool_versions },
    )
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps(profile: Option<DoctorProfile>) -> Vec<String> {
    if profile.is_some() {
        return vec![
            "doctor_profile_probe".to_string(),
            "doctor_profile_evaluate".to_string(),
        ];
    }

    vec![
        "doctor_probe_tools".to_string(),
        "doctor_probe_capabilities".to_string(),
    ]
}

/// Determinism guards planned for the `doctor` command.
pub fn deterministic_guards(profile: Option<DoctorProfile>) -> Vec<String> {
    let mut guards = vec![
        "rust_native_execution".to_string(),
        "fixed_tool_probe_order_jq_yq_mlr".to_string(),
        "fixed_capability_probe_order_jq_yq_mlr".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ];
    if profile.is_some() {
        guards.push("static_profile_requirement_table_v1".to_string());
        guards.push("fixed_profile_requirement_order".to_string());
    }
    guards
}

fn run_with_profile(profile: DoctorProfile) -> (DoctorCommandResponse, DoctorPipelineTrace) {
    let probed_tools: Vec<DoctorToolReport> =
        PROFILE_PROBE_TOOL_SPECS.iter().map(diagnose_tool).collect();
    let tool_lookup = tool_lookup(&probed_tools);
    let base_reports: Vec<DoctorToolReport> = BASE_TOOL_SPECS
        .iter()
        .map(|spec| {
            tool_lookup
                .get(spec.name)
                .cloned()
                .expect("base tool must exist in profile probes")
        })
        .collect();
    let capabilities = capability_reports(&tool_lookup, &CAPABILITY_SPECS);
    let profile_report = evaluate_profile(profile, &capabilities);
    let tool_versions = collect_tool_versions(&probed_tools);
    let exit_code = if profile_report.satisfied { 0 } else { 3 };

    (
        DoctorCommandResponse {
            exit_code,
            payload: json!({
                "tools": base_reports,
                "capabilities": capabilities,
                "profile": profile_report,
            }),
        },
        DoctorPipelineTrace { tool_versions },
    )
}

fn collect_tool_versions(reports: &[DoctorToolReport]) -> BTreeMap<String, String> {
    reports
        .iter()
        .filter_map(|report| {
            report
                .version
                .as_ref()
                .map(|version| (report.name.clone(), version.clone()))
        })
        .collect()
}

fn tool_lookup(reports: &[DoctorToolReport]) -> BTreeMap<&str, DoctorToolReport> {
    reports
        .iter()
        .map(|report| (report.name.as_str(), report.clone()))
        .collect()
}

fn capability_reports(
    tool_lookup: &BTreeMap<&str, DoctorToolReport>,
    specs: &[CapabilitySpec],
) -> Vec<DoctorCapabilityReport> {
    specs
        .iter()
        .map(|spec| diagnose_capability(spec, tool_lookup))
        .collect()
}

fn diagnose_capability(
    spec: &CapabilitySpec,
    tool_lookup: &BTreeMap<&str, DoctorToolReport>,
) -> DoctorCapabilityReport {
    let tool = tool_lookup
        .get(spec.tool)
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

    if spec.probe_args.is_empty() && spec.probe_stdin.is_none() {
        return DoctorCapabilityReport {
            name: spec.name.to_string(),
            tool: spec.tool.to_string(),
            available: true,
            message: "ok".to_string(),
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

fn evaluate_profile(
    profile: DoctorProfile,
    capabilities: &[DoctorCapabilityReport],
) -> DoctorProfileReport {
    let spec = PROFILE_SPECS
        .iter()
        .find(|spec| spec.profile == profile)
        .expect("profile spec must be defined");
    let capability_lookup: BTreeMap<&str, &DoctorCapabilityReport> = capabilities
        .iter()
        .map(|capability| (capability.name.as_str(), capability))
        .collect();

    let requirements: Vec<DoctorProfileRequirementReport> = spec
        .requirements
        .iter()
        .map(|requirement| {
            let capability = capability_lookup
                .get(requirement.capability)
                .expect("profile requirement capability must exist");
            DoctorProfileRequirementReport {
                capability: requirement.capability.to_string(),
                tool: capability.tool.clone(),
                reason: requirement.reason.to_string(),
                satisfied: capability.available,
                message: capability.message.clone(),
            }
        })
        .collect();
    let satisfied = requirements.iter().all(|requirement| requirement.satisfied);

    DoctorProfileReport {
        version: PROFILE_TABLE_VERSION.to_string(),
        name: profile.as_str().to_string(),
        description: spec.description.to_string(),
        satisfied,
        requirements,
    }
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
