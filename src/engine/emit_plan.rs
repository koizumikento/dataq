use std::error::Error;
use std::fmt;

use serde::Serialize;

use crate::cmd::{
    aggregate,
    r#assert::{self as assert_cmd, AssertInputNormalizeMode},
    canon, contract, doctor, join, merge, profile, sdiff,
};

const TOOL_ORDER: [&str; 3] = ["jq", "yq", "mlr"];

/// Request shape for static plan resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitPlanRequest {
    pub command: String,
    pub args: Vec<String>,
}

/// Deterministic static plan for one command.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EmitPlan {
    pub command: String,
    pub args: Vec<String>,
    pub stages: Vec<EmitPlanStage>,
    pub tools: Vec<EmitPlanTool>,
}

/// One stage in the resolved static plan.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EmitPlanStage {
    pub order: usize,
    pub step: String,
    pub tool: String,
    pub depends_on: Vec<String>,
}

/// Expected external-tool usage in deterministic order.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EmitPlanTool {
    pub name: String,
    pub expected: bool,
}

/// Static planning errors mapped to CLI input/usage failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitPlanError {
    UnknownCommand(String),
    InvalidArguments(String),
}

impl fmt::Display for EmitPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand(command) => {
                write!(f, "unsupported emit plan command `{command}`")
            }
            Self::InvalidArguments(message) => write!(f, "{message}"),
        }
    }
}

impl Error for EmitPlanError {}

/// Resolve a static execution plan from command + argument vector.
pub fn resolve(request: &EmitPlanRequest) -> Result<EmitPlan, EmitPlanError> {
    let command = normalize_command(request.command.as_str());
    let steps = resolve_steps(command.as_str(), &request.args)?;
    let stages = build_stages(command.as_str(), &steps);
    let tools = build_tool_expectations(&stages);

    Ok(EmitPlan {
        command,
        args: request.args.clone(),
        stages,
        tools,
    })
}

fn normalize_command(raw: &str) -> String {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized == "recipe run" {
        "recipe.run".to_string()
    } else {
        normalized
    }
}

fn resolve_steps(command: &str, args: &[String]) -> Result<Vec<String>, EmitPlanError> {
    match command {
        "canon" => Ok(canon::pipeline_steps()),
        "assert" => resolve_assert_steps(args),
        "sdiff" => Ok(sdiff::pipeline_steps()),
        "profile" => Ok(profile::pipeline_steps()),
        "join" => Ok(join::pipeline_steps()),
        "aggregate" => Ok(aggregate::pipeline_steps()),
        "merge" => Ok(merge::pipeline_steps()),
        "doctor" => Ok(doctor::pipeline_steps(None)),
        "contract" => Ok(contract::pipeline_steps()),
        "recipe" | "recipe.run" => resolve_recipe_steps(args),
        "mcp" => Ok(vec![
            "read_mcp_request".to_string(),
            "parse_mcp_request".to_string(),
            "dispatch_mcp_request".to_string(),
            "write_mcp_response".to_string(),
        ]),
        _ => Err(EmitPlanError::UnknownCommand(command.to_string())),
    }
}

fn resolve_assert_steps(args: &[String]) -> Result<Vec<String>, EmitPlanError> {
    reject_assigned_assert_help_value(args, "--rules-help")?;
    reject_assigned_assert_help_value(args, "--schema-help")?;

    let rules_help = has_flag(args, "--rules-help");
    let schema_help = has_flag(args, "--schema-help");
    if rules_help && schema_help {
        return Err(EmitPlanError::InvalidArguments(
            "`--rules-help` and `--schema-help` are mutually exclusive".to_string(),
        ));
    }

    let normalize_mode = parse_assert_normalize_mode(args)?;
    if normalize_mode.is_some() && (rules_help || schema_help) {
        return Err(EmitPlanError::InvalidArguments(
            "`--normalize` cannot be combined with assert help modes".to_string(),
        ));
    }

    if rules_help {
        return Ok(vec!["emit_assert_rules_help".to_string()]);
    }
    if schema_help {
        return Ok(vec!["emit_assert_schema_help".to_string()]);
    }
    Ok(assert_cmd::pipeline_steps(normalize_mode))
}

fn resolve_recipe_steps(args: &[String]) -> Result<Vec<String>, EmitPlanError> {
    if let Some(first) = args.first() {
        if !first.starts_with('-') && first != "run" {
            return Err(EmitPlanError::InvalidArguments(format!(
                "unsupported recipe subcommand `{first}` for emit plan"
            )));
        }
    }

    Ok(vec![
        "load_recipe_file".to_string(),
        "validate_recipe_schema".to_string(),
        "execute_step_<index>_<kind>".to_string(),
    ])
}

fn parse_assert_normalize_mode(
    args: &[String],
) -> Result<Option<AssertInputNormalizeMode>, EmitPlanError> {
    let mut normalize_value: Option<&str> = None;
    let mut index = 0usize;

    while index < args.len() {
        let current = args[index].as_str();
        if current == "--normalize" {
            if normalize_value.is_some() {
                return Err(EmitPlanError::InvalidArguments(
                    "`--normalize` can only be provided once".to_string(),
                ));
            }
            index += 1;
            let Some(value) = args.get(index) else {
                return Err(EmitPlanError::InvalidArguments(
                    "missing value for `--normalize`".to_string(),
                ));
            };
            normalize_value = Some(value.as_str());
        } else if let Some((flag, value)) = current.split_once('=') {
            if flag == "--normalize" {
                if normalize_value.is_some() {
                    return Err(EmitPlanError::InvalidArguments(
                        "`--normalize` can only be provided once".to_string(),
                    ));
                }
                normalize_value = Some(value);
            }
        }
        index += 1;
    }

    match normalize_value {
        None => Ok(None),
        Some("github-actions-jobs") => Ok(Some(AssertInputNormalizeMode::GithubActionsJobs)),
        Some("gitlab-ci-jobs") => Ok(Some(AssertInputNormalizeMode::GitlabCiJobs)),
        Some(other) => Err(EmitPlanError::InvalidArguments(format!(
            "`--normalize` must be `github-actions-jobs` or `gitlab-ci-jobs` (received `{other}`)"
        ))),
    }
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn reject_assigned_assert_help_value(args: &[String], flag: &str) -> Result<(), EmitPlanError> {
    let prefix = format!("{flag}=");
    if let Some(received) = args.iter().find(|arg| arg.starts_with(prefix.as_str())) {
        return Err(EmitPlanError::InvalidArguments(format!(
            "`{flag}` does not take a value (received `{received}`)"
        )));
    }
    Ok(())
}

fn build_stages(command: &str, steps: &[String]) -> Vec<EmitPlanStage> {
    steps
        .iter()
        .enumerate()
        .map(|(index, step)| EmitPlanStage {
            order: index + 1,
            step: step.clone(),
            tool: stage_tool(command, step.as_str()).to_string(),
            depends_on: if index == 0 {
                Vec::new()
            } else {
                vec![steps[index - 1].clone()]
            },
        })
        .collect()
}

fn stage_tool(command: &str, step: &str) -> &'static str {
    match command {
        "assert" if step == "normalize_assert_input" => "yq+jq+mlr",
        "join" if step == "execute_join_with_mlr" => "mlr",
        "aggregate" if step == "execute_aggregate_with_mlr" => "mlr",
        "doctor" => match step {
            "doctor_probe_jq" => "jq",
            "doctor_probe_yq" => "yq",
            "doctor_probe_mlr" => "mlr",
            _ => "rust",
        },
        _ => "rust",
    }
}

fn build_tool_expectations(stages: &[EmitPlanStage]) -> Vec<EmitPlanTool> {
    TOOL_ORDER
        .iter()
        .map(|tool| EmitPlanTool {
            name: (*tool).to_string(),
            expected: stages
                .iter()
                .any(|stage| stage.tool.split('+').any(|candidate| candidate == *tool)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{EmitPlanError, EmitPlanRequest, resolve};

    #[test]
    fn resolves_canon_plan_with_runtime_stage_order() {
        let plan = resolve(&EmitPlanRequest {
            command: "canon".to_string(),
            args: Vec::new(),
        })
        .expect("canon plan");

        let steps: Vec<String> = plan.stages.into_iter().map(|stage| stage.step).collect();
        assert_eq!(
            steps,
            vec![
                "read_input_values",
                "canonicalize_values",
                "write_output_values",
            ]
        );
    }

    #[test]
    fn resolves_assert_plan_with_normalize_stage() {
        let plan = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec!["--normalize".to_string(), "github-actions-jobs".to_string()],
        })
        .expect("assert plan");

        assert!(
            plan.stages
                .iter()
                .any(|stage| stage.step == "normalize_assert_input")
        );
        assert!(
            plan.tools
                .iter()
                .any(|tool| tool.name == "mlr" && tool.expected)
        );
    }

    #[test]
    fn rejects_unknown_command() {
        let error = resolve(&EmitPlanRequest {
            command: "unknown".to_string(),
            args: Vec::new(),
        })
        .expect_err("unknown must fail");

        assert_eq!(error, EmitPlanError::UnknownCommand("unknown".to_string()));
    }

    #[test]
    fn rejects_assigned_assert_help_flag_values() {
        let rules_help_error = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec!["--rules-help=true".to_string()],
        })
        .expect_err("assigned rules help value must fail");
        assert_eq!(
            rules_help_error,
            EmitPlanError::InvalidArguments(
                "`--rules-help` does not take a value (received `--rules-help=true`)".to_string()
            )
        );

        let schema_help_error = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec!["--schema-help=true".to_string()],
        })
        .expect_err("assigned schema help value must fail");
        assert_eq!(
            schema_help_error,
            EmitPlanError::InvalidArguments(
                "`--schema-help` does not take a value (received `--schema-help=true`)".to_string()
            )
        );
    }
}
