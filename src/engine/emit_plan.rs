use std::error::Error;
use std::fmt;

use serde::Serialize;

use crate::cmd::{
    aggregate,
    r#assert::{self as assert_cmd, AssertInputNormalizeMode},
    canon, contract, doctor, join, merge, profile, sdiff,
};

const TOOL_ORDER: [&str; 6] = ["jq", "yq", "mlr", "ajv", "duckdb", "check-jsonschema"];

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
        "transform-sql" => Ok(vec![
            "resolve_transform_sql_input".to_string(),
            "execute_transform_sql_with_duckdb".to_string(),
            "write_transform_sql_output".to_string(),
        ]),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssertSchemaPlanEngine {
    Jsonschema,
    Ajv,
    Checkjs,
}

impl AssertSchemaPlanEngine {
    fn from_flag_value(value: &str) -> Result<Self, EmitPlanError> {
        match value {
            "jsonschema" => Ok(Self::Jsonschema),
            "ajv" => Ok(Self::Ajv),
            "checkjs" => Ok(Self::Checkjs),
            other => Err(EmitPlanError::InvalidArguments(format!(
                "`--engine`/`--schema-engine` must be `jsonschema`, `ajv`, or `checkjs` (received `{other}`)"
            ))),
        }
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

    let uses_rules = has_flag_or_assigned_value(args, "--rules");
    let uses_schema = has_flag_or_assigned_value(args, "--schema");
    if uses_rules && uses_schema {
        return Err(EmitPlanError::InvalidArguments(
            "`--rules` and `--schema` are mutually exclusive".to_string(),
        ));
    }
    let schema_engine = parse_assert_schema_engine(args)?;
    if schema_engine.is_some() && !uses_schema {
        return Err(EmitPlanError::InvalidArguments(
            "`--engine`/`--schema-engine` are supported only with `--schema`".to_string(),
        ));
    }
    let schema_flag = parse_assert_custom_flag(args, "--schema-flag")?;
    let input_flag = parse_assert_custom_flag(args, "--input-flag")?;
    if (schema_flag.is_some() || input_flag.is_some())
        && schema_engine.unwrap_or(AssertSchemaPlanEngine::Jsonschema)
            != AssertSchemaPlanEngine::Checkjs
    {
        return Err(EmitPlanError::InvalidArguments(
            "`--schema-flag` and `--input-flag` are supported only with schema engine `checkjs`"
                .to_string(),
        ));
    }
    if uses_schema {
        let schema_engine = schema_engine.unwrap_or(AssertSchemaPlanEngine::Jsonschema);
        return Ok(assert_schema_pipeline_steps(normalize_mode, schema_engine));
    }

    Ok(assert_cmd::pipeline_steps(normalize_mode))
}

fn assert_schema_pipeline_steps(
    normalize: Option<AssertInputNormalizeMode>,
    schema_engine: AssertSchemaPlanEngine,
) -> Vec<String> {
    let validate_step = match schema_engine {
        AssertSchemaPlanEngine::Jsonschema => "validate_assert_schema_with_jsonschema",
        AssertSchemaPlanEngine::Ajv => "validate_assert_schema_with_ajv",
        AssertSchemaPlanEngine::Checkjs => "validate_assert_schema_with_check_jsonschema",
    };
    let mut steps = vec![
        "load_schema".to_string(),
        "resolve_input_format".to_string(),
        "read_input_values".to_string(),
        validate_step.to_string(),
    ];
    if normalize.is_some() {
        steps.insert(3, "normalize_assert_input".to_string());
    }
    steps
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

fn parse_assert_schema_engine(
    args: &[String],
) -> Result<Option<AssertSchemaPlanEngine>, EmitPlanError> {
    let mut engine_value: Option<AssertSchemaPlanEngine> = None;
    let mut schema_engine_value: Option<AssertSchemaPlanEngine> = None;
    let mut index = 0usize;

    while index < args.len() {
        let current = args[index].as_str();
        if current == "--engine" || current == "--schema-engine" {
            index += 1;
            let Some(value) = args.get(index) else {
                return Err(EmitPlanError::InvalidArguments(format!(
                    "missing value for `{current}`"
                )));
            };
            let parsed = AssertSchemaPlanEngine::from_flag_value(value)?;
            if current == "--engine" {
                if engine_value.is_some() {
                    return Err(EmitPlanError::InvalidArguments(
                        "`--engine` can only be provided once".to_string(),
                    ));
                }
                engine_value = Some(parsed);
            } else {
                if schema_engine_value.is_some() {
                    return Err(EmitPlanError::InvalidArguments(
                        "`--schema-engine` can only be provided once".to_string(),
                    ));
                }
                schema_engine_value = Some(parsed);
            }
        } else if let Some((flag, value)) = current.split_once('=') {
            if flag == "--engine" || flag == "--schema-engine" {
                let parsed = AssertSchemaPlanEngine::from_flag_value(value)?;
                if flag == "--engine" {
                    if engine_value.is_some() {
                        return Err(EmitPlanError::InvalidArguments(
                            "`--engine` can only be provided once".to_string(),
                        ));
                    }
                    engine_value = Some(parsed);
                } else {
                    if schema_engine_value.is_some() {
                        return Err(EmitPlanError::InvalidArguments(
                            "`--schema-engine` can only be provided once".to_string(),
                        ));
                    }
                    schema_engine_value = Some(parsed);
                }
            }
        }
        index += 1;
    }

    Ok(schema_engine_value.or(engine_value))
}

fn parse_assert_custom_flag(args: &[String], flag: &str) -> Result<Option<String>, EmitPlanError> {
    let mut value: Option<String> = None;
    let mut index = 0usize;

    while index < args.len() {
        let current = args[index].as_str();
        if current == flag {
            if value.is_some() {
                return Err(EmitPlanError::InvalidArguments(format!(
                    "`{flag}` can only be provided once"
                )));
            }
            index += 1;
            let Some(next) = args.get(index) else {
                return Err(EmitPlanError::InvalidArguments(format!(
                    "missing value for `{flag}`"
                )));
            };
            value = Some(next.clone());
        } else if let Some((candidate_flag, assigned_value)) = current.split_once('=') {
            if candidate_flag == flag {
                if value.is_some() {
                    return Err(EmitPlanError::InvalidArguments(format!(
                        "`{flag}` can only be provided once"
                    )));
                }
                value = Some(assigned_value.to_string());
            }
        }
        index += 1;
    }

    Ok(value)
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn has_flag_or_assigned_value(args: &[String], flag: &str) -> bool {
    let prefix = format!("{flag}=");
    args.iter()
        .any(|arg| arg == flag || arg.starts_with(prefix.as_str()))
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
        "assert" if step == "validate_assert_schema_with_ajv" => "ajv",
        "assert" if step == "validate_assert_schema_with_check_jsonschema" => "check-jsonschema",
        "join" if step == "execute_join_with_mlr" => "mlr",
        "aggregate" if step == "execute_aggregate_with_mlr" => "mlr",
        "transform-sql" if step == "execute_transform_sql_with_duckdb" => "duckdb",
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
        assert!(
            plan.tools
                .iter()
                .any(|tool| tool.name == "check-jsonschema" && !tool.expected)
        );
    }

    #[test]
    fn resolves_assert_schema_plan_defaults_to_jsonschema_stage() {
        let plan = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec!["--schema=schema.json".to_string()],
        })
        .expect("assert schema plan");

        let steps: Vec<String> = plan.stages.iter().map(|stage| stage.step.clone()).collect();
        assert_eq!(
            steps,
            vec![
                "load_schema",
                "resolve_input_format",
                "read_input_values",
                "validate_assert_schema_with_jsonschema",
            ]
        );
        assert!(
            plan.stages
                .iter()
                .all(|stage| stage.tool != "check-jsonschema")
        );
        assert!(plan.stages.iter().any(|stage| stage.tool == "rust"));
        assert!(
            plan.tools
                .iter()
                .any(|tool| tool.name == "check-jsonschema" && !tool.expected)
        );
    }

    #[test]
    fn resolves_assert_schema_plan_with_checkjsonschema_when_engine_checkjs() {
        let plan = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec![
                "--schema=schema.json".to_string(),
                "--engine=checkjs".to_string(),
            ],
        })
        .expect("assert schema checkjs plan");
        assert!(
            plan.stages
                .iter()
                .any(|stage| stage.step == "validate_assert_schema_with_check_jsonschema")
        );
        assert!(
            plan.tools
                .iter()
                .any(|tool| tool.name == "check-jsonschema" && tool.expected)
        );
    }

    #[test]
    fn resolves_assert_schema_plan_with_ajv_when_engine_ajv() {
        let plan = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec![
                "--schema=schema.json".to_string(),
                "--engine=ajv".to_string(),
            ],
        })
        .expect("assert schema ajv plan");
        assert!(
            plan.stages
                .iter()
                .any(|stage| stage.step == "validate_assert_schema_with_ajv")
        );
        assert!(plan.stages.iter().any(|stage| stage.tool == "ajv"));
        assert!(
            plan.tools
                .iter()
                .any(|tool| tool.name == "ajv" && tool.expected)
        );
    }

    #[test]
    fn rejects_rules_and_schema_combination_for_assert_plan() {
        let error = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec![
                "--rules".to_string(),
                "rules.yaml".to_string(),
                "--schema".to_string(),
            ],
        })
        .expect_err("rules + schema must fail");
        assert_eq!(
            error,
            EmitPlanError::InvalidArguments(
                "`--rules` and `--schema` are mutually exclusive".to_string()
            )
        );
    }

    #[test]
    fn rejects_assert_engine_override_without_schema_for_assert_plan() {
        let error = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec!["--engine=ajv".to_string()],
        })
        .expect_err("engine without schema must fail");
        assert_eq!(
            error,
            EmitPlanError::InvalidArguments(
                "`--engine`/`--schema-engine` are supported only with `--schema`".to_string()
            )
        );
    }

    #[test]
    fn rejects_assert_schema_flag_overrides_without_checkjs_engine() {
        let error = resolve(&EmitPlanRequest {
            command: "assert".to_string(),
            args: vec![
                "--schema=schema.json".to_string(),
                "--schema-flag=--custom-schema".to_string(),
                "--input-flag=--custom-input".to_string(),
            ],
        })
        .expect_err("custom flags without checkjs must fail");
        assert_eq!(
            error,
            EmitPlanError::InvalidArguments(
                "`--schema-flag` and `--input-flag` are supported only with schema engine `checkjs`"
                    .to_string()
            )
        );
    }

    #[test]
    fn resolves_transform_sql_plan_with_duckdb_stage_and_tools() {
        let plan = resolve(&EmitPlanRequest {
            command: "transform-sql".to_string(),
            args: Vec::new(),
        })
        .expect("transform-sql plan");

        assert_eq!(
            plan.stages
                .iter()
                .map(|stage| stage.step.as_str())
                .collect::<Vec<_>>(),
            vec![
                "resolve_transform_sql_input",
                "execute_transform_sql_with_duckdb",
                "write_transform_sql_output",
            ]
        );
        assert_eq!(plan.stages[1].tool, "duckdb");
        assert_eq!(
            plan.stages[1].depends_on,
            vec!["resolve_transform_sql_input".to_string()]
        );
        assert_eq!(
            plan.tools
                .iter()
                .map(|tool| (tool.name.as_str(), tool.expected))
                .collect::<Vec<_>>(),
            vec![
                ("jq", false),
                ("yq", false),
                ("mlr", false),
                ("ajv", false),
                ("duckdb", true),
                ("check-jsonschema", false),
            ]
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
