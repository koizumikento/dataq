use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::domain::report::{
    RecipeLockReport, RecipeReplayLockCheckReport, RecipeReplayLockMismatchReport,
    RecipeReplayReport, RecipeRunReport, RecipeStepReport,
};
use crate::domain::rules::AssertRules;
use crate::domain::value_path::ValuePath;
use crate::engine::r#assert::{self, AssertValidationError};
use crate::engine::canon::{CanonOptions, canonicalize_value, canonicalize_values};
use crate::engine::profile;
use crate::engine::sdiff::{self, DEFAULT_VALUE_DIFF_CAP, SdiffOptions};
use crate::io::{self, Format};
use crate::util::hash::DeterministicHasher;

pub const RECIPE_VERSION: &str = "dataq.recipe.v1";
const RECIPE_LOCK_VERSION: &str = "dataq.recipe.lock.v1";
const RECIPE_LOCK_TOOLS: [&str; 3] = ["jq", "yq", "mlr"];
const RECIPE_LOCK_TOOL_ORDER: [&str; 3] = ["jq", "mlr", "yq"];
const CANON_REQUIRES_INPUT_OR_PRIOR_VALUES: &str =
    "canon step requires `args.input` or prior in-memory values";
const ASSERT_REQUIRES_PRIOR_VALUES: &str =
    "assert step requires prior in-memory values (for example a preceding canon step)";
const PROFILE_REQUIRES_PRIOR_VALUES: &str =
    "profile step requires prior in-memory values (for example a preceding canon step)";
const SDIFF_REQUIRES_PRIOR_VALUES: &str =
    "sdiff step requires prior in-memory values (for example a preceding canon step)";

#[derive(Debug, Clone)]
pub struct RecipeExecution {
    pub report: RecipeRunReport,
    pub pipeline_steps: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RecipeLockExecution {
    pub report: RecipeLockReport,
    pub serialized: Vec<u8>,
    pub pipeline_steps: Vec<String>,
    pub tool_versions: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct RecipeReplayExecution {
    pub report: RecipeReplayReport,
    pub pipeline_steps: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RecipeExecutionError {
    pub kind: RecipeExecutionErrorKind,
    pub pipeline_steps: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum RecipeExecutionErrorKind {
    InputUsage(String),
    Internal(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecipeFile {
    version: String,
    steps: Vec<RecipeStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecipeLockFile {
    version: String,
    command_graph_hash: String,
    args_hash: String,
    tool_versions: BTreeMap<String, String>,
    dataq_version: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecipeStep {
    kind: String,
    args: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonStepArgs {
    input: Option<PathBuf>,
    from: Option<String>,
    #[serde(default = "default_true")]
    sort_keys: bool,
    #[serde(default)]
    normalize_time: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertStepArgs {
    rules: Option<Value>,
    schema: Option<Value>,
    rules_file: Option<PathBuf>,
    schema_file: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileStepArgs {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SdiffStepArgs {
    right: PathBuf,
    right_from: Option<String>,
    key: Option<String>,
    #[serde(default)]
    ignore_path: Vec<String>,
    #[serde(default)]
    value_diff_cap: Option<usize>,
}

struct StepOutcome {
    matched: bool,
    exit_code: i32,
    summary: Value,
    next_values: Option<Vec<Value>>,
}

pub fn run(recipe_path: &Path) -> Result<RecipeExecution, RecipeExecutionError> {
    let mut pipeline_steps = vec![
        "load_recipe_file".to_string(),
        "validate_recipe_schema".to_string(),
    ];
    let recipe_base_dir = recipe_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let loaded = match load_recipe_value(recipe_path) {
        Ok(value) => value,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    execute_loaded_recipe(loaded, recipe_base_dir.as_path(), &mut pipeline_steps)
}

/// Runs a recipe from an already-loaded JSON value.
///
/// `base_dir` is used to resolve relative file paths referenced by recipe steps.
/// When `None`, the current directory (`.`) is used.
pub fn run_from_value(
    recipe_value: Value,
    base_dir: Option<&Path>,
) -> Result<RecipeExecution, RecipeExecutionError> {
    let mut pipeline_steps = vec![
        "load_recipe_inline".to_string(),
        "validate_recipe_schema".to_string(),
    ];
    let resolved_base_dir = base_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    execute_loaded_recipe(
        recipe_value,
        resolved_base_dir.as_path(),
        &mut pipeline_steps,
    )
}

pub fn lock(recipe_path: &Path) -> Result<RecipeLockExecution, RecipeExecutionError> {
    let mut pipeline_steps = vec!["recipe_lock_parse".to_string()];
    let loaded = match load_recipe_value(recipe_path) {
        Ok(value) => value,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };
    let recipe = match parse_loaded_recipe(loaded) {
        Ok(recipe) => recipe,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };
    if let Err(kind) = validate_recipe_lock_steps(&recipe) {
        return Err(RecipeExecutionError {
            kind,
            pipeline_steps: pipeline_steps.clone(),
        });
    }

    pipeline_steps.push("recipe_lock_probe_tools".to_string());
    let tool_versions = match probe_recipe_lock_tools() {
        Ok(versions) => versions,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    pipeline_steps.push("recipe_lock_fingerprint".to_string());
    let args_hash = match hash_recipe_args(&recipe) {
        Ok(hash) => hash,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };
    let report = RecipeLockReport {
        version: RECIPE_LOCK_VERSION.to_string(),
        command_graph_hash: hash_recipe_command_graph(&recipe),
        args_hash,
        tool_versions: tool_versions.clone(),
        dataq_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let serialized = match serialize_recipe_lock_report(&report) {
        Ok(serialized) => serialized,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    Ok(RecipeLockExecution {
        report,
        serialized,
        pipeline_steps,
        tool_versions,
    })
}

pub fn replay(
    recipe_path: &Path,
    lock_path: &Path,
    strict: bool,
) -> Result<RecipeReplayExecution, RecipeExecutionError> {
    let mut pipeline_steps = vec!["recipe_replay_parse".to_string()];
    let recipe_base_dir = recipe_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let recipe = match load_and_parse_recipe(recipe_path) {
        Ok(recipe) => recipe,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };
    let lock = match load_and_parse_lock(lock_path) {
        Ok(lock) => lock,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    pipeline_steps.push("recipe_replay_verify_lock".to_string());
    let lock_check = match verify_lock_constraints(&recipe, &lock, strict) {
        Ok(report) => report,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    if strict && !lock_check.matched {
        return Ok(RecipeReplayExecution {
            report: RecipeReplayReport {
                matched: false,
                exit_code: 2,
                lock_check,
                steps: Vec::new(),
            },
            pipeline_steps,
        });
    }

    pipeline_steps.push("recipe_replay_execute".to_string());
    let run_report = match execute_recipe_steps(recipe, recipe_base_dir.as_path(), None) {
        Ok(report) => report,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    Ok(RecipeReplayExecution {
        report: RecipeReplayReport {
            matched: run_report.matched,
            exit_code: run_report.exit_code,
            lock_check,
            steps: run_report.steps,
        },
        pipeline_steps,
    })
}
fn execute_loaded_recipe(
    loaded: Value,
    recipe_base_dir: &Path,
    pipeline_steps: &mut Vec<String>,
) -> Result<RecipeExecution, RecipeExecutionError> {
    let recipe = match parse_loaded_recipe(loaded) {
        Ok(recipe) => recipe,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    execute_recipe(recipe, recipe_base_dir, pipeline_steps)
}

fn execute_recipe(
    recipe: RecipeFile,
    recipe_base_dir: &Path,
    pipeline_steps: &mut Vec<String>,
) -> Result<RecipeExecution, RecipeExecutionError> {
    let report = match execute_recipe_steps(recipe, recipe_base_dir, Some(pipeline_steps)) {
        Ok(report) => report,
        Err(kind) => {
            return Err(RecipeExecutionError {
                kind,
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    };

    Ok(RecipeExecution {
        report,
        pipeline_steps: pipeline_steps.clone(),
    })
}

fn execute_recipe_steps(
    recipe: RecipeFile,
    recipe_base_dir: &Path,
    mut pipeline_steps: Option<&mut Vec<String>>,
) -> Result<RecipeRunReport, RecipeExecutionErrorKind> {
    let mut current_values: Option<Vec<Value>> = None;
    let mut step_reports = Vec::with_capacity(recipe.steps.len());

    for (index, step) in recipe.steps.into_iter().enumerate() {
        if let Some(steps) = pipeline_steps.as_deref_mut() {
            steps.push(format!("execute_step_{index}_{}", step.kind));
        }

        let kind = step.kind.clone();
        let outcome = execute_step(step, current_values.as_deref(), recipe_base_dir)?;

        if let Some(next_values) = outcome.next_values {
            current_values = Some(next_values);
        }

        step_reports.push(RecipeStepReport {
            index,
            kind,
            matched: outcome.matched,
            exit_code: outcome.exit_code,
            summary: outcome.summary,
        });

        if !outcome.matched {
            return Ok(RecipeRunReport {
                matched: false,
                exit_code: 2,
                steps: step_reports,
            });
        }
    }

    Ok(RecipeRunReport {
        matched: true,
        exit_code: 0,
        steps: step_reports,
    })
}

fn execute_step(
    step: RecipeStep,
    current_values: Option<&[Value]>,
    recipe_base_dir: &Path,
) -> Result<StepOutcome, RecipeExecutionErrorKind> {
    match step.kind.as_str() {
        "canon" => execute_canon_step(step.args, current_values, recipe_base_dir),
        "assert" => execute_assert_step(step.args, current_values, recipe_base_dir),
        "profile" => execute_profile_step(step.args, current_values),
        "sdiff" => execute_sdiff_step(step.args, current_values, recipe_base_dir),
        other => Err(RecipeExecutionErrorKind::InputUsage(format!(
            "unknown recipe step kind `{other}`"
        ))),
    }
}

fn execute_canon_step(
    args: Map<String, Value>,
    current_values: Option<&[Value]>,
    recipe_base_dir: &Path,
) -> Result<StepOutcome, RecipeExecutionErrorKind> {
    let args: CanonStepArgs = parse_step_args("canon", args)?;

    let input_values = if let Some(path) = args.input.as_deref() {
        let resolved_path = resolve_recipe_path(recipe_base_dir, path);
        let format = resolve_step_input_format(
            args.from.as_deref(),
            resolved_path.as_path(),
            "canon.args.input",
        )?;
        read_values_from_path(resolved_path.as_path(), format)?
    } else if let Some(values) = current_values {
        values.to_vec()
    } else {
        return Err(RecipeExecutionErrorKind::InputUsage(
            CANON_REQUIRES_INPUT_OR_PRIOR_VALUES.to_string(),
        ));
    };

    let canonical = canonicalize_values(
        input_values,
        CanonOptions {
            sort_keys: args.sort_keys,
            normalize_time: args.normalize_time,
        },
    );

    Ok(StepOutcome {
        matched: true,
        exit_code: 0,
        summary: json!({
            "record_count": canonical.len(),
            "sort_keys": args.sort_keys,
            "normalize_time": args.normalize_time,
        }),
        next_values: Some(canonical),
    })
}

fn execute_assert_step(
    args: Map<String, Value>,
    current_values: Option<&[Value]>,
    recipe_base_dir: &Path,
) -> Result<StepOutcome, RecipeExecutionErrorKind> {
    let args: AssertStepArgs = parse_step_args("assert", args)?;
    let values = current_values.ok_or_else(|| {
        RecipeExecutionErrorKind::InputUsage(ASSERT_REQUIRES_PRIOR_VALUES.to_string())
    })?;

    let report = match resolve_assert_source(args, recipe_base_dir)? {
        AssertSource::Rules(rules) => {
            r#assert::execute_assert(values, &rules).map_err(map_assert_error)?
        }
        AssertSource::Schema(schema) => {
            r#assert::execute_assert_with_schema(values, &schema).map_err(map_assert_error)?
        }
    };

    let matched = report.matched;
    let summary = serde_json::to_value(report).map_err(|error| {
        RecipeExecutionErrorKind::Internal(format!("failed to serialize assert report: {error}"))
    })?;

    Ok(StepOutcome {
        matched,
        exit_code: if matched { 0 } else { 2 },
        summary,
        next_values: None,
    })
}

fn execute_profile_step(
    args: Map<String, Value>,
    current_values: Option<&[Value]>,
) -> Result<StepOutcome, RecipeExecutionErrorKind> {
    let _: ProfileStepArgs = parse_step_args("profile", args)?;
    let values = current_values.ok_or_else(|| {
        RecipeExecutionErrorKind::InputUsage(PROFILE_REQUIRES_PRIOR_VALUES.to_string())
    })?;

    let report = profile::profile_values(values);
    let summary = serde_json::to_value(report).map_err(|error| {
        RecipeExecutionErrorKind::Internal(format!("failed to serialize profile report: {error}"))
    })?;

    Ok(StepOutcome {
        matched: true,
        exit_code: 0,
        summary,
        next_values: None,
    })
}

fn execute_sdiff_step(
    args: Map<String, Value>,
    current_values: Option<&[Value]>,
    recipe_base_dir: &Path,
) -> Result<StepOutcome, RecipeExecutionErrorKind> {
    let args: SdiffStepArgs = parse_step_args("sdiff", args)?;
    let left_values = current_values.ok_or_else(|| {
        RecipeExecutionErrorKind::InputUsage(SDIFF_REQUIRES_PRIOR_VALUES.to_string())
    })?;

    let right_path = resolve_recipe_path(recipe_base_dir, args.right.as_path());
    let right_format = resolve_step_input_format(
        args.right_from.as_deref(),
        right_path.as_path(),
        "sdiff.args.right",
    )?;
    let right_values = read_values_from_path(right_path.as_path(), right_format)?;

    let parsed_key = args
        .key
        .as_deref()
        .map(ValuePath::parse_canonical)
        .transpose()
        .map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid sdiff key path `{}`: {error}",
                args.key.as_deref().unwrap_or_default()
            ))
        })?;

    let mut parsed_ignore_paths = Vec::with_capacity(args.ignore_path.len());
    for raw_path in &args.ignore_path {
        let parsed = ValuePath::parse_canonical(raw_path).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid sdiff ignore path `{raw_path}`: {error}"
            ))
        })?;
        parsed_ignore_paths.push(parsed);
    }
    parsed_ignore_paths.sort();
    parsed_ignore_paths.dedup();

    let options = SdiffOptions::new(args.value_diff_cap.unwrap_or(DEFAULT_VALUE_DIFF_CAP))
        .with_key_path(parsed_key)
        .with_ignore_paths(parsed_ignore_paths);

    let report = sdiff::structural_diff(left_values, &right_values, options).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!("failed to execute sdiff step: {error}"))
    })?;

    let matched = report.counts.equal
        && report.keys.left_only.is_empty()
        && report.keys.right_only.is_empty()
        && report.values.total == 0;

    let summary = serde_json::to_value(report).map_err(|error| {
        RecipeExecutionErrorKind::Internal(format!("failed to serialize sdiff report: {error}"))
    })?;

    Ok(StepOutcome {
        matched,
        exit_code: if matched { 0 } else { 2 },
        summary,
        next_values: None,
    })
}

fn resolve_assert_source(
    args: AssertStepArgs,
    recipe_base_dir: &Path,
) -> Result<AssertSource, RecipeExecutionErrorKind> {
    let rules_value = match (args.rules, args.rules_file) {
        (Some(value), None) => Some(value),
        (None, Some(path)) => {
            let resolved_path = resolve_recipe_path(recipe_base_dir, path.as_path());
            Some(read_single_value_from_path(
                resolved_path.as_path(),
                "assert.rules_file",
            )?)
        }
        (Some(_), Some(_)) => {
            return Err(RecipeExecutionErrorKind::InputUsage(
                "assert step args `rules` and `rules_file` are mutually exclusive".to_string(),
            ));
        }
        (None, None) => None,
    };

    let schema_value = match (args.schema, args.schema_file) {
        (Some(value), None) => Some(value),
        (None, Some(path)) => {
            let resolved_path = resolve_recipe_path(recipe_base_dir, path.as_path());
            Some(read_single_value_from_path(
                resolved_path.as_path(),
                "assert.schema_file",
            )?)
        }
        (Some(_), Some(_)) => {
            return Err(RecipeExecutionErrorKind::InputUsage(
                "assert step args `schema` and `schema_file` are mutually exclusive".to_string(),
            ));
        }
        (None, None) => None,
    };

    match (rules_value, schema_value) {
        (Some(rules), None) => {
            let parsed: AssertRules = serde_json::from_value(rules).map_err(|error| {
                RecipeExecutionErrorKind::InputUsage(format!(
                    "invalid assert rules in recipe step: {error}"
                ))
            })?;
            Ok(AssertSource::Rules(parsed))
        }
        (None, Some(schema)) => Ok(AssertSource::Schema(schema)),
        (None, None) => Err(RecipeExecutionErrorKind::InputUsage(
            "assert step requires exactly one of `rules`, `rules_file`, `schema`, or `schema_file`"
                .to_string(),
        )),
        (Some(_), Some(_)) => Err(RecipeExecutionErrorKind::InputUsage(
            "assert step cannot combine rules and schema sources".to_string(),
        )),
    }
}

enum AssertSource {
    Rules(AssertRules),
    Schema(Value),
}

fn load_and_parse_recipe(recipe_path: &Path) -> Result<RecipeFile, RecipeExecutionErrorKind> {
    let loaded = load_recipe_value(recipe_path)?;
    parse_loaded_recipe(loaded)
}

fn load_recipe_value(recipe_path: &Path) -> Result<Value, RecipeExecutionErrorKind> {
    let format = io::resolve_input_format(None, Some(recipe_path)).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!(
            "failed to resolve recipe format from `{}`: {error}",
            recipe_path.display()
        ))
    })?;

    if !matches!(format, Format::Json | Format::Yaml) {
        return Err(RecipeExecutionErrorKind::InputUsage(format!(
            "recipe file must be json or yaml by extension: `{}`",
            recipe_path.display()
        )));
    }

    read_single_value_from_path(recipe_path, "recipe.file")
}

fn load_and_parse_lock(lock_path: &Path) -> Result<RecipeLockFile, RecipeExecutionErrorKind> {
    let value = read_single_value_from_path(lock_path, "recipe.lock")?;
    parse_lock(value)
}

fn read_single_value_from_path(
    path: &Path,
    field_label: &str,
) -> Result<Value, RecipeExecutionErrorKind> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!(
            "failed to resolve format for `{field_label}` from `{}`: {error}",
            path.display()
        ))
    })?;

    let mut values = read_values_from_path(path, format)?;
    if values.len() != 1 {
        return Err(RecipeExecutionErrorKind::InputUsage(format!(
            "`{field_label}` must contain exactly one document"
        )));
    }
    Ok(values.remove(0))
}

fn parse_loaded_recipe(value: Value) -> Result<RecipeFile, RecipeExecutionErrorKind> {
    let recipe: RecipeFile = serde_json::from_value(value).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!("invalid recipe schema: {error}"))
    })?;

    if recipe.version != RECIPE_VERSION {
        return Err(RecipeExecutionErrorKind::InputUsage(format!(
            "recipe version must be `{RECIPE_VERSION}`"
        )));
    }

    Ok(recipe)
}

fn parse_lock(value: Value) -> Result<RecipeLockFile, RecipeExecutionErrorKind> {
    let lock: RecipeLockFile = serde_json::from_value(value).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!("invalid recipe lock schema: {error}"))
    })?;
    Ok(lock)
}

fn verify_lock_constraints(
    recipe: &RecipeFile,
    lock: &RecipeLockFile,
    strict: bool,
) -> Result<RecipeReplayLockCheckReport, RecipeExecutionErrorKind> {
    let expected_command_graph_hash = hash_recipe_command_graph(recipe);
    let expected_args_hash = hash_recipe_args(recipe)?;
    let expected_dataq_version = env!("CARGO_PKG_VERSION").to_string();
    let actual_tool_versions = collect_actual_tool_versions(lock);

    let mut mismatches = Vec::new();
    if lock.version != RECIPE_LOCK_VERSION {
        mismatches.push(RecipeReplayLockMismatchReport {
            constraint: "lock.version".to_string(),
            expected: RECIPE_LOCK_VERSION.to_string(),
            actual: lock.version.clone(),
        });
    }
    if lock.command_graph_hash != expected_command_graph_hash {
        mismatches.push(RecipeReplayLockMismatchReport {
            constraint: "lock.command_graph_hash".to_string(),
            expected: expected_command_graph_hash,
            actual: lock.command_graph_hash.clone(),
        });
    }
    if lock.args_hash != expected_args_hash {
        mismatches.push(RecipeReplayLockMismatchReport {
            constraint: "lock.args_hash".to_string(),
            expected: expected_args_hash,
            actual: lock.args_hash.clone(),
        });
    }
    if lock.dataq_version != expected_dataq_version {
        mismatches.push(RecipeReplayLockMismatchReport {
            constraint: "lock.dataq_version".to_string(),
            expected: expected_dataq_version,
            actual: lock.dataq_version.clone(),
        });
    }

    for tool_name in ordered_lock_tool_names(lock) {
        let actual = actual_tool_versions
            .get(tool_name.as_str())
            .cloned()
            .unwrap_or_else(|| format!("error: missing probed value for tool `{tool_name}`"));
        match lock.tool_versions.get(tool_name.as_str()) {
            Some(expected) => {
                if *expected != actual {
                    mismatches.push(RecipeReplayLockMismatchReport {
                        constraint: format!("lock.tool_versions.{tool_name}"),
                        expected: expected.clone(),
                        actual,
                    });
                }
            }
            None => {
                mismatches.push(RecipeReplayLockMismatchReport {
                    constraint: format!("lock.tool_versions.{tool_name}"),
                    expected: "required key in lock.tool_versions".to_string(),
                    actual: "missing".to_string(),
                });
            }
        }
    }

    Ok(RecipeReplayLockCheckReport {
        strict,
        matched: mismatches.is_empty(),
        mismatch_count: mismatches.len(),
        mismatches,
    })
}

fn ordered_lock_tool_names(lock: &RecipeLockFile) -> Vec<String> {
    let mut names: Vec<String> = RECIPE_LOCK_TOOL_ORDER
        .iter()
        .map(|tool| (*tool).to_string())
        .collect();
    for tool in lock.tool_versions.keys() {
        if !RECIPE_LOCK_TOOL_ORDER.contains(&tool.as_str()) {
            names.push(tool.clone());
        }
    }
    names
}

fn collect_actual_tool_versions(lock: &RecipeLockFile) -> BTreeMap<String, String> {
    let mut versions = BTreeMap::new();
    for tool_name in ordered_lock_tool_names(lock) {
        let value = match probe_recipe_lock_tool_version(tool_name.as_str()) {
            Ok(version) => version,
            Err(error) => lock_probe_failure_as_replay_value(error),
        };
        versions.insert(tool_name, value);
    }
    versions
}

fn lock_probe_failure_as_replay_value(error: RecipeExecutionErrorKind) -> String {
    match error {
        RecipeExecutionErrorKind::InputUsage(message)
        | RecipeExecutionErrorKind::Internal(message) => {
            format!("error: {message}")
        }
    }
}

fn parse_step_args<T: for<'de> Deserialize<'de>>(
    kind: &str,
    args: Map<String, Value>,
) -> Result<T, RecipeExecutionErrorKind> {
    serde_json::from_value(Value::Object(args)).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!("invalid `{kind}` step args: {error}"))
    })
}

fn validate_recipe_lock_steps(recipe: &RecipeFile) -> Result<(), RecipeExecutionErrorKind> {
    let mut has_in_memory_values = false;

    for step in &recipe.steps {
        match step.kind.as_str() {
            "canon" => {
                let args: CanonStepArgs = parse_step_args("canon", step.args.clone())?;
                validate_canon_step_args_for_lock(&args)?;
                if args.input.is_none() && !has_in_memory_values {
                    return Err(RecipeExecutionErrorKind::InputUsage(
                        CANON_REQUIRES_INPUT_OR_PRIOR_VALUES.to_string(),
                    ));
                }
                has_in_memory_values = true;
            }
            "assert" => {
                let args: AssertStepArgs = parse_step_args("assert", step.args.clone())?;
                if !has_in_memory_values {
                    return Err(RecipeExecutionErrorKind::InputUsage(
                        ASSERT_REQUIRES_PRIOR_VALUES.to_string(),
                    ));
                }
                validate_assert_step_args_for_lock(&args)?;
            }
            "profile" => {
                let _: ProfileStepArgs = parse_step_args("profile", step.args.clone())?;
                if !has_in_memory_values {
                    return Err(RecipeExecutionErrorKind::InputUsage(
                        PROFILE_REQUIRES_PRIOR_VALUES.to_string(),
                    ));
                }
            }
            "sdiff" => {
                let args: SdiffStepArgs = parse_step_args("sdiff", step.args.clone())?;
                if !has_in_memory_values {
                    return Err(RecipeExecutionErrorKind::InputUsage(
                        SDIFF_REQUIRES_PRIOR_VALUES.to_string(),
                    ));
                }
                validate_sdiff_step_args_for_lock(&args)?;
            }
            _ => {
                return Err(RecipeExecutionErrorKind::InputUsage(format!(
                    "unknown recipe step kind `{}`",
                    step.kind
                )));
            }
        }
    }
    Ok(())
}

fn validate_canon_step_args_for_lock(args: &CanonStepArgs) -> Result<(), RecipeExecutionErrorKind> {
    if args.input.is_some() {
        if let Some(raw) = args.from.as_deref() {
            Format::from_str(raw).map_err(|error| {
                RecipeExecutionErrorKind::InputUsage(format!(
                    "invalid format `{raw}` for `canon.args.input`: {error}"
                ))
            })?;
        } else if let Some(path) = args.input.as_deref() {
            validate_file_backed_arg_format_for_lock(path, "canon.args.input")?;
        }
    }
    Ok(())
}

fn validate_assert_step_args_for_lock(
    args: &AssertStepArgs,
) -> Result<(), RecipeExecutionErrorKind> {
    if args.rules.is_some() && args.rules_file.is_some() {
        return Err(RecipeExecutionErrorKind::InputUsage(
            "assert step args `rules` and `rules_file` are mutually exclusive".to_string(),
        ));
    }
    if args.schema.is_some() && args.schema_file.is_some() {
        return Err(RecipeExecutionErrorKind::InputUsage(
            "assert step args `schema` and `schema_file` are mutually exclusive".to_string(),
        ));
    }

    let has_rules_source = args.rules.is_some() || args.rules_file.is_some();
    let has_schema_source = args.schema.is_some() || args.schema_file.is_some();
    match (has_rules_source, has_schema_source) {
        (false, false) => {
            return Err(RecipeExecutionErrorKind::InputUsage(
                "assert step requires exactly one of `rules`, `rules_file`, `schema`, or `schema_file`"
                    .to_string(),
            ));
        }
        (true, true) => {
            return Err(RecipeExecutionErrorKind::InputUsage(
                "assert step cannot combine rules and schema sources".to_string(),
            ));
        }
        _ => {}
    }

    if let Some(rules) = args.rules.as_ref() {
        let _: AssertRules = serde_json::from_value(rules.clone()).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid assert rules in recipe step: {error}"
            ))
        })?;
    }
    if let Some(path) = args.rules_file.as_deref() {
        validate_file_backed_arg_format_for_lock(path, "assert.rules_file")?;
    }
    if let Some(path) = args.schema_file.as_deref() {
        validate_file_backed_arg_format_for_lock(path, "assert.schema_file")?;
    }

    Ok(())
}

fn validate_sdiff_step_args_for_lock(args: &SdiffStepArgs) -> Result<(), RecipeExecutionErrorKind> {
    if let Some(raw_format) = args.right_from.as_deref() {
        Format::from_str(raw_format).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid format `{raw_format}` for `sdiff.args.right`: {error}"
            ))
        })?;
    } else {
        validate_file_backed_arg_format_for_lock(args.right.as_path(), "sdiff.args.right")?;
    }

    if let Some(key_path) = args.key.as_deref() {
        ValuePath::parse_canonical(key_path).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid sdiff key path `{key_path}`: {error}"
            ))
        })?;
    }
    for raw_path in &args.ignore_path {
        ValuePath::parse_canonical(raw_path).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid sdiff ignore path `{raw_path}`: {error}"
            ))
        })?;
    }

    Ok(())
}

fn hash_recipe_command_graph(recipe: &RecipeFile) -> String {
    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.recipe.lock.command_graph.v1");
    hasher.update_len_prefixed(recipe.version.as_bytes());
    for (index, step) in recipe.steps.iter().enumerate() {
        hasher.update_len_prefixed(index.to_string().as_bytes());
        hasher.update_len_prefixed(step.kind.as_bytes());
    }
    hasher.finish_hex()
}

fn hash_recipe_args(recipe: &RecipeFile) -> Result<String, RecipeExecutionErrorKind> {
    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.recipe.lock.args.v1");
    for (index, step) in recipe.steps.iter().enumerate() {
        hasher.update_len_prefixed(index.to_string().as_bytes());
        hasher.update_len_prefixed(step.kind.as_bytes());

        let canonical_args = canonicalize_value(
            Value::Object(step.args.clone()),
            CanonOptions {
                sort_keys: true,
                normalize_time: false,
            },
        );
        let encoded = serde_json::to_vec(&canonical_args).map_err(|error| {
            RecipeExecutionErrorKind::Internal(format!(
                "failed to serialize recipe step args: {error}"
            ))
        })?;
        hasher.update_len_prefixed(encoded.as_slice());
    }
    Ok(hasher.finish_hex())
}

fn probe_recipe_lock_tools() -> Result<BTreeMap<String, String>, RecipeExecutionErrorKind> {
    let mut versions = BTreeMap::new();
    for tool in RECIPE_LOCK_TOOLS {
        versions.insert(tool.to_string(), probe_recipe_lock_tool_version(tool)?);
    }
    Ok(versions)
}

fn probe_recipe_lock_tool_version(tool_name: &str) -> Result<String, RecipeExecutionErrorKind> {
    let executable = resolve_recipe_lock_tool_executable(tool_name);
    let output = Command::new(&executable)
        .arg("--version")
        .output()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => RecipeExecutionErrorKind::InputUsage(format!(
                "failed to resolve tool `{tool_name}` at `{executable}`: file not found"
            )),
            std::io::ErrorKind::PermissionDenied => RecipeExecutionErrorKind::InputUsage(format!(
                "failed to execute tool `{tool_name}` at `{executable}`: not executable"
            )),
            _ => RecipeExecutionErrorKind::InputUsage(format!(
                "failed to execute tool `{tool_name}` at `{executable}`: {error}"
            )),
        })?;

    if !output.status.success() {
        return Err(RecipeExecutionErrorKind::InputUsage(format!(
            "failed to resolve tool version for `{tool_name}` from `{executable}`: `--version` exited with {}",
            status_label(output.status.code())
        )));
    }

    first_non_empty_line(&output.stdout)
        .or_else(|| first_non_empty_line(&output.stderr))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "failed to resolve tool version for `{tool_name}` from `{executable}`: empty `--version` output"
            ))
        })
}

fn resolve_recipe_lock_tool_executable(tool_name: &str) -> String {
    let env_key = match tool_name {
        "jq" => Some("DATAQ_JQ_BIN"),
        "yq" => Some("DATAQ_YQ_BIN"),
        "mlr" => Some("DATAQ_MLR_BIN"),
        _ => None,
    };

    env_key
        .and_then(|key| std::env::var(key).ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| tool_name.to_string())
}

fn serialize_recipe_lock_report(
    report: &RecipeLockReport,
) -> Result<Vec<u8>, RecipeExecutionErrorKind> {
    let value = serde_json::to_value(report).map_err(|error| {
        RecipeExecutionErrorKind::Internal(format!(
            "failed to serialize recipe lock report: {error}"
        ))
    })?;
    let canonical = canonicalize_value(
        value,
        CanonOptions {
            sort_keys: true,
            normalize_time: false,
        },
    );
    serde_json::to_vec(&canonical).map_err(|error| {
        RecipeExecutionErrorKind::Internal(format!(
            "failed to serialize canonical recipe lock: {error}"
        ))
    })
}

fn resolve_step_input_format(
    explicit: Option<&str>,
    path: &Path,
    field_label: &str,
) -> Result<Format, RecipeExecutionErrorKind> {
    if let Some(raw) = explicit {
        return Format::from_str(raw).map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "invalid format `{raw}` for `{field_label}`: {error}"
            ))
        });
    }

    io::resolve_input_format(None, Some(path)).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!(
            "failed to resolve format for `{field_label}` from `{}`: {error}",
            path.display()
        ))
    })
}

fn validate_file_backed_arg_format_for_lock(
    path: &Path,
    field_label: &str,
) -> Result<(), RecipeExecutionErrorKind> {
    io::resolve_input_format(None, Some(path))
        .map(|_| ())
        .map_err(|error| {
            RecipeExecutionErrorKind::InputUsage(format!(
                "failed to resolve format for `{field_label}` from `{}`: {error}",
                path.display()
            ))
        })
}

fn read_values_from_path(
    path: &Path,
    format: Format,
) -> Result<Vec<Value>, RecipeExecutionErrorKind> {
    let file = File::open(path).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!(
            "failed to open input file `{}`: {error}",
            path.display()
        ))
    })?;

    io::reader::read_values(file, format).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!(
            "failed to read input file `{}`: {error}",
            path.display()
        ))
    })
}

fn resolve_recipe_path(recipe_base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        recipe_base_dir.join(path)
    }
}

fn map_assert_error(error: AssertValidationError) -> RecipeExecutionErrorKind {
    match error {
        AssertValidationError::InputUsage(message) => RecipeExecutionErrorKind::InputUsage(message),
        AssertValidationError::Internal(message) => RecipeExecutionErrorKind::Internal(message),
    }
}

fn first_non_empty_line(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines().find(|line| !line.trim().is_empty())
}

fn status_label(code: Option<i32>) -> String {
    code.map(|value| value.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string())
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::{Map, Value, json};
    use tempfile::tempdir;

    use super::*;

    fn args_map(value: Value) -> Map<String, Value> {
        value.as_object().cloned().expect("args object")
    }

    fn step(kind: &str, args: Value) -> RecipeStep {
        RecipeStep {
            kind: kind.to_string(),
            args: args_map(args),
        }
    }

    fn recipe_with_steps(steps: Vec<RecipeStep>) -> RecipeFile {
        RecipeFile {
            version: RECIPE_VERSION.to_string(),
            steps,
        }
    }

    fn assert_input_usage_contains(error: RecipeExecutionErrorKind, expected: &str) {
        match error {
            RecipeExecutionErrorKind::InputUsage(message) => {
                assert!(
                    message.contains(expected),
                    "expected message to contain `{expected}`, got `{message}`"
                );
            }
            RecipeExecutionErrorKind::Internal(message) => {
                panic!("expected input usage error, got internal: {message}");
            }
        }
    }

    #[cfg(unix)]
    fn write_executable_script(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, body).expect("write script");
        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("set permissions");
    }

    #[test]
    fn parse_loaded_recipe_rejects_version_mismatch() {
        let error = parse_loaded_recipe(json!({
            "version": "dataq.recipe.v0",
            "steps": []
        }))
        .expect_err("version mismatch must fail");
        assert_input_usage_contains(error, "recipe version must be `dataq.recipe.v1`");
    }

    #[test]
    fn load_recipe_value_rejects_non_json_yaml_extension() {
        let temp = tempdir().expect("tempdir");
        let recipe_path = temp.path().join("recipe.txt");
        fs::write(
            &recipe_path,
            b"{\"version\":\"dataq.recipe.v1\",\"steps\":[]}",
        )
        .expect("write recipe");

        let error = load_recipe_value(recipe_path.as_path()).expect_err("txt extension must fail");
        assert_input_usage_contains(error, "failed to resolve recipe format from");
    }

    #[test]
    fn read_single_value_requires_exactly_one_document() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("values.json");
        fs::write(&path, b"[1,2]").expect("write values");

        let error =
            read_single_value_from_path(path.as_path(), "recipe.file").expect_err("must fail");
        assert_input_usage_contains(error, "`recipe.file` must contain exactly one document");
    }

    #[test]
    fn ordered_lock_tool_names_keeps_known_order_and_appends_custom_tools() {
        let lock = RecipeLockFile {
            version: RECIPE_LOCK_VERSION.to_string(),
            command_graph_hash: "a".to_string(),
            args_hash: "b".to_string(),
            tool_versions: BTreeMap::from([("custom".to_string(), "1.0.0".to_string())]),
            dataq_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        assert_eq!(
            ordered_lock_tool_names(&lock),
            vec![
                "jq".to_string(),
                "mlr".to_string(),
                "yq".to_string(),
                "custom".to_string()
            ]
        );
    }

    #[test]
    fn lock_probe_failure_values_are_human_readable() {
        assert_eq!(
            lock_probe_failure_as_replay_value(RecipeExecutionErrorKind::InputUsage(
                "missing tool".to_string()
            )),
            "error: missing tool"
        );
        assert_eq!(
            lock_probe_failure_as_replay_value(RecipeExecutionErrorKind::Internal(
                "boom".to_string()
            )),
            "error: boom"
        );
    }

    #[test]
    fn validate_recipe_lock_steps_requires_prior_values_for_assert() {
        let recipe = recipe_with_steps(vec![step("assert", json!({"schema": true}))]);
        let error = validate_recipe_lock_steps(&recipe).expect_err("assert without prior");
        assert_input_usage_contains(error, ASSERT_REQUIRES_PRIOR_VALUES);
    }

    #[test]
    fn validate_recipe_lock_steps_rejects_unknown_step_kind() {
        let recipe = recipe_with_steps(vec![step("mystery", json!({}))]);
        let error = validate_recipe_lock_steps(&recipe).expect_err("unknown step kind");
        assert_input_usage_contains(error, "unknown recipe step kind `mystery`");
    }

    #[test]
    fn execute_recipe_steps_runs_all_supported_step_kinds() {
        let temp = tempdir().expect("tempdir");
        let left_path = temp.path().join("left.json");
        let right_path = temp.path().join("right.json");
        fs::write(&left_path, b"[{\"id\":\"1\",\"name\":\"A\"}]").expect("write left");
        fs::write(&right_path, b"[{\"id\":1,\"name\":\"A\"}]").expect("write right");

        let recipe = recipe_with_steps(vec![
            step(
                "canon",
                json!({
                    "input": "left.json",
                    "from": "json",
                    "sort_keys": true,
                    "normalize_time": false
                }),
            ),
            step("assert", json!({"schema": true})),
            step("profile", json!({})),
            step(
                "sdiff",
                json!({
                    "right": "right.json",
                    "right_from": "json",
                    "key": "$[\"id\"]"
                }),
            ),
        ]);

        let report =
            execute_recipe_steps(recipe, temp.path(), None).expect("execute all recipe steps");
        assert!(report.matched);
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.steps.len(), 4);
        assert!(report.steps.iter().all(|step| step.matched));
    }

    #[test]
    fn execute_recipe_steps_stops_after_first_mismatch() {
        let temp = tempdir().expect("tempdir");
        let input_path = temp.path().join("input.json");
        fs::write(&input_path, b"[{\"id\":1}]").expect("write input");

        let recipe = recipe_with_steps(vec![
            step(
                "canon",
                json!({
                    "input": "input.json",
                    "from": "json"
                }),
            ),
            step("assert", json!({"schema": false})),
            step("profile", json!({})),
        ]);

        let report =
            execute_recipe_steps(recipe, temp.path(), None).expect("execute mismatching recipe");
        assert!(!report.matched);
        assert_eq!(report.exit_code, 2);
        assert_eq!(report.steps.len(), 2);
        assert_eq!(report.steps[1].kind, "assert");
        assert!(!report.steps[1].matched);
    }

    #[test]
    fn run_from_value_records_pipeline_steps() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join("input.json"), b"[{\"value\":\"1\"}]").expect("write input");
        let recipe_value = json!({
            "version": RECIPE_VERSION,
            "steps": [
                {
                    "kind": "canon",
                    "args": {
                        "input": "input.json",
                        "from": "json"
                    }
                }
            ]
        });

        let execution = run_from_value(recipe_value, Some(temp.path())).expect("run inline recipe");
        assert!(execution.report.matched);
        assert_eq!(
            execution.pipeline_steps,
            vec![
                "load_recipe_inline".to_string(),
                "validate_recipe_schema".to_string(),
                "execute_step_0_canon".to_string(),
            ]
        );
    }

    #[test]
    fn replay_strict_returns_exit_two_when_lock_mismatches() {
        let temp = tempdir().expect("tempdir");
        let recipe_path = temp.path().join("recipe.json");
        let lock_path = temp.path().join("recipe.lock.json");
        fs::write(
            &recipe_path,
            serde_json::to_vec(&json!({
                "version": RECIPE_VERSION,
                "steps": []
            }))
            .expect("serialize recipe"),
        )
        .expect("write recipe");
        fs::write(
            &lock_path,
            serde_json::to_vec(&json!({
                "version": "wrong",
                "command_graph_hash": "x",
                "args_hash": "y",
                "tool_versions": {},
                "dataq_version": "0.0.0"
            }))
            .expect("serialize lock"),
        )
        .expect("write lock");

        let replay_report = replay(recipe_path.as_path(), lock_path.as_path(), true)
            .expect("strict replay should return deterministic mismatch report");
        assert!(!replay_report.report.matched);
        assert_eq!(replay_report.report.exit_code, 2);
        assert!(replay_report.report.steps.is_empty());
        assert_eq!(
            replay_report.pipeline_steps,
            vec![
                "recipe_replay_parse".to_string(),
                "recipe_replay_verify_lock".to_string()
            ]
        );
    }

    #[test]
    fn verify_lock_constraints_can_match_with_actual_probed_values() {
        let recipe = recipe_with_steps(Vec::new());
        let mut lock = RecipeLockFile {
            version: RECIPE_LOCK_VERSION.to_string(),
            command_graph_hash: hash_recipe_command_graph(&recipe),
            args_hash: hash_recipe_args(&recipe).expect("args hash"),
            tool_versions: BTreeMap::new(),
            dataq_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        lock.tool_versions = collect_actual_tool_versions(&lock);

        let report =
            verify_lock_constraints(&recipe, &lock, false).expect("verify lock constraints");
        assert!(report.matched);
        assert_eq!(report.mismatch_count, 0);
        assert!(report.mismatches.is_empty());
    }

    #[test]
    fn resolve_step_input_format_prefers_explicit_and_reports_invalid() {
        assert_eq!(
            resolve_step_input_format(None, Path::new("input.json"), "canon.args.input")
                .expect("infer json format"),
            Format::Json
        );
        assert_eq!(
            resolve_step_input_format(Some("yaml"), Path::new("input.json"), "canon.args.input")
                .expect("explicit yaml"),
            Format::Yaml
        );

        let error = resolve_step_input_format(
            Some("unsupported"),
            Path::new("input.json"),
            "canon.args.input",
        )
        .expect_err("unsupported explicit format");
        assert_input_usage_contains(error, "invalid format `unsupported` for `canon.args.input`");
    }

    #[test]
    fn helper_functions_cover_path_resolution_and_labels() {
        assert_eq!(
            resolve_recipe_path(Path::new("/tmp/base"), Path::new("data.json")),
            PathBuf::from("/tmp/base/data.json")
        );
        assert_eq!(
            resolve_recipe_path(Path::new("/tmp/base"), Path::new("/tmp/abs.json")),
            PathBuf::from("/tmp/abs.json")
        );
        assert_eq!(status_label(Some(9)), "9");
        assert_eq!(status_label(None), "terminated by signal");
        assert_eq!(first_non_empty_line(b"\n\n  \nversion"), Some("version"));
        assert!(first_non_empty_line(&[0xff, 0x00]).is_none());
        assert!(default_true());
    }

    #[test]
    fn map_assert_error_preserves_input_usage_and_internal_kinds() {
        assert!(matches!(
            map_assert_error(AssertValidationError::InputUsage("invalid".to_string())),
            RecipeExecutionErrorKind::InputUsage(_)
        ));
        assert!(matches!(
            map_assert_error(AssertValidationError::Internal("boom".to_string())),
            RecipeExecutionErrorKind::Internal(_)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn probe_recipe_lock_tool_version_handles_empty_output() {
        let temp = tempdir().expect("tempdir");
        let script_path = temp.path().join("tool-empty");
        write_executable_script(&script_path, "#!/bin/sh\nexit 0\n");
        let tool_name = script_path.to_string_lossy().into_owned();

        let error = probe_recipe_lock_tool_version(&tool_name).expect_err("empty output");
        assert_input_usage_contains(error, "empty `--version` output");
    }

    #[cfg(unix)]
    #[test]
    fn probe_recipe_lock_tool_version_uses_first_non_empty_line_from_stderr() {
        let temp = tempdir().expect("tempdir");
        let script_path = temp.path().join("tool-stderr");
        write_executable_script(
            &script_path,
            "#!/bin/sh\necho \"   \"\necho \"tool 1.2.3\" 1>&2\nexit 0\n",
        );
        let tool_name = script_path.to_string_lossy().into_owned();

        let version = probe_recipe_lock_tool_version(&tool_name).expect("version");
        assert_eq!(version, "tool 1.2.3");
    }

    #[cfg(unix)]
    #[test]
    fn probe_recipe_lock_tool_version_reports_non_zero_status() {
        let temp = tempdir().expect("tempdir");
        let script_path = temp.path().join("tool-fail");
        write_executable_script(&script_path, "#!/bin/sh\nexit 5\n");
        let tool_name = script_path.to_string_lossy().into_owned();

        let error = probe_recipe_lock_tool_version(&tool_name).expect_err("status failure");
        assert_input_usage_contains(error, "`--version` exited with 5");
    }
}
