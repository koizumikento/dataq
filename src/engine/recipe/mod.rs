use std::collections::BTreeMap;
use std::env;
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
        versions.insert(
            tool_name.clone(),
            probe_recipe_lock_tool_version(tool_name.as_str()),
        );
    }
    versions
}

fn probe_recipe_lock_tool_version(tool_name: &str) -> String {
    if !RECIPE_LOCK_TOOL_ORDER.contains(&tool_name) {
        return format!("error: unsupported tool `{tool_name}`");
    }

    let executable = resolve_recipe_lock_tool_executable(tool_name);
    match Command::new(executable).arg("--version").output() {
        Ok(output) => {
            let captured = first_non_empty_line(&output.stdout)
                .or_else(|| first_non_empty_line(&output.stderr))
                .unwrap_or_else(|| "error: empty --version output".to_string());
            if output.status.success() {
                captured
            } else {
                format!(
                    "error: `--version` exited with status {} ({captured})",
                    status_label(output.status.code()),
                )
            }
        }
        Err(error) => match error.kind() {
            std::io::ErrorKind::NotFound => "error: unavailable in PATH".to_string(),
            std::io::ErrorKind::PermissionDenied => "error: not executable".to_string(),
            _ => format!("error: failed to execute --version ({error})"),
        },
    }
}

fn resolve_recipe_lock_tool_executable(tool_name: &str) -> String {
    match tool_name {
        "jq" => env::var("DATAQ_JQ_BIN").unwrap_or_else(|_| "jq".to_string()),
        "yq" => env::var("DATAQ_YQ_BIN").unwrap_or_else(|_| "yq".to_string()),
        "mlr" => env::var("DATAQ_MLR_BIN").unwrap_or_else(|_| "mlr".to_string()),
        _ => tool_name.to_string(),
    }
}

fn first_non_empty_line(bytes: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines()
        .find(|line| !line.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn status_label(code: Option<i32>) -> String {
    code.map(|value| value.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string())
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
        hasher.update_len_prefixed(&(index as u64).to_le_bytes());
        hasher.update_len_prefixed(step.kind.as_bytes());
    }
    hasher.finish_hex()
}

fn hash_recipe_args(recipe: &RecipeFile) -> Result<String, RecipeExecutionErrorKind> {
    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.recipe.lock.args.v1");
    for (index, step) in recipe.steps.iter().enumerate() {
        hasher.update_len_prefixed(&(index as u64).to_le_bytes());
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
