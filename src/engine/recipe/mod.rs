use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::domain::report::{RecipeRunReport, RecipeStepReport};
use crate::domain::rules::AssertRules;
use crate::domain::value_path::ValuePath;
use crate::engine::r#assert::{self, AssertValidationError};
use crate::engine::canon::{CanonOptions, canonicalize_values};
use crate::engine::profile;
use crate::engine::sdiff::{self, DEFAULT_VALUE_DIFF_CAP, SdiffOptions};
use crate::io::{self, Format};

pub const RECIPE_VERSION: &str = "dataq.recipe.v1";

#[derive(Debug, Clone)]
pub struct RecipeExecution {
    pub report: RecipeRunReport,
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

fn execute_loaded_recipe(
    loaded: Value,
    recipe_base_dir: &Path,
    pipeline_steps: &mut Vec<String>,
) -> Result<RecipeExecution, RecipeExecutionError> {
    let recipe = match parse_recipe(loaded) {
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
    let mut current_values: Option<Vec<Value>> = None;
    let mut step_reports = Vec::with_capacity(recipe.steps.len());

    for (index, step) in recipe.steps.into_iter().enumerate() {
        pipeline_steps.push(format!("execute_step_{index}_{}", step.kind));

        let kind = step.kind.clone();
        let outcome = match execute_step(step, current_values.as_deref(), recipe_base_dir) {
            Ok(outcome) => outcome,
            Err(kind) => {
                return Err(RecipeExecutionError {
                    kind,
                    pipeline_steps: pipeline_steps.clone(),
                });
            }
        };

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
            return Ok(RecipeExecution {
                report: RecipeRunReport {
                    matched: false,
                    exit_code: 2,
                    steps: step_reports,
                },
                pipeline_steps: pipeline_steps.clone(),
            });
        }
    }

    Ok(RecipeExecution {
        report: RecipeRunReport {
            matched: true,
            exit_code: 0,
            steps: step_reports,
        },
        pipeline_steps: pipeline_steps.clone(),
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
            "canon step requires `args.input` or prior in-memory values".to_string(),
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
        RecipeExecutionErrorKind::InputUsage(
            "assert step requires prior in-memory values (for example a preceding canon step)"
                .to_string(),
        )
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
        RecipeExecutionErrorKind::InputUsage(
            "profile step requires prior in-memory values (for example a preceding canon step)"
                .to_string(),
        )
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
        RecipeExecutionErrorKind::InputUsage(
            "sdiff step requires prior in-memory values (for example a preceding canon step)"
                .to_string(),
        )
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

fn parse_recipe(value: Value) -> Result<RecipeFile, RecipeExecutionErrorKind> {
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

fn parse_step_args<T: for<'de> Deserialize<'de>>(
    kind: &str,
    args: Map<String, Value>,
) -> Result<T, RecipeExecutionErrorKind> {
    serde_json::from_value(Value::Object(args)).map_err(|error| {
        RecipeExecutionErrorKind::InputUsage(format!("invalid `{kind}` step args: {error}"))
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

const fn default_true() -> bool {
    true
}
