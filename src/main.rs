use std::fs::File;
use std::io::{self, BufRead, Cursor, Read};
use std::path::PathBuf;
use std::process;

use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use dataq::cmd::{aggregate, r#assert, canon, doctor, join, merge, profile, recipe, sdiff};
use dataq::domain::error::CanonError;
use dataq::domain::report::{PipelineInput, PipelineInputSource, PipelineReport};
use dataq::engine::aggregate::AggregateMetric;
use dataq::engine::canon::canonicalize_value;
use dataq::engine::join::JoinHow;
use dataq::engine::merge::MergePolicy;
use dataq::io::format::jsonl::JsonlStreamError;
use dataq::io::{self as dataq_io, Format, IoError};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(
    name = "dataq",
    version,
    about = "Deterministic data preprocessing CLI"
)]
struct Cli {
    #[arg(long, global = true, default_value_t = false)]
    emit_pipeline: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Canonicalize input deterministically.
    Canon(CanonArgs),
    /// Validate input values against rule definitions.
    Assert(AssertArgs),
    /// Compare structural differences across two datasets.
    Sdiff(SdiffArgs),
    /// Generate deterministic field profile statistics.
    Profile(ProfileArgs),
    /// Join two datasets by key using deterministic JSON output.
    Join(JoinArgs),
    /// Aggregate grouped metrics with deterministic JSON output.
    Aggregate(AggregateArgs),
    /// Merge base and overlays with a deterministic merge policy.
    Merge(MergeArgs),
    /// Execute a declarative deterministic recipe.
    Recipe(RecipeArgs),
    /// Diagnose jq/yq/mlr availability and executability.
    Doctor,
}

#[derive(Debug, clap::Args)]
struct CanonArgs {
    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long, value_enum)]
    from: Option<CliInputFormat>,

    #[arg(long, value_enum)]
    to: Option<CanonOutputFormat>,

    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    sort_keys: bool,

    #[arg(long, default_value_t = false)]
    normalize_time: bool,
}

#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("assert_source")
        .args(["rules", "schema", "rules_help", "schema_help"])
        .required(true)
        .multiple(false)
))]
struct AssertArgs {
    #[arg(long)]
    rules: Option<PathBuf>,

    #[arg(long)]
    schema: Option<PathBuf>,

    #[arg(long, conflicts_with_all = ["rules_help", "schema_help"])]
    input: Option<PathBuf>,

    /// Normalize raw input into assert-friendly records before validation.
    #[arg(long, value_enum, conflicts_with_all = ["rules_help", "schema_help"])]
    normalize: Option<CliAssertNormalizeMode>,

    /// Print machine-readable rules help for `--rules` and exit.
    #[arg(long, default_value_t = false)]
    rules_help: bool,

    /// Print machine-readable JSON Schema help for `--schema` and exit.
    #[arg(long, default_value_t = false)]
    schema_help: bool,
}

#[derive(Debug, clap::Args)]
struct SdiffArgs {
    #[arg(long)]
    left: PathBuf,

    #[arg(long)]
    right: PathBuf,

    #[arg(long)]
    key: Option<String>,

    #[arg(long = "ignore-path")]
    ignore_path: Vec<String>,

    #[arg(long, default_value_t = false)]
    fail_on_diff: bool,

    #[arg(long, default_value_t = sdiff::DEFAULT_VALUE_DIFF_CAP)]
    value_diff_cap: usize,
}

#[derive(Debug, clap::Args)]
struct ProfileArgs {
    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long, value_enum)]
    from: CliInputFormat,
}

#[derive(Debug, clap::Args)]
struct MergeArgs {
    #[arg(long)]
    base: PathBuf,

    #[arg(long, required = true, num_args = 1..)]
    overlay: Vec<PathBuf>,

    #[arg(long, value_enum)]
    policy: CliMergePolicy,
}

#[derive(Debug, clap::Args)]
struct JoinArgs {
    #[arg(long)]
    left: PathBuf,

    #[arg(long)]
    right: PathBuf,

    #[arg(long)]
    on: String,

    #[arg(long, value_enum)]
    how: CliJoinHow,
}

#[derive(Debug, clap::Args)]
struct AggregateArgs {
    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    group_by: String,

    #[arg(long, value_enum)]
    metric: CliAggregateMetric,

    #[arg(long)]
    target: String,
}

#[derive(Debug, clap::Args)]
struct RecipeArgs {
    #[command(subcommand)]
    command: RecipeSubcommand,
}

#[derive(Debug, Subcommand)]
enum RecipeSubcommand {
    /// Run a declarative recipe file.
    Run(RecipeRunArgs),
}

#[derive(Debug, clap::Args)]
struct RecipeRunArgs {
    #[arg(long)]
    file: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliInputFormat {
    Json,
    Yaml,
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CanonOutputFormat {
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMergePolicy {
    LastWins,
    DeepMerge,
    ArrayReplace,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliJoinHow {
    Inner,
    Left,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliAggregateMetric {
    Count,
    Sum,
    Avg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliAssertNormalizeMode {
    GithubActionsJobs,
    GitlabCiJobs,
}

impl From<CliInputFormat> for Format {
    fn from(value: CliInputFormat) -> Self {
        match value {
            CliInputFormat::Json => Self::Json,
            CliInputFormat::Yaml => Self::Yaml,
            CliInputFormat::Csv => Self::Csv,
            CliInputFormat::Jsonl => Self::Jsonl,
        }
    }
}

impl From<CanonOutputFormat> for Format {
    fn from(value: CanonOutputFormat) -> Self {
        match value {
            CanonOutputFormat::Json => Self::Json,
            CanonOutputFormat::Jsonl => Self::Jsonl,
        }
    }
}

impl From<CliMergePolicy> for MergePolicy {
    fn from(value: CliMergePolicy) -> Self {
        match value {
            CliMergePolicy::LastWins => Self::LastWins,
            CliMergePolicy::DeepMerge => Self::DeepMerge,
            CliMergePolicy::ArrayReplace => Self::ArrayReplace,
        }
    }
}

impl From<CliJoinHow> for JoinHow {
    fn from(value: CliJoinHow) -> Self {
        match value {
            CliJoinHow::Inner => Self::Inner,
            CliJoinHow::Left => Self::Left,
        }
    }
}

impl From<CliAggregateMetric> for AggregateMetric {
    fn from(value: CliAggregateMetric) -> Self {
        match value {
            CliAggregateMetric::Count => Self::Count,
            CliAggregateMetric::Sum => Self::Sum,
            CliAggregateMetric::Avg => Self::Avg,
        }
    }
}

impl From<CliAssertNormalizeMode> for r#assert::AssertInputNormalizeMode {
    fn from(value: CliAssertNormalizeMode) -> Self {
        match value {
            CliAssertNormalizeMode::GithubActionsJobs => Self::GithubActionsJobs,
            CliAssertNormalizeMode::GitlabCiJobs => Self::GitlabCiJobs,
        }
    }
}

#[derive(Serialize)]
struct CliError<'a> {
    error: &'a str,
    message: String,
    code: i32,
    details: Value,
}

fn main() {
    process::exit(run());
}

fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => return handle_parse_error(error),
    };

    let emit_pipeline = cli.emit_pipeline;
    match cli.command {
        Commands::Canon(args) => run_canon(args, emit_pipeline),
        Commands::Assert(args) => run_assert(args, emit_pipeline),
        Commands::Sdiff(args) => run_sdiff(args, emit_pipeline),
        Commands::Profile(args) => run_profile(args, emit_pipeline),
        Commands::Join(args) => run_join(args, emit_pipeline),
        Commands::Aggregate(args) => run_aggregate(args, emit_pipeline),
        Commands::Merge(args) => run_merge(args, emit_pipeline),
        Commands::Recipe(args) => run_recipe(args, emit_pipeline),
        Commands::Doctor => run_doctor(emit_pipeline),
    }
}

fn handle_parse_error(error: clap::Error) -> i32 {
    match error.kind() {
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
            print!("{error}");
            0
        }
        _ => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"kind": "cli_parse_error"}),
                3,
            );
            3
        }
    }
}

fn run_canon(args: CanonArgs, emit_pipeline: bool) -> i32 {
    let output_format = args.to.map(Into::into).unwrap_or(Format::Json);
    let options = canon::CanonCommandOptions {
        sort_keys: args.sort_keys,
        normalize_time: args.normalize_time,
    };
    let mut input_format = args.from.map(Into::into);

    let stdout = io::stdout();
    let mut output = stdout.lock();
    let exit_code = if let Some(path) = args.input.as_ref() {
        if input_format.is_none() {
            match dataq_io::resolve_input_format(None, Some(path.as_path())) {
                Ok(resolved) => input_format = Some(resolved),
                Err(error) => {
                    emit_error(
                        "input_usage_error",
                        error.to_string(),
                        json!({"command": "canon", "input": path}),
                        3,
                    );
                    if emit_pipeline {
                        emit_pipeline_report(&build_canon_pipeline_report(
                            &args,
                            input_format,
                            options,
                        ));
                    }
                    return 3;
                }
            }
        }

        let resolved_input_format = input_format.expect("input format must be resolved");
        match File::open(path) {
            Ok(file) => match run_canon_with_format(
                file,
                &mut output,
                resolved_input_format,
                output_format,
                options,
            ) {
                Ok(()) => 0,
                Err(error) => {
                    let (exit_code, error_kind) = map_canon_error(&error);
                    emit_error(
                        error_kind,
                        error.to_string(),
                        json!({"command": "canon"}),
                        exit_code,
                    );
                    exit_code
                }
            },
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    format!("failed to open input file `{}`: {error}", path.display()),
                    json!({"command": "canon", "input": path}),
                    3,
                );
                3
            }
        }
    } else {
        match input_format {
            Some(resolved_input_format) => {
                let stdin = io::stdin();
                match run_canon_with_format(
                    stdin.lock(),
                    &mut output,
                    resolved_input_format,
                    output_format,
                    options,
                ) {
                    Ok(()) => 0,
                    Err(error) => {
                        let (exit_code, error_kind) = map_canon_error(&error);
                        emit_error(
                            error_kind,
                            error.to_string(),
                            json!({"command": "canon"}),
                            exit_code,
                        );
                        exit_code
                    }
                }
            }
            None => {
                if output_format == Format::Jsonl {
                    match run_canon_jsonl_autodetect_stdin(&mut output, options) {
                        Ok(detected) => {
                            input_format = Some(detected);
                            0
                        }
                        Err(CanonStdinAutodetectError::Input(error)) => {
                            emit_error(
                                "input_usage_error",
                                error.to_string(),
                                json!({"command": "canon"}),
                                3,
                            );
                            3
                        }
                        Err(CanonStdinAutodetectError::Canon(error)) => {
                            let (exit_code, error_kind) = map_canon_error(&error);
                            emit_error(
                                error_kind,
                                error.to_string(),
                                json!({"command": "canon"}),
                                exit_code,
                            );
                            exit_code
                        }
                    }
                } else {
                    let stdin = io::stdin();
                    let mut input = Vec::new();
                    if let Err(error) = stdin.lock().read_to_end(&mut input) {
                        emit_error(
                            "input_usage_error",
                            format!("failed to read stdin: {error}"),
                            json!({"command": "canon"}),
                            3,
                        );
                        3
                    } else {
                        match dataq_io::autodetect_stdin_input_format(&input) {
                            Ok(detected) => {
                                input_format = Some(detected);
                                match run_canon_with_format(
                                    Cursor::new(input),
                                    &mut output,
                                    detected,
                                    output_format,
                                    options,
                                ) {
                                    Ok(()) => 0,
                                    Err(error) => {
                                        let (exit_code, error_kind) = map_canon_error(&error);
                                        emit_error(
                                            error_kind,
                                            error.to_string(),
                                            json!({"command": "canon"}),
                                            exit_code,
                                        );
                                        exit_code
                                    }
                                }
                            }
                            Err(error) => {
                                emit_error(
                                    "input_usage_error",
                                    error.to_string(),
                                    json!({"command": "canon"}),
                                    3,
                                );
                                3
                            }
                        }
                    }
                }
            }
        }
    };

    if emit_pipeline {
        emit_pipeline_report(&build_canon_pipeline_report(&args, input_format, options));
    }
    exit_code
}

fn run_assert(args: AssertArgs, emit_pipeline: bool) -> i32 {
    let input = args.input.clone();
    let normalize_mode = args.normalize.map(Into::into);
    let input_format = input
        .as_deref()
        .map(|path| dataq_io::resolve_input_format(None, Some(path)).ok())
        .unwrap_or(Some(Format::Json));
    let rules_format = args
        .rules
        .as_deref()
        .and_then(|path| dataq_io::resolve_input_format(None, Some(path)).ok());
    let schema_format = args
        .schema
        .as_deref()
        .and_then(|path| dataq_io::resolve_input_format(None, Some(path)).ok());
    let mut steps = r#assert::pipeline_steps(normalize_mode);
    let mut deterministic_guards = r#assert::deterministic_guards(normalize_mode);
    let mut trace = r#assert::AssertPipelineTrace::default();

    let exit_code = if args.rules_help {
        steps = vec!["emit_assert_rules_help".to_string()];
        deterministic_guards = vec![
            "rust_native_execution".to_string(),
            "assert_help_payload_static_schema".to_string(),
        ];
        emit_assert_rules_help()
    } else if args.schema_help {
        steps = vec!["emit_assert_schema_help".to_string()];
        deterministic_guards = vec![
            "rust_native_execution".to_string(),
            "assert_help_payload_static_schema".to_string(),
        ];
        emit_assert_schema_help()
    } else {
        let command_args = r#assert::AssertCommandArgs {
            input: input.clone(),
            from: if input.is_some() {
                None
            } else {
                Some(Format::Json)
            },
            rules: args.rules.clone(),
            schema: args.schema.clone(),
        };

        let stdin = io::stdin();
        let (response, run_trace) = r#assert::run_with_stdin_and_normalize_with_trace(
            &command_args,
            stdin.lock(),
            normalize_mode,
        );
        trace = run_trace;

        match response.exit_code {
            0 | 2 => {
                if emit_json_stdout(&response.payload) {
                    response.exit_code
                } else {
                    emit_error(
                        "internal_error",
                        "failed to serialize assert response".to_string(),
                        json!({"command": "assert"}),
                        1,
                    );
                    1
                }
            }
            3 | 1 => {
                if emit_json_stderr(&response.payload) {
                    response.exit_code
                } else {
                    emit_error(
                        "internal_error",
                        "failed to serialize assert error".to_string(),
                        json!({"command": "assert"}),
                        1,
                    );
                    1
                }
            }
            other => {
                emit_error(
                    "internal_error",
                    format!("unexpected assert exit code: {other}"),
                    json!({"command": "assert"}),
                    1,
                );
                1
            }
        }
    };

    if emit_pipeline {
        let pipeline_report = build_assert_pipeline_report(
            &args,
            input_format,
            rules_format,
            schema_format,
            steps,
            deterministic_guards,
            &trace,
        );
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn emit_assert_rules_help() -> i32 {
    if emit_json_stdout(&r#assert::rules_help_payload()) {
        0
    } else {
        emit_error(
            "internal_error",
            "failed to serialize assert rules help".to_string(),
            json!({"command": "assert"}),
            1,
        );
        1
    }
}

fn emit_assert_schema_help() -> i32 {
    if emit_json_stdout(&r#assert::schema_help_payload()) {
        0
    } else {
        emit_error(
            "internal_error",
            "failed to serialize assert schema help".to_string(),
            json!({"command": "assert"}),
            1,
        );
        1
    }
}

fn run_merge(args: MergeArgs, emit_pipeline: bool) -> i32 {
    let base_format = dataq_io::resolve_input_format(None, Some(args.base.as_path())).ok();
    let overlay_formats: Vec<Option<Format>> = args
        .overlay
        .iter()
        .map(|path| dataq_io::resolve_input_format(None, Some(path.as_path())).ok())
        .collect();
    let pipeline_report = build_merge_pipeline_report(&args, base_format, &overlay_formats);

    let command_args = merge::MergeCommandArgs {
        base: args.base.clone(),
        overlays: args.overlay.clone(),
        policy: args.policy.into(),
    };
    let response = merge::run(&command_args);

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize merge output".to_string(),
                    json!({"command": "merge"}),
                    1,
                );
                1
            }
        }
        3 => {
            if emit_json_stderr(&response.payload) {
                3
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize merge error".to_string(),
                    json!({"command": "merge"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected merge exit code: {other}"),
                json!({"command": "merge"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_join(args: JoinArgs, emit_pipeline: bool) -> i32 {
    let left_format = dataq_io::resolve_input_format(None, Some(args.left.as_path())).ok();
    let right_format = dataq_io::resolve_input_format(None, Some(args.right.as_path())).ok();
    let command_args = join::JoinCommandArgs {
        left: args.left.clone(),
        right: args.right.clone(),
        on: args.on.clone(),
        how: args.how.into(),
    };
    let (response, trace) = join::run_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize join output".to_string(),
                    json!({"command": "join"}),
                    1,
                );
                1
            }
        }
        3 => {
            if emit_json_stderr(&response.payload) {
                3
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize join error".to_string(),
                    json!({"command": "join"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected join exit code: {other}"),
                json!({"command": "join"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_join_pipeline_report(&args, left_format, right_format, &trace);
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_aggregate(args: AggregateArgs, emit_pipeline: bool) -> i32 {
    let input_format = dataq_io::resolve_input_format(None, Some(args.input.as_path())).ok();
    let command_args = aggregate::AggregateCommandArgs {
        input: args.input.clone(),
        group_by: args.group_by.clone(),
        metric: args.metric.into(),
        target: args.target.clone(),
    };
    let (response, trace) = aggregate::run_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize aggregate output".to_string(),
                    json!({"command": "aggregate"}),
                    1,
                );
                1
            }
        }
        3 => {
            if emit_json_stderr(&response.payload) {
                3
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize aggregate error".to_string(),
                    json!({"command": "aggregate"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected aggregate exit code: {other}"),
                json!({"command": "aggregate"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_aggregate_pipeline_report(&args, input_format, &trace);
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_sdiff(args: SdiffArgs, emit_pipeline: bool) -> i32 {
    let options =
        match sdiff::parse_options(args.value_diff_cap, args.key.as_deref(), &args.ignore_path) {
            Ok(options) => options,
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    error.to_string(),
                    json!({
                        "command": "sdiff",
                        "key": args.key,
                        "ignore_path": args.ignore_path,
                    }),
                    3,
                );
                if emit_pipeline {
                    emit_pipeline_report(&build_sdiff_pipeline_report(&args, None, None));
                }
                return 3;
            }
        };

    let left_path = args.left.display().to_string();
    let right_path = args.right.display().to_string();

    let left_format = match dataq_io::resolve_input_format(None, Some(args.left.as_path())) {
        Ok(format) => format,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"command": "sdiff", "side": "left", "path": &left_path}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_sdiff_pipeline_report(&args, None, None));
            }
            return 3;
        }
    };
    let left_format_opt = Some(left_format);

    let right_format = match dataq_io::resolve_input_format(None, Some(args.right.as_path())) {
        Ok(format) => format,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"command": "sdiff", "side": "right", "path": &right_path}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_sdiff_pipeline_report(&args, left_format_opt, None));
            }
            return 3;
        }
    };
    let right_format_opt = Some(right_format);

    let left_values = match read_values_from_path(&args.left, left_format) {
        Ok(values) => values,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error,
                json!({"command": "sdiff", "side": "left", "path": &left_path}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_sdiff_pipeline_report(
                    &args,
                    left_format_opt,
                    right_format_opt,
                ));
            }
            return 3;
        }
    };
    let right_values = match read_values_from_path(&args.right, right_format) {
        Ok(values) => values,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error,
                json!({"command": "sdiff", "side": "right", "path": &right_path}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_sdiff_pipeline_report(
                    &args,
                    left_format_opt,
                    right_format_opt,
                ));
            }
            return 3;
        }
    };

    let report = match sdiff::execute_with_options(&left_values, &right_values, options) {
        Ok(report) => report,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({
                    "command": "sdiff",
                    "key": args.key,
                    "ignore_path": args.ignore_path,
                }),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_sdiff_pipeline_report(
                    &args,
                    left_format_opt,
                    right_format_opt,
                ));
            }
            return 3;
        }
    };
    let success_exit_code = if args.fail_on_diff && report.values.total > 0 {
        2
    } else {
        0
    };
    let exit_code = match serde_json::to_string(&report) {
        Ok(serialized) => {
            println!("{serialized}");
            success_exit_code
        }
        Err(error) => {
            emit_error(
                "internal_error",
                format!("failed to serialize diff report: {error}"),
                json!({"command": "sdiff"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        emit_pipeline_report(&build_sdiff_pipeline_report(
            &args,
            left_format_opt,
            right_format_opt,
        ));
    }
    exit_code
}

fn run_profile(args: ProfileArgs, emit_pipeline: bool) -> i32 {
    let input_format = Some(args.from.into());
    let pipeline_report = build_profile_pipeline_report(&args, input_format);

    let command_args = profile::ProfileCommandArgs {
        input: args.input,
        from: input_format,
    };

    let stdin = io::stdin();
    let response = profile::run_with_stdin(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize profile response".to_string(),
                    json!({"command": "profile"}),
                    1,
                );
                1
            }
        }
        3 | 1 => {
            if emit_json_stderr(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize profile error".to_string(),
                    json!({"command": "profile"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected profile exit code: {other}"),
                json!({"command": "profile"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_doctor(emit_pipeline: bool) -> i32 {
    let response = doctor::run();
    let exit_code = match response.exit_code {
        0 | 3 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize doctor response".to_string(),
                    json!({"command": "doctor"}),
                    1,
                );
                1
            }
        }
        1 => {
            if emit_json_stderr(&response.payload) {
                1
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize doctor error".to_string(),
                    json!({"command": "doctor"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected doctor exit code: {other}"),
                json!({"command": "doctor"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_doctor_pipeline_report();
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_recipe(args: RecipeArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        RecipeSubcommand::Run(run_args) => run_recipe_run(run_args, emit_pipeline),
    }
}

fn run_recipe_run(args: RecipeRunArgs, emit_pipeline: bool) -> i32 {
    let recipe_format = dataq_io::resolve_input_format(None, Some(args.file.as_path())).ok();
    let command_args = recipe::RecipeCommandArgs {
        file: args.file.clone(),
    };
    let (response, trace) = recipe::run_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize recipe response".to_string(),
                    json!({"command": "recipe"}),
                    1,
                );
                1
            }
        }
        3 | 1 => {
            if emit_json_stderr(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize recipe error".to_string(),
                    json!({"command": "recipe"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected recipe exit code: {other}"),
                json!({"command": "recipe"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_recipe_pipeline_report(&args, recipe_format, trace.steps);
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn read_values_from_path(path: &PathBuf, format: Format) -> Result<Vec<Value>, String> {
    let file = File::open(path)
        .map_err(|error| format!("failed to open input file `{}`: {error}", path.display()))?;
    dataq_io::reader::read_values(file, format).map_err(|error| error.to_string())
}

fn run_canon_with_format<R: Read, W: io::Write>(
    input: R,
    output: W,
    input_format: Format,
    output_format: Format,
    options: canon::CanonCommandOptions,
) -> Result<(), CanonError> {
    if output_format == Format::Jsonl {
        return run_canon_jsonl_stream(input, output, input_format, options);
    }
    canon::run(input, output, input_format, output_format, options)
}

#[derive(Debug)]
enum CanonStdinAutodetectError {
    Input(IoError),
    Canon(CanonError),
}

fn run_canon_jsonl_autodetect_stdin<W: io::Write>(
    mut output: W,
    options: canon::CanonCommandOptions,
) -> Result<Format, CanonStdinAutodetectError> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut buffered_input = Vec::new();
    let mut prefetched_values = Vec::new();
    let mut non_empty_lines = 0usize;

    while non_empty_lines < 2 {
        let mut line = Vec::new();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(IoError::from)
            .map_err(CanonStdinAutodetectError::Input)?;
        if bytes_read == 0 {
            break;
        }
        buffered_input.extend_from_slice(&line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        non_empty_lines += 1;
        let trimmed = trim_ascii_whitespace(&line);
        match serde_json::from_slice(trimmed) {
            Ok(parsed) => prefetched_values.push(parsed),
            Err(_) => {
                return run_canon_jsonl_with_buffered_stdin(
                    reader,
                    buffered_input,
                    output,
                    options,
                );
            }
        }
    }

    if non_empty_lines >= 2 {
        let canon_options = options.into();
        for value in prefetched_values {
            let canonical = canonicalize_value(value, canon_options);
            write_jsonl_stream_value(&mut output, &canonical)
                .map_err(CanonStdinAutodetectError::Canon)?;
        }
        dataq_io::reader::read_jsonl_stream(reader, |value| {
            let canonical = canonicalize_value(value, canon_options);
            write_jsonl_stream_value(&mut output, &canonical)
        })
        .map_err(|error| match error {
            JsonlStreamError::Read(source) => {
                CanonStdinAutodetectError::Canon(CanonError::ReadInput {
                    format: Format::Jsonl,
                    source,
                })
            }
            JsonlStreamError::Emit(source) => CanonStdinAutodetectError::Canon(source),
        })?;
        return Ok(Format::Jsonl);
    }

    run_canon_jsonl_with_buffered_stdin(reader, buffered_input, output, options)
}

fn run_canon_jsonl_with_buffered_stdin<R: Read, W: io::Write>(
    mut reader: R,
    mut buffered_input: Vec<u8>,
    output: W,
    options: canon::CanonCommandOptions,
) -> Result<Format, CanonStdinAutodetectError> {
    reader
        .read_to_end(&mut buffered_input)
        .map_err(IoError::from)
        .map_err(CanonStdinAutodetectError::Input)?;
    let detected = dataq_io::autodetect_stdin_input_format(&buffered_input)
        .map_err(CanonStdinAutodetectError::Input)?;
    run_canon_with_format(
        Cursor::new(buffered_input),
        output,
        detected,
        Format::Jsonl,
        options,
    )
    .map_err(CanonStdinAutodetectError::Canon)?;
    Ok(detected)
}

fn trim_ascii_whitespace(input: &[u8]) -> &[u8] {
    let Some(start) = input.iter().position(|byte| !byte.is_ascii_whitespace()) else {
        return &input[0..0];
    };
    let end = input
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .expect("start implies end")
        + 1;
    &input[start..end]
}

fn run_canon_jsonl_stream<R: Read, W: io::Write>(
    input: R,
    mut output: W,
    input_format: Format,
    options: canon::CanonCommandOptions,
) -> Result<(), CanonError> {
    let canon_options = options.into();
    if input_format == Format::Jsonl {
        return dataq_io::reader::read_jsonl_stream(input, |value| {
            let canonical = canonicalize_value(value, canon_options);
            write_jsonl_stream_value(&mut output, &canonical)
        })
        .map_err(|error| match error {
            JsonlStreamError::Read(source) => CanonError::ReadInput {
                format: Format::Jsonl,
                source,
            },
            JsonlStreamError::Emit(source) => source,
        });
    }

    let values = dataq_io::reader::read_values(input, input_format).map_err(|source| {
        CanonError::ReadInput {
            format: input_format,
            source,
        }
    })?;
    for value in values {
        let canonical = canonicalize_value(value, canon_options);
        write_jsonl_stream_value(&mut output, &canonical)?;
    }
    Ok(())
}

fn write_jsonl_stream_value<W: io::Write>(output: &mut W, value: &Value) -> Result<(), CanonError> {
    dataq_io::format::jsonl::write_jsonl_value(&mut *output, value).map_err(|source| {
        CanonError::WriteOutput {
            format: Format::Jsonl,
            source,
        }
    })?;
    output.flush().map_err(|source| CanonError::WriteOutput {
        format: Format::Jsonl,
        source: IoError::from(source),
    })
}

fn map_canon_error(error: &CanonError) -> (i32, &'static str) {
    match error {
        CanonError::ReadInput { .. } => (3, "input_usage_error"),
        CanonError::WriteOutput { source, .. } => match source {
            IoError::Io(_) => (1, "internal_error"),
            _ => (3, "input_usage_error"),
        },
    }
}

fn build_canon_pipeline_report(
    args: &CanonArgs,
    input_format: Option<Format>,
    options: canon::CanonCommandOptions,
) -> PipelineReport {
    let source = if let Some(path) = &args.input {
        PipelineInputSource::path(
            "input",
            path.display().to_string(),
            format_label(input_format),
        )
    } else {
        PipelineInputSource::stdin("input", format_label(input_format))
    };

    PipelineReport::new(
        "canon",
        PipelineInput::new(vec![source]),
        canon::pipeline_steps(),
        canon::deterministic_guards(options),
    )
}

fn build_assert_pipeline_report(
    args: &AssertArgs,
    input_format: Option<Format>,
    rules_format: Option<Format>,
    schema_format: Option<Format>,
    steps: Vec<String>,
    deterministic_guards: Vec<String>,
    trace: &r#assert::AssertPipelineTrace,
) -> PipelineReport {
    let mut sources = Vec::with_capacity(2);
    if let Some(path) = &args.rules {
        sources.push(PipelineInputSource::path(
            "rules",
            path.display().to_string(),
            format_label(rules_format),
        ));
    }
    if let Some(path) = &args.schema {
        sources.push(PipelineInputSource::path(
            "schema",
            path.display().to_string(),
            format_label(schema_format),
        ));
    }
    if !args.rules_help && !args.schema_help {
        if let Some(path) = &args.input {
            sources.push(PipelineInputSource::path(
                "input",
                path.display().to_string(),
                format_label(input_format),
            ));
        } else {
            sources.push(PipelineInputSource::stdin(
                "input",
                format_label(input_format),
            ));
        }
    }

    let mut report = PipelineReport::new(
        "assert",
        PipelineInput::new(sources),
        steps,
        deterministic_guards,
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_sdiff_pipeline_report(
    args: &SdiffArgs,
    left_format: Option<Format>,
    right_format: Option<Format>,
) -> PipelineReport {
    PipelineReport::new(
        "sdiff",
        PipelineInput::new(vec![
            PipelineInputSource::path(
                "left",
                args.left.display().to_string(),
                format_label(left_format),
            ),
            PipelineInputSource::path(
                "right",
                args.right.display().to_string(),
                format_label(right_format),
            ),
        ]),
        sdiff::pipeline_steps(),
        sdiff::deterministic_guards(),
    )
}

fn build_profile_pipeline_report(
    args: &ProfileArgs,
    input_format: Option<Format>,
) -> PipelineReport {
    let source = if let Some(path) = &args.input {
        PipelineInputSource::path(
            "input",
            path.display().to_string(),
            format_label(input_format),
        )
    } else {
        PipelineInputSource::stdin("input", format_label(input_format))
    };

    PipelineReport::new(
        "profile",
        PipelineInput::new(vec![source]),
        profile::pipeline_steps(),
        profile::deterministic_guards(),
    )
}

fn build_merge_pipeline_report(
    args: &MergeArgs,
    base_format: Option<Format>,
    overlay_formats: &[Option<Format>],
) -> PipelineReport {
    let mut sources = Vec::with_capacity(1 + args.overlay.len());
    sources.push(PipelineInputSource::path(
        "base",
        args.base.display().to_string(),
        format_label(base_format),
    ));
    for (idx, overlay) in args.overlay.iter().enumerate() {
        let label = format!("overlay[{idx}]");
        let format = overlay_formats.get(idx).copied().flatten();
        sources.push(PipelineInputSource::path(
            label,
            overlay.display().to_string(),
            format_label(format),
        ));
    }

    PipelineReport::new(
        "merge",
        PipelineInput::new(sources),
        merge::pipeline_steps(),
        merge::deterministic_guards(),
    )
}

fn build_join_pipeline_report(
    args: &JoinArgs,
    left_format: Option<Format>,
    right_format: Option<Format>,
    trace: &join::JoinPipelineTrace,
) -> PipelineReport {
    let mut report = PipelineReport::new(
        "join",
        PipelineInput::new(vec![
            PipelineInputSource::path(
                "left",
                args.left.display().to_string(),
                format_label(left_format),
            ),
            PipelineInputSource::path(
                "right",
                args.right.display().to_string(),
                format_label(right_format),
            ),
        ]),
        join::pipeline_steps(),
        join::deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_aggregate_pipeline_report(
    args: &AggregateArgs,
    input_format: Option<Format>,
    trace: &aggregate::AggregatePipelineTrace,
) -> PipelineReport {
    let mut report = PipelineReport::new(
        "aggregate",
        PipelineInput::new(vec![PipelineInputSource::path(
            "input",
            args.input.display().to_string(),
            format_label(input_format),
        )]),
        aggregate::pipeline_steps(),
        aggregate::deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_doctor_pipeline_report() -> PipelineReport {
    let mut report = PipelineReport::new(
        "doctor",
        PipelineInput::new(Vec::new()),
        doctor::pipeline_steps(),
        doctor::deterministic_guards(),
    );
    for tool in ["jq", "yq", "mlr"] {
        report = report.mark_external_tool_used(tool);
    }
    report
}

fn build_recipe_pipeline_report(
    args: &RecipeRunArgs,
    recipe_format: Option<Format>,
    steps: Vec<String>,
) -> PipelineReport {
    let step_names = if steps.is_empty() {
        vec![
            "load_recipe_file".to_string(),
            "validate_recipe_schema".to_string(),
        ]
    } else {
        steps
    };

    PipelineReport::new(
        "recipe",
        PipelineInput::new(vec![PipelineInputSource::path(
            "recipe",
            args.file.display().to_string(),
            format_label(recipe_format),
        )]),
        step_names,
        recipe::deterministic_guards(),
    )
}

fn format_label(format: Option<Format>) -> Option<&'static str> {
    format.map(Format::as_str)
}

fn emit_json_stdout(value: &Value) -> bool {
    match serde_json::to_string(value) {
        Ok(serialized) => {
            println!("{serialized}");
            true
        }
        Err(_) => false,
    }
}

fn emit_json_stderr(value: &Value) -> bool {
    match serde_json::to_string(value) {
        Ok(serialized) => {
            eprintln!("{serialized}");
            true
        }
        Err(_) => false,
    }
}

fn emit_pipeline_report(report: &PipelineReport) {
    match serde_json::to_string(report) {
        Ok(serialized) => eprintln!("{serialized}"),
        Err(error) => emit_error(
            "internal_error",
            format!("failed to serialize pipeline report: {error}"),
            json!({"command": "emit_pipeline"}),
            1,
        ),
    }
}

fn emit_error(error: &'static str, message: String, details: Value, code: i32) {
    let payload = CliError {
        error,
        message,
        code,
        details,
    };
    match serde_json::to_string(&payload) {
        Ok(serialized) => eprintln!("{serialized}"),
        Err(_) => eprintln!(
            "{{\"error\":\"internal_error\",\"message\":\"failed to serialize error\",\"code\":1}}"
        ),
    }
}
