use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process;

use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use dataq::cmd::{r#assert, canon, merge, profile, sdiff};
use dataq::domain::error::CanonError;
use dataq::domain::report::{PipelineInput, PipelineInputSource, PipelineReport};
use dataq::engine::merge::MergePolicy;
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
    /// Merge base and overlays with a deterministic merge policy.
    Merge(MergeArgs),
}

#[derive(Debug, clap::Args)]
struct CanonArgs {
    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long, value_enum)]
    from: CliInputFormat,

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
        .args(["rules", "schema", "rules_help"])
        .required(true)
        .multiple(false)
))]
struct AssertArgs {
    #[arg(long)]
    rules: Option<PathBuf>,

    #[arg(long)]
    schema: Option<PathBuf>,

    #[arg(long, conflicts_with = "rules_help")]
    input: Option<PathBuf>,

    /// Print machine-readable rules help for `--rules` and exit.
    #[arg(long, default_value_t = false)]
    rules_help: bool,
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
        Commands::Merge(args) => run_merge(args, emit_pipeline),
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
    let input_format: Format = args.from.into();
    let output_format = args.to.map(Into::into).unwrap_or(Format::Json);
    let options = canon::CanonCommandOptions {
        sort_keys: args.sort_keys,
        normalize_time: args.normalize_time,
    };
    let pipeline_report = build_canon_pipeline_report(&args, input_format, options);

    let stdout = io::stdout();
    let mut output = stdout.lock();
    let exit_code = if let Some(path) = args.input {
        match File::open(&path) {
            Ok(file) => match canon::run(file, &mut output, input_format, output_format, options) {
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
            Err(err) => {
                emit_error(
                    "input_usage_error",
                    format!("failed to open input file `{}`: {err}", path.display()),
                    json!({"command": "canon", "input": path}),
                    3,
                );
                3
            }
        }
    } else {
        let stdin = io::stdin();
        match canon::run(
            stdin.lock(),
            &mut output,
            input_format,
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
    };

    if emit_pipeline {
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_assert(args: AssertArgs, emit_pipeline: bool) -> i32 {
    if args.rules_help {
        if emit_json_stdout(&r#assert::rules_help_payload()) {
            return 0;
        }
        emit_error(
            "internal_error",
            "failed to serialize assert rules help".to_string(),
            json!({"command": "assert"}),
            1,
        );
        return 1;
    }

    let input = args.input.clone();
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
    let pipeline_report =
        build_assert_pipeline_report(&args, input_format, rules_format, schema_format);
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
    let response = r#assert::run_with_stdin(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
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
    };

    if emit_pipeline {
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
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

fn run_sdiff(args: SdiffArgs, emit_pipeline: bool) -> i32 {
    let options = match sdiff::parse_options(
        sdiff::DEFAULT_VALUE_DIFF_CAP,
        args.key.as_deref(),
        &args.ignore_path,
    ) {
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
    let exit_code = match serde_json::to_string(&report) {
        Ok(serialized) => {
            println!("{serialized}");
            0
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

fn read_values_from_path(path: &PathBuf, format: Format) -> Result<Vec<Value>, String> {
    let file = File::open(path)
        .map_err(|error| format!("failed to open input file `{}`: {error}", path.display()))?;
    dataq_io::reader::read_values(file, format).map_err(|error| error.to_string())
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
    input_format: Format,
    options: canon::CanonCommandOptions,
) -> PipelineReport {
    let source = if let Some(path) = &args.input {
        PipelineInputSource::path(
            "input",
            path.display().to_string(),
            Some(input_format.as_str()),
        )
    } else {
        PipelineInputSource::stdin("input", Some(input_format.as_str()))
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

    PipelineReport::new(
        "assert",
        PipelineInput::new(sources),
        r#assert::pipeline_steps(),
        r#assert::deterministic_guards(),
    )
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
