use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process;

use clap::error::ErrorKind;
use clap::{Parser, Subcommand, ValueEnum};
use dataq::cmd::{r#assert, canon, sdiff};
use dataq::domain::error::CanonError;
use dataq::domain::report::{PipelineInput, PipelineInputSource, PipelineReport};
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
struct AssertArgs {
    #[arg(long)]
    rules: PathBuf,

    #[arg(long)]
    input: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct SdiffArgs {
    #[arg(long)]
    left: PathBuf,

    #[arg(long)]
    right: PathBuf,
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
    let input = args.input.clone();
    let input_format = input
        .as_deref()
        .map(|path| dataq_io::resolve_input_format(None, Some(path)).ok())
        .unwrap_or(Some(Format::Json));
    let rules_format = dataq_io::resolve_input_format(None, Some(args.rules.as_path())).ok();
    let pipeline_report = build_assert_pipeline_report(&args, input_format, rules_format);
    let command_args = r#assert::AssertCommandArgs {
        input: input.clone(),
        from: if input.is_some() {
            None
        } else {
            Some(Format::Json)
        },
        rules: args.rules,
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

fn run_sdiff(args: SdiffArgs, emit_pipeline: bool) -> i32 {
    let mut left_format_opt = None;
    let mut right_format_opt = None;
    let left_path = args.left.display().to_string();
    let right_path = args.right.display().to_string();

    let exit_code = match dataq_io::resolve_input_format(None, Some(args.left.as_path())) {
        Ok(left_format) => {
            left_format_opt = Some(left_format);
            match dataq_io::resolve_input_format(None, Some(args.right.as_path())) {
                Ok(right_format) => {
                    right_format_opt = Some(right_format);
                    match read_values_from_path(&args.left, left_format) {
                        Ok(left_values) => match read_values_from_path(&args.right, right_format) {
                            Ok(right_values) => {
                                let report = sdiff::execute(&left_values, &right_values);
                                match serde_json::to_string(&report) {
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
                                }
                            }
                            Err(error) => {
                                emit_error(
                                    "input_usage_error",
                                    error,
                                    json!({"command": "sdiff", "side": "right", "path": &right_path}),
                                    3,
                                );
                                3
                            }
                        },
                        Err(error) => {
                            emit_error(
                                "input_usage_error",
                                error,
                                json!({"command": "sdiff", "side": "left", "path": &left_path}),
                                3,
                            );
                            3
                        }
                    }
                }
                Err(error) => {
                    emit_error(
                        "input_usage_error",
                        error.to_string(),
                        json!({"command": "sdiff", "side": "right", "path": &right_path}),
                        3,
                    );
                    3
                }
            }
        }
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"command": "sdiff", "side": "left", "path": &left_path}),
                3,
            );
            3
        }
    };

    if emit_pipeline {
        let pipeline_report = build_sdiff_pipeline_report(&args, left_format_opt, right_format_opt);
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
) -> PipelineReport {
    let mut sources = Vec::with_capacity(2);
    sources.push(PipelineInputSource::path(
        "rules",
        args.rules.display().to_string(),
        format_label(rules_format),
    ));
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
