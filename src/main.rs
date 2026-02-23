use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process;

use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use dataq::cmd::{r#assert, canon, sdiff};
use dataq::domain::error::CanonError;
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
#[command(group(
    ArgGroup::new("assert_source")
        .args(["rules", "schema"])
        .required(true)
        .multiple(false)
))]
struct AssertArgs {
    #[arg(long)]
    rules: Option<PathBuf>,

    #[arg(long)]
    schema: Option<PathBuf>,

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

    match cli.command {
        Commands::Canon(args) => run_canon(args),
        Commands::Assert(args) => run_assert(args),
        Commands::Sdiff(args) => run_sdiff(args),
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

fn run_canon(args: CanonArgs) -> i32 {
    let input_format: Format = args.from.into();
    let output_format = args.to.map(Into::into).unwrap_or(Format::Json);
    let options = canon::CanonCommandOptions {
        sort_keys: args.sort_keys,
        normalize_time: args.normalize_time,
    };

    let stdout = io::stdout();
    let mut output = stdout.lock();
    let result = if let Some(path) = args.input {
        match File::open(&path) {
            Ok(file) => canon::run(file, &mut output, input_format, output_format, options),
            Err(err) => {
                emit_error(
                    "input_usage_error",
                    format!("failed to open input file `{}`: {err}", path.display()),
                    json!({"command": "canon", "input": path}),
                    3,
                );
                return 3;
            }
        }
    } else {
        let stdin = io::stdin();
        canon::run(
            stdin.lock(),
            &mut output,
            input_format,
            output_format,
            options,
        )
    };

    match result {
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

fn run_assert(args: AssertArgs) -> i32 {
    let input = args.input;
    let command_args = r#assert::AssertCommandArgs {
        input: input.clone(),
        from: if input.is_some() {
            None
        } else {
            Some(Format::Json)
        },
        rules: args.rules,
        schema: args.schema,
    };

    let stdin = io::stdin();
    let response = r#assert::run_with_stdin(&command_args, stdin.lock());

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
}

fn run_sdiff(args: SdiffArgs) -> i32 {
    let left_format = match dataq_io::resolve_input_format(None, Some(args.left.as_path())) {
        Ok(format) => format,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"command": "sdiff", "side": "left", "path": args.left}),
                3,
            );
            return 3;
        }
    };
    let right_format = match dataq_io::resolve_input_format(None, Some(args.right.as_path())) {
        Ok(format) => format,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({"command": "sdiff", "side": "right", "path": args.right}),
                3,
            );
            return 3;
        }
    };

    let left_values = match read_values_from_path(&args.left, left_format) {
        Ok(values) => values,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error,
                json!({"command": "sdiff", "side": "left", "path": args.left}),
                3,
            );
            return 3;
        }
    };
    let right_values = match read_values_from_path(&args.right, right_format) {
        Ok(values) => values,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error,
                json!({"command": "sdiff", "side": "right", "path": args.right}),
                3,
            );
            return 3;
        }
    };

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
