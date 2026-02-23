use std::path::PathBuf;
use std::process;

use clap::{Parser, ValueEnum};
use dataq::io::{self, Format};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(
    name = "dataq",
    version,
    about = "Deterministic data preprocessing CLI"
)]
struct Cli {
    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long, value_enum)]
    from: Option<CliFormat>,

    #[arg(long, value_enum)]
    to: Option<CliFormat>,

    #[arg(long, default_value_t = true)]
    sort_keys: bool,

    #[arg(long, default_value_t = false)]
    normalize_time: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliFormat {
    Json,
    Yaml,
    Csv,
    Jsonl,
}

impl From<CliFormat> for Format {
    fn from(value: CliFormat) -> Self {
        match value {
            CliFormat::Json => Self::Json,
            CliFormat::Yaml => Self::Yaml,
            CliFormat::Csv => Self::Csv,
            CliFormat::Jsonl => Self::Jsonl,
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
    let cli = Cli::parse();
    let from = cli.from.map(Into::into);
    let to = cli.to.map(Into::into);

    let input_format = match io::resolve_input_format(from, cli.input.as_deref()) {
        Ok(format) => format,
        Err(err) => {
            emit_error(
                "format_resolution_error",
                err.to_string(),
                json!({
                    "kind": "input",
                    "input": cli.input.as_ref().map(|path| path.to_string_lossy().into_owned()),
                    "from": from.map(|f| f.as_str()),
                }),
            );
            return 3;
        }
    };

    let output_format = match io::resolve_output_format(to, cli.output.as_deref()) {
        Ok(format) => format,
        Err(err) => {
            emit_error(
                "format_resolution_error",
                err.to_string(),
                json!({
                    "kind": "output",
                    "output": cli.output.as_ref().map(|path| path.to_string_lossy().into_owned()),
                    "to": to.map(|f| f.as_str()),
                }),
            );
            return 3;
        }
    };

    emit_error(
        "command_not_implemented",
        "phase0 provides CLI entry and IO/util foundations only".to_string(),
        json!({
            "resolved_input_format": input_format.as_str(),
            "resolved_output_format": output_format.as_str(),
            "sort_keys": cli.sort_keys,
            "normalize_time": cli.normalize_time,
        }),
    );
    3
}

fn emit_error(error: &'static str, message: String, details: Value) {
    let payload = CliError {
        error,
        message,
        code: 3,
        details,
    };
    match serde_json::to_string(&payload) {
        Ok(serialized) => eprintln!("{serialized}"),
        Err(_) => eprintln!(
            "{{\"error\":\"internal_error\",\"message\":\"failed to serialize error\",\"code\":3}}"
        ),
    }
}
