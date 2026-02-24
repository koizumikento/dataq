use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, Cursor, Read};
use std::path::PathBuf;
use std::process::{self, Command};

use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use dataq::cmd::{
    aggregate, r#assert, canon, contract, doctor, emit, gate, join, mcp, merge, profile, recipe,
    sdiff,
};
use dataq::domain::error::CanonError;
use dataq::domain::report::{
    PipelineFingerprint, PipelineInput, PipelineInputSource, PipelineReport,
};
use dataq::engine::aggregate::AggregateMetric;
use dataq::engine::canon::canonicalize_value;
use dataq::engine::join::JoinHow;
use dataq::engine::merge::MergePolicy;
use dataq::io::format::jsonl::JsonlStreamError;
use dataq::io::{self as dataq_io, Format, IoError};
use dataq::util::hash::DeterministicHasher;
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
    /// Run deterministic quality gates.
    Gate(GateArgs),
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
    Doctor(DoctorArgs),
    /// Emit machine-readable output contracts for subcommands.
    Contract(ContractArgs),
    /// Emit static execution plans for existing subcommands.
    Emit(EmitArgs),
    /// Handle a single MCP JSON-RPC request from stdin.
    Mcp,
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
struct GateArgs {
    #[command(subcommand)]
    command: GateSubcommand,
}

#[derive(Debug, Subcommand)]
enum GateSubcommand {
    /// Validate input rows against a JSON Schema gate.
    Schema(GateSchemaArgs),
}

#[derive(Debug, clap::Args)]
struct GateSchemaArgs {
    #[arg(long)]
    schema: PathBuf,

    #[arg(long)]
    input: Option<PathBuf>,

    /// Optional ingest preset before schema validation.
    #[arg(long)]
    from: Option<String>,
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

    #[arg(long = "policy-path", value_name = "path=policy")]
    policy_path: Vec<String>,
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

#[derive(Debug, clap::Args)]
struct DoctorArgs {
    #[arg(long, default_value_t = false)]
    capabilities: bool,

    #[arg(long, value_enum)]
    profile: Option<CliDoctorProfile>,
}

#[derive(Debug, clap::Args)]
#[command(group(
    ArgGroup::new("contract_target")
        .args(["command", "all"])
        .required(true)
        .multiple(false)
))]
struct ContractArgs {
    #[arg(long, value_enum)]
    command: Option<CliContractCommand>,

    #[arg(long, default_value_t = false)]
    all: bool,
}

#[derive(Debug, clap::Args)]
struct EmitArgs {
    #[command(subcommand)]
    command: EmitSubcommand,
}

#[derive(Debug, Subcommand)]
enum EmitSubcommand {
    /// Resolve static stage plan for one subcommand.
    Plan(EmitPlanArgs),
}

#[derive(Debug, clap::Args)]
struct EmitPlanArgs {
    #[arg(long)]
    command: String,

    #[arg(long)]
    args: Option<String>,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliDoctorProfile {
    Core,
    CiJobs,
    Doc,
    Api,
    Notes,
    Book,
    Scan,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliContractCommand {
    Canon,
    Assert,
    GateSchema,
    Sdiff,
    Profile,
    Merge,
    Doctor,
    Recipe,
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

impl From<CliContractCommand> for contract::ContractCommand {
    fn from(value: CliContractCommand) -> Self {
        match value {
            CliContractCommand::Canon => Self::Canon,
            CliContractCommand::Assert => Self::Assert,
            CliContractCommand::GateSchema => Self::GateSchema,
            CliContractCommand::Sdiff => Self::Sdiff,
            CliContractCommand::Profile => Self::Profile,
            CliContractCommand::Merge => Self::Merge,
            CliContractCommand::Doctor => Self::Doctor,
            CliContractCommand::Recipe => Self::Recipe,
        }
    }
}

impl From<CliDoctorProfile> for doctor::DoctorProfile {
    fn from(value: CliDoctorProfile) -> Self {
        match value {
            CliDoctorProfile::Core => Self::Core,
            CliDoctorProfile::CiJobs => Self::CiJobs,
            CliDoctorProfile::Doc => Self::Doc,
            CliDoctorProfile::Api => Self::Api,
            CliDoctorProfile::Notes => Self::Notes,
            CliDoctorProfile::Book => Self::Book,
            CliDoctorProfile::Scan => Self::Scan,
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
        Commands::Gate(args) => run_gate(args, emit_pipeline),
        Commands::Sdiff(args) => run_sdiff(args, emit_pipeline),
        Commands::Profile(args) => run_profile(args, emit_pipeline),
        Commands::Join(args) => run_join(args, emit_pipeline),
        Commands::Aggregate(args) => run_aggregate(args, emit_pipeline),
        Commands::Merge(args) => run_merge(args, emit_pipeline),
        Commands::Recipe(args) => run_recipe(args, emit_pipeline),
        Commands::Doctor(args) => run_doctor(args, emit_pipeline),
        Commands::Contract(args) => run_contract(args, emit_pipeline),
        Commands::Emit(args) => run_emit(args, emit_pipeline),
        Commands::Mcp => run_mcp(),
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
    let mut fingerprint_context = FingerprintContext::default();
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
                        let pipeline_report =
                            build_canon_pipeline_report(&args, input_format, options);
                        emit_pipeline_report_with_context(&pipeline_report, &fingerprint_context);
                    }
                    return 3;
                }
            }
        }

        let resolved_input_format = input_format.expect("input format must be resolved");
        let path_string = path.display().to_string();
        match fs::read(path) {
            Ok(bytes) => {
                fingerprint_context.input_hash =
                    hash_consumed_input_entries(&[ConsumedInputHashEntry {
                        label: "input",
                        source: "path",
                        path: Some(path_string.as_str()),
                        format: Some(resolved_input_format.as_str()),
                        bytes: bytes.as_slice(),
                    }]);
                match run_canon_with_format(
                    Cursor::new(bytes),
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
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    format!("failed to read input file `{}`: {error}", path.display()),
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
                                fingerprint_context.input_hash =
                                    hash_consumed_input_entries(&[ConsumedInputHashEntry {
                                        label: "input",
                                        source: "stdin",
                                        path: None,
                                        format: Some(detected.as_str()),
                                        bytes: input.as_slice(),
                                    }]);
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
        let pipeline_report = build_canon_pipeline_report(&args, input_format, options);
        emit_pipeline_report_with_context(&pipeline_report, &fingerprint_context);
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

fn run_gate(args: GateArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        GateSubcommand::Schema(schema_args) => run_gate_schema(schema_args, emit_pipeline),
    }
}

fn run_gate_schema(args: GateSchemaArgs, emit_pipeline: bool) -> i32 {
    let schema_format = dataq_io::resolve_input_format(None, Some(args.schema.as_path())).ok();
    let preset = gate::resolve_preset(args.from.as_deref()).ok().flatten();
    let input_is_stdin = args
        .input
        .as_deref()
        .map(gate::is_stdin_path)
        .unwrap_or(true);
    let input_format = if input_is_stdin {
        if preset.is_some() {
            Some(Format::Yaml)
        } else {
            Some(Format::Json)
        }
    } else if preset.is_some() {
        Some(Format::Yaml)
    } else {
        args.input
            .as_deref()
            .and_then(|path| dataq_io::resolve_input_format(None, Some(path)).ok())
    };

    let command_args = gate::GateSchemaCommandArgs {
        schema: args.schema.clone(),
        input: args.input.clone(),
        from: args.from.clone(),
    };

    let stdin = io::stdin();
    let (response, trace) = gate::run_schema_with_stdin_and_trace(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize gate schema response".to_string(),
                    json!({"command": "gate.schema"}),
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
                    "failed to serialize gate schema error".to_string(),
                    json!({"command": "gate.schema"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected gate schema exit code: {other}"),
                json!({"command": "gate.schema"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_gate_schema_pipeline_report(
            &args,
            input_is_stdin,
            input_format,
            schema_format,
            &trace,
        );
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
    let response = merge::run_with_policy_paths(&command_args, &args.policy_path);

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
        left: join::JoinCommandInput::Path(args.left.clone()),
        right: join::JoinCommandInput::Path(args.right.clone()),
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
        input: aggregate::AggregateCommandInput::Path(args.input.clone()),
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

    let (left_values, left_bytes) = match read_values_from_path(&args.left, left_format) {
        Ok(value) => value,
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
    let (right_values, right_bytes) = match read_values_from_path(&args.right, right_format) {
        Ok(value) => value,
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
        let fingerprint_context = FingerprintContext {
            input_hash: hash_consumed_input_entries(&[
                ConsumedInputHashEntry {
                    label: "left",
                    source: "path",
                    path: Some(left_path.as_str()),
                    format: Some(left_format.as_str()),
                    bytes: left_bytes.as_slice(),
                },
                ConsumedInputHashEntry {
                    label: "right",
                    source: "path",
                    path: Some(right_path.as_str()),
                    format: Some(right_format.as_str()),
                    bytes: right_bytes.as_slice(),
                },
            ]),
            ..Default::default()
        };
        emit_pipeline_report_with_context(
            &build_sdiff_pipeline_report(&args, left_format_opt, right_format_opt),
            &fingerprint_context,
        );
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

fn run_doctor(args: DoctorArgs, emit_pipeline: bool) -> i32 {
    let command_input = doctor::DoctorCommandInput {
        capabilities: args.capabilities,
        profile: args.profile.map(Into::into),
    };
    let (response, trace) = doctor::run_with_input_and_trace(command_input);
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
        let fingerprint_context = FingerprintContext {
            preferred_tool_versions: trace.tool_versions,
            ..Default::default()
        };
        let pipeline_report = build_doctor_pipeline_report(command_input.profile);
        emit_pipeline_report_with_context(&pipeline_report, &fingerprint_context);
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
        file_path: Some(args.file.clone()),
        recipe: None,
        base_dir: None,
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

fn run_mcp() -> i32 {
    let stdin = io::stdin();
    let stdout = io::stdout();
    mcp::run_single_request(stdin.lock(), stdout.lock())
}

fn run_contract(args: ContractArgs, emit_pipeline: bool) -> i32 {
    let response = if args.all {
        contract::run_all()
    } else if let Some(command) = args.command {
        contract::run_for_command(command.into())
    } else {
        emit_error(
            "input_usage_error",
            "either `--command` or `--all` must be specified".to_string(),
            json!({"command": "contract"}),
            3,
        );
        return 3;
    };

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize contract response".to_string(),
                    json!({"command": "contract"}),
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
                    "failed to serialize contract error".to_string(),
                    json!({"command": "contract"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected contract exit code: {other}"),
                json!({"command": "contract"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_contract_pipeline_report();
        emit_pipeline_report(&pipeline_report);
    }

    exit_code
}

fn run_emit(args: EmitArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        EmitSubcommand::Plan(plan_args) => run_emit_plan(plan_args, emit_pipeline),
    }
}

fn run_emit_plan(args: EmitPlanArgs, emit_pipeline: bool) -> i32 {
    let parsed_args = match emit::parse_args_json(args.args.as_deref()) {
        Ok(values) => values,
        Err(message) => {
            emit_error(
                "input_usage_error",
                message,
                json!({"command": "emit", "subcommand": "plan"}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_emit_plan_pipeline_report());
            }
            return 3;
        }
    };

    let response = emit::run_plan(&emit::EmitPlanCommandArgs {
        command: args.command,
        args: parsed_args,
    });

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize emit plan response".to_string(),
                    json!({"command": "emit", "subcommand": "plan"}),
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
                    "failed to serialize emit plan error".to_string(),
                    json!({"command": "emit", "subcommand": "plan"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected emit plan exit code: {other}"),
                json!({"command": "emit", "subcommand": "plan"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        emit_pipeline_report(&build_emit_plan_pipeline_report());
    }

    exit_code
}

fn read_values_from_path(path: &PathBuf, format: Format) -> Result<(Vec<Value>, Vec<u8>), String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read input file `{}`: {error}", path.display()))?;
    let values = dataq_io::reader::read_values(std::io::Cursor::new(bytes.as_slice()), format)
        .map_err(|error| error.to_string())?;
    Ok((values, bytes))
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

fn build_gate_schema_pipeline_report(
    args: &GateSchemaArgs,
    input_is_stdin: bool,
    input_format: Option<Format>,
    schema_format: Option<Format>,
    trace: &r#assert::AssertPipelineTrace,
) -> PipelineReport {
    let mut sources = Vec::with_capacity(2);
    sources.push(PipelineInputSource::path(
        "schema",
        args.schema.display().to_string(),
        format_label(schema_format),
    ));
    if input_is_stdin {
        sources.push(PipelineInputSource::stdin(
            "input",
            format_label(input_format),
        ));
    } else if let Some(path) = &args.input {
        sources.push(PipelineInputSource::path(
            "input",
            path.display().to_string(),
            format_label(input_format),
        ));
    }

    let mut report = PipelineReport::new(
        "gate.schema",
        PipelineInput::new(sources),
        gate::pipeline_steps(),
        gate::deterministic_guards(),
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

fn build_doctor_pipeline_report(profile: Option<doctor::DoctorProfile>) -> PipelineReport {
    let mut report = PipelineReport::new(
        "doctor",
        PipelineInput::new(Vec::new()),
        doctor::pipeline_steps(profile),
        doctor::deterministic_guards(profile),
    );
    for tool in doctor::pipeline_external_tools(profile) {
        report = report.mark_external_tool_used(&tool);
    }
    report
}

fn build_contract_pipeline_report() -> PipelineReport {
    PipelineReport::new(
        "contract",
        PipelineInput::new(Vec::new()),
        contract::pipeline_steps(),
        contract::deterministic_guards(),
    )
}

fn build_emit_plan_pipeline_report() -> PipelineReport {
    PipelineReport::new(
        "emit",
        PipelineInput::new(Vec::new()),
        emit::pipeline_steps(),
        emit::deterministic_guards(),
    )
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

fn build_pipeline_fingerprint(
    report: &PipelineReport,
    context: &FingerprintContext,
) -> PipelineFingerprint {
    PipelineFingerprint {
        command: report.command.clone(),
        args_hash: hash_normalized_args(),
        input_hash: context.input_hash.clone(),
        tool_versions: collect_used_tool_versions(report, &context.preferred_tool_versions),
        dataq_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

fn hash_normalized_args() -> String {
    let args: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|arg| arg.to_string_lossy().into_owned())
        .filter(|arg| arg != "--emit-pipeline")
        .collect();

    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.execution_fingerprint.args.v1");
    for arg in &args {
        hasher.update_len_prefixed(arg.as_bytes());
    }
    hasher.finish_hex()
}

fn hash_consumed_input_entries(entries: &[ConsumedInputHashEntry<'_>]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }

    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(b"dataq.execution_fingerprint.input.v1");
    for entry in entries {
        hasher.update_len_prefixed(entry.label.as_bytes());
        hasher.update_len_prefixed(entry.source.as_bytes());
        if let Some(path) = entry.path {
            hasher.update_len_prefixed(path.as_bytes());
        }
        hasher.update_len_prefixed(entry.bytes);
        if let Some(format) = entry.format {
            hasher.update_len_prefixed(format.as_bytes());
        } else {
            hasher.update_len_prefixed(&[]);
        }
    }
    Some(hasher.finish_hex())
}

fn collect_used_tool_versions(
    report: &PipelineReport,
    preferred_versions: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    report
        .external_tools
        .iter()
        .filter(|tool| tool.used)
        .map(|tool| {
            let version = preferred_versions
                .get(tool.name.as_str())
                .cloned()
                .unwrap_or_else(|| detect_tool_version(&tool.name, report.command.as_str()));
            (tool.name.clone(), version)
        })
        .collect()
}

fn detect_tool_version(tool_name: &str, command_name: &str) -> String {
    let executable = resolve_tool_executable(tool_name, command_name);
    match Command::new(&executable).arg("--version").output() {
        Ok(output) if output.status.success() => first_non_empty_line(&output.stdout)
            .or_else(|| first_non_empty_line(&output.stderr))
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "error: empty --version output".to_string()),
        Ok(output) => format!(
            "error: --version exited with {}",
            status_label(output.status.code())
        ),
        Err(error) => match error.kind() {
            io::ErrorKind::NotFound => "error: unavailable in PATH".to_string(),
            io::ErrorKind::PermissionDenied => "error: not executable".to_string(),
            other => format!("error: failed to execute --version ({other:?})"),
        },
    }
}

fn resolve_tool_executable(tool_name: &str, command_name: &str) -> String {
    if !matches!(command_name, "assert" | "gate.schema") {
        return tool_name.to_string();
    }

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

fn first_non_empty_line(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines().find(|line| !line.trim().is_empty())
}

fn status_label(code: Option<i32>) -> String {
    code.map(|value| value.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string())
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
    emit_pipeline_report_with_context(report, &FingerprintContext::default());
}

fn emit_pipeline_report_with_context(report: &PipelineReport, context: &FingerprintContext) {
    let report = report
        .clone()
        .with_fingerprint(build_pipeline_fingerprint(report, context));
    match serde_json::to_string(&report) {
        Ok(serialized) => eprintln!("{serialized}"),
        Err(error) => emit_error(
            "internal_error",
            format!("failed to serialize pipeline report: {error}"),
            json!({"command": "emit_pipeline"}),
            1,
        ),
    }
}

#[derive(Debug, Default, Clone)]
struct FingerprintContext {
    input_hash: Option<String>,
    preferred_tool_versions: BTreeMap<String, String>,
}

struct ConsumedInputHashEntry<'a> {
    label: &'a str,
    source: &'a str,
    path: Option<&'a str>,
    format: Option<&'a str>,
    bytes: &'a [u8],
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
