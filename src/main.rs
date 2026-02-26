use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Command};

use clap::error::ErrorKind;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use dataq::cmd::{
    aggregate, r#assert, canon, codex, contract, diff, doctor, emit, gate, ingest, ingest_api,
    ingest_yaml_jobs, join, mcp, merge, profile, recipe, scan, sdiff, transform,
};
use dataq::domain::error::CanonError;
use dataq::domain::ingest::IngestYamlJobsMode;
use dataq::domain::report::{
    PipelineFingerprint, PipelineInput, PipelineInputSource, PipelineReport,
};
use dataq::engine::aggregate::AggregateMetric;
use dataq::engine::canon::canonicalize_value;
use dataq::engine::ingest as ingest_engine;
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
    /// Ingest external sources into deterministic JSON records.
    Ingest(IngestArgs),
    /// Validate input values against rule definitions.
    Assert(AssertArgs),
    /// Run deterministic quality gates.
    Gate(GateArgs),
    /// Compare structural differences across two datasets.
    Sdiff(SdiffArgs),
    /// Compare normalized outputs resolved from source presets or files.
    Diff(DiffArgs),
    /// Generate deterministic field profile statistics.
    Profile(ProfileArgs),
    /// Join two datasets by key using deterministic JSON output.
    Join(JoinArgs),
    /// Aggregate grouped metrics with deterministic JSON output.
    Aggregate(AggregateArgs),
    /// Transform rowsets with fixed `jq -> mlr` stages.
    Transform(TransformArgs),
    /// Scan repository text with deterministic structured match output.
    Scan(ScanArgs),
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
    /// Install or manage dataq Codex skill assets.
    Codex(CodexArgs),
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
struct IngestArgs {
    #[command(subcommand)]
    command: IngestSubcommand,
}

#[derive(Debug, Subcommand)]
enum IngestSubcommand {
    /// Fetch and normalize one API response deterministically.
    Api(IngestApiArgs),
    /// Extract and normalize job definitions from YAML.
    YamlJobs(IngestYamlJobsArgs),
    /// Fetch and normalize `nb` notes into deterministic JSON.
    Notes(IngestNotesArgs),
    /// Extract deterministic schema fields from a document.
    Doc(IngestDocArgs),
    /// Parse mdBook `SUMMARY.md` and metadata from a book root.
    Book(IngestBookArgs),
}

#[derive(Debug, clap::Args)]
struct IngestApiArgs {
    #[arg(long)]
    url: String,

    #[arg(
        long,
        value_enum,
        default_value_t = CliIngestApiMethod::Get,
        ignore_case = true
    )]
    method: CliIngestApiMethod,

    #[arg(long = "header")]
    header: Vec<String>,

    #[arg(long)]
    body: Option<String>,

    #[arg(long)]
    expect_status: Option<u16>,
}

#[derive(Debug, clap::Args)]
struct IngestYamlJobsArgs {
    #[arg(long)]
    input: PathBuf,

    #[arg(long, value_enum)]
    mode: CliIngestYamlJobsMode,
}

#[derive(Debug, clap::Args)]
struct IngestNotesArgs {
    #[arg(long = "tag")]
    tag: Vec<String>,

    #[arg(long)]
    since: Option<String>,

    #[arg(long)]
    until: Option<String>,

    #[arg(long, value_enum, default_value_t = CliIngestNotesOutput::Json)]
    to: CliIngestNotesOutput,
}

#[derive(Debug, clap::Args)]
struct IngestDocArgs {
    #[arg(long)]
    input: String,

    #[arg(long, value_enum)]
    from: CliIngestDocFormat,
}

#[derive(Debug, clap::Args)]
struct IngestBookArgs {
    #[arg(long)]
    root: PathBuf,

    #[arg(long, default_value_t = false)]
    include_files: bool,
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
    /// Apply policy rules and report deterministic violations.
    Policy(GatePolicyArgs),
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
struct GatePolicyArgs {
    #[arg(long)]
    rules: PathBuf,

    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long, value_enum)]
    source: Option<CliGatePolicySource>,
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
struct DiffArgs {
    #[command(subcommand)]
    command: DiffSubcommand,
}

#[derive(Debug, Subcommand)]
enum DiffSubcommand {
    /// Compare two sources (file or preset) via structural diff.
    Source(DiffSourceArgs),
}

#[derive(Debug, clap::Args)]
struct DiffSourceArgs {
    #[arg(long)]
    left: String,

    #[arg(long)]
    right: String,

    #[arg(long, default_value_t = false)]
    fail_on_diff: bool,
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
struct TransformArgs {
    #[command(subcommand)]
    command: TransformSubcommand,
}

#[derive(Debug, Subcommand)]
enum TransformSubcommand {
    /// Run fixed two-stage rowset transform (`jq` then `mlr`).
    Rowset(TransformRowsetArgs),
}

#[derive(Debug, clap::Args)]
struct TransformRowsetArgs {
    /// Input path or `-` for stdin.
    #[arg(long)]
    input: String,

    /// jq filter used in stage 1.
    #[arg(long = "jq-filter")]
    jq_filter: String,

    /// mlr verb/arguments used in stage 2.
    #[arg(long = "mlr", required = true, num_args = 1.., allow_hyphen_values = true)]
    mlr: Vec<String>,
}

#[derive(Debug, clap::Args)]
struct ScanArgs {
    #[command(subcommand)]
    command: ScanSubcommand,
}

#[derive(Debug, Subcommand)]
enum ScanSubcommand {
    /// Scan text files for a regex pattern.
    Text(ScanTextArgs),
}

#[derive(Debug, clap::Args)]
struct ScanTextArgs {
    #[arg(long)]
    pattern: String,

    #[arg(long)]
    path: Option<PathBuf>,

    #[arg(long = "glob")]
    glob: Vec<String>,

    #[arg(long)]
    max_matches: Option<usize>,

    #[arg(long, default_value_t = false)]
    policy_mode: bool,

    /// Enable optional jq projection stage for parsed matches.
    #[arg(long, default_value_t = false)]
    jq_project: bool,
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
    /// Generate deterministic lock metadata for a recipe file.
    Lock(RecipeLockArgs),
    /// Replay a recipe under lock constraints.
    Replay(RecipeReplayArgs),
}

#[derive(Debug, clap::Args)]
struct RecipeRunArgs {
    #[arg(long)]
    file: PathBuf,
}

#[derive(Debug, clap::Args)]
struct RecipeLockArgs {
    #[arg(long)]
    file: PathBuf,

    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct RecipeReplayArgs {
    #[arg(long)]
    file: PathBuf,

    #[arg(long)]
    lock: PathBuf,

    #[arg(long, default_value_t = false)]
    strict: bool,
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

#[derive(Debug, clap::Args)]
struct CodexArgs {
    #[command(subcommand)]
    command: CodexSubcommand,
}

#[derive(Debug, Subcommand)]
enum CodexSubcommand {
    /// Install embedded dataq skill assets into a Codex skills root.
    InstallSkill(CodexInstallSkillArgs),
}

#[derive(Debug, clap::Args)]
struct CodexInstallSkillArgs {
    #[arg(long)]
    dest: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    force: bool,
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
enum CliIngestApiMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliIngestYamlJobsMode {
    GithubActions,
    GitlabCi,
    GenericMap,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliIngestNotesOutput {
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliIngestDocFormat {
    Md,
    Html,
    Docx,
    Rst,
    Latex,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliGatePolicySource {
    ScanText,
    IngestDoc,
    IngestApi,
    IngestNotes,
    IngestBook,
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
    IngestApi,
    Ingest,
    Assert,
    GateSchema,
    Gate,
    Sdiff,
    DiffSource,
    Profile,
    IngestDoc,
    #[value(name = "ingest-notes")]
    IngestNotes,
    IngestBook,
    Scan,
    #[value(name = "transform-rowset")]
    TransformRowset,
    Merge,
    Doctor,
    #[value(name = "recipe-run", alias = "recipe")]
    RecipeRun,
    RecipeLock,
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

impl From<CliGatePolicySource> for gate::GatePolicySourcePreset {
    fn from(value: CliGatePolicySource) -> Self {
        match value {
            CliGatePolicySource::ScanText => Self::ScanText,
            CliGatePolicySource::IngestDoc => Self::IngestDoc,
            CliGatePolicySource::IngestApi => Self::IngestApi,
            CliGatePolicySource::IngestNotes => Self::IngestNotes,
            CliGatePolicySource::IngestBook => Self::IngestBook,
        }
    }
}

impl From<CliContractCommand> for contract::ContractCommand {
    fn from(value: CliContractCommand) -> Self {
        match value {
            CliContractCommand::Canon => Self::Canon,
            CliContractCommand::IngestApi => Self::IngestApi,
            CliContractCommand::Ingest => Self::Ingest,
            CliContractCommand::Assert => Self::Assert,
            CliContractCommand::GateSchema => Self::GateSchema,
            CliContractCommand::Gate => Self::Gate,
            CliContractCommand::Sdiff => Self::Sdiff,
            CliContractCommand::DiffSource => Self::DiffSource,
            CliContractCommand::Profile => Self::Profile,
            CliContractCommand::IngestDoc => Self::IngestDoc,
            CliContractCommand::IngestNotes => Self::IngestNotes,
            CliContractCommand::IngestBook => Self::IngestBook,
            CliContractCommand::Scan => Self::Scan,
            CliContractCommand::TransformRowset => Self::TransformRowset,
            CliContractCommand::Merge => Self::Merge,
            CliContractCommand::Doctor => Self::Doctor,
            CliContractCommand::RecipeRun => Self::RecipeRun,
            CliContractCommand::RecipeLock => Self::RecipeLock,
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

impl From<CliIngestApiMethod> for ingest_api::IngestApiMethod {
    fn from(value: CliIngestApiMethod) -> Self {
        match value {
            CliIngestApiMethod::Get => Self::Get,
            CliIngestApiMethod::Post => Self::Post,
            CliIngestApiMethod::Put => Self::Put,
            CliIngestApiMethod::Patch => Self::Patch,
            CliIngestApiMethod::Delete => Self::Delete,
        }
    }
}

impl From<CliIngestYamlJobsMode> for IngestYamlJobsMode {
    fn from(value: CliIngestYamlJobsMode) -> Self {
        match value {
            CliIngestYamlJobsMode::GithubActions => Self::GithubActions,
            CliIngestYamlJobsMode::GitlabCi => Self::GitlabCi,
            CliIngestYamlJobsMode::GenericMap => Self::GenericMap,
        }
    }
}

impl From<CliIngestDocFormat> for ingest::IngestDocInputFormat {
    fn from(value: CliIngestDocFormat) -> Self {
        match value {
            CliIngestDocFormat::Md => Self::Md,
            CliIngestDocFormat::Html => Self::Html,
            CliIngestDocFormat::Docx => Self::Docx,
            CliIngestDocFormat::Rst => Self::Rst,
            CliIngestDocFormat::Latex => Self::Latex,
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
        Commands::Ingest(args) => run_ingest(args, emit_pipeline),
        Commands::Assert(args) => run_assert(args, emit_pipeline),
        Commands::Gate(args) => run_gate(args, emit_pipeline),
        Commands::Sdiff(args) => run_sdiff(args, emit_pipeline),
        Commands::Diff(args) => run_diff(args, emit_pipeline),
        Commands::Profile(args) => run_profile(args, emit_pipeline),
        Commands::Join(args) => run_join(args, emit_pipeline),
        Commands::Aggregate(args) => run_aggregate(args, emit_pipeline),
        Commands::Transform(args) => run_transform(args, emit_pipeline),
        Commands::Scan(args) => run_scan(args, emit_pipeline),
        Commands::Merge(args) => run_merge(args, emit_pipeline),
        Commands::Recipe(args) => run_recipe(args, emit_pipeline),
        Commands::Doctor(args) => run_doctor(args, emit_pipeline),
        Commands::Contract(args) => run_contract(args, emit_pipeline),
        Commands::Emit(args) => run_emit(args, emit_pipeline),
        Commands::Codex(args) => run_codex(args, emit_pipeline),
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

fn run_ingest(args: IngestArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        IngestSubcommand::Api(api_args) => run_ingest_api(api_args, emit_pipeline),
        IngestSubcommand::YamlJobs(args) => run_ingest_yaml_jobs(args, emit_pipeline),
        IngestSubcommand::Notes(args) => run_ingest_notes(args, emit_pipeline),
        IngestSubcommand::Doc(args) => run_ingest_doc(args, emit_pipeline),
        IngestSubcommand::Book(args) => run_ingest_book(args, emit_pipeline),
    }
}

fn run_ingest_api(args: IngestApiArgs, emit_pipeline: bool) -> i32 {
    let command_args = ingest_api::IngestApiCommandArgs {
        url: args.url.clone(),
        method: args.method.into(),
        headers: args.header.clone(),
        body: args.body.clone(),
        expect_status: args.expect_status,
    };

    let (response, trace) = ingest_api::run_with_trace(&command_args);
    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize ingest api response".to_string(),
                    json!({"command": "ingest api"}),
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
                    "failed to serialize ingest api error".to_string(),
                    json!({"command": "ingest api"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected ingest api exit code: {other}"),
                json!({"command": "ingest api"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_ingest_api_pipeline_report(&args, &trace);
        emit_pipeline_report(&pipeline_report);
    }

    exit_code
}

fn run_ingest_yaml_jobs(args: IngestYamlJobsArgs, emit_pipeline: bool) -> i32 {
    let mode: IngestYamlJobsMode = args.mode.into();
    let input_is_stdin = ingest_yaml_jobs::path_is_stdin(args.input.as_path());
    let command_args = ingest_yaml_jobs::IngestYamlJobsCommandArgs {
        input: if input_is_stdin {
            ingest_yaml_jobs::IngestYamlJobsInput::Stdin
        } else {
            ingest_yaml_jobs::IngestYamlJobsInput::Path(args.input.clone())
        },
        mode,
    };

    let stdin = io::stdin();
    let (response, trace) = ingest_yaml_jobs::run_with_stdin_and_trace(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize ingest yaml-jobs output".to_string(),
                    json!({"command": "ingest yaml-jobs"}),
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
                    "failed to serialize ingest yaml-jobs error".to_string(),
                    json!({"command": "ingest yaml-jobs"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected ingest yaml-jobs exit code: {other}"),
                json!({"command": "ingest yaml-jobs"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let report = build_ingest_yaml_jobs_pipeline_report(&args, mode, input_is_stdin, &trace);
        emit_pipeline_report(&report);
    }
    exit_code
}

fn run_ingest_notes(args: IngestNotesArgs, emit_pipeline: bool) -> i32 {
    let time_range =
        match ingest_engine::resolve_time_range(args.since.as_deref(), args.until.as_deref()) {
            Ok(value) => value,
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    error.to_string(),
                    json!({"command": "ingest.notes"}),
                    3,
                );
                return 3;
            }
        };

    let command_args = ingest::IngestNotesCommandArgs {
        tags: args.tag.clone(),
        since: time_range.since,
        until: time_range.until,
    };
    let (response, trace) = ingest::run_notes_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 => match (args.to, &response.payload) {
            (CliIngestNotesOutput::Json, _) => {
                if emit_json_stdout(&response.payload) {
                    0
                } else {
                    emit_error(
                        "internal_error",
                        "failed to serialize ingest notes response".to_string(),
                        json!({"command": "ingest.notes"}),
                        1,
                    );
                    1
                }
            }
            (CliIngestNotesOutput::Jsonl, Value::Array(values)) => {
                if emit_jsonl_stdout(values) {
                    0
                } else {
                    emit_error(
                        "internal_error",
                        "failed to serialize ingest notes JSONL response".to_string(),
                        json!({"command": "ingest.notes"}),
                        1,
                    );
                    1
                }
            }
            (CliIngestNotesOutput::Jsonl, _) => {
                emit_error(
                    "internal_error",
                    "ingest notes response must be an array for JSONL output".to_string(),
                    json!({"command": "ingest.notes"}),
                    1,
                );
                1
            }
        },
        3 | 1 => {
            if emit_json_stderr(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize ingest notes error".to_string(),
                    json!({"command": "ingest.notes"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected ingest notes exit code: {other}"),
                json!({"command": "ingest.notes"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_ingest_notes_pipeline_report(&trace);
        emit_pipeline_report(&pipeline_report);
    }

    exit_code
}

fn run_ingest_doc(args: IngestDocArgs, emit_pipeline: bool) -> i32 {
    let from: ingest::IngestDocInputFormat = args.from.into();
    let input_path = if args.input == "-" {
        None
    } else {
        Some(PathBuf::from(&args.input))
    };

    let command_args = ingest::IngestDocCommandArgs {
        input: input_path.clone(),
        from,
    };
    let stdin = io::stdin();
    let response = ingest::run_with_stdin(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize ingest doc response".to_string(),
                    json!({"command": "ingest.doc"}),
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
                    "failed to serialize ingest doc error".to_string(),
                    json!({"command": "ingest.doc"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected ingest doc exit code: {other}"),
                json!({"command": "ingest.doc"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_ingest_doc_pipeline_report(&args, input_path, from);
        emit_pipeline_report(&pipeline_report);
    }

    exit_code
}

fn run_ingest_book(args: IngestBookArgs, emit_pipeline: bool) -> i32 {
    let (response, trace) = ingest::run_book_with_trace(&ingest::IngestBookCommandArgs {
        root: args.root.clone(),
        include_files: args.include_files,
        verify_mdbook_meta: ingest::resolve_verify_mdbook_meta(),
    });

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize ingest book response".to_string(),
                    json!({"command": "ingest.book"}),
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
                    "failed to serialize ingest book error".to_string(),
                    json!({"command": "ingest.book"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected ingest book exit code: {other}"),
                json!({"command": "ingest.book"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_ingest_book_pipeline_report(&args, &trace);
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
        GateSubcommand::Policy(policy_args) => run_gate_policy(policy_args, emit_pipeline),
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

fn run_gate_policy(args: GatePolicyArgs, emit_pipeline: bool) -> i32 {
    let rules_format = dataq_io::resolve_input_format(None, Some(args.rules.as_path())).ok();
    let input_is_stdin = args
        .input
        .as_deref()
        .map(gate::is_stdin_path)
        .unwrap_or(true);
    let input_format = if input_is_stdin {
        Some(Format::Json)
    } else {
        args.input
            .as_deref()
            .and_then(|path| dataq_io::resolve_input_format(None, Some(path)).ok())
    };

    let source = args.source.map(Into::into);
    let command_args = gate::GatePolicyCommandArgs {
        rules: args.rules.clone(),
        input: args.input.clone(),
        source,
    };

    let stdin = io::stdin();
    let response = gate::run_policy_with_stdin(&command_args, stdin.lock());

    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize gate policy response".to_string(),
                    json!({"command": "gate.policy"}),
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
                    "failed to serialize gate policy error".to_string(),
                    json!({"command": "gate.policy"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected gate policy exit code: {other}"),
                json!({"command": "gate.policy"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_gate_policy_pipeline_report(
            &args,
            input_is_stdin,
            input_format,
            rules_format,
            source,
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

fn run_scan(args: ScanArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        ScanSubcommand::Text(text_args) => run_scan_text(text_args, emit_pipeline),
    }
}

fn run_transform(args: TransformArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        TransformSubcommand::Rowset(rowset_args) => {
            let (rowset_args, emit_pipeline) =
                normalize_transform_rowset_global_flags(rowset_args, emit_pipeline);
            run_transform_rowset(rowset_args, emit_pipeline)
        }
    }
}

fn normalize_transform_rowset_global_flags(
    mut args: TransformRowsetArgs,
    emit_pipeline: bool,
) -> (TransformRowsetArgs, bool) {
    let mut resolved_emit_pipeline = emit_pipeline;
    args.mlr.retain(|arg| {
        if arg == "--emit-pipeline" {
            resolved_emit_pipeline = true;
            false
        } else {
            true
        }
    });
    (args, resolved_emit_pipeline)
}

fn run_transform_rowset(args: TransformRowsetArgs, emit_pipeline: bool) -> i32 {
    let (input, input_format) = if args.input == "-" {
        let stdin = io::stdin();
        let mut bytes = Vec::new();
        if let Err(error) = stdin.lock().read_to_end(&mut bytes) {
            emit_error(
                "input_usage_error",
                format!("failed to read stdin: {error}"),
                json!({"command": "transform.rowset"}),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_transform_rowset_pipeline_report(
                    &args,
                    None,
                    true,
                    &transform::TransformRowsetPipelineTrace::default(),
                ));
            }
            return 3;
        }

        let format = match dataq_io::autodetect_stdin_input_format(&bytes) {
            Ok(format) => format,
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    error.to_string(),
                    json!({"command": "transform.rowset"}),
                    3,
                );
                if emit_pipeline {
                    emit_pipeline_report(&build_transform_rowset_pipeline_report(
                        &args,
                        None,
                        true,
                        &transform::TransformRowsetPipelineTrace::default(),
                    ));
                }
                return 3;
            }
        };

        let values = match dataq_io::reader::read_values(Cursor::new(bytes), format) {
            Ok(values) => values,
            Err(error) => {
                emit_error(
                    "input_usage_error",
                    format!("failed to read input: {error}"),
                    json!({"command": "transform.rowset"}),
                    3,
                );
                if emit_pipeline {
                    emit_pipeline_report(&build_transform_rowset_pipeline_report(
                        &args,
                        Some(format),
                        true,
                        &transform::TransformRowsetPipelineTrace::default(),
                    ));
                }
                return 3;
            }
        };

        (
            transform::TransformRowsetCommandInput::Inline(values),
            Some(format),
        )
    } else {
        let path = PathBuf::from(args.input.clone());
        let format = dataq_io::resolve_input_format(None, Some(path.as_path())).ok();
        (transform::TransformRowsetCommandInput::Path(path), format)
    };

    let command_args = transform::TransformRowsetCommandArgs {
        input,
        jq_filter: args.jq_filter.clone(),
        mlr: args.mlr.clone(),
    };
    let (response, trace) = transform::run_rowset_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize transform rowset output".to_string(),
                    json!({"command": "transform.rowset"}),
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
                    "failed to serialize transform rowset error".to_string(),
                    json!({"command": "transform.rowset"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected transform rowset exit code: {other}"),
                json!({"command": "transform.rowset"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report =
            build_transform_rowset_pipeline_report(&args, input_format, args.input == "-", &trace);
        emit_pipeline_report(&pipeline_report);
    }
    exit_code
}

fn run_scan_text(args: ScanTextArgs, emit_pipeline: bool) -> i32 {
    let path = args.path.clone().unwrap_or_else(|| PathBuf::from("."));
    let command_args = scan::ScanTextCommandArgs {
        pattern: args.pattern.clone(),
        path: path.clone(),
        glob: args.glob.clone(),
        max_matches: args.max_matches,
        policy_mode: args.policy_mode,
        jq_project: args.jq_project,
    };
    let (response, trace) = scan::run_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize scan text output".to_string(),
                    json!({"command": "scan"}),
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
                    "failed to serialize scan text error".to_string(),
                    json!({"command": "scan"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected scan text exit code: {other}"),
                json!({"command": "scan"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_scan_text_pipeline_report(&args, &path, &trace);
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

fn run_diff(args: DiffArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        DiffSubcommand::Source(source_args) => run_diff_source(source_args, emit_pipeline),
    }
}

fn run_diff_source(args: DiffSourceArgs, emit_pipeline: bool) -> i32 {
    let execution = match diff::execute(&args.left, &args.right) {
        Ok(execution) => execution,
        Err(error) => {
            emit_error(
                "input_usage_error",
                error.to_string(),
                json!({
                    "command": "diff.source",
                    "left": args.left,
                    "right": args.right,
                }),
                3,
            );
            if emit_pipeline {
                emit_pipeline_report(&build_diff_source_pipeline_report(&args, None, None, &[]));
            }
            return 3;
        }
    };

    let success_exit_code = if args.fail_on_diff && execution.report.values.total > 0 {
        2
    } else {
        0
    };
    let response_payload = diff::DiffSourceReport::new(execution.report, execution.sources);
    let exit_code = match serde_json::to_string(&response_payload) {
        Ok(serialized) => {
            println!("{serialized}");
            success_exit_code
        }
        Err(error) => {
            emit_error(
                "internal_error",
                format!("failed to serialize diff source report: {error}"),
                json!({"command": "diff.source"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let left_path = execution.left.metadata.path.as_str();
        let right_path = execution.right.metadata.path.as_str();
        let fingerprint_context = FingerprintContext {
            input_hash: hash_consumed_input_entries(&[
                ConsumedInputHashEntry {
                    label: "left",
                    source: execution.left.hash_source.as_str(),
                    path: Some(left_path),
                    format: Some(execution.left.format.as_str()),
                    bytes: execution.left.bytes.as_slice(),
                },
                ConsumedInputHashEntry {
                    label: "right",
                    source: execution.right.hash_source.as_str(),
                    path: Some(right_path),
                    format: Some(execution.right.format.as_str()),
                    bytes: execution.right.bytes.as_slice(),
                },
            ]),
            ..Default::default()
        };
        emit_pipeline_report_with_context(
            &build_diff_source_pipeline_report(
                &args,
                Some(&execution.left),
                Some(&execution.right),
                &execution.used_tools,
            ),
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
        RecipeSubcommand::Lock(lock_args) => run_recipe_lock(lock_args, emit_pipeline),
        RecipeSubcommand::Replay(replay_args) => run_recipe_replay(replay_args, emit_pipeline),
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

fn run_recipe_lock(args: RecipeLockArgs, emit_pipeline: bool) -> i32 {
    let recipe_format = dataq_io::resolve_input_format(None, Some(args.file.as_path())).ok();
    let command_args = recipe::RecipeLockCommandArgs {
        file_path: args.file.clone(),
    };
    let (response, trace, serialized_lock) = recipe::lock_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 => {
            if let Some(serialized_lock) = serialized_lock {
                if let Some(out_path) = args.out.as_ref() {
                    match fs::write(out_path, serialized_lock.as_slice()) {
                        Ok(()) => 0,
                        Err(error) => {
                            emit_error(
                                "input_usage_error",
                                format!(
                                    "failed to write recipe lock file `{}`: {error}",
                                    out_path.display()
                                ),
                                json!({"command": "recipe", "subcommand": "lock"}),
                                3,
                            );
                            3
                        }
                    }
                } else if emit_bytes_stdout(serialized_lock.as_slice()) {
                    0
                } else {
                    emit_error(
                        "internal_error",
                        "failed to emit recipe lock response".to_string(),
                        json!({"command": "recipe", "subcommand": "lock"}),
                        1,
                    );
                    1
                }
            } else {
                emit_error(
                    "internal_error",
                    "recipe lock payload bytes were unavailable".to_string(),
                    json!({"command": "recipe", "subcommand": "lock"}),
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
                    "failed to serialize recipe lock error".to_string(),
                    json!({"command": "recipe", "subcommand": "lock"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected recipe lock exit code: {other}"),
                json!({"command": "recipe", "subcommand": "lock"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let fingerprint_context = FingerprintContext {
            preferred_tool_versions: trace.tool_versions.clone(),
            ..Default::default()
        };
        let pipeline_report = build_recipe_lock_pipeline_report(
            &args,
            recipe_format,
            trace.steps,
            &trace.tool_versions,
        );
        emit_pipeline_report_with_context(&pipeline_report, &fingerprint_context);
    }
    exit_code
}

fn run_recipe_replay(args: RecipeReplayArgs, emit_pipeline: bool) -> i32 {
    let recipe_format = dataq_io::resolve_input_format(None, Some(args.file.as_path())).ok();
    let lock_format = dataq_io::resolve_input_format(None, Some(args.lock.as_path())).ok();
    let command_args = recipe::RecipeReplayCommandArgs {
        file_path: args.file.clone(),
        lock_path: args.lock.clone(),
        strict: args.strict,
    };
    let (response, trace) = recipe::replay_with_trace(&command_args);

    let exit_code = match response.exit_code {
        0 | 2 => {
            if emit_json_stdout(&response.payload) {
                response.exit_code
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize recipe replay response".to_string(),
                    json!({"command": "recipe", "subcommand": "replay"}),
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
                    "failed to serialize recipe replay error".to_string(),
                    json!({"command": "recipe", "subcommand": "replay"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected recipe replay exit code: {other}"),
                json!({"command": "recipe", "subcommand": "replay"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report =
            build_recipe_replay_pipeline_report(&args, recipe_format, lock_format, trace.steps);
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

fn run_codex(args: CodexArgs, emit_pipeline: bool) -> i32 {
    match args.command {
        CodexSubcommand::InstallSkill(install_skill_args) => {
            run_codex_install_skill(install_skill_args, emit_pipeline)
        }
    }
}

fn run_codex_install_skill(args: CodexInstallSkillArgs, emit_pipeline: bool) -> i32 {
    let command_args = codex::CodexInstallSkillCommandArgs {
        dest_root: args.dest.clone(),
        force: args.force,
    };
    let (response, trace) = codex::install_skill_with_trace(&command_args);
    let exit_code = match response.exit_code {
        0 => {
            if emit_json_stdout(&response.payload) {
                0
            } else {
                emit_error(
                    "internal_error",
                    "failed to serialize codex install-skill response".to_string(),
                    json!({"command": "codex", "subcommand": "install-skill"}),
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
                    "failed to serialize codex install-skill error".to_string(),
                    json!({"command": "codex", "subcommand": "install-skill"}),
                    1,
                );
                1
            }
        }
        other => {
            emit_error(
                "internal_error",
                format!("unexpected codex install-skill exit code: {other}"),
                json!({"command": "codex", "subcommand": "install-skill"}),
                1,
            );
            1
        }
    };

    if emit_pipeline {
        let pipeline_report = build_codex_install_skill_pipeline_report(&args, &trace);
        emit_pipeline_report(&pipeline_report);
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

fn build_ingest_api_pipeline_report(
    args: &IngestApiArgs,
    trace: &ingest_api::IngestApiPipelineTrace,
) -> PipelineReport {
    let mut report = PipelineReport::new(
        "ingest_api",
        PipelineInput::new(vec![PipelineInputSource {
            label: "url".to_string(),
            source: "url".to_string(),
            path: Some(args.url.clone()),
            format: Some("http".to_string()),
        }]),
        ingest_api::pipeline_steps(),
        ingest_api::deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_ingest_yaml_jobs_pipeline_report(
    args: &IngestYamlJobsArgs,
    mode: IngestYamlJobsMode,
    input_is_stdin: bool,
    trace: &ingest_yaml_jobs::IngestYamlJobsPipelineTrace,
) -> PipelineReport {
    let source = if input_is_stdin {
        PipelineInputSource::stdin("input", Some(Format::Yaml.as_str()))
    } else {
        PipelineInputSource::path(
            "input",
            args.input.display().to_string(),
            Some(Format::Yaml.as_str()),
        )
    };

    let mut report = PipelineReport::new(
        "ingest_yaml_jobs",
        PipelineInput::new(vec![source]),
        ingest_yaml_jobs::pipeline_steps(),
        ingest_yaml_jobs::deterministic_guards(mode),
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
        gate::schema_pipeline_steps(),
        gate::schema_deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_gate_policy_pipeline_report(
    args: &GatePolicyArgs,
    input_is_stdin: bool,
    input_format: Option<Format>,
    rules_format: Option<Format>,
    source: Option<gate::GatePolicySourcePreset>,
) -> PipelineReport {
    let mut sources = Vec::with_capacity(2);
    sources.push(PipelineInputSource::path(
        "rules",
        args.rules.display().to_string(),
        format_label(rules_format),
    ));

    if input_is_stdin {
        sources.push(PipelineInputSource::stdin(
            "input",
            format_label(input_format),
        ));
    } else if let Some(path) = args.input.as_deref() {
        sources.push(PipelineInputSource::path(
            "input",
            path.display().to_string(),
            format_label(input_format),
        ));
    }

    PipelineReport::new(
        "gate.policy",
        PipelineInput::new(sources),
        gate::policy_pipeline_steps(),
        gate::policy_deterministic_guards(source),
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

fn build_diff_source_pipeline_report(
    args: &DiffSourceArgs,
    left_source: Option<&diff::ResolvedDiffSource>,
    right_source: Option<&diff::ResolvedDiffSource>,
    used_tools: &[String],
) -> PipelineReport {
    let left_input = if let Some(source) = left_source {
        PipelineInputSource {
            label: "left".to_string(),
            source: source.metadata.kind.clone(),
            path: Some(source.metadata.path.clone()),
            format: Some(source.metadata.format.clone()),
        }
    } else {
        PipelineInputSource {
            label: "left".to_string(),
            source: "locator".to_string(),
            path: Some(args.left.clone()),
            format: None,
        }
    };

    let right_input = if let Some(source) = right_source {
        PipelineInputSource {
            label: "right".to_string(),
            source: source.metadata.kind.clone(),
            path: Some(source.metadata.path.clone()),
            format: Some(source.metadata.format.clone()),
        }
    } else {
        PipelineInputSource {
            label: "right".to_string(),
            source: "locator".to_string(),
            path: Some(args.right.clone()),
            format: None,
        }
    };

    let mut report = PipelineReport::new(
        "diff.source",
        PipelineInput::new(vec![left_input, right_input]),
        diff::pipeline_steps(),
        diff::deterministic_guards(),
    );
    for tool in used_tools {
        report = report.mark_external_tool_used(tool);
    }
    report
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

fn build_ingest_notes_pipeline_report(trace: &ingest::IngestNotesPipelineTrace) -> PipelineReport {
    let mut report = PipelineReport::new(
        "ingest.notes",
        PipelineInput::new(Vec::new()),
        ingest::notes_pipeline_steps(),
        ingest::notes_deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_ingest_doc_pipeline_report(
    args: &IngestDocArgs,
    input_path: Option<PathBuf>,
    from: ingest::IngestDocInputFormat,
) -> PipelineReport {
    let source = if let Some(path) = input_path {
        PipelineInputSource::path("input", path.display().to_string(), Some(from.as_str()))
    } else if args.input == "-" {
        PipelineInputSource::stdin("input", Some(from.as_str()))
    } else {
        PipelineInputSource::path("input", args.input.clone(), Some(from.as_str()))
    };

    PipelineReport::new(
        "ingest.doc",
        PipelineInput::new(vec![source]),
        ingest::pipeline_steps(),
        ingest::deterministic_guards(),
    )
    .mark_external_tool_used("pandoc")
    .mark_external_tool_used("jq")
}

fn build_ingest_book_pipeline_report(
    args: &IngestBookArgs,
    trace: &ingest::IngestBookPipelineTrace,
) -> PipelineReport {
    let mut report = PipelineReport::new(
        "ingest.book",
        PipelineInput::new(vec![PipelineInputSource::path(
            "root",
            args.root.display().to_string(),
            None,
        )]),
        ingest::pipeline_steps_book(),
        ingest::deterministic_guards_book(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn build_scan_text_pipeline_report(
    _args: &ScanTextArgs,
    path: &Path,
    trace: &scan::ScanTextPipelineTrace,
) -> PipelineReport {
    let mut report = ensure_external_tool(
        PipelineReport::new(
            "scan",
            PipelineInput::new(vec![PipelineInputSource::path(
                "path",
                path.display().to_string(),
                None,
            )]),
            scan::pipeline_steps(),
            scan::deterministic_guards(),
        ),
        "rg",
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
}

fn ensure_external_tool(mut report: PipelineReport, tool_name: &str) -> PipelineReport {
    if !report
        .external_tools
        .iter()
        .any(|tool| tool.name == tool_name)
    {
        report
            .external_tools
            .push(dataq::domain::report::ExternalToolUsage {
                name: tool_name.to_string(),
                used: false,
            });
    }
    report
}

fn build_transform_rowset_pipeline_report(
    args: &TransformRowsetArgs,
    input_format: Option<Format>,
    stdin_input: bool,
    trace: &transform::TransformRowsetPipelineTrace,
) -> PipelineReport {
    let source = if stdin_input {
        PipelineInputSource::stdin("input", format_label(input_format))
    } else {
        PipelineInputSource::path("input", args.input.as_str(), format_label(input_format))
    };
    let mut report = PipelineReport::new(
        "transform.rowset",
        PipelineInput::new(vec![source]),
        transform::pipeline_steps(),
        transform::deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report.with_stage_diagnostics(trace.stage_diagnostics.clone())
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

fn build_codex_install_skill_pipeline_report(
    args: &CodexInstallSkillArgs,
    trace: &codex::CodexInstallSkillPipelineTrace,
) -> PipelineReport {
    let mut sources = Vec::new();
    if let Some(dest_root) = args.dest.as_ref() {
        sources.push(PipelineInputSource::path(
            "dest_root",
            dest_root.display().to_string(),
            None,
        ));
    } else if let Some(resolved_root) = trace.resolved_root.as_ref() {
        sources.push(PipelineInputSource::path(
            "dest_root",
            resolved_root.display().to_string(),
            None,
        ));
    }

    if let Some(destination_path) = trace.destination_path.as_ref() {
        sources.push(PipelineInputSource::path(
            "destination",
            destination_path.display().to_string(),
            None,
        ));
    }

    PipelineReport::new(
        "codex.install-skill",
        PipelineInput::new(sources),
        codex::install_skill_pipeline_steps(),
        codex::install_skill_deterministic_guards(),
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
        recipe::deterministic_guards_run(),
    )
}

fn build_recipe_lock_pipeline_report(
    args: &RecipeLockArgs,
    recipe_format: Option<Format>,
    steps: Vec<String>,
    tool_versions: &BTreeMap<String, String>,
) -> PipelineReport {
    let step_names = if steps.is_empty() {
        vec![
            "recipe_lock_parse".to_string(),
            "recipe_lock_probe_tools".to_string(),
            "recipe_lock_fingerprint".to_string(),
        ]
    } else {
        steps
    };

    let mut report = PipelineReport::new(
        "recipe",
        PipelineInput::new(vec![PipelineInputSource::path(
            "recipe",
            args.file.display().to_string(),
            format_label(recipe_format),
        )]),
        step_names,
        recipe::deterministic_guards_lock(),
    );
    for tool_name in tool_versions.keys() {
        report = report.mark_external_tool_used(tool_name);
    }
    report
}

fn build_recipe_replay_pipeline_report(
    args: &RecipeReplayArgs,
    recipe_format: Option<Format>,
    lock_format: Option<Format>,
    steps: Vec<String>,
) -> PipelineReport {
    let step_names = if steps.is_empty() {
        vec![
            "recipe_replay_parse".to_string(),
            "recipe_replay_verify_lock".to_string(),
            "recipe_replay_execute".to_string(),
        ]
    } else {
        steps
    };

    PipelineReport::new(
        "recipe",
        PipelineInput::new(vec![
            PipelineInputSource::path(
                "recipe",
                args.file.display().to_string(),
                format_label(recipe_format),
            ),
            PipelineInputSource::path(
                "lock",
                args.lock.display().to_string(),
                format_label(lock_format),
            ),
        ]),
        step_names,
        recipe::deterministic_guards_replay(),
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
                .unwrap_or_else(|| detect_tool_version(&tool.name));
            (tool.name.clone(), version)
        })
        .collect()
}

fn detect_tool_version(tool_name: &str) -> String {
    let executable = resolve_tool_executable(tool_name);
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

fn resolve_tool_executable(tool_name: &str) -> String {
    let env_key = match tool_name {
        "jq" => Some("DATAQ_JQ_BIN"),
        "yq" => Some("DATAQ_YQ_BIN"),
        "mlr" => Some("DATAQ_MLR_BIN"),
        "xh" => Some("DATAQ_XH_BIN"),
        "pandoc" => Some("DATAQ_PANDOC_BIN"),
        "mdbook" => Some("DATAQ_MDBOOK_BIN"),
        "rg" => Some("DATAQ_RG_BIN"),
        "nb" => Some("DATAQ_NB_BIN"),
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

fn emit_jsonl_stdout(values: &[Value]) -> bool {
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    dataq_io::format::jsonl::write_jsonl(&mut writer, values).is_ok()
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

fn emit_bytes_stdout(bytes: &[u8]) -> bool {
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    writer.write_all(bytes).is_ok() && writer.write_all(b"\n").is_ok()
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn cli_enum_conversions_cover_all_variants() {
        assert_eq!(Format::from(CliInputFormat::Json), Format::Json);
        assert_eq!(Format::from(CliInputFormat::Yaml), Format::Yaml);
        assert_eq!(Format::from(CliInputFormat::Csv), Format::Csv);
        assert_eq!(Format::from(CliInputFormat::Jsonl), Format::Jsonl);

        assert_eq!(Format::from(CanonOutputFormat::Json), Format::Json);
        assert_eq!(Format::from(CanonOutputFormat::Jsonl), Format::Jsonl);

        assert_eq!(
            MergePolicy::from(CliMergePolicy::LastWins),
            MergePolicy::LastWins
        );
        assert_eq!(
            MergePolicy::from(CliMergePolicy::DeepMerge),
            MergePolicy::DeepMerge
        );
        assert_eq!(
            MergePolicy::from(CliMergePolicy::ArrayReplace),
            MergePolicy::ArrayReplace
        );

        assert_eq!(JoinHow::from(CliJoinHow::Inner), JoinHow::Inner);
        assert_eq!(JoinHow::from(CliJoinHow::Left), JoinHow::Left);

        assert_eq!(
            AggregateMetric::from(CliAggregateMetric::Count),
            AggregateMetric::Count
        );
        assert_eq!(
            AggregateMetric::from(CliAggregateMetric::Sum),
            AggregateMetric::Sum
        );
        assert_eq!(
            AggregateMetric::from(CliAggregateMetric::Avg),
            AggregateMetric::Avg
        );

        assert_eq!(
            r#assert::AssertInputNormalizeMode::from(CliAssertNormalizeMode::GithubActionsJobs),
            r#assert::AssertInputNormalizeMode::GithubActionsJobs
        );
        assert_eq!(
            r#assert::AssertInputNormalizeMode::from(CliAssertNormalizeMode::GitlabCiJobs),
            r#assert::AssertInputNormalizeMode::GitlabCiJobs
        );

        assert_eq!(
            gate::GatePolicySourcePreset::from(CliGatePolicySource::ScanText),
            gate::GatePolicySourcePreset::ScanText
        );
        assert_eq!(
            gate::GatePolicySourcePreset::from(CliGatePolicySource::IngestDoc),
            gate::GatePolicySourcePreset::IngestDoc
        );
        assert_eq!(
            gate::GatePolicySourcePreset::from(CliGatePolicySource::IngestApi),
            gate::GatePolicySourcePreset::IngestApi
        );
        assert_eq!(
            gate::GatePolicySourcePreset::from(CliGatePolicySource::IngestNotes),
            gate::GatePolicySourcePreset::IngestNotes
        );
        assert_eq!(
            gate::GatePolicySourcePreset::from(CliGatePolicySource::IngestBook),
            gate::GatePolicySourcePreset::IngestBook
        );

        assert_eq!(
            contract::ContractCommand::from(CliContractCommand::TransformRowset),
            contract::ContractCommand::TransformRowset
        );
        assert_eq!(
            contract::ContractCommand::from(CliContractCommand::RecipeRun),
            contract::ContractCommand::RecipeRun
        );
        assert_eq!(
            contract::ContractCommand::from(CliContractCommand::RecipeLock),
            contract::ContractCommand::RecipeLock
        );

        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Core),
            doctor::DoctorProfile::Core
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::CiJobs),
            doctor::DoctorProfile::CiJobs
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Doc),
            doctor::DoctorProfile::Doc
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Api),
            doctor::DoctorProfile::Api
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Notes),
            doctor::DoctorProfile::Notes
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Book),
            doctor::DoctorProfile::Book
        );
        assert_eq!(
            doctor::DoctorProfile::from(CliDoctorProfile::Scan),
            doctor::DoctorProfile::Scan
        );

        assert_eq!(
            ingest_api::IngestApiMethod::from(CliIngestApiMethod::Get),
            ingest_api::IngestApiMethod::Get
        );
        assert_eq!(
            ingest_api::IngestApiMethod::from(CliIngestApiMethod::Post),
            ingest_api::IngestApiMethod::Post
        );
        assert_eq!(
            ingest_api::IngestApiMethod::from(CliIngestApiMethod::Put),
            ingest_api::IngestApiMethod::Put
        );
        assert_eq!(
            ingest_api::IngestApiMethod::from(CliIngestApiMethod::Patch),
            ingest_api::IngestApiMethod::Patch
        );
        assert_eq!(
            ingest_api::IngestApiMethod::from(CliIngestApiMethod::Delete),
            ingest_api::IngestApiMethod::Delete
        );

        assert_eq!(
            IngestYamlJobsMode::from(CliIngestYamlJobsMode::GithubActions),
            IngestYamlJobsMode::GithubActions
        );
        assert_eq!(
            IngestYamlJobsMode::from(CliIngestYamlJobsMode::GitlabCi),
            IngestYamlJobsMode::GitlabCi
        );
        assert_eq!(
            IngestYamlJobsMode::from(CliIngestYamlJobsMode::GenericMap),
            IngestYamlJobsMode::GenericMap
        );

        assert_eq!(
            ingest::IngestDocInputFormat::from(CliIngestDocFormat::Md),
            ingest::IngestDocInputFormat::Md
        );
        assert_eq!(
            ingest::IngestDocInputFormat::from(CliIngestDocFormat::Html),
            ingest::IngestDocInputFormat::Html
        );
        assert_eq!(
            ingest::IngestDocInputFormat::from(CliIngestDocFormat::Docx),
            ingest::IngestDocInputFormat::Docx
        );
        assert_eq!(
            ingest::IngestDocInputFormat::from(CliIngestDocFormat::Rst),
            ingest::IngestDocInputFormat::Rst
        );
        assert_eq!(
            ingest::IngestDocInputFormat::from(CliIngestDocFormat::Latex),
            ingest::IngestDocInputFormat::Latex
        );
    }

    #[test]
    fn canon_and_whitespace_helpers_cover_error_mappings() {
        assert_eq!(trim_ascii_whitespace(b"  abc \n"), b"abc");
        assert!(trim_ascii_whitespace(b" \t\r\n").is_empty());

        let read_error = CanonError::ReadInput {
            format: Format::Json,
            source: dataq_io::IoError::UnsupportedFormat {
                format: "bad".to_string(),
            },
        };
        assert_eq!(map_canon_error(&read_error), (3, "input_usage_error"));

        let write_io = CanonError::WriteOutput {
            format: Format::Json,
            source: dataq_io::IoError::Io(std::io::Error::other("boom")),
        };
        assert_eq!(map_canon_error(&write_io), (1, "internal_error"));

        let write_usage = CanonError::WriteOutput {
            format: Format::Json,
            source: dataq_io::IoError::UnsupportedFormat {
                format: "bad".to_string(),
            },
        };
        assert_eq!(map_canon_error(&write_usage), (3, "input_usage_error"));
    }

    #[test]
    fn fingerprint_and_tool_helpers_cover_hashing_and_versions() {
        assert_eq!(format_label(Some(Format::Json)), Some("json"));
        assert_eq!(format_label(None), None);

        let hash = hash_normalized_args();
        assert_eq!(hash.len(), 16);

        let empty_hash = hash_consumed_input_entries(&[]);
        assert_eq!(empty_hash, None);

        let entries = [ConsumedInputHashEntry {
            label: "input",
            source: "inline",
            path: None,
            format: Some("json"),
            bytes: br#"{"id":1}"#,
        }];
        let input_hash = hash_consumed_input_entries(&entries);
        assert!(input_hash.is_some());
        assert_eq!(input_hash.as_ref().map(String::len), Some(16));

        let preferred_versions =
            BTreeMap::from([(String::from("jq"), String::from("jq-test 1.0.0"))]);
        let report = PipelineReport::new(
            "canon",
            PipelineInput::new(Vec::new()),
            vec!["step".to_string()],
            vec!["guard".to_string()],
        )
        .mark_external_tool_used("jq");

        let used_versions = collect_used_tool_versions(&report, &preferred_versions);
        assert_eq!(used_versions.get("jq"), Some(&"jq-test 1.0.0".to_string()));

        let context = FingerprintContext {
            input_hash: input_hash.clone(),
            preferred_tool_versions: preferred_versions.clone(),
        };
        let fingerprint = build_pipeline_fingerprint(&report, &context);
        assert_eq!(fingerprint.command, "canon");
        assert_eq!(fingerprint.input_hash, input_hash);
        assert_eq!(
            fingerprint.tool_versions.get("jq"),
            Some(&"jq-test 1.0.0".to_string())
        );
        assert_eq!(fingerprint.args_hash.len(), 16);

        let missing_tool_version = detect_tool_version("__dataq_missing_tool_for_test__");
        assert_eq!(missing_tool_version, "error: unavailable in PATH");
    }

    #[test]
    fn executable_and_output_helpers_cover_fallback_paths() {
        assert_eq!(
            resolve_tool_executable("__unknown_tool__"),
            "__unknown_tool__".to_string()
        );
        assert_eq!(status_label(Some(12)), "12".to_string());
        assert_eq!(status_label(None), "terminated by signal".to_string());

        assert_eq!(first_non_empty_line(b"\n\nhello\n"), Some("hello"));
        assert_eq!(first_non_empty_line(b"\n\t\n"), None);
        assert_eq!(first_non_empty_line(&[0xff, 0xfe]), None);

        assert!(emit_json_stdout(&json!({"ok": true})));
        assert!(emit_json_stderr(&json!({"ok": true})));
        assert!(emit_jsonl_stdout(&[json!({"id": 1}), json!({"id": 2})]));
        assert!(emit_bytes_stdout(br#"{"ok":true}"#));
    }

    #[test]
    fn pipeline_report_builders_cover_main_command_shapes() {
        let canon_args = CanonArgs {
            input: Some(PathBuf::from("input.json")),
            from: Some(CliInputFormat::Json),
            to: Some(CanonOutputFormat::Json),
            sort_keys: true,
            normalize_time: false,
        };
        let canon_report = build_canon_pipeline_report(
            &canon_args,
            Some(Format::Json),
            canon::CanonCommandOptions::default(),
        );
        assert_eq!(canon_report.command, "canon");
        assert_eq!(canon_report.input.sources.len(), 1);

        let assert_args = AssertArgs {
            rules: Some(PathBuf::from("rules.json")),
            schema: None,
            input: Some(PathBuf::from("input.json")),
            normalize: Some(CliAssertNormalizeMode::GithubActionsJobs),
            rules_help: false,
            schema_help: false,
        };
        let assert_trace = r#assert::AssertPipelineTrace {
            used_tools: vec!["yq".to_string()],
            stage_diagnostics: Vec::new(),
        };
        let assert_report = build_assert_pipeline_report(
            &assert_args,
            Some(Format::Json),
            Some(Format::Json),
            None,
            vec!["step".to_string()],
            vec!["guard".to_string()],
            &assert_trace,
        );
        assert_eq!(assert_report.command, "assert");
        assert_eq!(assert_report.input.sources.len(), 2);

        let ingest_api_args = IngestApiArgs {
            url: "https://example.test".to_string(),
            method: CliIngestApiMethod::Get,
            header: vec!["accept:application/json".to_string()],
            body: None,
            expect_status: None,
        };
        let ingest_api_trace = ingest_api::IngestApiPipelineTrace {
            used_tools: vec!["xh".to_string()],
            stage_diagnostics: Vec::new(),
        };
        let ingest_api_report =
            build_ingest_api_pipeline_report(&ingest_api_args, &ingest_api_trace);
        assert_eq!(ingest_api_report.command, "ingest_api");

        let ingest_yaml_jobs_args = IngestYamlJobsArgs {
            input: PathBuf::from("jobs.yml"),
            mode: CliIngestYamlJobsMode::GithubActions,
        };
        let ingest_yaml_jobs_trace = ingest_yaml_jobs::IngestYamlJobsPipelineTrace {
            used_tools: vec!["yq".to_string()],
            stage_diagnostics: Vec::new(),
        };
        let ingest_yaml_jobs_report = build_ingest_yaml_jobs_pipeline_report(
            &ingest_yaml_jobs_args,
            IngestYamlJobsMode::GithubActions,
            false,
            &ingest_yaml_jobs_trace,
        );
        assert_eq!(ingest_yaml_jobs_report.command, "ingest_yaml_jobs");

        let gate_schema_args = GateSchemaArgs {
            schema: PathBuf::from("schema.json"),
            input: Some(PathBuf::from("input.json")),
            from: None,
        };
        let gate_schema_trace = r#assert::AssertPipelineTrace::default();
        let gate_schema_report = build_gate_schema_pipeline_report(
            &gate_schema_args,
            false,
            Some(Format::Json),
            Some(Format::Json),
            &gate_schema_trace,
        );
        assert_eq!(gate_schema_report.command, "gate.schema");

        let gate_policy_args = GatePolicyArgs {
            rules: PathBuf::from("rules.json"),
            input: Some(PathBuf::from("input.json")),
            source: Some(CliGatePolicySource::ScanText),
        };
        let gate_policy_report = build_gate_policy_pipeline_report(
            &gate_policy_args,
            false,
            Some(Format::Json),
            Some(Format::Json),
            Some(gate::GatePolicySourcePreset::ScanText),
        );
        assert_eq!(gate_policy_report.command, "gate.policy");

        let sdiff_args = SdiffArgs {
            left: PathBuf::from("left.json"),
            right: PathBuf::from("right.json"),
            key: Some("$.id".to_string()),
            ignore_path: vec!["$.updated_at".to_string()],
            value_diff_cap: sdiff::DEFAULT_VALUE_DIFF_CAP,
            fail_on_diff: false,
        };
        let sdiff_report =
            build_sdiff_pipeline_report(&sdiff_args, Some(Format::Json), Some(Format::Json));
        assert_eq!(sdiff_report.command, "sdiff");

        let diff_source_args = DiffSourceArgs {
            left: "left.json".to_string(),
            right: "right.json".to_string(),
            fail_on_diff: false,
        };
        let diff_source_report =
            build_diff_source_pipeline_report(&diff_source_args, None, None, &[]);
        assert_eq!(diff_source_report.command, "diff.source");

        let profile_args = ProfileArgs {
            input: Some(PathBuf::from("input.json")),
            from: CliInputFormat::Json,
        };
        let profile_report = build_profile_pipeline_report(&profile_args, Some(Format::Json));
        assert_eq!(profile_report.command, "profile");

        let ingest_doc_args = IngestDocArgs {
            input: "-".to_string(),
            from: CliIngestDocFormat::Md,
        };
        let ingest_doc_report = build_ingest_doc_pipeline_report(
            &ingest_doc_args,
            None,
            ingest::IngestDocInputFormat::Md,
        );
        assert_eq!(ingest_doc_report.command, "ingest.doc");

        let ingest_book_args = IngestBookArgs {
            root: PathBuf::from("book"),
            include_files: true,
        };
        let ingest_book_trace = ingest::IngestBookPipelineTrace {
            used_tools: vec!["jq".to_string()],
            stage_diagnostics: Vec::new(),
        };
        let ingest_book_report =
            build_ingest_book_pipeline_report(&ingest_book_args, &ingest_book_trace);
        assert_eq!(ingest_book_report.command, "ingest.book");

        let scan_text_args = ScanTextArgs {
            pattern: "TODO".to_string(),
            path: Some(PathBuf::from(".")),
            glob: vec!["*.rs".to_string()],
            max_matches: Some(10),
            policy_mode: false,
            jq_project: false,
        };
        let scan_trace = scan::ScanTextPipelineTrace {
            used_tools: vec!["rg".to_string()],
            stage_diagnostics: Vec::new(),
        };
        let scan_report =
            build_scan_text_pipeline_report(&scan_text_args, Path::new("."), &scan_trace);
        assert_eq!(scan_report.command, "scan");

        let transform_args = TransformRowsetArgs {
            input: "-".to_string(),
            jq_filter: ".".to_string(),
            mlr: vec!["cat".to_string()],
        };
        let transform_trace = transform::TransformRowsetPipelineTrace::default();
        let transform_report = build_transform_rowset_pipeline_report(
            &transform_args,
            Some(Format::Json),
            true,
            &transform_trace,
        );
        assert_eq!(transform_report.command, "transform.rowset");

        let merge_args = MergeArgs {
            base: PathBuf::from("base.json"),
            overlay: vec![PathBuf::from("overlay.json")],
            policy: CliMergePolicy::LastWins,
            policy_path: vec!["$.cfg=deep-merge".to_string()],
        };
        let merge_report =
            build_merge_pipeline_report(&merge_args, Some(Format::Json), &[Some(Format::Json)]);
        assert_eq!(merge_report.command, "merge");

        let join_args = JoinArgs {
            left: PathBuf::from("left.json"),
            right: PathBuf::from("right.json"),
            on: "id".to_string(),
            how: CliJoinHow::Inner,
        };
        let join_trace = join::JoinPipelineTrace::default();
        let join_report = build_join_pipeline_report(
            &join_args,
            Some(Format::Json),
            Some(Format::Json),
            &join_trace,
        );
        assert_eq!(join_report.command, "join");

        let aggregate_args = AggregateArgs {
            input: PathBuf::from("input.json"),
            group_by: "team".to_string(),
            metric: CliAggregateMetric::Count,
            target: "value".to_string(),
        };
        let aggregate_trace = aggregate::AggregatePipelineTrace::default();
        let aggregate_report =
            build_aggregate_pipeline_report(&aggregate_args, Some(Format::Json), &aggregate_trace);
        assert_eq!(aggregate_report.command, "aggregate");

        let doctor_report = build_doctor_pipeline_report(Some(doctor::DoctorProfile::Core));
        assert_eq!(doctor_report.command, "doctor");
        let contract_report = build_contract_pipeline_report();
        assert_eq!(contract_report.command, "contract");
        let emit_report = build_emit_plan_pipeline_report();
        assert_eq!(emit_report.command, "emit");

        let codex_args = CodexInstallSkillArgs {
            dest: Some(PathBuf::from("/tmp/.codex/skills")),
            force: true,
        };
        let codex_trace = codex::CodexInstallSkillPipelineTrace {
            resolved_root: Some(PathBuf::from("/tmp/.codex/skills")),
            destination_path: Some(PathBuf::from("/tmp/.codex/skills/dataq")),
        };
        let codex_report = build_codex_install_skill_pipeline_report(&codex_args, &codex_trace);
        assert_eq!(codex_report.command, "codex.install-skill");

        let recipe_run_args = RecipeRunArgs {
            file: PathBuf::from("recipe.json"),
        };
        let recipe_report =
            build_recipe_pipeline_report(&recipe_run_args, Some(Format::Json), Vec::new());
        assert_eq!(recipe_report.command, "recipe");

        let recipe_lock_args = RecipeLockArgs {
            file: PathBuf::from("recipe.json"),
            out: Some(PathBuf::from("recipe.lock.json")),
        };
        let tool_versions = BTreeMap::from([("jq".to_string(), "jq-1.8.1".to_string())]);
        let recipe_lock_report = build_recipe_lock_pipeline_report(
            &recipe_lock_args,
            Some(Format::Json),
            Vec::new(),
            &tool_versions,
        );
        assert_eq!(recipe_lock_report.command, "recipe");

        let recipe_replay_args = RecipeReplayArgs {
            file: PathBuf::from("recipe.json"),
            lock: PathBuf::from("recipe.lock.json"),
            strict: true,
        };
        let recipe_replay_report = build_recipe_replay_pipeline_report(
            &recipe_replay_args,
            Some(Format::Json),
            Some(Format::Json),
            Vec::new(),
        );
        assert_eq!(recipe_replay_report.command, "recipe");

        let report = PipelineReport::new(
            "scan",
            PipelineInput::new(Vec::new()),
            vec!["step".to_string()],
            vec!["guard".to_string()],
        )
        .mark_external_tool_used("rg");
        let ensured = ensure_external_tool(report, "rg");
        assert!(ensured.external_tools.iter().any(|tool| tool.name == "rg"));
    }

    #[test]
    fn canon_jsonl_helpers_cover_stream_and_buffered_modes() {
        let mut out = Vec::new();
        run_canon_jsonl_stream(
            Cursor::new(br#"[{"b":"2","a":"1"}]"#),
            &mut out,
            Format::Json,
            canon::CanonCommandOptions::default(),
        )
        .expect("json input to jsonl stream");
        let output_text = String::from_utf8(out).expect("utf8");
        assert!(output_text.contains(r#"{"a":1,"b":2}"#));

        let mut out = Vec::new();
        run_canon_jsonl_stream(
            Cursor::new(b"{\"b\":\"2\",\"a\":\"1\"}\n{\"x\":\"true\"}\n"),
            &mut out,
            Format::Jsonl,
            canon::CanonCommandOptions::default(),
        )
        .expect("jsonl input to jsonl stream");
        let output_text = String::from_utf8(out).expect("utf8");
        assert!(output_text.lines().count() >= 2);

        let mut out = Vec::new();
        let detected = run_canon_jsonl_with_buffered_stdin(
            Cursor::new(Vec::<u8>::new()),
            br#"[{"x":"1"}]"#.to_vec(),
            &mut out,
            canon::CanonCommandOptions::default(),
        )
        .expect("buffered stdin fallback");
        assert_eq!(detected, Format::Json);

        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("write failed"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("flush failed"))
            }
        }

        let mut broken = BrokenWriter;
        let err = write_jsonl_stream_value(&mut broken, &json!({"id": 1}))
            .expect_err("writer failure should map to canon error");
        let (code, label) = map_canon_error(&err);
        assert_eq!(code, 3);
        assert_eq!(label, "input_usage_error");
    }

    #[test]
    fn run_wrappers_cover_input_usage_exit_paths() {
        assert_eq!(
            run_ingest_notes(
                IngestNotesArgs {
                    tag: vec!["".to_string()],
                    since: None,
                    until: None,
                    to: CliIngestNotesOutput::Json,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_ingest_doc(
                IngestDocArgs {
                    input: "/definitely-missing/dataq-ingest-doc.md".to_string(),
                    from: CliIngestDocFormat::Md,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_ingest_book(
                IngestBookArgs {
                    root: PathBuf::from("/definitely-missing/dataq-book"),
                    include_files: false,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_gate_schema(
                GateSchemaArgs {
                    schema: PathBuf::from("/definitely-missing/schema.json"),
                    input: Some(PathBuf::from("/definitely-missing/input.json")),
                    from: None,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_gate_policy(
                GatePolicyArgs {
                    rules: PathBuf::from("/definitely-missing/rules.json"),
                    input: Some(PathBuf::from("/definitely-missing/input.json")),
                    source: Some(CliGatePolicySource::ScanText),
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_merge(
                MergeArgs {
                    base: PathBuf::from("/definitely-missing/base.json"),
                    overlay: vec![PathBuf::from("/definitely-missing/overlay.json")],
                    policy: CliMergePolicy::LastWins,
                    policy_path: Vec::new(),
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_join(
                JoinArgs {
                    left: PathBuf::from("/definitely-missing/left.json"),
                    right: PathBuf::from("/definitely-missing/right.json"),
                    on: "id".to_string(),
                    how: CliJoinHow::Inner,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_aggregate(
                AggregateArgs {
                    input: PathBuf::from("/definitely-missing/input.json"),
                    group_by: "team".to_string(),
                    metric: CliAggregateMetric::Count,
                    target: "value".to_string(),
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_scan_text(
                ScanTextArgs {
                    pattern: "[".to_string(),
                    path: Some(PathBuf::from(".")),
                    glob: Vec::new(),
                    max_matches: None,
                    policy_mode: false,
                    jq_project: false,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_transform_rowset(
                TransformRowsetArgs {
                    input: "/definitely-missing/input.json".to_string(),
                    jq_filter: ".".to_string(),
                    mlr: vec!["cat".to_string()],
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_sdiff(
                SdiffArgs {
                    left: PathBuf::from("/definitely-missing/left.json"),
                    right: PathBuf::from("/definitely-missing/right.json"),
                    key: Some("$.id".to_string()),
                    ignore_path: Vec::new(),
                    fail_on_diff: false,
                    value_diff_cap: sdiff::DEFAULT_VALUE_DIFF_CAP,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_diff_source(
                DiffSourceArgs {
                    left: "/definitely-missing/left.json".to_string(),
                    right: "/definitely-missing/right.json".to_string(),
                    fail_on_diff: false,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_profile(
                ProfileArgs {
                    input: Some(PathBuf::from("/definitely-missing/input.json")),
                    from: CliInputFormat::Json,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_recipe_run(
                RecipeRunArgs {
                    file: PathBuf::from("/definitely-missing/recipe.txt"),
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_recipe_lock(
                RecipeLockArgs {
                    file: PathBuf::from("/definitely-missing/recipe.txt"),
                    out: None,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_recipe_replay(
                RecipeReplayArgs {
                    file: PathBuf::from("/definitely-missing/recipe.json"),
                    lock: PathBuf::from("/definitely-missing/recipe.lock.json"),
                    strict: true,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_contract(
                ContractArgs {
                    command: None,
                    all: false,
                },
                true,
            ),
            3
        );

        assert_eq!(
            run_emit_plan(
                EmitPlanArgs {
                    command: "canon".to_string(),
                    args: Some("not-json".to_string()),
                },
                true,
            ),
            3
        );

        let temp = tempdir().expect("tempdir");
        assert_eq!(
            run_codex_install_skill(
                CodexInstallSkillArgs {
                    dest: Some(temp.path().join(".codex/skills")),
                    force: false,
                },
                true,
            ),
            0
        );
    }

    #[test]
    fn subcommand_dispatchers_route_to_command_handlers() {
        let ingest_exit = run_ingest(
            IngestArgs {
                command: IngestSubcommand::Doc(IngestDocArgs {
                    input: "/definitely-missing/ingest-doc.md".to_string(),
                    from: CliIngestDocFormat::Md,
                }),
            },
            false,
        );
        assert_eq!(ingest_exit, 3);

        let gate_exit = run_gate(
            GateArgs {
                command: GateSubcommand::Schema(GateSchemaArgs {
                    schema: PathBuf::from("/definitely-missing/schema.json"),
                    input: Some(PathBuf::from("/definitely-missing/input.json")),
                    from: None,
                }),
            },
            false,
        );
        assert_eq!(gate_exit, 3);

        let transform_exit = run_transform(
            TransformArgs {
                command: TransformSubcommand::Rowset(TransformRowsetArgs {
                    input: "/definitely-missing/input.json".to_string(),
                    jq_filter: ".".to_string(),
                    mlr: vec!["cat".to_string()],
                }),
            },
            false,
        );
        assert_eq!(transform_exit, 3);

        let scan_exit = run_scan(
            ScanArgs {
                command: ScanSubcommand::Text(ScanTextArgs {
                    pattern: "[".to_string(),
                    path: Some(PathBuf::from(".")),
                    glob: Vec::new(),
                    max_matches: None,
                    policy_mode: false,
                    jq_project: false,
                }),
            },
            false,
        );
        assert_eq!(scan_exit, 3);

        let diff_exit = run_diff(
            DiffArgs {
                command: DiffSubcommand::Source(DiffSourceArgs {
                    left: "/definitely-missing/left.json".to_string(),
                    right: "/definitely-missing/right.json".to_string(),
                    fail_on_diff: false,
                }),
            },
            false,
        );
        assert_eq!(diff_exit, 3);

        let recipe_exit = run_recipe(
            RecipeArgs {
                command: RecipeSubcommand::Run(RecipeRunArgs {
                    file: PathBuf::from("/definitely-missing/recipe.txt"),
                }),
            },
            false,
        );
        assert_eq!(recipe_exit, 3);

        let emit_exit = run_emit(
            EmitArgs {
                command: EmitSubcommand::Plan(EmitPlanArgs {
                    command: "canon".to_string(),
                    args: Some("not-json".to_string()),
                }),
            },
            false,
        );
        assert_eq!(emit_exit, 3);

        let codex_temp = tempdir().expect("tempdir");
        let codex_exit = run_codex(
            CodexArgs {
                command: CodexSubcommand::InstallSkill(CodexInstallSkillArgs {
                    dest: Some(codex_temp.path().join(".codex/skills")),
                    force: false,
                }),
            },
            false,
        );
        assert_eq!(codex_exit, 0);
    }
}
