use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::adapters::jq;
use crate::adapters::pandoc::{self, PandocInputFormat};
use crate::domain::ingest::{
    GenericMapJobRecord, GithubActionsJobRecord, GitlabCiJobRecord, IngestYamlJobsMode,
};
use crate::domain::report::IngestDocReport;
use crate::util::hash::DeterministicHasher;

/// Supported `ingest doc --from` formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestDocInputFormat {
    Md,
    Html,
    Docx,
    Rst,
    Latex,
}

impl IngestDocInputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Md => "md",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::Rst => "rst",
            Self::Latex => "latex",
        }
    }

    fn as_pandoc(self) -> PandocInputFormat {
        match self {
            Self::Md => PandocInputFormat::Markdown,
            Self::Html => PandocInputFormat::Html,
            Self::Docx => PandocInputFormat::Docx,
            Self::Rst => PandocInputFormat::Rst,
            Self::Latex => PandocInputFormat::Latex,
        }
    }
}

/// Domain errors for deterministic ingest row shaping.
#[derive(Debug, Error)]
pub enum IngestYamlJobsError {
    #[error("normalized `{mode}` row {index} does not match expected schema: {source}")]
    RowShape {
        mode: &'static str,
        index: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize normalized `{mode}` row {index}: {source}")]
    Serialize {
        mode: &'static str,
        index: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// Domain errors for ingest document extraction.
#[derive(Debug, Error)]
pub enum IngestDocError {
    #[error("{0}")]
    Input(String),
    #[error("input is not valid UTF-8 for `--from {from}`")]
    InvalidUtf8 { from: &'static str },
    #[error("ingest doc requires `pandoc` in PATH")]
    MissingPandoc,
    #[error("failed to parse document with pandoc: {0}")]
    PandocExecution(String),
    #[error("pandoc produced invalid JSON AST: {0}")]
    PandocParse(String),
    #[error("ingest doc requires `jq` in PATH")]
    MissingJq,
    #[error("failed to project pandoc AST with jq: {0}")]
    JqExecution(String),
    #[error("jq projection for ingest doc was not valid schema: {0}")]
    ProjectionSchema(String),
}

/// Normalized optional RFC3339 UTC range used by `ingest notes`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestNotesTimeRange {
    pub since: Option<String>,
    pub until: Option<String>,
}

/// Time-range validation errors for `ingest notes`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IngestNotesRangeError {
    #[error("`--since` must be RFC3339: {0}")]
    InvalidSince(String),
    #[error("`--until` must be RFC3339: {0}")]
    InvalidUntil(String),
    #[error("invalid time range: `--since` must be less than or equal to `--until`")]
    InvalidRange,
}

/// Normalization/filter errors for projected note rows.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IngestNotesNormalizeError {
    #[error("internal range parse failure for `--since`: {0}")]
    InvalidSinceBoundary(String),
    #[error("internal range parse failure for `--until`: {0}")]
    InvalidUntilBoundary(String),
    #[error("note row {index} must be an object")]
    RowNotObject { index: usize },
    #[error("note row {index} has empty `created_at`")]
    MissingCreatedAt { index: usize },
    #[error("note row {index} has invalid `created_at` `{value}`")]
    InvalidCreatedAt { index: usize, value: String },
    #[error("note row {index} has invalid `updated_at` `{value}`")]
    InvalidUpdatedAt { index: usize, value: String },
    #[error("note row {index} has invalid `tags` shape")]
    InvalidTags { index: usize },
}

/// Validates optional RFC3339 boundaries and normalizes them to RFC3339 UTC.
pub fn resolve_time_range(
    since: Option<&str>,
    until: Option<&str>,
) -> Result<IngestNotesTimeRange, IngestNotesRangeError> {
    let since_dt = match since {
        Some(value) => Some(parse_rfc3339_utc(value).map_err(IngestNotesRangeError::InvalidSince)?),
        None => None,
    };
    let until_dt = match until {
        Some(value) => Some(parse_rfc3339_utc(value).map_err(IngestNotesRangeError::InvalidUntil)?),
        None => None,
    };

    if let (Some(since), Some(until)) = (since_dt, until_dt)
        && since > until
    {
        return Err(IngestNotesRangeError::InvalidRange);
    }

    Ok(IngestNotesTimeRange {
        since: since_dt.map(format_rfc3339_utc),
        until: until_dt.map(format_rfc3339_utc),
    })
}

/// Filters and canonicalizes projected note rows into deterministic output.
pub fn finalize_notes(
    rows: Vec<Value>,
    required_tags: &[String],
    since: Option<&str>,
    until: Option<&str>,
) -> Result<Vec<Value>, IngestNotesNormalizeError> {
    let since_dt = since
        .map(|value| {
            parse_rfc3339_utc(value).map_err(IngestNotesNormalizeError::InvalidSinceBoundary)
        })
        .transpose()?;
    let until_dt = until
        .map(|value| {
            parse_rfc3339_utc(value).map_err(IngestNotesNormalizeError::InvalidUntilBoundary)
        })
        .transpose()?;

    let required_tags: std::collections::BTreeSet<String> = required_tags
        .iter()
        .map(|tag| tag.trim())
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    let mut normalized = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let map = row
            .as_object()
            .ok_or(IngestNotesNormalizeError::RowNotObject { index })?;
        let id = value_to_string(map.get("id")).trim().to_string();
        let title = value_to_string(map.get("title")).trim().to_string();
        let body = value_to_string(map.get("body"));

        let created_raw = value_to_string(map.get("created_at"));
        if created_raw.trim().is_empty() {
            return Err(IngestNotesNormalizeError::MissingCreatedAt { index });
        }
        let created_dt = parse_rfc3339_utc(&created_raw).map_err(|_| {
            IngestNotesNormalizeError::InvalidCreatedAt {
                index,
                value: created_raw.clone(),
            }
        })?;

        let updated_dt = optional_value_to_string(map.get("updated_at"))
            .map(|value| {
                parse_rfc3339_utc(&value)
                    .map_err(|_| IngestNotesNormalizeError::InvalidUpdatedAt { index, value })
            })
            .transpose()?;

        let tags = normalize_tags(map.get("tags"), index)?;
        if !required_tags.is_empty()
            && !required_tags
                .iter()
                .all(|tag| tags.iter().any(|value| value == tag))
        {
            continue;
        }
        if since_dt.is_some_and(|boundary| created_dt < boundary) {
            continue;
        }
        if until_dt.is_some_and(|boundary| created_dt > boundary) {
            continue;
        }

        let metadata = normalize_metadata(map.get("metadata"));
        let mut out = Map::new();
        out.insert("id".to_string(), Value::String(id.clone()));
        out.insert("title".to_string(), Value::String(title));
        out.insert("body".to_string(), Value::String(body));
        out.insert(
            "tags".to_string(),
            Value::Array(tags.into_iter().map(Value::String).collect()),
        );
        out.insert(
            "created_at".to_string(),
            Value::String(format_rfc3339_utc(created_dt)),
        );
        out.insert(
            "updated_at".to_string(),
            updated_dt
                .map(format_rfc3339_utc)
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        out.insert("metadata".to_string(), Value::Object(metadata));
        normalized.push((created_dt, id, Value::Object(out)));
    }

    normalized.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| value_literal(&left.2).cmp(&value_literal(&right.2)))
    });
    Ok(normalized.into_iter().map(|(_, _, row)| row).collect())
}

/// Validates and re-shapes normalized rows into deterministic mode-specific schemas.
pub fn shape_rows(
    mode: IngestYamlJobsMode,
    rows: Vec<Value>,
) -> Result<Vec<Value>, IngestYamlJobsError> {
    match mode {
        IngestYamlJobsMode::GithubActions => shape_rows_typed::<GithubActionsJobRecord>(
            rows,
            IngestYamlJobsMode::GithubActions.as_str(),
        ),
        IngestYamlJobsMode::GitlabCi => {
            shape_rows_typed::<GitlabCiJobRecord>(rows, IngestYamlJobsMode::GitlabCi.as_str())
        }
        IngestYamlJobsMode::GenericMap => {
            shape_rows_typed::<GenericMapJobRecord>(rows, IngestYamlJobsMode::GenericMap.as_str())
        }
    }
}

/// Run stage1 pandoc AST conversion and stage2 jq projection for document ingest.
pub fn ingest_document(
    input: &[u8],
    from: IngestDocInputFormat,
) -> Result<IngestDocReport, IngestDocError> {
    let pandoc_format = from.as_pandoc();
    if pandoc_format.requires_utf8() && std::str::from_utf8(input).is_err() {
        return Err(IngestDocError::InvalidUtf8 {
            from: from.as_str(),
        });
    }

    let ast = pandoc::to_json_ast(input, pandoc_format).map_err(|error| match error {
        pandoc::PandocError::Unavailable => IngestDocError::MissingPandoc,
        pandoc::PandocError::Execution(message) => IngestDocError::PandocExecution(message),
        pandoc::PandocError::Parse(source) => IngestDocError::PandocParse(source.to_string()),
        pandoc::PandocError::Spawn(source) => IngestDocError::PandocExecution(source.to_string()),
        pandoc::PandocError::Stdin(source) => IngestDocError::PandocExecution(source.to_string()),
    })?;

    let projected = jq::project_document_ast(&ast).map_err(|error| match error {
        jq::JqError::Unavailable => IngestDocError::MissingJq,
        jq::JqError::Execution(message) => IngestDocError::JqExecution(message),
        jq::JqError::Parse(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Spawn(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Stdin(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Serialize(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::OutputShape | jq::JqError::OutputObjectShape => {
            IngestDocError::ProjectionSchema("jq output must be a JSON object".to_string())
        }
    })?;

    serde_json::from_value(projected)
        .map_err(|error| IngestDocError::ProjectionSchema(error.to_string()))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "ingest_doc_pandoc_ast".to_string(),
        "ingest_doc_jq_project".to_string(),
    ]
}

/// Determinism guards planned for `ingest doc` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "pandoc_execution_with_explicit_arg_arrays".to_string(),
        "jq_execution_with_explicit_arg_arrays".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_schema_key_order".to_string(),
        "source_order_preserved_for_arrays".to_string(),
    ]
}

fn shape_rows_typed<T>(
    rows: Vec<Value>,
    mode: &'static str,
) -> Result<Vec<Value>, IngestYamlJobsError>
where
    T: DeserializeOwned + serde::Serialize,
{
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let typed: T =
                serde_json::from_value(row).map_err(|source| IngestYamlJobsError::RowShape {
                    mode,
                    index,
                    source,
                })?;
            serde_json::to_value(typed).map_err(|source| IngestYamlJobsError::Serialize {
                mode,
                index,
                source,
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct IngestBookOptions {
    pub root: PathBuf,
    pub include_files: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookReport {
    pub book: IngestBookMetadata,
    pub summary: IngestBookSummary,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub authors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    pub multilingual: bool,
    pub src: String,
    pub summary_path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookSummary {
    pub chapter_count: usize,
    pub order: Vec<IngestBookOrderItem>,
    pub chapters: Vec<IngestBookChapter>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookOrderItem {
    pub index: usize,
    pub depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_index: Option<usize>,
    pub title: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookChapter {
    pub index: usize,
    pub title: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<IngestBookFileMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<IngestBookChapter>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestBookFileMetadata {
    pub size_bytes: u64,
    pub content_hash: String,
}

#[derive(Debug, Error)]
pub enum IngestBookError {
    #[error("book root `{path}` does not exist or is not a directory")]
    InvalidRoot { path: String },
    #[error("book metadata file `{path}` does not exist")]
    MissingBookToml { path: String },
    #[error("failed to read book metadata file `{path}`: {source}")]
    ReadBookToml {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse book metadata file `{path}`: {source}")]
    ParseBookToml {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("summary file `{path}` does not exist")]
    MissingSummary { path: String },
    #[error("failed to read summary file `{path}`: {source}")]
    ReadSummary {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid SUMMARY.md line {line}: {message}")]
    SummaryParse { line: usize, message: String },
    #[error("chapter path `{path}` at SUMMARY.md line {line} resolves outside book root")]
    ChapterPathOutsideRoot { line: usize, path: String },
    #[error("summary does not contain any chapter entries")]
    EmptySummary,
    #[error("missing chapter files referenced by SUMMARY.md: {paths:?}")]
    MissingChapterFiles { paths: Vec<String> },
    #[error("failed to read chapter file `{path}` for metadata: {source}")]
    ReadChapterFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Deserialize)]
struct BookTomlRoot {
    #[serde(default)]
    book: BookTomlBookSection,
}

#[derive(Debug, Default, Deserialize)]
struct BookTomlBookSection {
    title: Option<String>,
    #[serde(default)]
    authors: Vec<String>,
    description: Option<String>,
    language: Option<String>,
    multilingual: Option<bool>,
    src: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedBookMetadata {
    title: Option<String>,
    authors: Vec<String>,
    description: Option<String>,
    language: Option<String>,
    multilingual: bool,
    src: String,
}

#[derive(Debug, Clone)]
struct FlatChapter {
    index: usize,
    depth: usize,
    parent_index: Option<usize>,
    title: String,
    path: String,
    absolute_path: PathBuf,
}

pub fn ingest_book(options: &IngestBookOptions) -> Result<IngestBookReport, IngestBookError> {
    let root = canonicalize_root(options.root.as_path())?;
    let metadata = load_book_metadata(root.as_path())?;
    let summary_path =
        normalize_lexical_path(root.join(Path::new(metadata.src.as_str()))).join("SUMMARY.md");
    if !summary_path.is_file() {
        return Err(IngestBookError::MissingSummary {
            path: normalize_path_string(&summary_path),
        });
    }

    let summary_text =
        fs::read_to_string(&summary_path).map_err(|source| IngestBookError::ReadSummary {
            path: normalize_path_string(&summary_path),
            source,
        })?;

    let chapters = parse_summary(
        root.as_path(),
        summary_path.as_path(),
        summary_text.as_str(),
    )?;

    let missing_paths: Vec<String> = chapters
        .iter()
        .filter(|chapter| !chapter.absolute_path.is_file())
        .map(|chapter| chapter.path.clone())
        .collect();
    if !missing_paths.is_empty() {
        return Err(IngestBookError::MissingChapterFiles {
            paths: missing_paths,
        });
    }

    let order: Vec<IngestBookOrderItem> = chapters
        .iter()
        .map(|chapter| IngestBookOrderItem {
            index: chapter.index,
            depth: chapter.depth,
            parent_index: chapter.parent_index,
            title: chapter.title.clone(),
            path: chapter.path.clone(),
        })
        .collect();

    let chapter_nodes = build_chapter_tree(&chapters, options.include_files)?;
    Ok(IngestBookReport {
        book: IngestBookMetadata {
            title: metadata.title,
            authors: metadata.authors,
            description: metadata.description,
            language: metadata.language,
            multilingual: metadata.multilingual,
            src: metadata.src,
            summary_path: normalize_path_string(
                summary_path
                    .strip_prefix(root.as_path())
                    .unwrap_or(summary_path.as_path()),
            ),
        },
        summary: IngestBookSummary {
            chapter_count: chapters.len(),
            order,
            chapters: chapter_nodes,
        },
    })
}

fn canonicalize_root(root: &Path) -> Result<PathBuf, IngestBookError> {
    let canonical = fs::canonicalize(root).map_err(|_| IngestBookError::InvalidRoot {
        path: normalize_path_string(root),
    })?;
    if !canonical.is_dir() {
        return Err(IngestBookError::InvalidRoot {
            path: normalize_path_string(root),
        });
    }
    Ok(canonical)
}

fn load_book_metadata(root: &Path) -> Result<ResolvedBookMetadata, IngestBookError> {
    let book_toml_path = root.join("book.toml");
    if !book_toml_path.is_file() {
        return Err(IngestBookError::MissingBookToml {
            path: normalize_path_string(&book_toml_path),
        });
    }

    let book_toml_text =
        fs::read_to_string(&book_toml_path).map_err(|source| IngestBookError::ReadBookToml {
            path: normalize_path_string(&book_toml_path),
            source,
        })?;
    let parsed: BookTomlRoot = toml::from_str(book_toml_text.as_str()).map_err(|source| {
        IngestBookError::ParseBookToml {
            path: normalize_path_string(&book_toml_path),
            source,
        }
    })?;
    let mut metadata = parsed.book;
    let src = if metadata.src.as_deref().unwrap_or("").trim().is_empty() {
        "src".to_string()
    } else {
        normalize_relative_path_string(metadata.src.as_deref().unwrap_or("src"))
    };
    Ok(ResolvedBookMetadata {
        title: metadata.title.take(),
        authors: metadata.authors,
        description: metadata.description.take(),
        language: metadata.language.take(),
        multilingual: metadata.multilingual.unwrap_or(false),
        src,
    })
}

fn parse_summary(
    root: &Path,
    summary_path: &Path,
    content: &str,
) -> Result<Vec<FlatChapter>, IngestBookError> {
    let summary_dir = summary_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| root.to_path_buf());
    let mut chapters = Vec::new();
    let mut indent_stack: Vec<(usize, usize)> = Vec::new();

    for (line_number, line) in content.lines().enumerate() {
        let line_no = line_number + 1;
        let parsed = match parse_summary_line(line) {
            Some(parsed) => parsed,
            None => continue,
        };

        while let Some((indent, _)) = indent_stack.last() {
            if parsed.indent > *indent {
                break;
            }
            indent_stack.pop();
        }
        let parent_index = indent_stack.last().map(|(_, index)| *index);
        let depth = indent_stack.len();
        let relative_target = sanitize_summary_target(parsed.target.as_str(), line_no)?;
        let absolute_target = normalize_lexical_path(summary_dir.join(relative_target));
        let relative_to_root = absolute_target.strip_prefix(root).map_err(|_| {
            IngestBookError::ChapterPathOutsideRoot {
                line: line_no,
                path: parsed.target.clone(),
            }
        })?;

        let index = chapters.len() + 1;
        chapters.push(FlatChapter {
            index,
            depth,
            parent_index,
            title: parsed.title,
            path: normalize_path_string(relative_to_root),
            absolute_path: absolute_target,
        });
        indent_stack.push((parsed.indent, index));
    }

    if chapters.is_empty() {
        return Err(IngestBookError::EmptySummary);
    }
    Ok(chapters)
}

struct ParsedSummaryLine {
    indent: usize,
    title: String,
    target: String,
}

fn parse_summary_line(line: &str) -> Option<ParsedSummaryLine> {
    let indent = leading_indent_width(line);
    let trimmed = line.trim_start_matches([' ', '\t']);
    if trimmed.starts_with('#') || trimmed.is_empty() {
        return None;
    }

    let bullet = trimmed.chars().next()?;
    if !matches!(bullet, '-' | '*' | '+') {
        return None;
    }
    if !trimmed
        .chars()
        .nth(1)
        .map(char::is_whitespace)
        .unwrap_or(false)
    {
        return None;
    }

    let body = trimmed[1..].trim_start();
    if !body.starts_with('[') {
        return None;
    }
    let close_bracket = body.find("](")?;
    if close_bracket <= 1 {
        return None;
    }
    let title = body[1..close_bracket].trim();
    if title.is_empty() {
        return None;
    }
    let tail = &body[(close_bracket + 2)..];
    let close_paren = tail.find(')')?;
    let target = tail[..close_paren].trim();
    if target.is_empty() {
        return None;
    }

    Some(ParsedSummaryLine {
        indent,
        title: title.to_string(),
        target: target.to_string(),
    })
}

fn sanitize_summary_target(target: &str, line: usize) -> Result<PathBuf, IngestBookError> {
    if target.contains("://") || target.starts_with("mailto:") {
        return Err(IngestBookError::SummaryParse {
            line,
            message: "external links are not supported".to_string(),
        });
    }

    let mut trimmed = target.trim();
    if let Some(index) = trimmed.find(char::is_whitespace) {
        trimmed = &trimmed[..index];
    }
    trimmed = trimmed
        .split_once('#')
        .map(|(prefix, _)| prefix)
        .unwrap_or(trimmed);
    trimmed = trimmed
        .split_once('?')
        .map(|(prefix, _)| prefix)
        .unwrap_or(trimmed);
    if trimmed.is_empty() {
        return Err(IngestBookError::SummaryParse {
            line,
            message: "chapter link target is empty".to_string(),
        });
    }

    let path = PathBuf::from(trimmed);
    if path.is_absolute() {
        return Err(IngestBookError::SummaryParse {
            line,
            message: "absolute chapter links are not supported".to_string(),
        });
    }
    Ok(path)
}

fn build_chapter_tree(
    chapters: &[FlatChapter],
    include_files: bool,
) -> Result<Vec<IngestBookChapter>, IngestBookError> {
    let mut children: BTreeMap<Option<usize>, Vec<usize>> = BTreeMap::new();
    for chapter in chapters {
        children
            .entry(chapter.parent_index)
            .or_default()
            .push(chapter.index);
    }
    build_chapter_nodes(None, chapters, &children, include_files)
}

fn build_chapter_nodes(
    parent: Option<usize>,
    chapters: &[FlatChapter],
    children: &BTreeMap<Option<usize>, Vec<usize>>,
    include_files: bool,
) -> Result<Vec<IngestBookChapter>, IngestBookError> {
    let mut nodes = Vec::new();
    if let Some(indices) = children.get(&parent) {
        for index in indices {
            let chapter = &chapters[*index - 1];
            let file_metadata = if include_files {
                Some(load_file_metadata(
                    chapter.absolute_path.as_path(),
                    chapter.path.as_str(),
                )?)
            } else {
                None
            };
            nodes.push(IngestBookChapter {
                index: chapter.index,
                title: chapter.title.clone(),
                path: chapter.path.clone(),
                file: file_metadata,
                children: build_chapter_nodes(
                    Some(chapter.index),
                    chapters,
                    children,
                    include_files,
                )?,
            });
        }
    }
    Ok(nodes)
}

fn load_file_metadata(
    absolute_path: &Path,
    normalized_path: &str,
) -> Result<IngestBookFileMetadata, IngestBookError> {
    let bytes = fs::read(absolute_path).map_err(|source| IngestBookError::ReadChapterFile {
        path: normalized_path.to_string(),
        source,
    })?;
    let mut hasher = DeterministicHasher::new();
    hasher.update_len_prefixed(bytes.as_slice());
    Ok(IngestBookFileMetadata {
        size_bytes: bytes.len() as u64,
        content_hash: hasher.finish_hex(),
    })
}

fn leading_indent_width(line: &str) -> usize {
    let mut width = 0;
    for ch in line.chars() {
        match ch {
            ' ' => width += 1,
            '\t' => width += 4,
            _ => break,
        }
    }
    width
}

fn normalize_relative_path_string(path: &str) -> String {
    let normalized = normalize_lexical_path(Path::new(path));
    let as_string = normalize_path_string(&normalized);
    if as_string.is_empty() {
        ".".to_string()
    } else {
        as_string
    }
}

fn normalize_path_string(path: &Path) -> String {
    let mut normalized = path.to_string_lossy().replace('\\', "/");
    while normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    normalized
}

fn normalize_lexical_path(path: impl AsRef<Path>) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.as_ref().components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push("..");
                }
            }
            Component::Normal(segment) => normalized.push(segment),
        }
    }
    normalized
}

fn value_to_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn optional_value_to_string(value: Option<&Value>) -> Option<String> {
    let value = value_to_string(value);
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn normalize_tags(
    value: Option<&Value>,
    index: usize,
) -> Result<Vec<String>, IngestNotesNormalizeError> {
    let mut tags = std::collections::BTreeSet::new();
    match value {
        None | Some(Value::Null) => {}
        Some(Value::Array(items)) => {
            for item in items {
                let tag = value_to_string(Some(item)).trim().to_string();
                if !tag.is_empty() {
                    tags.insert(tag);
                }
            }
        }
        Some(_) => return Err(IngestNotesNormalizeError::InvalidTags { index }),
    }
    Ok(tags.into_iter().collect())
}

fn normalize_metadata(value: Option<&Value>) -> Map<String, Value> {
    let mut metadata = Map::new();
    let object = value.and_then(Value::as_object);
    if let Some(notebook) = optional_value_to_string(object.and_then(|map| map.get("notebook"))) {
        metadata.insert("notebook".to_string(), Value::String(notebook));
    }
    if let Some(path) = optional_value_to_string(object.and_then(|map| map.get("path"))) {
        metadata.insert("path".to_string(), Value::String(path));
    }
    metadata
}

fn value_literal(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

fn parse_rfc3339_utc(input: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|error| error.to_string())
}

fn format_rfc3339_utc(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

#[cfg(test)]
mod ingest_book_tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{IngestBookError, IngestBookOptions, ingest_book};

    #[test]
    fn parses_nested_summary_in_deterministic_order() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();
        let src = root.join("src");
        fs::create_dir_all(&src).expect("create src");
        fs::write(
            root.join("book.toml"),
            r#"[book]
title = "Sample Book"
authors = ["alice", "bob"]
language = "en"
src = "src"
"#,
        )
        .expect("write book.toml");
        fs::write(
            src.join("SUMMARY.md"),
            r#"# Summary

- [Intro](intro.md)
  - [Install](guide/install.md)
  - [Usage](guide/usage.md)
- [Appendix](appendix.md)
"#,
        )
        .expect("write summary");
        fs::write(src.join("intro.md"), "# Intro\n").expect("write intro");
        fs::create_dir_all(src.join("guide")).expect("create guide");
        fs::write(src.join("guide/install.md"), "# Install\n").expect("write install");
        fs::write(src.join("guide/usage.md"), "# Usage\n").expect("write usage");
        fs::write(src.join("appendix.md"), "# Appendix\n").expect("write appendix");

        let report = ingest_book(&IngestBookOptions {
            root: root.to_path_buf(),
            include_files: false,
        })
        .expect("ingest");

        assert_eq!(report.book.title.as_deref(), Some("Sample Book"));
        assert_eq!(report.summary.chapter_count, 4);
        assert_eq!(report.summary.order[0].path, "src/intro.md");
        assert_eq!(report.summary.order[1].parent_index, Some(1));
        assert_eq!(report.summary.order[1].path, "src/guide/install.md");
        assert_eq!(report.summary.order[2].parent_index, Some(1));
        assert_eq!(report.summary.order[3].path, "src/appendix.md");
        assert_eq!(report.summary.chapters.len(), 2);
        assert_eq!(report.summary.chapters[0].children.len(), 2);
    }

    #[test]
    fn missing_chapter_files_are_reported_as_input_error() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();
        let src = root.join("src");
        fs::create_dir_all(&src).expect("create src");
        fs::write(
            root.join("book.toml"),
            r#"[book]
title = "Broken Book"
src = "src"
"#,
        )
        .expect("write book.toml");
        fs::write(
            src.join("SUMMARY.md"),
            r#"# Summary

- [Intro](intro.md)
"#,
        )
        .expect("write summary");

        let error = ingest_book(&IngestBookOptions {
            root: root.to_path_buf(),
            include_files: false,
        })
        .expect_err("must fail");
        match error {
            IngestBookError::MissingChapterFiles { paths } => {
                assert_eq!(paths, vec!["src/intro.md".to_string()]);
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        IngestNotesRangeError, IngestYamlJobsError, finalize_notes, resolve_time_range, shape_rows,
    };
    use crate::domain::ingest::IngestYamlJobsMode;

    #[test]
    fn github_actions_shape_rejects_unknown_field() {
        let err = shape_rows(
            IngestYamlJobsMode::GithubActions,
            vec![json!({
                "job_id": "build",
                "runs_on": "ubuntu-latest",
                "steps_count": 1,
                "uses_unpinned_action": false,
                "extra": true
            })],
        )
        .expect_err("unknown field must fail");

        assert!(matches!(
            err,
            IngestYamlJobsError::RowShape { index: 0, .. }
        ));
    }

    #[test]
    fn generic_map_shape_keeps_mode_specific_fields() {
        let rows = shape_rows(
            IngestYamlJobsMode::GenericMap,
            vec![json!({
                "job_name": "build",
                "field_count": 2,
                "has_stage": true,
                "has_script": true
            })],
        )
        .expect("shape rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["job_name"], json!("build"));
        assert_eq!(rows[0]["field_count"], json!(2));
    }

    #[test]
    fn normalizes_offsets_to_utc() {
        let range = resolve_time_range(
            Some("2025-01-01T09:00:00+09:00"),
            Some("2025-01-01T10:00:00+09:00"),
        )
        .expect("range");
        assert_eq!(range.since.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(range.until.as_deref(), Some("2025-01-01T01:00:00Z"));
    }

    #[test]
    fn rejects_since_after_until() {
        let error = resolve_time_range(Some("2025-01-02T00:00:00Z"), Some("2025-01-01T00:00:00Z"))
            .expect_err("invalid range");
        assert_eq!(error, IngestNotesRangeError::InvalidRange);
    }

    #[test]
    fn finalize_notes_filters_and_sorts_deterministically() {
        let rows = vec![
            json!({
                "id": "n-2",
                "title": "two",
                "body": "b",
                "tags": ["work", "urgent"],
                "created_at": "2025-01-31T23:59:59Z",
                "updated_at": null,
                "metadata": {"notebook": "ops", "path": "ops/n-2"}
            }),
            json!({
                "id": "n-1",
                "title": "one",
                "body": "a",
                "tags": ["work"],
                "created_at": "2025-01-15T09:00:00+09:00",
                "updated_at": "2025-01-16T09:30:00+09:00",
                "metadata": {"notebook": "ops", "path": "ops/n-1"}
            }),
        ];

        let normalized = finalize_notes(
            rows,
            &["work".to_string()],
            Some("2025-01-15T00:00:00Z"),
            Some("2025-01-31T23:59:59Z"),
        )
        .expect("normalize");

        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0]["id"], json!("n-1"));
        assert_eq!(normalized[0]["created_at"], json!("2025-01-15T00:00:00Z"));
        assert_eq!(normalized[1]["id"], json!("n-2"));
        assert_eq!(normalized[1]["created_at"], json!("2025-01-31T23:59:59Z"));
    }
}
