use std::cmp::Ordering;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// One structured text match parsed from `rg --json` output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanTextMatch {
    pub path: String,
    pub line: usize,
    pub column: usize,
    pub text: String,
    pub line_text: String,
}

#[derive(Debug, Error)]
pub enum ScanParseError {
    #[error("failed to parse rg JSON event on line {line}: {source}")]
    ParseJson {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("rg JSON event on line {line} is missing field `{field}`")]
    MissingField { line: usize, field: &'static str },
    #[error("rg JSON event on line {line} has invalid field `{field}`")]
    InvalidField { line: usize, field: &'static str },
}

/// Parses `rg --json` output into deterministically ordered matches.
pub fn parse_rg_json_stream(
    raw_stream: &str,
    invocation_root: &Path,
) -> Result<Vec<ScanTextMatch>, ScanParseError> {
    let mut matches = Vec::new();

    for (index, raw_line) in raw_stream.lines().enumerate() {
        if raw_line.trim().is_empty() {
            continue;
        }
        let line_no = index + 1;
        let event: Value =
            serde_json::from_str(raw_line).map_err(|source| ScanParseError::ParseJson {
                line: line_no,
                source,
            })?;
        if event
            .get("type")
            .and_then(Value::as_str)
            .filter(|event_type| *event_type == "match")
            .is_none()
        {
            continue;
        }

        let path_raw = event
            .get("data")
            .and_then(|data| data.get("path"))
            .and_then(|path| path.get("text"))
            .and_then(Value::as_str)
            .ok_or(ScanParseError::MissingField {
                line: line_no,
                field: "data.path.text",
            })?;
        let path = stable_relative_path(path_raw, invocation_root);

        let line_number_u64 = event
            .get("data")
            .and_then(|data| data.get("line_number"))
            .and_then(Value::as_u64)
            .ok_or(ScanParseError::MissingField {
                line: line_no,
                field: "data.line_number",
            })?;
        let line_number =
            usize::try_from(line_number_u64).map_err(|_| ScanParseError::InvalidField {
                line: line_no,
                field: "data.line_number",
            })?;

        let line_text = event
            .get("data")
            .and_then(|data| data.get("lines"))
            .and_then(|lines| lines.get("text"))
            .and_then(Value::as_str)
            .ok_or(ScanParseError::MissingField {
                line: line_no,
                field: "data.lines.text",
            })?
            .to_string();

        let submatches = event
            .get("data")
            .and_then(|data| data.get("submatches"))
            .and_then(Value::as_array)
            .ok_or(ScanParseError::MissingField {
                line: line_no,
                field: "data.submatches",
            })?;

        if submatches.is_empty() {
            matches.push(ScanTextMatch {
                path: path.clone(),
                line: line_number,
                column: 1,
                text: line_text.clone(),
                line_text: line_text.clone(),
            });
            continue;
        }

        for submatch in submatches {
            let start_u64 = submatch.get("start").and_then(Value::as_u64).ok_or(
                ScanParseError::MissingField {
                    line: line_no,
                    field: "data.submatches[].start",
                },
            )?;
            let start = usize::try_from(start_u64).map_err(|_| ScanParseError::InvalidField {
                line: line_no,
                field: "data.submatches[].start",
            })?;
            let text = submatch
                .get("match")
                .and_then(|matched| matched.get("text"))
                .and_then(Value::as_str)
                .ok_or(ScanParseError::MissingField {
                    line: line_no,
                    field: "data.submatches[].match.text",
                })?
                .to_string();

            matches.push(ScanTextMatch {
                path: path.clone(),
                line: line_number,
                column: start.saturating_add(1),
                text,
                line_text: line_text.clone(),
            });
        }
    }

    matches.sort_by(compare_matches);
    Ok(matches)
}

fn stable_relative_path(raw_path: &str, invocation_root: &Path) -> String {
    let path = Path::new(raw_path);
    let relative_buf = if path.is_absolute() {
        relative_from_absolute(path, invocation_root)
    } else {
        None
    };
    let relative = relative_buf.as_deref().unwrap_or(path);
    let normalized = strip_curdir_components(relative);
    let literal = normalized.to_string_lossy().replace('\\', "/");
    if literal.is_empty() {
        ".".to_string()
    } else {
        literal
    }
}

fn relative_from_absolute(path: &Path, base: &Path) -> Option<PathBuf> {
    if !path.is_absolute() || !base.is_absolute() {
        return None;
    }

    let path_components: Vec<Component<'_>> = path.components().collect();
    let base_components: Vec<Component<'_>> = base.components().collect();

    if path_components.first() != base_components.first() {
        return None;
    }

    let mut shared_prefix = 0usize;
    while shared_prefix < path_components.len()
        && shared_prefix < base_components.len()
        && path_components[shared_prefix] == base_components[shared_prefix]
    {
        shared_prefix += 1;
    }

    let mut relative = PathBuf::new();
    for component in &base_components[shared_prefix..] {
        if matches!(
            component,
            Component::Normal(_) | Component::CurDir | Component::ParentDir
        ) {
            relative.push("..");
        }
    }
    for component in &path_components[shared_prefix..] {
        relative.push(component.as_os_str());
    }

    if relative.as_os_str().is_empty() {
        relative.push(".");
    }
    Some(relative)
}

fn strip_curdir_components(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        if component == Component::CurDir {
            continue;
        }
        out.push(component);
    }
    out
}

fn compare_matches(left: &ScanTextMatch, right: &ScanTextMatch) -> Ordering {
    left.path
        .cmp(&right.path)
        .then_with(|| left.line.cmp(&right.line))
        .then_with(|| left.column.cmp(&right.column))
        .then_with(|| left.text.cmp(&right.text))
        .then_with(|| left.line_text.cmp(&right.line_text))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ScanTextMatch, parse_rg_json_stream};

    #[test]
    fn parses_multiline_match_event() {
        let raw = concat!(
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/repo/a.txt\"},",
            "\"lines\":{\"text\":\"foo\\nbar\\n\"},\"line_number\":2,",
            "\"submatches\":[{\"match\":{\"text\":\"foo\\nbar\"},\"start\":0,\"end\":7}]}}\n"
        );

        let parsed = parse_rg_json_stream(raw, Path::new("/repo")).expect("parse stream");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].path, "a.txt");
        assert_eq!(parsed[0].line, 2);
        assert_eq!(parsed[0].column, 1);
        assert_eq!(parsed[0].text, "foo\nbar");
        assert_eq!(parsed[0].line_text, "foo\nbar\n");
    }

    #[test]
    fn sorts_by_path_line_column() {
        let raw = concat!(
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/repo/b.txt\"},\"lines\":{\"text\":\"x\"},\"line_number\":5,\"submatches\":[{\"match\":{\"text\":\"x\"},\"start\":8,\"end\":9}]}}\n",
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/repo/a.txt\"},\"lines\":{\"text\":\"x\"},\"line_number\":3,\"submatches\":[{\"match\":{\"text\":\"x\"},\"start\":0,\"end\":1}]}}\n",
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/repo/b.txt\"},\"lines\":{\"text\":\"x\"},\"line_number\":2,\"submatches\":[{\"match\":{\"text\":\"x\"},\"start\":1,\"end\":2}]}}\n"
        );

        let parsed = parse_rg_json_stream(raw, Path::new("/repo")).expect("parse stream");
        let keys: Vec<(String, usize, usize)> = parsed
            .iter()
            .map(|item| (item.path.clone(), item.line, item.column))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("a.txt".to_string(), 3, 1),
                ("b.txt".to_string(), 2, 2),
                ("b.txt".to_string(), 5, 9),
            ]
        );
    }

    #[test]
    fn strips_leading_dot_for_relative_paths() {
        let raw = concat!(
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"./notes/todo.txt\"},\"lines\":{\"text\":\"x\"},\"line_number\":1,\"submatches\":[{\"match\":{\"text\":\"x\"},\"start\":0,\"end\":1}]}}\n"
        );
        let parsed = parse_rg_json_stream(raw, Path::new("/repo")).expect("parse stream");

        assert_eq!(
            parsed,
            vec![ScanTextMatch {
                path: "notes/todo.txt".to_string(),
                line: 1,
                column: 1,
                text: "x".to_string(),
                line_text: "x".to_string(),
            }]
        );
    }

    #[test]
    fn computes_relative_path_for_absolute_paths_outside_root() {
        let raw = concat!(
            "{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/tmp/dataq-scan/file.txt\"},\"lines\":{\"text\":\"x\"},\"line_number\":1,\"submatches\":[{\"match\":{\"text\":\"x\"},\"start\":0,\"end\":1}]}}\n"
        );
        let parsed = parse_rg_json_stream(raw, Path::new("/Users/example/workspace/repo"))
            .expect("parse stream");

        assert_eq!(parsed[0].path, "../../../../tmp/dataq-scan/file.txt");
    }
}
