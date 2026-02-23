use std::fmt;

use thiserror::Error;

/// Canonical value path segment used by diff reports and CLI options.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

/// Parsed canonical path (`$["field"][0]["nested"]`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValuePath {
    segments: Vec<PathSegment>,
}

impl ValuePath {
    pub fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn parse_canonical(input: &str) -> Result<Self, ValuePathError> {
        if !input.starts_with('$') {
            return Err(ValuePathError::new(
                input,
                "path must start with `$`".to_string(),
            ));
        }

        let bytes = input.as_bytes();
        let mut cursor = 1;
        let mut segments = Vec::new();

        while cursor < bytes.len() {
            if bytes[cursor] != b'[' {
                return Err(ValuePathError::new(
                    input,
                    format!("expected `[` at byte {cursor}"),
                ));
            }
            cursor += 1;
            if cursor >= bytes.len() {
                return Err(ValuePathError::new(
                    input,
                    "path cannot end inside `[`".to_string(),
                ));
            }

            if bytes[cursor] == b'"' {
                let string_start = cursor;
                cursor += 1;
                let mut escaped = false;
                while cursor < bytes.len() {
                    let byte = bytes[cursor];
                    if escaped {
                        escaped = false;
                        cursor += 1;
                        continue;
                    }
                    if byte == b'\\' {
                        escaped = true;
                        cursor += 1;
                        continue;
                    }
                    if byte == b'"' {
                        break;
                    }
                    cursor += 1;
                }

                if cursor >= bytes.len() || bytes[cursor] != b'"' {
                    return Err(ValuePathError::new(
                        input,
                        "unterminated quoted key".to_string(),
                    ));
                }

                let string_end = cursor;
                cursor += 1;
                if cursor >= bytes.len() || bytes[cursor] != b']' {
                    return Err(ValuePathError::new(
                        input,
                        format!("expected `]` at byte {cursor}"),
                    ));
                }

                let encoded_key = &input[string_start..=string_end];
                let key = serde_json::from_str(encoded_key).map_err(|error| {
                    ValuePathError::new(input, format!("invalid quoted key: {error}"))
                })?;
                segments.push(PathSegment::Key(key));
                cursor += 1;
                continue;
            }

            let index_start = cursor;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if index_start == cursor {
                return Err(ValuePathError::new(
                    input,
                    format!("expected quoted key or numeric index at byte {cursor}"),
                ));
            }
            if cursor >= bytes.len() || bytes[cursor] != b']' {
                return Err(ValuePathError::new(
                    input,
                    format!("expected `]` at byte {cursor}"),
                ));
            }

            let index_literal = &input[index_start..cursor];
            let index = index_literal
                .parse::<usize>()
                .map_err(|error| ValuePathError::new(input, format!("invalid index: {error}")))?;
            segments.push(PathSegment::Index(index));
            cursor += 1;
        }

        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }
}

impl fmt::Display for ValuePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("$")?;
        for segment in &self.segments {
            match segment {
                PathSegment::Key(key) => {
                    let encoded = serde_json::to_string(key).map_err(|_| fmt::Error)?;
                    write!(formatter, "[{encoded}]")?;
                }
                PathSegment::Index(index) => {
                    write!(formatter, "[{index}]")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("invalid canonical path `{input}`: {reason}")]
pub struct ValuePathError {
    input: String,
    reason: String,
}

impl ValuePathError {
    fn new(input: &str, reason: String) -> Self {
        Self {
            input: input.to_string(),
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PathSegment, ValuePath};

    #[test]
    fn parses_root_path() {
        let path = ValuePath::parse_canonical("$").expect("parse root");
        assert_eq!(path.segments(), &[]);
        assert_eq!(path.to_string(), "$");
    }

    #[test]
    fn parses_escaped_keys_and_indexes() {
        let path = ValuePath::parse_canonical(r#"$["a.b"][12]["quote\"key"]"#).expect("parse");
        assert_eq!(
            path.segments(),
            &[
                PathSegment::Key("a.b".to_string()),
                PathSegment::Index(12),
                PathSegment::Key("quote\"key".to_string())
            ]
        );
        assert_eq!(path.to_string(), r#"$["a.b"][12]["quote\"key"]"#);
    }

    #[test]
    fn rejects_non_canonical_path() {
        let error = ValuePath::parse_canonical("$.a").expect_err("must reject dotted path");
        assert!(error.to_string().contains("expected `[`"));
    }
}
