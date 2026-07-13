use thiserror::Error;

/// Errors produced while resolving, reading, or writing supported data formats.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("unsupported format: {format}")]
    UnsupportedFormat { format: String },

    #[error("could not resolve {kind} format; pass --{kind} format or use a known file extension")]
    UnresolvedFormat { kind: &'static str },

    #[error("unsupported {kind} file extension: {path}")]
    UnsupportedPathExtension { kind: &'static str, path: String },

    #[error("could not autodetect stdin input format; tried JSONL -> JSON -> YAML -> CSV")]
    StdinAutodetectFailed,

    #[error("json parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("yaml parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    /// A CSV header name appeared more than once.
    #[error(
        "duplicate CSV header `{name}`: first_index={first_index}, duplicate_index={duplicate_index} (0-based column indices)"
    )]
    DuplicateCsvHeader {
        /// The exact, case-sensitive header name that was repeated.
        name: String,
        /// The zero-based column index of the header's first occurrence.
        first_index: usize,
        /// The zero-based column index of the first repeated occurrence.
        duplicate_index: usize,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("csv row {index} must be an object")]
    InvalidCsvRow { index: usize },
}
