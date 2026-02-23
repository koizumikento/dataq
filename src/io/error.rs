use thiserror::Error;

#[derive(Debug, Error)]
pub enum IoError {
    #[error("unsupported format: {format}")]
    UnsupportedFormat { format: String },

    #[error("could not resolve {kind} format; pass --{kind} format or use a known file extension")]
    UnresolvedFormat { kind: &'static str },

    #[error("unsupported {kind} file extension: {path}")]
    UnsupportedPathExtension { kind: &'static str, path: String },

    #[error("json parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("yaml parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("csv row {index} must be an object")]
    InvalidCsvRow { index: usize },
}
