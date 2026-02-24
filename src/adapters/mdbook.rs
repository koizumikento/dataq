use std::path::Path;
use std::process::Command;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MdbookError {
    #[error("`mdbook` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn mdbook: {0}")]
    Spawn(std::io::Error),
    #[error("mdbook execution failed: {0}")]
    Execution(String),
}

pub fn verify_book_metadata(root: &Path) -> Result<(), MdbookError> {
    let mdbook_bin = std::env::var("DATAQ_MDBOOK_BIN").unwrap_or_else(|_| "mdbook".to_string());
    let output = match Command::new(&mdbook_bin)
        .arg("test")
        .arg(root.as_os_str())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(MdbookError::Unavailable);
        }
        Err(error) => return Err(MdbookError::Spawn(error)),
    };

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8(output.stderr)
        .unwrap_or_else(|_| "failed to decode mdbook stderr".to_string());
    let message = if stderr.trim().is_empty() {
        format!(
            "mdbook test exited with status {}",
            output
                .status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string())
        )
    } else {
        stderr.trim().to_string()
    };
    Err(MdbookError::Execution(message))
}
