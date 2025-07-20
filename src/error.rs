use std::{error::Error, fmt::Debug};
use thiserror::Error;

#[derive(Error)]
pub enum SmartnessError {
    #[error("failed to create process runtime with Tokio")]
    ProcessRuntimeBuildError(#[source] std::io::Error),
}

impl Debug for SmartnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self)?;
        if let Some(source) = self.source() {
            writeln!(f, "Caused by:\n\t{}", source)?;
        }
        Ok(())
    }
}
