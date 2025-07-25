use std::sync::Arc;

use scylla::client::session::Session;

use crate::{config::smarteness_settings::SmartnessSettings, error::SmartnessError};

// function that will apply script as startup step.
// if startup_enabled setting is true, it will run...
pub fn startup_op(
    smartness_settings: &SmartnessSettings,
    session: Arc<Session>,
) -> Result<(), SmartnessError> {
    if smartness_settings.startup_enabled.is_some() && smartness_settings.startup_enabled.unwrap() {
        // run startup operation using session...
    }
    Ok(())
}

// function that will apply write operations as a warmup step.
// if warmup_enabled setting is true, it will run...
pub fn warmup_op(
    smartness_settings: &SmartnessSettings,
    session: Arc<Session>,
) -> Result<(), SmartnessError> {
    Ok(())
}

// function that will send a write operation using write_script from settings
pub fn write_op(session: Arc<Session>, insert: String) -> Result<(), SmartnessError> {
    Ok(())
}

// function that will send a read operation using read_script from settings
pub fn read_op(session: Arc<Session>, insert: String) -> Result<(), SmartnessError> {
    Ok(())
}
