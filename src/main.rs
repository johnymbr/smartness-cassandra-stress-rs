use std::{fs::File, path::Path};

use clap::Parser;
use error::SmartnessError;

use crate::config::{
    metrics_runtime, process_runtime::ProcessRuntime, smarteness_config::SmartnessConfig,
};

mod config;
mod error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'w', long)]
    workload: String,
}

fn main() -> Result<(), SmartnessError> {
    let args = Args::parse();

    println!("Workload Path {:?}", args.workload);

    let smartness_config = SmartnessConfig::new(args.workload)?;

    // we will check if dataset file exists...
    let dataset_path = Path::new(&smartness_config.dataset_path);
    if !dataset_path.exists() {
        return Err(SmartnessError::DatasetFileDoesNotExist);
    }

    let dataset_file = File::open(dataset_path).map_err(SmartnessError::DatasetFileOpenError)?;

    // Metrics runtime
    let metrics_runtime = metrics_runtime::create_runtime(&smartness_config)?;

    // Process runtime
    let process_runtime = ProcessRuntime::new(&smartness_config, dataset_file)?;
    process_runtime.config_runtime()?;

    metrics_runtime.shutdown_background();
    println!("Metrics Runtime stopped.");

    Ok(())
}
