use std::{fs::File, path::Path, time::Duration};

use clap::Parser;
use csv::StringRecord;
use error::SmartnessError;
use tokio::time::sleep;
use tokio_util::task::TaskTracker;

use crate::config::{
    metrics_runtime,
    process_runtime::{self, ProcessRuntime},
    smarteness_config::SmartnessConfig,
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
    let _metrics_runtime = metrics_runtime::create_runtime()?;

    // Process runtime
    let _process_runtime = ProcessRuntime::new(&smartness_config, dataset_file)?;

    Ok(())
}
