use std::{fs::File, path::Path, time::Duration};

use clap::Parser;
use error::SmartnessError;

use crate::config::{
    metrics_runtime, process_runtime::ProcessRuntime, smarteness_settings::SmartnessSettings,
};

mod config;
mod csql;
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

    let smartness_settings = SmartnessSettings::new(args.workload)?;

    println!("Settings loaded.");

    // we will check if dataset file exists...
    let dataset_path = Path::new(&smartness_settings.dataset_path);
    if !dataset_path.exists() {
        return Err(SmartnessError::DatasetFileDoesNotExist);
    }

    let dataset_file = File::open(dataset_path).map_err(SmartnessError::DatasetFileOpenError)?;

    // Process runtime
    let process_runtime = ProcessRuntime::new(&smartness_settings, dataset_file)?;
    process_runtime.handle_startup()?;
    {
        let dataset_file_warmup =
            File::open(dataset_path).map_err(SmartnessError::DatasetFileOpenError)?;
        process_runtime.handle_warmup(dataset_file_warmup)?;
    }

    // Metrics runtime
    let metrics_runtime =
        metrics_runtime::create_runtime(&smartness_settings, process_runtime.session.clone())?;

    process_runtime.start_runtime()?;
    process_runtime.shutdown();
    println!("Process Runtime stopped.");

    metrics_runtime.shutdown_timeout(Duration::from_secs(2));
    println!("Metrics Runtime stopped.");

    Ok(())
}
