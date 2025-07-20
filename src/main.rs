use std::time::Duration;

use clap::Parser;
use error::SmartnessError;
use tokio::time::sleep;
use tokio_util::task::TaskTracker;

mod error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'w', long)]
    workers: Option<usize>,

    #[arg(long)]
    csv_path: String,

    #[arg(long)]
    metrics_dir: String,
}

fn main() -> Result<(), SmartnessError> {
    let args = Args::parse();

    println!("Worker {:?}", args.workers);
    println!("Csv Path {}", args.csv_path);
    println!("Metrics DIR {}", args.metrics_dir);

    // TODO: create a Tokio runtime with number of workers equal args.workers...
    let mut process_runtime = tokio::runtime::Builder::new_multi_thread();
    if let Some(workers) = args.workers {
        process_runtime.worker_threads(workers);
    }

    let process_runtime = process_runtime
        .thread_name("cassandra-reqs-pool")
        .enable_all()
        .build()
        .map_err(SmartnessError::ProcessRuntimeBuildError)?;

    process_runtime.block_on(async {
        let tracker = TaskTracker::new();

        for i in 0..100 {
            tracker.spawn(async move {
                sleep(Duration::from_secs(1)).await;
                println!("Task {} shutting down", i);
            });
        }

        tracker.close();
        tracker.wait().await;

        println!("This is printed after all of the tasks.");
    });

    Ok(())
}
