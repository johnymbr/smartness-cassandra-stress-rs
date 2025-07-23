use std::fs::File;

use csv::StringRecord;
use tokio::runtime::Runtime;

use crate::{config::smarteness_config::SmartnessConfig, error::SmartnessError};

pub struct ProcessRuntime<'a> {
    runtime: Runtime,
    dataset_file: File,
    smartness_config: &'a SmartnessConfig,
}

impl<'a> ProcessRuntime<'a> {
    pub fn new(
        smartness_config: &'a SmartnessConfig,
        dataset_file: File,
    ) -> Result<Self, SmartnessError> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        if let Some(workers) = smartness_config.workers {
            runtime.worker_threads(workers);
        }

        let runtime = runtime
            .thread_name("cassandra-reqs-pool")
            .enable_all()
            .build()
            .map_err(SmartnessError::ProcessRuntimeBuildError)?;

        // here we will handle when we will use cycles or time to run our tests...

        Ok(Self {
            runtime,
            smartness_config,
            dataset_file,
        })
    }

    pub fn config_runtime(&self) -> Result<(), SmartnessConfig> {
        // running time has precendency over cycle...
        if let Some(running_time) = &self.smartness_config.running_time {
            println!("Running time: {}", running_time);

            &self.runtime.spawn(async {
                let mut count = 0;
                let mut rdr = csv::Reader::from_reader(&self.dataset_file);

                {
                    let empty_header = StringRecord::new();
                    let _headers = rdr.headers().unwrap_or(&empty_header);
                    // println!("CSV Headers: {:?}", headers);
                }

                let mut iter = rdr.into_records();
                let pos = iter.reader().position().clone();
                loop {
                    if let Some(record) = iter.next() {
                        if let Ok(record) = record {
                            tokio::spawn(async move {
                                if count % 10000 == 0 {
                                    println!(
                                        "Cycle: {} - CSV Record: C0 = {:?}, C1 = {:?}",
                                        count, &record[0], &record[1]
                                    );
                                }
                            });
                            count += 1;
                        }
                    } else {
                        if iter.reader_mut().seek(pos.clone()).is_ok() {
                            iter = iter.into_reader().into_records();
                        }
                    }
                }
            });
        }

        if let Some(cycles) = smartness_config.cycles {
            process_runtime.block_on(async {
                let tracker = TaskTracker::new();

                let mut count = 0;
                let mut rdr = csv::Reader::from_reader(dataset_file);

                {
                    let empty_header = StringRecord::new();
                    let headers = rdr.headers().unwrap_or(&empty_header);
                    println!("CSV Headers: {:?}", headers);
                }

                for result in rdr.records() {
                    if count <= cycles {
                        if let Ok(record) = result {
                            tracker.spawn(async move {
                                if count % 100 == 0 {
                                    println!(
                                        "Cycle: {} - CSV Record: C0 = {:?}, C1 = {:?}",
                                        count, &record[0], &record[1]
                                    );
                                }
                            });
                            count += 1;
                        }
                    } else {
                        break;
                    }
                }

                tracker.close();
                tracker.wait().await;

                sleep(Duration::from_secs(5)).await;

                println!("This is printed after all of the tasks.");
            });
        }

        println!("Creating signal_runtime...");
        let signal_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cassandra-signal-pool")
            .enable_all()
            .build()
            .map_err(SmartnessError::MetricsRuntimeBuildError)?;

        let mut interrupt = Box::pin(tokio::signal::ctrl_c());
        signal_runtime.block_on(async {
            tokio::select! {
                _ = &mut interrupt => {
                    println!("It was interrupted....");
                },
                _ = tokio::time::sleep(Duration::from_secs(smartness_config.running_time.unwrap() as u64 * 60)) => {
                    println!("We had a timeout....");
                }
            }

            println!("Shutdown process_runtime");
            process_runtime.shutdown_background();
            sleep(Duration::from_secs(2)).await;
            println!("Shutdown metrics_runtime");
            metrics_runtime.shutdown_background();
        });

        Ok(())
    }
}
