use std::{fs::File, sync::Arc, time::Duration};

use csv::{Reader, StringRecord};
use tokio::{runtime::Runtime, time::sleep};
use tokio_util::task::TaskTracker;

use crate::{config::smarteness_config::SmartnessConfig, error::SmartnessError};

pub struct ProcessRuntime<'a> {
    runtime: Arc<Runtime>,
    dataset_file: Arc<File>,
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
            runtime: Arc::new(runtime),
            smartness_config,
            dataset_file: Arc::new(dataset_file),
        })
    }

    pub fn config_runtime(&self) -> Result<(), SmartnessError> {
        let dataset_file = Arc::clone(&self.dataset_file);

        // running time has precendency over cycle...
        if let Some(running_time) = &self.smartness_config.running_time {
            println!("Running time: {}", running_time);

            let runtime = Arc::clone(&self.runtime);

            let main_task = runtime.spawn(async move {
                let mut count = 0;
                let mut rdr = Reader::from_reader(dataset_file);

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
                    _ = tokio::time::sleep(Duration::from_secs(*running_time as u64 * 60)) => {
                        println!("We had a timeout....");
                    }
                }

                main_task.abort();
                println!("ProcessRuntime -> Production Task aborted...");
                sleep(Duration::from_secs(2)).await;
            });
        } else if let Some(cycles) = &self.smartness_config.cycles {
            let runtime = Arc::clone(&self.runtime);
            runtime.block_on(async {
                let tracker = TaskTracker::new();

                let mut count = 0;
                let mut rdr = Reader::from_reader(dataset_file);

                {
                    let empty_header = StringRecord::new();
                    let _headers = rdr.headers().unwrap_or(&empty_header);
                    // println!("CSV Headers: {:?}", headers);
                }

                let mut iter = rdr.into_records();
                let pos = iter.reader().position().clone();
                loop {
                    if count <= *cycles {
                        break;
                    }

                    if let Some(record) = iter.next() {
                        if let Ok(record) = record {
                            tracker.spawn(async move {
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

                tracker.close();
                tracker.wait().await;

                sleep(Duration::from_secs(2)).await;

                println!("This is printed after all of the tasks.");
            });
        }

        Ok(())
    }
}
