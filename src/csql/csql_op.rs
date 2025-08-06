use std::{fs::File, sync::Arc, time::Duration};

use csv::{Reader, StringRecord};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    response::PagingState,
    value::CqlValue,
};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{config::smarteness_settings::SmartnessSettings, error::SmartnessError};

// function that will create a ScyllaDB sessions for writers and readers...
// this session will be used in other functions...
pub async fn create_session(
    smartness_settings: &SmartnessSettings,
) -> Result<(Session, Session), SmartnessError> {
    println!("Create Write Session started.");

    let write_session = SessionBuilder::new()
        .known_node(format!(
            "{}:{}",
            smartness_settings.cassandra_host.as_ref().unwrap().clone(),
            smartness_settings.cassandra_port.unwrap()
        ))
        .user(
            smartness_settings
                .cassandra_username
                .as_ref()
                .unwrap()
                .clone(),
            smartness_settings
                .cassandra_password
                .as_ref()
                .unwrap()
                .clone(),
        )
        .connection_timeout(Duration::from_secs(60))
        .build()
        .await
        .map_err(SmartnessError::ScyllaSessionError)?;

    println!("Create Read Session started.");

    let read_session = SessionBuilder::new()
        .known_node(format!(
            "{}:{}",
            smartness_settings.cassandra_host.as_ref().unwrap().clone(),
            smartness_settings.cassandra_port.unwrap()
        ))
        .user(
            smartness_settings
                .cassandra_username
                .as_ref()
                .unwrap()
                .clone(),
            smartness_settings
                .cassandra_password
                .as_ref()
                .unwrap()
                .clone(),
        )
        .connection_timeout(Duration::from_secs(60))
        .build()
        .await
        .map_err(SmartnessError::ScyllaSessionError)?;

    println!("Create Sessions finished.");

    Ok((write_session, read_session))
}

// function that will apply script as startup step.
// if startup_enabled setting is true, it will run...
pub async fn startup_op(
    smartness_settings: &SmartnessSettings,
    session: Arc<Session>,
) -> Result<(), SmartnessError> {
    if smartness_settings.startup_enabled.is_some() && smartness_settings.startup_enabled.unwrap() {
        println!("Startup Operations started.");

        // create keyspace
        session
            .query_unpaged(
                smartness_settings
                    .startup_create_schema_script
                    .as_ref()
                    .unwrap()
                    .clone(),
                (),
            )
            .await
            .map(|_| ())
            .map_err(SmartnessError::CsqlCreateKeyspaceError)?;

        println!("Create Schema applied.");

        // drop table
        session
            .query_unpaged(
                smartness_settings
                    .startup_drop_table_script
                    .as_ref()
                    .unwrap()
                    .clone(),
                (),
            )
            .await
            .map(|_| ())
            .map_err(SmartnessError::CsqlDropTableError)?;

        println!("Drop table applied.");

        // create table
        session
            .query_unpaged(
                smartness_settings
                    .startup_create_table_script
                    .as_ref()
                    .unwrap()
                    .clone(),
                (),
            )
            .await
            .map(|_| ())
            .map_err(SmartnessError::CsqlCreateTableError)?;

        println!("Create table applied.");
        println!("Startup Operations finished.");
    }

    println!("Waiting 30s to cluster apply schema creation...");

    sleep(Duration::from_secs(30)).await;
    Ok(())
}

// function that will apply write operations as a warmup step.
// if warmup_enabled setting is true, it will run...
pub async fn warmup_op(
    smartness_settings: &SmartnessSettings,
    session: Arc<Session>,
    dataset_file: File,
) -> Result<(), SmartnessError> {
    if smartness_settings.warmup_enabled.is_some() && smartness_settings.warmup_enabled.unwrap() {
        println!("Warmup Operations started.");

        let mut rdr = Reader::from_reader(dataset_file);
        let cols_qty = smartness_settings.cols_qty.unwrap();

        {
            let empty_header = StringRecord::new();
            let _headers = rdr.headers().unwrap_or(&empty_header);
        }

        let mut iter = rdr.into_records();
        let pos = iter.reader().position().clone();

        for _i in 0..smartness_settings.warmup_qty_ops.unwrap() {
            if let Some(record) = iter.next() {
                if let Ok(record) = record {
                    let uuid = Uuid::new_v4();

                    let mut cql_values = Vec::<CqlValue>::new();
                    cql_values.push(CqlValue::Uuid(uuid));

                    let mut count_col = 0;
                    for value in record.iter() {
                        if cols_qty != -1 && count_col >= cols_qty {
                            break;
                        }

                        cql_values.push(CqlValue::Text(value.to_owned()));

                        count_col += 1;
                    }

                    // create table
                    session
                        .query_unpaged(
                            smartness_settings.write_script.as_ref().unwrap().clone(),
                            cql_values,
                        )
                        .await
                        .map(|_| ())
                        .map_err(SmartnessError::WarmupInsertOpError)?;
                }
            } else {
                if iter.reader_mut().seek(pos.clone()).is_ok() {
                    iter = iter.into_reader().into_records();
                }
            }
        }

        println!("Warmup Operation finished.");
    }
    Ok(())
}

// function that will send a write operation using write_script from settings
pub async fn write_op<'a>(
    session: Arc<Session>,
    insert: &'a str,
    values: Vec<CqlValue>,
) -> Result<(), SmartnessError> {
    // execute write operation
    session
        .query_unpaged(insert, values)
        .await
        .map(|_| ())
        .map_err(SmartnessError::CsqlWriteOpError)?;
    Ok(())
}

// function that will send a read operation using read_script from settings
pub async fn read_op(session: Arc<Session>, read: &str) -> Result<(), SmartnessError> {
    // execute read operation
    session
        .query_single_page(read, (), PagingState::start())
        .await
        .map(|_| ())
        .map_err(SmartnessError::CsqlReadOpError)?;
    Ok(())
}
