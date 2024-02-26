use crate::bus::message;
use crate::config::load_configuration;
use crate::http::server::run_http_server;
use crate::http::state::HttpServerState;
use axum::http::StatusCode;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use storage::postgresql::postgresql::PostgresStorage;
use storage::sqlite::sqlite::SqliteStorage;
use storage::storage::GenericStorage;
use tracing::event;
use tracing::Level;
mod bus;
mod config;
mod datamodel;
mod http;
mod importers;
mod infer;
mod name_to_uuid;
mod storage;

#[tokio::main]
async fn main() {
    // Load configuration
    load_configuration().expect("Failed to load configuration");
    let config = config::get().expect("Failed to get configuration");

    /*let (tx, rx) = tokio::sync::mpsc::channel(100); // Channel with buffer size 100

    // Simulate event emitter
    tokio::spawn(async move {
        for i in 1..=1004 {
            tx.send(i).await.unwrap(); // Send events
        }
    });

    // Create a stream from the receiver and buffer it in chunks
    use futures::stream::StreamExt;

    //let mut buffered_stream = rx.chunks(10); // Buffer size of 10
    let mut buffered_stream = ReceiverStream::new(rx).chunks(10); // Chunk size of 10

    // Process chunks of events
    while let Some(events) = buffered_stream.next().await {
        // `events` is a Vec containing a chunk of events
        println!("Handling chunk of events: {:?}", events);
    }*/

    let sqlite_connection_string = config.sqlite_connection_string.clone();
    if sqlite_connection_string.is_none() {
        eprintln!("No SQLite connection string provided");
        std::process::exit(1);
    }
    let sqlite_storage = SqliteStorage::connect(sqlite_connection_string.unwrap().as_str())
        .await
        .expect("Failed to connect to SQLite");

    sqlite_storage
        .create_or_migrate()
        .await
        .expect("Failed to create or migrate database");

    /*let postgres_connection_string = config.postgres_connection_string.clone();
    if postgres_connection_string.is_none() {
        eprintln!("No PostgreSQL connection string provided");
        std::process::exit(1);
    }
    let postgres_storage = PostgresStorage::connect(postgres_connection_string.unwrap().as_str())
        .await
        .expect("Failed to connect to PostgreSQL");

    postgres_storage
        .create_or_migrate()
        .await
        .expect("Failed to create or migrate database");*/

    let timescaledb_connection_string = config.timescaledb_connection_string.clone();
    if timescaledb_connection_string.is_none() {
        eprintln!("No TimescaleDB connection string provided");
        std::process::exit(1);
    }
    let timescaledb_storage = storage::timescaledb::timescaledb::TimeScaleDBStorage::connect(
        timescaledb_connection_string.unwrap().as_str(),
    )
    .await
    .expect("Failed to connect to TimescaleDB");

    timescaledb_storage
        .create_or_migrate()
        .await
        .expect("Failed to create or migrate database");

    let columns = infer::columns::infer_column(vec![], false, true);
    let _ = infer::datetime_guesser::likely_datetime_column(&vec![], &vec![]);
    let _ = infer::geo_guesser::likely_geo_columns(&vec![], &vec![]);

    /*let event_bus = event_bus::EVENT_BUS
        .get_or_init(|| event_bus::init_event_bus())
        .await;
    */

    let event_bus = bus::event_bus::init_event_bus();
    //let mut wololo = event_bus.main_bus_receiver.activate_cloned();
    let mut wololo2 = event_bus.main_bus_receiver.activate_cloned();

    // Exit the program if a panic occurs
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    // spawn a task that prints the events to stdout
    /*tokio::spawn(async move {
        while let Ok(message) = wololo.recv().await {
            //println!("Received event a: {:?}", message);

            use crate::storage::storage::StorageInstance;
            let toto: &dyn StorageInstance = &sqlite_storage;

            match message {
                message::Message::Publish(message::PublishMessage {
                    batch,
                    sync_receiver: _,
                    sync_sender,
                }) => {
                    toto.publish(batch, sync_sender)
                        .await
                        .expect("Failed to publish batch sqlite");
                    println!("Published batch sqlite");
                    //sync_receiver.activate().recv().await.unwrap();
                } /*message::Message::SyncRequest(message::RequestSyncMessage { sender }) => {
                      println!("Received sync request");
                      toto.sync().await.unwrap();
                      sender.broadcast(()).await.unwrap();
                  }*/
            }
        }
        println!("Done");
        // exit program
        std::process::exit(0);
    });*/
    tokio::spawn(async move {
        while let Ok(message) = wololo2.recv().await {
            //println!("Received event a: {:?}", message);

            use crate::storage::storage::StorageInstance;
            //let toto: &dyn StorageInstance = &postgres_storage;
            let toto: &dyn StorageInstance = &timescaledb_storage;

            match message {
                message::Message::Publish(message::PublishMessage {
                    batch,
                    sync_receiver: _,
                    sync_sender,
                }) => {
                    toto.publish(batch, sync_sender)
                        .await
                        .expect("Failed to publish batch postgresql");
                    println!("Published batch postgresql");
                    //sync_receiver.activate().recv().await.unwrap();
                } /*message::Message::SyncRequest(message::RequestSyncMessage { sender }) => {
                      println!("Received sync request");
                      toto.sync().await.unwrap();
                      sender.broadcast(()).await.unwrap();
                  }*/
            }
        }
        println!("Done");
        // exit program
        std::process::exit(0);
    });
    /*tokio::spawn(async move {
        while let Some(event) = wololo2.recv().await.ok() {
            println!("Received event b: {:?}", event);
        }
    });*/

    let endpoint = config.endpoint;
    let port = config.port;
    println!("📡 HTTP server listening on {}:{}", endpoint, port);
    match run_http_server(
        HttpServerState {
            name: Arc::new("SensApp".to_string()),
            event_bus,
        },
        SocketAddr::from((endpoint, port)),
    )
    .await
    {
        Ok(_) => {
            event!(Level::INFO, "HTTP server stopped");
        }
        Err(err) => {
            event!(Level::ERROR, "HTTP server failed: {:?}", err);
        }
    }
}

async fn handler() -> &'static str {
    "Hello, world!"
}

async fn publish_stream_handler(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
    let mut count = 0usize;
    let mut stream = body.into_data_stream();

    loop {
        let chunk = stream.try_next().await;
        match chunk {
            Ok(bytes) => match bytes {
                Some(bytes) => count += bytes.into_iter().filter(|b| *b == b'\n').count(),
                None => break,
            },
            Err(_) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error reading body".to_string(),
                ))
            }
        }
    }

    Ok(count.to_string())
}

async fn publish_csv(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
    let stream = body.into_data_stream();
    let stream = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
    let reader = stream.into_async_read();
    let mut csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .create_reader(reader);

    println!("{:?}", csv_reader.has_headers());
    println!("{:?}", csv_reader.headers().await.unwrap());
    let mut records = csv_reader.records();

    println!("Reading CSV");
    while let Some(record) = records.next().await {
        let record = record.unwrap();
        println!("{:?}", record);
    }
    println!("Done reading CSV");

    Ok("ok".to_string())
}
