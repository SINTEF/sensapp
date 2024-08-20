#![forbid(unsafe_code)]
use crate::bus::message;
use crate::config::load_configuration;
use crate::ingestors::http::server::run_http_server;
use crate::ingestors::http::state::HttpServerState;
use bus::EventBus;
use config::SensAppConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use storage::storage_factory::create_storage_from_connection_string;
use tracing::event;
use tracing::Level;
mod bus;
mod config;
mod crud;
mod datamodel;
mod importers;
mod infer;
mod ingestors;
mod name_to_uuid;
mod parsing;
mod storage;

fn main() {
    load_configuration().expect("Failed to load configuration");
    let config = config::get().expect("Failed to get configuration");

    if let Some(sentry_dsn) = &config.sentry_dsn {
        let _sentry = sentry::init((
            sentry_dsn.as_str(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                debug: true,
                ..Default::default()
            },
        ));
    }

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install CryptoProvider");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(async_main(config));
}

async fn async_main(config: Arc<SensAppConfig>) {
    // sentry::capture_message("Hello, Sentry 2!", sentry::Level::Info);

    // amqp_example().await.expect("Failed to start AMQP example");

    // Load configuration

    sinteflake::set_instance_id(config.instance_id).unwrap();
    sinteflake::set_instance_id_async(config.instance_id)
        .await
        .unwrap();

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

    /*let sqlite_connection_string = config.sqlite_connection_string.clone();
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
        .expect("Failed to create or migrate database");*/

    let storage = create_storage_from_connection_string("sqlite://test.db")
        //let storage = create_storage_from_connection_string("postgres://localhost:5432/sensapp")
        // password is postgres
        //let storage = create_storage_from_connection_string(
        //    "timescaledb://postgres:postgres@localhost:5432/sensapp",
        //)
        //let storage = create_storage_from_connection_string("duckdb://caca2.db")
        //let storage = create_storage_from_connection_string(
        //    "bigquery://key.json?project_id=smartbuildinghub&dataset_id=sensapp_dev_3",
        //)
        //let storage = create_storage_from_connection_string("rrdcached://localhost:42217?preset=munin")
        .await
        .expect("Failed to create storage");

    if !config.skip_migrations {
        storage
            .create_or_migrate()
            .await
            .expect("Failed to create or migrate database");
    }

    /*let duckdb_storage = DuckDBStorage::connect("sensapp.db")
        .await
        .expect("Failed to connect to DuckDB");

    duckdb_storage
        .create_or_migrate()
        .await
        .expect("Failed to create or migrate database");*/

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

    /*let timescaledb_connection_string = config.timescaledb_connection_string.clone();
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
    let _ = infer::geo_guesser::likely_geo_columns(&vec![], &vec![]);*/

    /*let event_bus = event_bus::EVENT_BUS
        .get_or_init(|| event_bus::init_event_bus())
        .await;
    */

    let event_bus = Arc::new(EventBus::new());
    let mut wololo = event_bus.main_bus_receiver.activate_cloned();
    // let mut wololo2 = event_bus.main_bus_receiver.activate_cloned();

    // Exit the program if a panic occurs
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let storage_for_publish = storage.clone();

    // spawn a task that prints the events to stdout
    tokio::spawn(async move {
        while let Ok(message) = wololo.recv().await {
            println!("Received event a: {:?}", message);

            use crate::storage::storage::StorageInstance;
            //let toto: &dyn StorageInstance = &storage;
            let toto: &dyn StorageInstance = storage_for_publish.as_ref();

            match message {
                message::Message::Publish(message::PublishMessage {
                    batch,
                    sync_receiver: _,
                    sync_sender,
                }) => {
                    let start_time = std::time::Instant::now();
                    toto.publish(batch, sync_sender)
                        .await
                        .expect("Failed to publish batch sqlite");
                    let elapsed = start_time.elapsed();
                    //println!("Published batch sqlite: {:?}", elapsed);
                    println!("Published batch bigquery: {:?}", elapsed);
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
        while let Ok(message) = wololo2.recv().await {
            //println!("Received event a: {:?}", message);

            use crate::storage::storage::StorageInstance;
            //let toto: &dyn StorageInstance = &postgres_storage;
            //let toto: &dyn StorageInstance = &timescaledb_storage;
            let toto: &dyn StorageInstance = &duckdb_storage;

            match message {
                message::Message::Publish(message::PublishMessage {
                    batch,
                    sync_receiver: _,
                    sync_sender,
                }) => {
                    let start_time = std::time::Instant::now();
                    toto.publish(batch, sync_sender)
                        .await
                        .expect("Failed to publish batch duckdb");
                    let elapsed = start_time.elapsed();
                    println!("Published batch duckdb: {:?}", elapsed);
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
    /*tokio::spawn(async move {
        while let Some(event) = wololo2.recv().await.ok() {
            println!("Received event b: {:?}", event);
        }
    });*/

    let wololo = config.clone();
    let opcua_event_bus = event_bus.clone();
    //tokio::task::spawn_blocking(move || {
    if let Some(opcua_configs) = wololo.opcua.as_ref() {
        for opcua_config in opcua_configs {
            let cloned_config = opcua_config.clone();
            let cloned_event_bus = opcua_event_bus.clone();
            tokio::spawn(async move {
                ingestors::opcua::opcua_client(cloned_config, cloned_event_bus)
                    .await
                    .expect("Failed to start OPC UA client");
            });
            //.await
            //.expect("Failed to start OPC UA client");
        }
        println!("OPC UA clients started");
    }

    let mqtt_event_bus = event_bus.clone();
    if let Some(mqtt_configs) = config.mqtt.as_ref() {
        for mqtt_config in mqtt_configs {
            let cloned_config = mqtt_config.clone();
            let cloned_event_bus = mqtt_event_bus.clone();
            tokio::spawn(async move {
                ingestors::mqtt::mqtt_client(cloned_config, cloned_event_bus)
                    .await
                    .expect("Failed to start MQTT client");
            });
        }
        println!("MQTT clients started");
    }
    //});
    //.await
    //.expect("Failed to start OPC UA clients");

    let endpoint = config.endpoint;
    let port = config.port;
    println!("📡 HTTP server listening on {}:{}", endpoint, port);
    match run_http_server(
        HttpServerState {
            name: Arc::new("SensApp".to_string()),
            event_bus,
            //storage: storage.clone(),
            storage,
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

// async fn handler() -> &'static str {
//     "Hello, world!"
// }

// async fn publish_stream_handler(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
//     let mut count = 0usize;
//     let mut stream = body.into_data_stream();

//     loop {
//         let chunk = stream.try_next().await;
//         match chunk {
//             Ok(bytes) => match bytes {
//                 Some(bytes) => count += bytes.into_iter().filter(|b| *b == b'\n').count(),
//                 None => break,
//             },
//             Err(_) => {
//                 return Err((
//                     StatusCode::INTERNAL_SERVER_ERROR,
//                     "Error reading body".to_string(),
//                 ))
//             }
//         }
//     }

//     Ok(count.to_string())
// }

// async fn publish_csv(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
//     let stream = body.into_data_stream();
//     let stream = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
//     let reader = stream.into_async_read();
//     let mut csv_reader = csv_async::AsyncReaderBuilder::new()
//         .has_headers(true)
//         .delimiter(b';')
//         .create_reader(reader);

//     println!("{:?}", csv_reader.has_headers());
//     println!("{:?}", csv_reader.headers().await.unwrap());
//     let mut records = csv_reader.records();

//     println!("Reading CSV");
//     while let Some(record) = records.next().await {
//         let record = record.unwrap();
//         println!("{:?}", record);
//     }
//     println!("Done reading CSV");

//     Ok("ok".to_string())
// }
