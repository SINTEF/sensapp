use crate::bus::EventBus;
use crate::config::opcua::{OpcuaConfig, OpcuaSubscriptionConfig};
use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::datamodel::SensAppVec;
use crate::ingestors::opcua::opcua_browser::opcua_browser;
use crate::ingestors::opcua::opcua_utils::{
    data_values_to_typed_samples, monitored_item_to_sensor,
};
use anyhow::{Context, Error, Result};
use opcua::client::prelude::*;
use opcua::{
    client::prelude::{ClientBuilder, Session},
    sync::RwLock,
};
use sentry::integrations::anyhow::capture_anyhow;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

fn subscribe<CB>(
    session: Arc<RwLock<Session>>,
    subscription: OpcuaSubscriptionConfig,
    callback: CB,
) -> Result<(u32, Vec<MonitoredItemCreateResult>)>
where
    CB: OnSubscriptionNotification + Send + Sync + 'static,
{
    let namespace = subscription.namespace;
    let mut identifier_node_ids = subscription
        .identifiers
        .into_iter()
        .map(|identifier| NodeId::new(namespace, identifier))
        .collect::<Vec<NodeId>>();

    if let Some(autodiscovery) = subscription.autodiscovery.clone() {
        if autodiscovery.enabled {
            let variables = opcua_browser(session.clone(), subscription.namespace, autodiscovery)?;
            println!("Found variables: {}", variables.len());
            for variable in &variables {
                println!("Subscribed to variable: {:?}", variable);
            }
            // Append the autodiscovered variables to the list of identifiers
            identifier_node_ids.extend(variables);
        }
    }

    let session = session.read();
    let subscription_id = session.create_subscription(
        subscription.publishing_interval,
        subscription.lifetime_count,
        subscription.max_keep_alive_count,
        subscription.max_notifications_per_publish,
        subscription.priority,
        true, // publishing enabled
        callback,
    )?;

    // Convert the identifiers node ids to monitored items create requests
    let items = identifier_node_ids
        .into_iter()
        .map(|identifier| identifier.into())
        .collect::<Vec<MonitoredItemCreateRequest>>();

    let results =
        session.create_monitored_items(subscription_id, TimestampsToReturn::Both, &items)?;

    Ok((subscription_id, results))
}

pub async fn opcua_client(config: OpcuaConfig, event_bus: Arc<EventBus>) -> Result<()> {
    if config.logging {
        opcua::console_logging::init();
    }

    let subscriptions = config.subscriptions.clone();

    /*if subscriptions.is_empty() {
        anyhow::bail!("No subscriptions defined");
    }*/

    let client_builder: ClientBuilder = config.into();

    let mut client = match client_builder.client() {
        Some(client) => client,
        None => anyhow::bail!("Failed to create client"),
    };

    let (sender, receiver) = mpsc::unbounded_channel::<Batch>();

    let session = tokio::task::spawn_blocking(move || -> Result<Arc<RwLock<Session>>> {
        let endpoints = client
            .get_server_endpoints()
            .context("Failed to get server endpoints")?;

        let session = client
            .new_session(&endpoints)
            //.new_session_from_id("default", &endpoints)
            /* .new_session_from_info(SessionInfo {
                endpoint: endpoints.first().unwrap().clone(),
                user_identity_token: IdentityToken::Anonymous,
                preferred_locales: vec![],
            })*/
            .map_err(|e| Error::msg(format!("Failed to create session: {}", e)))?;

        // Get a mutable reference to the session
        {
            let mut session_write = session.write();
            session_write
                .connect_and_activate()
                .map_err(|status_code| {
                    Error::msg(format!(
                        "Failed to connect and activate session: {:?}",
                        status_code
                    ))
                })?;
        }

        for subscription in subscriptions {
            let sender = sender.clone();
            let _ = subscribe(
                session.clone(),
                subscription,
                DataChangeCallback::new(move |changed_monitored_items| {
                    let data_change_batch = changed_monitored_items
                        .iter()
                        .map(|&item| -> Result<SingleSensorBatch, Error> {
                            let sensor = match monitored_item_to_sensor(item) {
                                Ok(sensor) => sensor,
                                Err(e) => {
                                    return Err(
                                        e.context("Failed to convert monitored item to sensor")
                                    );
                                }
                            };

                            let samples = match data_values_to_typed_samples(
                                sensor.sensor_type,
                                item.values(),
                            ) {
                                Ok(samples) => samples,
                                Err(e) => {
                                    return Err(e.context(
                                        "Failed to convert data values to typed samples",
                                    ));
                                }
                            };

                            Ok(SingleSensorBatch::new(Arc::new(sensor), samples))
                        })
                        .collect::<Result<SensAppVec<SingleSensorBatch>, Error>>();

                    match data_change_batch {
                        Ok(data_change_batch) => {
                            sender
                                .send(Batch::new(data_change_batch))
                                // This is an unexpected error, so we kinda have to panic
                                .expect("Failed to send data change batch");
                        }
                        Err(e) => {
                            capture_anyhow(&e);
                        }
                    };
                }),
            )?;
        }
        println!("subscribed");
        Ok(session)
    })
    .await??;

    // Start the forever loop
    let a = Session::run_async(session);

    let worker: JoinHandle<Result<()>> = tokio::spawn(async move {
        let mut receiver = receiver;
        while let Some(batch) = receiver.recv().await {
            event_bus.publish(batch).await?;
        }
        Ok(())
    });
    let _ = worker.await?;

    // Stop the forever loop
    a.send(SessionCommand::Stop)
        .map_err(|_| Error::msg("Failed to stop session"))?;

    // We go there only if the run failed,
    // for example if the server is unreachable for too long.
    Err(Error::msg("Session run failed"))
}
