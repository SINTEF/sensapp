use crate::{bus::EventBus, config::mqtt::MqttConfig};
use anyhow::{bail, Context, Result};
use rand::distributions::Alphanumeric;
use rand::Rng;
use rumqttc::{AsyncClient, MqttOptions, Transport};
use std::{sync::Arc, time::Duration};

fn random_client_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(18)
        .map(char::from)
        .collect()
}

fn get_client_id(config: &MqttConfig) -> String {
    config.client_id.clone().unwrap_or_else(|| {
        let mut client_id = "sensapp-".to_string();
        client_id.push_str(&random_client_id());
        client_id
    })
}

fn make_websocket_client_options(config: &MqttConfig, tls: bool) -> Result<MqttOptions> {
    let url = &config.url;
    let parsed_url =
        url::Url::parse(url).context(format!("Failed to parse MQTT WS URL: {}", url))?;

    let port = parsed_url.port().unwrap_or(8000);

    let mut mqtt_options = MqttOptions::new(get_client_id(config), url, port);

    if tls {
        mqtt_options.set_transport(Transport::wss_with_default_config());
    } else {
        mqtt_options.set_transport(Transport::Ws);
    }

    configure_mqtt_options(config, mqtt_options)
}

fn make_client_options(config: &MqttConfig) -> Result<MqttOptions> {
    let ws = config.url.starts_with("ws://");
    let ws_tls = config.url.starts_with("wss://");
    if ws || ws_tls {
        return make_websocket_client_options(config, ws_tls);
    }

    let mut parsed_url = url::Url::parse(&config.url)
        .with_context(|| format!("Failed to parse MQTT URL: {}", config.url))?;

    // The rumqttc crate requires the client_id as a GET parameter in the URL.
    // I don't like it so it can be passed as a normal argument. However it still
    // must be passed in the URL as the client_id is not mutable.
    let has_client_id = parsed_url.query_pairs().any(|(key, _)| key == "client_id");
    let url = if has_client_id {
        if config.client_id.is_some() {
            bail!("client_id is not allowed in `url` when it is set in MqttConfig");
        }
        config.url.clone()
    } else {
        let mut queries = parsed_url.query_pairs_mut();
        queries.append_pair("client_id", &get_client_id(config));
        queries.finish().to_string()
    };

    let mqtt_options = MqttOptions::parse_url(url).context("Failed to parse MQTT URL")?;

    configure_mqtt_options(config, mqtt_options)
}

fn configure_mqtt_options(
    config: &MqttConfig,
    mut mqtt_options: MqttOptions,
) -> Result<MqttOptions> {
    mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive_seconds));

    if let Some(username) = &config.username.clone() {
        let password = config.password.clone().unwrap_or_default();
        mqtt_options.set_credentials(username, password);
    }

    Ok(mqtt_options)
}

pub async fn mqtt_client(config: MqttConfig, _event_bus: Arc<EventBus>) -> Result<()> {
    let mqtt_options = make_client_options(&config)?;

    let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

    client
        .subscribe("GAIA/AIS", rumqttc::QoS::AtLeastOnce)
        .await?;

    loop {
        let notification = event_loop.poll().await;
        match notification {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                let topic = publish.topic;
                let payload = publish.payload;
                println!("Received message on topic: {}", topic);
                println!("Payload: {:?}", payload);
                /*let mut geobuf = geobuf::geobuf_pb::Data::new();
                use protobuf::Message;
                geobuf.merge_from_bytes(&payload); //.unwrap();
                match geobuf::decode::Decoder::decode(&geobuf).unwrap() {
                    serde_json::Value::Object(geojson) => {
                        println!("GeoJSON: {:?}", geojson);
                    }
                    _ => {}
                }*/
            }
            Ok(_) => {}
            Err(e) => {
                bail!("MQTT client error: {:?}", e);
            }
        }
    }
}
