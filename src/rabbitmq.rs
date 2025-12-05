/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-22
 *  @Filename: rabbitmq.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use std::sync::Arc;

use async_channel::{Receiver, Sender};
use bytes::BytesMut;
use futures_lite::StreamExt;
use lapin::{
    Connection, ConnectionProperties, message::Delivery, options::*, types::AMQPValue,
    types::FieldTable,
};
use tokio::sync::Mutex;

use crate::{parser::Reply, tool::CommandID};

/// Configuration options for the RabbitMQ service.
#[derive(Clone)]
pub struct RabbitMQConfig {
    /// Actor name
    pub actor_name: String,
    /// The RabbitMQ server URL.
    pub url: String,
    /// The exchange to publish messages to.
    pub exchange: String,
    /// Whether to monitor TCP replies and publish them to RabbitMQ.
    pub monitor_tcp_replies: bool,
}

impl RabbitMQConfig {
    pub fn default(actor_name: String) -> Self {
        Self {
            actor_name,
            url: String::from("amqp://localhost:5672"),
            exchange: String::from("sdss_exchange"),
            monitor_tcp_replies: true,
        }
    }
}

pub async fn handle_replies(
    channel: lapin::Channel,
    config: RabbitMQConfig,
    rabbitmq_receiver: Receiver<Reply>,
    command_id_pool_mutex: Arc<Mutex<CommandID>>,
) -> Result<(), String> {
    loop {
        match rabbitmq_receiver.recv().await {
            Ok(reply) => {
                let mut command_id_pool = command_id_pool_mutex.lock().await;

                log::debug!("Publishing reply from actor to RabbitMQ");

                let sender = config.actor_name.clone();

                let command_id = reply.command_id;

                let uuid = if let Some(uuid) = command_id_pool.get_uuid(command_id as u16) {
                    uuid.clone()
                } else {
                    log::warn!("No UUID found for command ID {}.", command_id);
                    String::from("00000000-0000-0000-0000-000000000000")
                };

                let commander_id = if let Some(commander) = command_id_pool.get_commander(&uuid) {
                    commander.clone()
                } else {
                    String::from("broadcast")
                };

                let mut headers = FieldTable::default();
                headers.insert(
                    "message_code".into(),
                    AMQPValue::LongString(reply.code.to_string().into()),
                );
                headers.insert(
                    "command_id".into(),
                    AMQPValue::LongString(uuid.clone().into()),
                );
                headers.insert(
                    "commander_id".into(),
                    AMQPValue::LongString(commander_id.clone().into()),
                );
                headers.insert("sender".into(), AMQPValue::LongString(sender.into()));
                headers.insert("internal".into(), AMQPValue::Boolean(false));

                let routing_key = format!("reply.{}", commander_id);

                // Prepare the properties.
                let properties = lapin::BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_headers(headers)
                    .with_correlation_id(uuid.clone().into());

                let payload = serde_json::to_vec(&reply.keywords).unwrap();

                // Here you would add the logic to publish the reply to RabbitMQ.
                if let Err(e) = channel
                    .basic_publish(
                        &config.exchange,
                        routing_key.as_str(),
                        BasicPublishOptions::default(),
                        payload.as_slice(),
                        properties,
                    )
                    .await
                {
                    log::error!("Failed to publish reply to RabbitMQ: {}", e);
                }

                if reply.code == ':' || reply.code.to_ascii_lowercase() == 'f' {
                    log::debug!(
                        "Command {} is finished with code '{}'. Returning command_id to the pool.",
                        reply.command_id,
                        reply.code,
                    );
                    command_id_pool.finish_command(command_id as u16);
                }
            }
            Err(_) => {
                log::warn!("RabbitMQ receiver channel closed");
                return Ok(());
            }
        }
    }
}

pub async fn start_rabbitmq_service(
    config: RabbitMQConfig,
    tcp_sender: Sender<BytesMut>,
    rabbitmq_receiver: Receiver<Reply>,
) -> Result<(), String> {
    let command_id_pool_mutex = Arc::new(Mutex::new(CommandID::new()));

    let connection_properties = ConnectionProperties::default();
    let connection = match Connection::connect(&config.url, connection_properties).await {
        Ok(conn) => conn,
        Err(e) => {
            log::error!("Failed to connect to RabbitMQ: {}", e);
            return Err(e.to_string());
        }
    };

    log::debug!("Connected to RabbitMQ at {}", config.url);

    let channel = match connection.create_channel().await {
        Ok(channel) => channel,
        Err(e) => {
            log::error!("Failed to create RabbitMQ channel: {}", e);
            return Err(e.to_string());
        }
    };

    let mut queue_options = QueueDeclareOptions::default();
    queue_options.auto_delete = false;
    queue_options.exclusive = true;
    let queue = match channel
        .queue_declare(
            format!("{}_commands", config.actor_name).as_str(),
            queue_options,
            FieldTable::default(),
        )
        .await
    {
        Ok(q) => q,
        Err(e) => {
            log::error!("Failed to declare RabbitMQ queue: {}", e);
            return Err(e.to_string());
        }
    };

    let exchange_declare_options = ExchangeDeclareOptions {
        auto_delete: true,
        ..Default::default()
    };

    match channel
        .exchange_declare(
            &config.exchange,
            lapin::ExchangeKind::Topic,
            exchange_declare_options,
            FieldTable::default(),
        )
        .await
    {
        Ok(_) => (),
        Err(e) => {
            log::error!("Failed to declare RabbitMQ exchange: {}", e);
            return Err(e.to_string());
        }
    };

    match channel
        .queue_bind(
            queue.name().as_str(),
            &config.exchange,
            format!("command.{}.#", config.actor_name).as_str(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(_) => (),
        Err(e) => {
            log::error!("Failed to bind RabbitMQ queue: {}", e);
            return Err(e.to_string());
        }
    };

    let mut consumer = match channel
        .basic_consume(
            queue.name().as_str(),
            format!("{}_consumer", config.actor_name).as_str(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(consumer) => consumer,
        Err(e) => {
            log::error!("Failed to start RabbitMQ consumer: {}", e);
            return Err(e.to_string());
        }
    };

    if config.monitor_tcp_replies {
        let channel_b = match connection.create_channel().await {
            Ok(channel) => channel,
            Err(e) => {
                log::error!("Failed to create RabbitMQ channel: {}", e);
                return Err(e.to_string());
            }
        };
        let config_clone = config.clone();
        let command_id_pool_clone = command_id_pool_mutex.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_replies(
                channel_b,
                config_clone,
                rabbitmq_receiver,
                command_id_pool_clone,
            )
            .await
            {
                log::error!("Failed to handle RabbitMQ replies: {}", e);
            }
        });
    }

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.unwrap();
        match delivery.ack(BasicAckOptions::default()).await {
            Ok(_) => (),
            Err(e) => {
                log::error!("Failed to acknowledge RabbitMQ message: {}", e);
                continue;
            }
        }
        process_command(&tcp_sender, &delivery, &command_id_pool_mutex).await;
    }

    Ok(())
}

fn get_header_value(delivery: &Delivery, key: &str) -> Option<AMQPValue> {
    if let Some(headers) = delivery.properties.headers() {
        headers.inner().get(key).cloned()
    } else {
        None
    }
}

async fn process_command(
    tcp_sender: &Sender<BytesMut>,
    delivery: &Delivery,
    command_id_pool: &Arc<Mutex<CommandID>>,
) {
    let command_id = get_header_value(delivery, "command_id");
    let command_id = if command_id.is_none() {
        log::warn!("Command ID not found in message headers");
        return;
    } else if let AMQPValue::LongString(id) = command_id.unwrap() {
        id.to_string()
    } else {
        log::warn!("Command ID header is not a LongString");
        return;
    };

    let commander_id = get_header_value(delivery, "commander_id");
    let commander_id = if commander_id.is_none() {
        log::warn!("Commander ID not found in message headers");
        return;
    } else if let AMQPValue::LongString(id) = commander_id.unwrap() {
        id.to_string()
    } else {
        log::warn!("Commander ID header is not a LongString");
        return;
    };

    let command_string: CommandPayload = serde_json::from_slice(&delivery.data).unwrap();

    log::debug!(
        "Processing command from commander {} with UUID '{}' and command string '{}'",
        commander_id,
        command_id,
        command_string.command_string
    );

    let mut command_id_pool = command_id_pool.lock().await;
    let tcp_command_id = command_id_pool.get_command_id();
    log::debug!(
        "Mapped UUID '{}' to TCP command ID {}",
        command_id,
        tcp_command_id
    );

    let tcp_command_string = format!("{} {}", tcp_command_id, command_string.command_string);

    log::debug!("Queuing TCP command: '{}'", tcp_command_string);

    tcp_sender
        .send(BytesMut::from(tcp_command_string.as_bytes()))
        .await
        .unwrap();

    command_id_pool.register_command(&command_id, &commander_id, tcp_command_id);
}

#[derive(serde::Deserialize, Debug)]
struct CommandPayload {
    command_string: String,
}
