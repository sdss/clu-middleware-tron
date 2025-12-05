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
    pub uri: String,
    /// The exchange to publish messages to.
    pub exchange: String,
    /// Whether to monitor TCP replies and publish them to RabbitMQ.
    pub monitor_tcp_replies: bool,
}

impl RabbitMQConfig {
    /// Creates a default RabbitMQ configuration for an `actor_name`.
    pub fn default(actor_name: String) -> Self {
        Self {
            actor_name,
            uri: String::from("amqp://localhost:5672"),
            exchange: String::from("sdss_exchange"),
            monitor_tcp_replies: true,
        }
    }
}

/// Handles replies received from the RabbitMQ receiver channel and publishes them to RabbitMQ.
///
/// # Arguments
/// * `channel` - The Lapin channel to use for publishing messages.
/// * `config` - The RabbitMQ configuration.
/// * `rabbitmq_receiver` - The receiver channel from which replies are received.
/// * `command_id_pool_mutex` - A mutex-protected CommandID pool for tracking command UUIDs.
///
pub async fn handle_replies(
    channel: lapin::Channel,
    config: RabbitMQConfig,
    rabbitmq_receiver: Receiver<Reply>,
    command_id_pool_mutex: Arc<Mutex<CommandID>>,
) -> Result<(), String> {
    loop {
        // Loop to receive replies from the queue. These messages are put here by the TCP client handler.
        match rabbitmq_receiver.recv().await {
            Ok(reply) => {
                // Lock the command ID pool to access UUIDs and commanders.
                let mut command_id_pool = command_id_pool_mutex.lock().await;

                log::debug!("Publishing reply from actor to RabbitMQ");

                // Actor sending the message, i.e., us.
                let sender = config.actor_name.clone();

                // The numeric command ID from the reply.
                let command_id = reply.command_id;

                // Get the UUID associated with the command ID from the pool.
                // If the command ID is not found or this is a broadcast (command_id=0)
                // use a default UUID.
                let mut uuid = String::from("00000000-0000-0000-0000-000000000000");
                let mut is_broadcast = false;
                if let Some(found_uuid) = command_id_pool.get_uuid(command_id as u16) {
                    uuid = found_uuid.clone();
                } else if command_id == 0 {
                    log::debug!("Command ID is 0, using broadcast UUID for reply publishing.");
                    is_broadcast = true;
                } else {
                    log::warn!("No UUID found for command ID {}.", command_id);
                    is_broadcast = true;
                };

                // Get the commander ID associated with the UUID from the pool. This is a '.'
                // concatenated string of the client chain that sent the command and the consumer
                // (i.e., us). If not found, use "broadcast".
                let commander_id = if let Some(commander) = command_id_pool.get_commander(&uuid) {
                    commander.clone()
                } else {
                    String::from("broadcast")
                };

                // Prepare the header fields.
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

                // Prepare the properties. Correlation ID is the command UUID.
                let properties = lapin::BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_headers(headers)
                    .with_correlation_id(uuid.clone().into());

                // Routing key is 'reply.<commander_id>' unless this is a broadcast,
                // in which case it is 'reply.broadcast'.
                let routing_key = if is_broadcast {
                    "reply.broadcast".to_string()
                } else {
                    format!("reply.{}", commander_id)
                };

                // Serialize the keywords to JSON for the message payload.
                let payload = serde_json::to_vec(&reply.keywords).unwrap();

                // Publish the message to RabbitMQ.
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

                // If the reply code indicates the command is finished, return the command ID to the pool.
                if reply.code == ':' || reply.code.eq_ignore_ascii_case(&'f') {
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

/// Starts the RabbitMQ service to listen for commands and handle replies.
///
/// # Arguments
/// * `config` - The RabbitMQ configuration.
/// * `tcp_sender` - The sender channel to send TCP commands.
/// * `rabbitmq_receiver` - The receiver channel to receive replies from TCP.
///
pub async fn start_rabbitmq_service(
    config: RabbitMQConfig,
    tcp_sender: Sender<BytesMut>,
    rabbitmq_receiver: Receiver<Reply>,
) -> Result<(), String> {
    // Create a mutex-protected CommandID pool.
    let command_id_pool_mutex = Arc::new(Mutex::new(CommandID::new()));

    // Connect to RabbitMQ.
    let connection_properties = ConnectionProperties::default();
    let connection = match Connection::connect(&config.uri, connection_properties).await {
        Ok(conn) => conn,
        Err(e) => {
            log::error!("Failed to connect to RabbitMQ: {}", e);
            return Err(e.to_string());
        }
    };

    log::debug!("Connected to RabbitMQ at {}", config.uri);

    // Create a channel.
    let channel = match connection.create_channel().await {
        Ok(channel) => channel,
        Err(e) => {
            log::error!("Failed to create RabbitMQ channel: {}", e);
            return Err(e.to_string());
        }
    };
    log::debug!("Created RabbitMQ channel");

    // Declare a queue for receiving commands. It needs to be exclusive and not auto-delete.
    let queue_options = lapin::options::QueueDeclareOptions {
        auto_delete: false,
        exclusive: true,
        ..Default::default()
    };
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
    log::debug!("Declared RabbitMQ queue '{}'", queue.name().as_str());

    // Declare the exchange. This needs to be auto-delete but not durable.
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
    log::debug!("Declared RabbitMQ exchange '{}'", config.exchange.as_str());

    // Bind the queue to the exchange with the routing key 'command.<actor_name>.#'.
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
    log::debug!(
        "Bound RabbitMQ queue '{}' to exchange '{}' with routing key 'command.{}.#'",
        queue.name().as_str(),
        config.exchange.as_str(),
        config.actor_name.as_str()
    );

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
    log::debug!("Started RabbitMQ consumer");

    // If monitoring TCP replies, spawn a task to handle them. This needs a separate channel
    // since it will be moved into the task. But we want to create the channel here after declaring
    // the exchange and queue. Not totally sure why this is necessary, but if one creates another
    // channel before declaring the exchange the channel closes immediately. So either one does
    // this or declares the exchange for the channel used for publishing replies.
    if config.monitor_tcp_replies {
        let channel_b = match connection.create_channel().await {
            Ok(channel) => channel,
            Err(e) => {
                log::error!("Failed to create RabbitMQ channel: {}", e);
                return Err(e.to_string());
            }
        };

        // Clone the config for the task.
        let config_clone = config.clone();

        // Clone the command ID pool mutex for the task.
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
                // Non-fatal error, just log it, but maybe we should abort the whole service?
                log::error!("Failed to handle RabbitMQ replies: {}", e);
            }
        });
    }

    // Process incoming commands.
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

/// Processes a command received from RabbitMQ and sends it to the actor.
///
/// # Arguments
/// * `tcp_sender` - The sender channel to send TCP commands.
/// * `delivery` - The RabbitMQ delivery containing the command.
/// * `command_id_pool` - A mutex-protected CommandID pool for tracking command UUIDs.
///
pub async fn process_command(
    tcp_sender: &Sender<BytesMut>,
    delivery: &Delivery,
    command_id_pool: &Arc<Mutex<CommandID>>,
) {
    // Extract command_id and commander_id from headers.
    let command_id = get_header_value(delivery, "command_id");
    let command_id = match command_id {
        Some(value) => match value {
            AMQPValue::LongString(id) => id.to_string(),
            _ => {
                log::warn!("Command ID header is not a LongString");
                return;
            }
        },
        None => {
            log::warn!("Command ID not found in message headers");
            return;
        }
    };

    let commander_id = get_header_value(delivery, "commander_id");
    let commander_id = match commander_id {
        Some(value) => match value {
            AMQPValue::LongString(id) => id.to_string(),
            _ => {
                log::warn!("Commander ID header is not a LongString");
                return;
            }
        },
        None => {
            log::warn!("Commander ID not found in message headers");
            return;
        }
    };

    // Deserialize the command payload.
    let command_string: CommandPayload = serde_json::from_slice(&delivery.data).unwrap();

    log::debug!(
        "Processing command from commander {} with UUID '{}' and command string '{}'",
        commander_id,
        command_id,
        command_string.command_string
    );

    // Get a free TCP command ID from the pool.
    let mut command_id_pool = command_id_pool.lock().await;
    let tcp_command_id = command_id_pool.get_command_id();
    log::debug!(
        "Mapped UUID '{}' to TCP command ID {}",
        command_id,
        tcp_command_id
    );

    // Prepare the TCP command string: "<tcp_command_id> <command_string>" and send it to the TCP queue.
    let tcp_command_string = format!("{} {}", tcp_command_id, command_string.command_string);

    log::debug!("Queuing TCP command: '{}'", tcp_command_string);

    tcp_sender
        .send(BytesMut::from(tcp_command_string.as_bytes()))
        .await
        .unwrap();

    // Register the mapping of TCP command ID to UUID and commander ID.
    command_id_pool.register_command(&command_id, &commander_id, tcp_command_id);
}

/// Represents the payload of a command message.
#[derive(serde::Deserialize, Debug)]
struct CommandPayload {
    /// The actual command string to be executed by the actor.
    command_string: String,
}
