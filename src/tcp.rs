/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: tcp.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use std::time::Duration;

use async_channel::{Receiver, Sender};
use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::sleep;

use crate::parser::{Reply, parse_reply};

/// Configuration options for the TCP client.
pub struct TCPClientConfig {
    /// Hostname or IP address of the TCP server.
    pub host: String,
    /// Port number of the TCP server.
    pub port: u16,
    /// Whether the client should attempt to reconnect on disconnection.
    pub reconnect: bool,
    /// Delay in seconds before attempting to reconnect.
    pub reconnect_delay: f32,
    /// Propagate parsed message to the RabbitMQ exchange.
    pub propagate_to_rabbitmq: bool,
}

impl Default for TCPClientConfig {
    /// Creates a default TCP client configuration.
    fn default() -> Self {
        Self {
            host: String::from("127.0.0.1"),
            port: 8080,
            reconnect: false,
            reconnect_delay: 5.0,
            propagate_to_rabbitmq: false,
        }
    }
}

/// Starts a TCP client that connects to the specified host and port,
/// reads incoming messages, and processes them using the [parse_reply] function.
///
/// # Arguments
///
/// * `config` - Configuration options for the TCP client.
/// * `tcp_receiver` - An async channel receiver for sending messages to the TCP server.
/// * `rabbitmq_sender` - An async channel sender for propagating parsed messages to RabbitMQ.
///
pub async fn start_tcp_client(
    config: TCPClientConfig,
    tcp_receiver: Receiver<BytesMut>,
    rabbitmq_sender: Sender<Reply>,
) -> Result<(), String> {
    // Main connection loop. Reconnects if the connection is lost and reconnect is enabled.
    loop {
        // Attempt to connect to the TCP server.
        let stream = match TcpStream::connect((config.host.as_str(), config.port)).await {
            Ok(s) => {
                log::debug!("Connected to TCP server at {}:{}", config.host, config.port);
                s
            }
            Err(e) => {
                log::error!(
                    "Failed to connect to TCP server at {}:{}: {}",
                    config.host,
                    config.port,
                    e
                );

                if config.reconnect {
                    log::warn!("Reconnecting in {} seconds...", config.reconnect_delay);
                    sleep(Duration::from_secs_f32(config.reconnect_delay)).await;
                    continue;
                } else {
                    return Err("Failed to connect to TCP server".to_string());
                }
            }
        };

        // Split the stream into reader and writer and create buffered versions.
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Clone the receiver for the sending task. This is necessary because we are in a loop
        // and if we reconnect the original receiver would be moved.
        let tcp_receiver_clone = tcp_receiver.clone();

        // Monitor the TCP queue for commands to send to the actor.
        tokio::spawn(async move {
            while let Ok(message) = tcp_receiver_clone.recv().await {
                log::debug!("Processing command to send to actor: {:?}", message);
                let message_lf = [message.as_ref(), b"\n"].concat();
                if let Err(e) = writer.write_all(&message_lf).await {
                    log::error!("Failed to send command to actor: {}", e);
                    continue;
                }
                if let Err(e) = writer.flush().await {
                    log::error!("Failed to flush command to actor: {}", e);
                    continue;
                }
                log::debug!("Command sent to actor: {:?}", message);
            }
        });

        // Read from the actor TCP stream. Parse replies from the actor and
        // propagate them to RabbitMQ if configured.
        loop {
            // Read a line from the TCP stream. Handle EOF and errors.
            let mut line: Vec<u8> = Vec::new();
            match reader.read_until(b'\n', &mut line).await {
                Ok(0) => {
                    log::debug!("Connection closed by client");
                    if config.reconnect {
                        log::warn!("Reconnecting in {} seconds...", config.reconnect_delay);
                        sleep(Duration::from_secs_f32(config.reconnect_delay)).await;
                        break; // Break to outer loop to reconnect
                    }
                    return Ok(()); // connection closed
                }

                Ok(_) => {
                    // Successfully read a line. Parse and process it.

                    // Strip newline characters
                    while let Some(&last) = line.last() {
                        if last == b'\n' || last == b'\r' {
                            line.pop();
                        } else {
                            break;
                        }
                    }

                    log::debug!(
                        "Received reply from {}:{}: {:?}",
                        config.host,
                        config.port,
                        bytes::Bytes::from(line.clone())
                    );

                    if let Some(reply) = parse_reply(&line) {
                        log::info!(
                            "Parsed reply: user_id={}, command_id={}, code={}, keywords={}",
                            reply.user_id,
                            reply.command_id,
                            reply.code,
                            serde_json::to_string(&reply.keywords).unwrap()
                        );

                        if config.propagate_to_rabbitmq {
                            log::debug!("Sending reply to RabbitMQ service for processing.");
                            log::debug!("Reply: {:?}", reply);
                            if let Err(e) = rabbitmq_sender.send(reply).await {
                                log::error!("Failed to send reply to RabbitMQ queue: {}", e);
                            } else {
                                log::debug!("Reply sent to RabbitMQ service.");
                            }
                        }
                    } else {
                        log::warn!(
                            "Failed to parse reply from {}:{}: {:?}",
                            config.host,
                            config.port,
                            bytes::Bytes::from(line.clone())
                        );
                        continue;
                    }
                }

                Err(e) => {
                    log::error!("Failed to read from stream: {}", e);
                    if config.reconnect {
                        break;
                    } else {
                        return Err(e.to_string());
                    }
                }
            }
        }
    }
}
