/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: tcp.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use std::io;
use std::time::Duration;

use tokio::net::TcpStream;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::sleep;

use crate::parser::parse_reply;

/// Configuration options for the TCP client.
pub(crate) struct TCPClientConfig {
    /// Whether the client should attempt to reconnect on disconnection.
    pub reconnect: bool,
    /// Delay in seconds before attempting to reconnect.
    pub reconnect_delay: f32,
    /// Whether to log received messages.
    pub log_messages: bool,
    /// Log level for message logging.
    pub log_level: log::Level,
    /// Propagate parsed message to the RabbitMQ exchange.
    pub propagate_to_rabbitmq: bool,
}

/// Default configuration for the TCP client.
impl Default for TCPClientConfig {
    fn default() -> Self {
        Self {
            reconnect: false,
            reconnect_delay: 5.0,
            log_messages: false,
            log_level: log::Level::Info,
            propagate_to_rabbitmq: false,
        }
    }
}

/// Starts a TCP client that connects to the specified host and port,
/// reads incoming messages, and processes them using the [parse_reply] function.
///
/// # Arguments
///
/// * `host` - The hostname or IP address of the TCP server.
/// * `port` - The port number of the TCP server.
/// * `config` - Configuration options for the TCP client.
///
pub(crate) async fn start_tcp_client(
    host: &str,
    port: u16,
    config: TCPClientConfig,
) -> Result<(), io::Error> {
    let mut socket: TcpStream;

    loop {
        match TcpStream::connect((host, port)).await {
            Ok(s) => {
                log::debug!("Connected to TCP server at {}:{}", host, port);
                socket = s;
            }

            Err(e) => {
                log::error!(
                    "Failed to connect to TCP server at {}:{}: {}",
                    host,
                    port,
                    e
                );

                if config.reconnect {
                    log::warn!("Reconnecting in {} seconds...", config.reconnect_delay);
                    sleep(Duration::from_secs_f32(config.reconnect_delay)).await;
                    continue;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "Failed to connect to TCP server",
                    ));
                }
            }
        }

        let mut reader = BufReader::new(socket);

        loop {
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
                    // Strip newline characters
                    while let Some(&last) = line.last() {
                        if last == b'\n' || last == b'\r' {
                            line.pop();
                        } else {
                            break;
                        }
                    }

                    if config.log_messages && log::log_enabled!(config.log_level) {
                        log::log!(
                            config.log_level,
                            "Received from {}:{}: {:?}",
                            host,
                            port,
                            bytes::Bytes::from(line.clone())
                        );
                    }

                    if let Some(reply) = parse_reply(&line) {
                        if config.log_messages && log::log_enabled!(config.log_level) {
                            log::info!(
                                "Parsed reply: commander={}, command_id={}, code={}, keywords={}",
                                reply.commander,
                                reply.command_id,
                                reply.code,
                                serde_json::to_string(&reply.keywords).unwrap()
                            )
                        }
                    } else {
                        log::warn!(
                            "Failed to parse reply from {}:{}: {:?}",
                            host,
                            port,
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
                        return Err(e);
                    }
                }
            }
        }
    }
}
