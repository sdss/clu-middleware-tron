/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: main.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

mod cli;
mod parser;
mod rabbitmq;
mod tcp;
mod tools;

use clap::Parser;
use cli::{Cli, Commands};
use colored::Colorize;

use crate::tcp::TCPClientConfig;

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default()
        .default_filter_or("info")
        .default_write_style_or("auto");
    env_logger::init_from_env(env);

    // Parse command line arguments
    let cli = Cli::parse();

    // Selected subcommand
    let command = cli.command;

    if let Commands::Start {} = command {
        log::info!("Starting TCP and RabbitMQ communication streams ...");
        // Here you would add the logic to start the communication streams.
    } else if let Commands::Listen { service } = command {
        match service {
            cli::Service::RabbitMQ => {
                log::info!("Listening for RabbitMQ messages ...");
                let url = "amqp://localhost:5672";
                if let Err(e) = rabbitmq::start_rabbitmq_service(url).await {
                    log::error!("Failed to start RabbitMQ service: {}", e);
                }
            }

            cli::Service::TCP => {
                let port = cli.port;
                log::info!(
                    "Listening for TCP messages on port {} ...",
                    port.to_string().cyan()
                );

                // Launch TCP client.
                let host = "127.0.0.1";
                let mut config = TCPClientConfig::default();
                config.log_messages = true;
                config.reconnect = cli.reconnect;

                if let Err(e) = tcp::start_tcp_client(host, port, config).await {
                    log::error!("Failed to start TCP server: {}", e);
                }
            }
        }
    }
}
