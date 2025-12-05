/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: main.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

mod cli;

use std::process::ExitCode;

use async_channel::unbounded;
use clap::Parser;
use colored::Colorize;

use cli::{Cli, Commands};
use clu_middleware_tron::{parser, rabbitmq, tcp};

#[tokio::main]
async fn main() -> ExitCode {
    let env = env_logger::Env::default()
        .default_filter_or("info")
        .default_write_style_or("auto");
    env_logger::init_from_env(env);

    // Parse command line arguments
    let cli = Cli::parse();

    // Global arguments
    let host = cli.host.as_str();
    let port = cli.port;

    // Selected subcommand
    let command = cli.command;

    // Create queues for TCP and RabbitMQ
    let (tcp_sender, tcp_receiver) = unbounded::<bytes::BytesMut>();
    let (rabbitmq_sender, rabbitmq_receiver) = unbounded::<parser::Reply>();

    if let Commands::Start { actor_name } = command {
        log::info!("Starting TCP and RabbitMQ communication streams ...");

        let mut rabbitmq_config = rabbitmq::RabbitMQConfig::default(actor_name);
        rabbitmq_config.monitor_tcp_replies = true;

        let mut tcp_config = tcp::TCPClientConfig::default();
        tcp_config.host = host.to_string();
        tcp_config.port = port;
        tcp_config.reconnect = cli.reconnect;
        tcp_config.propagate_to_rabbitmq = true;
        tcp_config.log_messages = true;
        tcp_config.log_level = log::Level::Debug;

        let mut tasks = tokio::task::JoinSet::new();

        tasks.spawn(async move {
            rabbitmq::start_rabbitmq_service(rabbitmq_config, tcp_sender, rabbitmq_receiver).await
        });

        tasks.spawn(async move {
            tcp::start_tcp_client(tcp_config, tcp_receiver, rabbitmq_sender).await
        });

        // Await tasks
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(_) => (),
                Err(e) => {
                    log::error!("One of the communication tasks has failed: {}", e);
                    return ExitCode::FAILURE;
                }
            }
        }
    } else if let Commands::Listen {
        actor_name,
        service,
    } = command
    {
        match service {
            cli::Service::RabbitMQ => {
                log::info!("Listening for RabbitMQ messages ...");

                let mut rabbitmq_config = rabbitmq::RabbitMQConfig::default(actor_name);
                rabbitmq_config.monitor_tcp_replies = false;

                if let Err(e) =
                    rabbitmq::start_rabbitmq_service(rabbitmq_config, tcp_sender, rabbitmq_receiver)
                        .await
                {
                    log::error!("Failed to start RabbitMQ service: {}", e);
                    return ExitCode::FAILURE;
                }
            }

            cli::Service::TCP => {
                log::info!(
                    "Listening for TCP messages on port {} ...",
                    port.to_string().cyan()
                );

                // Launch TCP client.
                let mut tcp_config = tcp::TCPClientConfig::default();
                tcp_config.host = host.to_string();
                tcp_config.port = port;
                tcp_config.log_messages = true;
                tcp_config.reconnect = cli.reconnect;

                if let Err(e) =
                    tcp::start_tcp_client(tcp_config, tcp_receiver, rabbitmq_sender).await
                {
                    log::error!("Failed to start TCP server: {}", e);
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    ExitCode::SUCCESS
}
