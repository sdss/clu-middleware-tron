/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: main.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

mod cli;

use std::{env, process::ExitCode};

use async_channel::unbounded;
use clap::Parser;

use cli::{Cli, Commands};
use clu_middleware_tron::{parser, rabbitmq, tcp};

#[tokio::main]
async fn main() -> ExitCode {
    // Initialize logger. Set the default level to trace if RUST_LOG is not set.
    // We will adjust the level later based on verbosity.
    let env = env_logger::Env::default()
        .default_filter_or("trace")
        .default_write_style_or("auto");
    env_logger::init_from_env(env);

    // Parse command line arguments
    let cli = Cli::parse();

    // TCP arguments
    let host = cli.host.as_str();
    let port = cli.port;

    // RabbitMQ arguments
    let rabbitmq_uri = cli.rabbitmq_uri.as_str();
    let rabbitmq_exchange = cli.rabbitmq_exchange.as_str();

    // Handle verbosity. If the RUST_LOG env variable is not set, adjust the log level
    // according to the verbosity flag.
    if env::var(env_logger::DEFAULT_FILTER_ENV).is_err() {
        match cli.verbose {
            0 => log::set_max_level(log::LevelFilter::Warn),
            1 => log::set_max_level(log::LevelFilter::Info),
            2 => log::set_max_level(log::LevelFilter::Debug),
            _ => log::set_max_level(log::LevelFilter::Trace),
        }
    }

    // Selected subcommand
    let command = cli.command;

    // Create queues for TCP and RabbitMQ
    let (tcp_sender, tcp_receiver) = unbounded::<bytes::BytesMut>();
    let (rabbitmq_sender, rabbitmq_receiver) = unbounded::<parser::Reply>();

    if let Commands::Start { actor_name } = command {
        // Start both TCP and RabbitMQ communication streams.

        log::info!("Starting TCP and RabbitMQ communication streams ...");

        // RabbitMQ configuration
        let mut rabbitmq_config = rabbitmq::RabbitMQConfig::default(actor_name);
        rabbitmq_config.uri = rabbitmq_uri.to_string();
        rabbitmq_config.exchange = rabbitmq_exchange.to_string();
        rabbitmq_config.monitor_tcp_replies = true;

        // TCP configuration
        let tcp_config = tcp::TCPClientConfig {
            host: host.to_string(),
            port,
            reconnect: cli.reconnect,
            propagate_to_rabbitmq: true,
            ..tcp::TCPClientConfig::default()
        };

        // Launch both services
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
                // Listen for RabbitMQ messages without processing them.

                log::info!("Listening for RabbitMQ messages ...");

                // Launch RabbitMQ service.
                let mut rabbitmq_config = rabbitmq::RabbitMQConfig::default(actor_name);
                rabbitmq_config.uri = rabbitmq_uri.to_string();
                rabbitmq_config.exchange = rabbitmq_exchange.to_string();
                rabbitmq_config.monitor_tcp_replies = false;

                if let Err(e) =
                    rabbitmq::start_rabbitmq_service(rabbitmq_config, tcp_sender, rabbitmq_receiver)
                        .await
                {
                    log::error!("Failed to start RabbitMQ service: {}", e);
                    return ExitCode::FAILURE;
                }
            }

            cli::Service::Tcp => {
                // Listen for TCP messages without processing them.

                log::info!("Listening for TCP messages on port {} ...", port);

                // Launch TCP client.
                let tcp_config = tcp::TCPClientConfig {
                    host: host.to_string(),
                    port,
                    reconnect: cli.reconnect,
                    propagate_to_rabbitmq: false,
                    ..tcp::TCPClientConfig::default()
                };

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
