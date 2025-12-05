/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: cli.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use clap::Parser;
use clap::{Subcommand, ValueEnum};
use clap_cargo::style::CLAP_STYLING;

/// clu-middleware-tron command line interface.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(styles = CLAP_STYLING)]
pub(crate) struct Cli {
    /// TCP host to use for communication.
    #[arg(
        short = 'H',
        long,
        global = true,
        default_value_t = String::from("127.0.0.1"),
        env = "TCP_HOST"
    )]
    pub(crate) host: String,

    /// TCP port to use for communication.
    #[arg(short = 'p', long, default_value_t = 8080, env = "TCP_PORT")]
    pub(crate) port: u16,

    /// RabbitMQ URI.
    #[arg(
        short = 'u',
        long,
        global = true,
        default_value_t = String::from("amqp://guest:guest@127.0.0.1:5672/%2f"),
        env = "RABBITMQ_URI"
    )]
    pub(crate) rabbitmq_uri: String,

    /// RabbitMQ exchange.
    #[arg(
        short = 'e',
        long,
        global = true,
        default_value_t = String::from("sdss_exchange"),
        env = "RABBITMQ_EXCHANGE"
    )]
    pub(crate) rabbitmq_exchange: String,

    /// Try to reconnect if the service is disconnected.
    #[arg(short = 'r', long, global = true, default_value_t = false)]
    pub(crate) reconnect: bool,

    /// Verbosity level. Can be used multiple times to increase verbosity. Ignored
    /// if RUST_LOG environment variable is set.
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    pub(crate) verbose: u8,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// Starts the TCP and RabbitMQ communication streams.
    Start {
        /// Actor name
        #[arg()]
        actor_name: String,
    },

    /// Listens for incoming TCP or RabbitMQ messages without processing them.
    Listen {
        /// Service to listen to.
        #[arg(value_enum)]
        service: Service,

        /// Actor name
        #[arg()]
        actor_name: String,
    },
}

#[derive(ValueEnum, Copy, Clone, Debug)]
#[value(rename_all = "lowercase")]
pub(crate) enum Service {
    RabbitMQ,
    TCP,
}
