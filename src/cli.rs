/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: cli.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

pub use clap::Parser;
use clap::{Subcommand, ValueEnum};
pub use clap_cargo::style::CLAP_STYLING;

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
    #[arg(
        short = 'p',
        long,
        global = true,
        default_value_t = 8080,
        env = "TCP_PORT"
    )]
    pub(crate) port: u16,

    /// Try to reconnect if the service is disconnected.
    #[arg(short = 'r', long, global = true, default_value_t = false)]
    pub(crate) reconnect: bool,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// Starts the TCP and RabbitMQ communication streams.
    Start {},

    /// Listens for incoming TCP or RabbitMQ messages without processing them.
    Listen {
        /// Service to listen to.
        #[arg(value_enum)]
        service: Service,
    },
}

#[derive(ValueEnum, Copy, Clone, Debug)]
#[value(rename_all = "lowercase")]
pub(crate) enum Service {
    RabbitMQ,
    TCP,
}
