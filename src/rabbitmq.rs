/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-22
 *  @Filename: rabbitmq.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use futures_lite::StreamExt;
use lapin::{Connection, ConnectionProperties, Result, options::*, types::FieldTable};

pub(crate) async fn start_rabbitmq_service(url: &str) -> Result<()> {
    let connection = Connection::connect(url, ConnectionProperties::default()).await?;

    log::debug!("Connected to RabbitMQ at {}", url);

    let channel = connection.create_channel().await?;
    let mut queue_options = QueueDeclareOptions::default();
    queue_options.auto_delete = true;
    let queue = channel
        .queue_declare("test_queue", queue_options, FieldTable::default())
        .await?;

    channel
        .queue_bind(
            queue.name().as_str(),
            "sdss_exchange",
            "command.test",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            queue.name().as_str(),
            "consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    loop {
        if let Some(delivery) = consumer.next().await {
            let delivery = delivery?;
            log::info!(
                "Received message: {}",
                String::from_utf8_lossy(&delivery.data)
            );

            delivery.ack(BasicAckOptions::default()).await?;
        } else {
            // No message available, sleep for a while
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    Ok(())
}
