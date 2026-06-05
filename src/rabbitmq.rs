use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use tracing::{error, info, warn};

use crate::config::RabbitMqConfig;
use crate::position::{PlayerPositionService, PlayerSession};

pub struct RabbitMqConsumer {
    config: RabbitMqConfig,
    service: Arc<PlayerPositionService>,
}

impl RabbitMqConsumer {
    pub fn new(config: RabbitMqConfig, service: Arc<PlayerPositionService>) -> Self {
        Self { config, service }
    }

    pub async fn run(&self) -> Result<()> {
        if !self.config.enabled {
            info!("RabbitMQ consumer is disabled");
            return Ok(());
        }

        let addr = format!(
            "amqp://{}:{}@{}:{}",
            self.config.username, self.config.password, self.config.hostname, self.config.port
        );

        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .context("Failed to connect to RabbitMQ")?;
        info!("Connected to RabbitMQ at {}", self.config.hostname);

        let channel = conn.create_channel().await?;

        // Check queue exists (passive declare, same as C# QueueDeclarePassive)
        let _ = channel
            .queue_declare(
                &self.config.queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let mut consumer = channel
            .basic_consume(
                &self.config.queue,
                "aviabot_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("RabbitMQ consumer started on queue '{}'", self.config.queue);

        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    let payload = String::from_utf8_lossy(&delivery.data);
                    match serde_json::from_str::<PlayerSession>(&payload) {
                        Ok(session) => {
                            self.service.handle_session(session);
                        }
                        Err(e) => {
                            warn!("Failed to parse PlayerSession: {} (payload: {})", e, payload);
                        }
                    }
                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                        error!("Failed to ack RabbitMQ message: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("RabbitMQ delivery error: {:?}", e);
                }
            }
        }

        warn!("RabbitMQ consumer stream ended");
        Ok(())
    }
}
