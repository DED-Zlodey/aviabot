use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use serde::Deserialize;
use tracing::{error, info, trace, warn};

use crate::config::RabbitMqConfig;
use crate::position::{PlayerPositionService, PlayerSession};
use crate::seq_logger::{SeqEvent, SeqLogger};
use crate::ts3_client_list::{Ts3ClientInfo, Ts3ClientList};

// ---------------------------------------------------------------------------
// TS3 event payloads (published by the separate query bot).
// Envelope format:
//   { "eventType": "userConnected", "timestamp": "...", "payload": { ... } }
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct Ts3EventEnvelope {
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[allow(dead_code)]
    pub timestamp: String,
    pub payload: Ts3UserPayload,
}

#[derive(Debug, Deserialize)]
pub struct Ts3UserPayload {
    #[serde(rename = "clientId")]
    pub client_id: u16,
    #[serde(rename = "uniqueId")]
    pub unique_id: String,
    #[allow(dead_code)]
    #[serde(rename = "nickname")]
    pub nickname: String,
    #[serde(rename = "channelId")]
    pub channel_id: Option<u64>,
    #[allow(dead_code)]
    #[serde(rename = "countryCode")]
    pub country_code: Option<String>,
    #[serde(rename = "isInputMuted")]
    pub is_input_muted: Option<bool>,
    #[serde(rename = "isOutputMuted")]
    pub is_output_muted: Option<bool>,
}

// ---------------------------------------------------------------------------
// Consumer for IL-2 player sessions (legacy queue).
// ---------------------------------------------------------------------------

pub struct RabbitMqConsumer {
    config: RabbitMqConfig,
    service: Arc<PlayerPositionService>,
    seq_logger: Option<Arc<SeqLogger>>,
}

impl RabbitMqConsumer {
    pub fn new(
        config: RabbitMqConfig,
        service: Arc<PlayerPositionService>,
        seq_logger: Option<Arc<SeqLogger>>,
    ) -> Self {
        Self {
            config,
            service,
            seq_logger,
        }
    }

    pub async fn run(&self) -> Result<()> {
        if !self.config.enabled {
            info!("RabbitMQ IL-2 consumer is disabled");
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
                "aviabot_il2_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("RabbitMQ IL-2 consumer started on queue '{}'", self.config.queue);

        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    let payload = String::from_utf8_lossy(&delivery.data);
                    match serde_json::from_str::<PlayerSession>(&payload) {
                        Ok(session) => {
                            if let Some(ref seq) = self.seq_logger {
                                let event = SeqEvent::new(
                                    "RabbitMQ {event}: {gamer_name}",
                                    "Debug",
                                )
                                .with_property("event", session.event.clone())
                                .with_property("gamer_name", session.gamer_name.clone())
                                .with_property("country", session.country)
                                .with_property("uid", session.team_speak_id.clone().unwrap_or_default())
                                .with_property("x", session.x.unwrap_or(0.0))
                                .with_property("y", session.y.unwrap_or(0.0))
                                .with_property("z", session.z.unwrap_or(0.0))
                                .with_property("type", session.aircraft_type.clone().unwrap_or_default())
                                .with_property("name", session.name.clone().unwrap_or_default())
                                .with_property("payload", payload.to_string());
                                seq.log(event);
                            }
                            self.service.handle_session(session);
                        }
                        Err(e) => {
                            warn!("Failed to parse PlayerSession: {} (payload: {})", e, payload);
                            if let Some(ref seq) = self.seq_logger {
                                let event = SeqEvent::new(
                                    "Failed to parse PlayerSession: {error}",
                                    "Warning",
                                )
                                .with_property("error", e.to_string())
                                .with_property("payload", payload.to_string());
                                seq.log(event);
                            }
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

        warn!("RabbitMQ IL-2 consumer stream ended");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Consumer for TS3 query bot events (join/leave/moved).
// ---------------------------------------------------------------------------

pub struct RabbitMqTs3Consumer {
    config: RabbitMqConfig,
    ts3_client_list: Arc<Ts3ClientList>,
}

impl RabbitMqTs3Consumer {
    pub fn new(config: RabbitMqConfig, ts3_client_list: Arc<Ts3ClientList>) -> Self {
        Self {
            config,
            ts3_client_list,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let queue = match self.config.ts3_events_queue.as_deref() {
            Some(q) if !q.is_empty() => q,
            _ => {
                info!("RabbitMQ TS3 consumer is disabled (no ts3_events_queue set)");
                return Ok(());
            }
        };

        if !self.config.enabled {
            info!("RabbitMQ TS3 consumer is disabled (rabbitmq.enabled = false)");
            return Ok(());
        }

        let addr = format!(
            "amqp://{}:{}@{}:{}",
            self.config.username, self.config.password, self.config.hostname, self.config.port
        );

        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .context("Failed to connect to RabbitMQ (TS3)")?;
        info!("Connected to RabbitMQ (TS3) at {}", self.config.hostname);

        let channel = conn.create_channel().await?;

        // Declare the queue (create if missing) and bind it to the TS3 events exchange.
        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        if let Some(ref exchange) = self.config.ts3_events_exchange {
            channel
                .queue_bind(
                    queue,
                    exchange.as_str(),
                    "", // routing key – fanout or default binding
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
            info!(
                "Bound queue '{}' to exchange '{}'",
                queue, exchange
            );
        }

        let mut consumer = channel
            .basic_consume(
                queue,
                "aviabot_ts3_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("RabbitMQ TS3 consumer started on queue '{}'", queue);

        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    let payload = String::from_utf8_lossy(&delivery.data);
                    self.handle_message(&payload).await;
                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                        error!("Failed to ack TS3 RabbitMQ message: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("RabbitMQ TS3 delivery error: {:?}", e);
                }
            }
        }

        warn!("RabbitMQ TS3 consumer stream ended");
        Ok(())
    }

    async fn handle_message(&self, payload: &str) {
        let envelope = match serde_json::from_str::<Ts3EventEnvelope>(payload) {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to parse TS3 event envelope: {} (payload: {})", e, payload);
                return;
            }
        };

        let ev = &envelope.event_type;
        let p = &envelope.payload;

        match ev.as_str() {
            "userConnected" => {
                info!(
                    "RabbitMQ TS3 connect: {} -> client_id {} (channel {:?})",
                    p.unique_id, p.client_id, p.channel_id
                );
                self.ts3_client_list.insert_or_update(Ts3ClientInfo {
                    client_id: p.client_id,
                    uid: Some(p.unique_id.clone()),
                });
            }
            "userDisconnected" => {
                info!(
                    "RabbitMQ TS3 disconnect: {} -> client_id {}",
                    p.unique_id, p.client_id
                );
                self.ts3_client_list.remove(p.client_id);
            }
            "userMoved" => {
                info!(
                    "RabbitMQ TS3 move: {} -> client_id {} (channel {:?})",
                    p.unique_id, p.client_id, p.channel_id
                );
                self.ts3_client_list.insert_or_update(Ts3ClientInfo {
                    client_id: p.client_id,
                    uid: Some(p.unique_id.clone()),
                });
            }
            "userAudioStateChanged" => {
                // Audio mute state does not affect uid -> client_id mapping.
                trace!(
                    "RabbitMQ TS3 audio state: {} input_muted={:?} output_muted={:?}",
                    p.unique_id, p.is_input_muted, p.is_output_muted
                );
            }
            other => {
                warn!(
                    "Unknown TS3 event type '{}' in TS3 queue (payload: {})",
                    other, payload
                );
            }
        }
    }
}
