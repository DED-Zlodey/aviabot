mod audio;
mod config;
mod db;
mod position;
mod rabbitmq;
mod seq_logger;
mod test_load;
mod ts3_client;
mod ts3_client_list;

use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info, warn};
use crate::ts3_client_list::{Ts3ClientInfo, Ts3ClientList};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(e) = run().await {
        error!("Fatal error: {:#}", e);
        std::process::exit(1);
    }
}


async fn run() -> Result<()> {
    let cfg = config::Config::load("config.toml")?;
    info!("Configuration loaded");

    let position_service = Arc::new(position::PlayerPositionService::new());

    // Optional Seq logger for structured remote logging.
    let seq_logger = if cfg.seq.enabled {
        info!(
            "Seq logging enabled at {} (app={})",
            cfg.seq.url, cfg.seq.application_name
        );
        Some(seq_logger::SeqLogger::new(
            cfg.seq.url.clone(),
            cfg.seq.api_key.clone(),
            cfg.seq.application_name.clone(),
        ))
    } else {
        info!("Seq logging disabled");
        None
    };

    // Shared TS3 client list cache (populated from DB + runtime events).
    let client_list = Arc::new(Ts3ClientList::new());

    // Seed client list from PostgreSQL if enabled.
    if cfg.db.enabled {
        match db::connect(&cfg.db.url).await {
            Ok(pool) => {
                match db::fetch_online_users(&pool).await {
                    Ok(users) => {
                        for (uid, client_id) in users {
                            client_list.insert_or_update(Ts3ClientInfo {
                                client_id,
                                uid: Some(uid),
                            });
                        }
                    }
                    Err(e) => {
                        warn!("Failed to fetch online TS3 users from DB: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to connect to PostgreSQL: {}", e);
            }
        }
    }

    // Start RabbitMQ IL-2 consumer (player positions / sessions).
    let rabbit = rabbitmq::RabbitMqConsumer::new(
        cfg.rabbitmq.clone(),
        position_service.clone(),
        seq_logger.clone(),
    );
    let rabbit_task = tokio::spawn(async move {
        if let Err(e) = rabbit.run().await {
            error!("RabbitMQ IL-2 consumer error: {:?}", e);
        }
    });

    // Start RabbitMQ TS3 consumer (query bot events: join/leave/moved).
    let client_list_for_ts3_rabbit = client_list.clone();
    let rabbit_ts3 = rabbitmq::RabbitMqTs3Consumer::new(
        cfg.rabbitmq.clone(),
        client_list_for_ts3_rabbit,
    );
    let rabbit_ts3_task = tokio::spawn(async move {
        if let Err(e) = rabbit_ts3.run().await {
            error!("RabbitMQ TS3 consumer error: {:?}", e);
        }
    });

    // Start TS3 client (blocks until shutdown)
    let ts3 = ts3_client::Ts3Client::new(cfg.ts3, cfg.relay, cfg.audio, position_service);
    let ts3_result = ts3.run(client_list).await;

    // Shutdown
    rabbit_task.abort();
    rabbit_ts3_task.abort();
    ts3_result?;
    Ok(())
}
