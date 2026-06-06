mod audio;
mod config;
mod position;
mod rabbitmq;
mod seq_logger;
mod test_load;
mod ts3_client;
mod ts3_client_list;

use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info};

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

    // Start RabbitMQ consumer
    let rabbit = rabbitmq::RabbitMqConsumer::new(
        cfg.rabbitmq.clone(),
        position_service.clone(),
        seq_logger.clone(),
    );
    let rabbit_task = tokio::spawn(async move {
        if let Err(e) = rabbit.run().await {
            error!("RabbitMQ consumer error: {:?}", e);
        }
    });

    // Start TS3 client (blocks until shutdown)
    let ts3 = ts3_client::Ts3Client::new(cfg.ts3, cfg.relay, cfg.audio, position_service);
    let ts3_result = ts3.run().await;

    // Shutdown
    rabbit_task.abort();
    ts3_result?;
    Ok(())
}
