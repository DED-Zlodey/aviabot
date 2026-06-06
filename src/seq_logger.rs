use std::sync::Arc;

use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Single event in CLEF format for Seq.
#[derive(Debug, Clone)]
pub struct SeqEvent {
    pub message_template: String,
    pub level: &'static str,
    pub properties: Map<String, Value>,
}

impl SeqEvent {
    pub fn new(template: impl Into<String>, level: &'static str) -> Self {
        Self {
            message_template: template.into(),
            level,
            properties: Map::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Async batch sender to a Seq server.
/// Events are queued on an unbounded channel and flushed in the background.
pub struct SeqLogger {
    tx: mpsc::UnboundedSender<SeqEvent>,
}

impl SeqLogger {
    pub fn new(seq_url: String, api_key: Option<String>, app_name: String) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<SeqEvent>();
        let client = Client::new();

        tokio::spawn(async move {
            let mut batch: Vec<SeqEvent> = Vec::with_capacity(128);
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            info!("Seq background sender started for {}", seq_url);

            loop {
                tokio::select! {
                    Some(event) = rx.recv() => {
                        trace!("Seq event queued: {}", event.message_template);
                        batch.push(event);
                        if batch.len() >= 100 {
                            Self::send_batch(&client, &seq_url, &api_key, &app_name, &batch).await;
                            batch.clear();
                        }
                    }
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            info!("Seq flushing {} events", batch.len());
                            Self::send_batch(&client, &seq_url, &api_key, &app_name, &batch).await;
                            batch.clear();
                        }
                    }
                }
            }
        });

        Arc::new(Self { tx })
    }

    /// Send an event. Never blocks the caller.
    pub fn log(&self, event: SeqEvent) {
        if let Err(e) = self.tx.send(event) {
            warn!("Seq channel closed, dropping event: {:?}", e);
        }
    }

    async fn send_batch(
        client: &Client,
        seq_url: &str,
        api_key: &Option<String>,
        app_name: &str,
        events: &[SeqEvent],
    ) {
        let mut body = String::with_capacity(events.len() * 256);
        for event in events {
            let mut obj = json!({
                "@t": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "@mt": &event.message_template,
                "@l": event.level,
                "Application": app_name,
            });
            if let Some(map) = obj.as_object_mut() {
                for (k, v) in &event.properties {
                    map.insert(k.clone(), v.clone());
                }
            }
            body.push_str(&obj.to_string());
            body.push('\n');
        }

        let url = format!("{}/api/events/raw?clef", seq_url.trim_end_matches('/'));
        let mut req = client
            .post(&url)
            .header("Content-Type", "application/vnd.serilog.clef")
            .body(body);

        if let Some(key) = api_key {
            req = req.header("X-Seq-ApiKey", key);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    info!("Seq batch sent ({} events) -> {}", events.len(), status);
                } else {
                    let body = resp.text().await.unwrap_or_default();
                    warn!("Seq rejected batch: {} — {}", status, body);
                }
            }
            Err(e) => {
                error!("Seq batch send failed: {:?}", e);
            }
        }
    }
}
