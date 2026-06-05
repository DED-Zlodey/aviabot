use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub ts3: Ts3Config,
    pub rabbitmq: RabbitMqConfig,
    pub relay: RelayConfig,
    pub audio: AudioConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ts3Config {
    pub address: String,
    pub name: String,
    pub channel: String,
    pub channel_password: Option<String>,
    pub server_password: Option<String>,
    pub identity_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RabbitMqConfig {
    pub enabled: bool,
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub queue: String,
    pub exchange: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub max_distance: f64,
    pub coalition_check: bool,
    pub radio_effects_enabled: bool,
    /// If set, bypass position/coalition routing and whisper the mix to this TS3 client id.
    /// Useful for testing that the audio pipeline works end-to-end.
    #[serde(default)]
    pub force_whisper_client_id: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AudioConfig {
    #[serde(default = "default_output_gain")]
    pub output_gain: f32,
    #[serde(default = "default_frame_ms")]
    pub frame_ms: u32,
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default)]
    pub synthetic_speakers: usize,
}

fn default_output_gain() -> f32 { 5.0 }
fn default_frame_ms() -> u32 { 20 }
fn default_sample_rate() -> u32 { 48000 }

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
