use std::collections::HashSet;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam::channel::{bounded, Receiver, Sender};
use futures::stream::{StreamExt, TryStreamExt};
use tracing::{debug, error, info, trace, warn};
use tsclientlib::{Connection, DisconnectOptions, Identity, InMessage, StreamItem};

use tsproto_packets::packets::{AudioData, CodecType, OutAudio};

use crate::audio::mixer::{Mixer, MixerInput, MixerOutput, RoutingSnapshot};
use crate::config::{AudioConfig, RelayConfig, Ts3Config};
use crate::position::PlayerPositionService;
use crate::test_load;
use crate::ts3_client_list::Ts3ClientList;

pub struct Ts3Client {
    config: Ts3Config,
    relay: RelayConfig,
    audio: AudioConfig,
    position_service: Arc<PlayerPositionService>,
}

impl Ts3Client {
    pub fn new(config: Ts3Config, relay: RelayConfig, audio: AudioConfig, position_service: Arc<PlayerPositionService>) -> Self {
        Self { config, relay, audio, position_service }
    }

    pub async fn run(&self, client_list: Arc<Ts3ClientList>) -> Result<()> {
        // Channels between TS3 client and audio mixer
        let (mixer_input_tx, mixer_input_rx): (Sender<MixerInput>, Receiver<MixerInput>) =
            bounded(4096);
        let (mixer_output_tx, mixer_output_rx): (Sender<MixerOutput>, Receiver<MixerOutput>) =
            bounded(64);
        let (routing_tx, routing_rx): (Sender<RoutingSnapshot>, Receiver<RoutingSnapshot>) =
            bounded(2);

        let mut mixer = Mixer::start(mixer_input_rx, mixer_output_tx, routing_rx)
            .context("Failed to start audio mixer")?;

        // Start synthetic load test if configured
        if self.audio.synthetic_speakers > 0 {
            test_load::start_synthetic_load(
                self.audio.synthetic_speakers,
                1000, // base client id for synthetic speakers
                mixer_input_tx.clone(),
            );
        }

        let con = self.connect().await.context("Failed to connect to TS3")?;
        info!("Connected to TS3 server");
        let con = Arc::new(tokio::sync::Mutex::new(con));

        // Spawn task to read TS3 events. We acquire the lock only for one event at a time
        // so that other tasks (routing, send_audio) can access the Connection in between.
        let con_clone = con.clone();
        let mixer_input_tx_clone = mixer_input_tx.clone();
        let mut event_task = tokio::spawn(async move {
            loop {
                let next_item = {
                    let mut guard = con_clone.lock().await;
                    let mut events = guard.events();
                    let item = events.next().await;
                    drop(events);
                    item
                };
                match next_item {
                    Some(Ok(StreamItem::Audio(packet))) => {
                        let data = packet.data().data();
                        match data {
                            AudioData::S2CWhisper { from, data, .. } if !data.is_empty() => {
                                trace!("Incoming whisper from {} ({} bytes)", from, data.len());
                                if mixer_input_tx_clone.try_send((*from, data.to_vec())).is_err() {
                                    trace!("Mixer input full, dropping packet from {}", from);
                                }
                            }
                            AudioData::S2C { from, data, .. } if !data.is_empty() => {
                                trace!("Incoming voice from {} ({} bytes)", from, data.len());
                                if mixer_input_tx_clone.try_send((*from, data.to_vec())).is_err() {
                                    trace!("Mixer input full, dropping packet from {}", from);
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(StreamItem::BookEvents(book_events))) => {
                        for event in book_events {
                            debug!("Book event: {:?}", event);
                        }
                    }
                    Some(Ok(StreamItem::MessageEvent(InMessage::ClientIds(msg)))) => {
                        for part in msg.iter() {
                            debug!("ClientIds response: uid={:?} client_id={}", part.client_uid, part.client_id.0);
                        }
                    }
                    Some(Ok(StreamItem::MessageEvent(msg))) => {
                        debug!("TS3 message event: {:?}", msg);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        error!("TS3 event error: {:?}", e);
                        break;
                    }
                    None => {
                        warn!("TS3 event stream ended");
                        break;
                    }
                }
            }
        });

        // Spawn periodic routing snapshot updater
        let position_service = self.position_service.clone();
        let relay = self.relay.clone();
        let audio = self.audio.clone();
        let client_list_clone2 = client_list.clone();
        let mut routing_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));

            loop {
                interval.tick().await;

                let positions = position_service.snapshot();
                let required_uids: HashSet<String> = positions.to_uid_map().into_keys().collect();

                // Build uid -> client_id map solely from RabbitMQ consumer data.
                let mut uid_to_client_id = client_list_clone2.uid_to_client_id();
                uid_to_client_id.retain(|uid, _| required_uids.contains(uid));

                debug!("Routing snapshot built from {} TS3 client(s) with known UID", uid_to_client_id.len());

                let snapshot = RoutingSnapshot {
                    uid_to_client_id,
                    positions,
                    max_distance: relay.max_distance,
                    coalition_check: relay.coalition_check,
                    radio_effects_enabled: relay.radio_effects_enabled,
                    output_gain: audio.output_gain,
                    force_whisper_client_id: relay.force_whisper_client_id,
                };
                if routing_tx.try_send(snapshot).is_err() {
                    trace!("Routing channel full, dropping snapshot");
                }
            }
        });

        // Bridge mixer output back to async TS3 sender
        let (async_tx, mut async_rx) = tokio::sync::mpsc::channel::<MixerOutput>(64);
        let _bridge_handle = std::thread::spawn(move || {
            while let Ok(item) = mixer_output_rx.recv() {
                if async_tx.blocking_send(item).is_err() {
                    break;
                }
            }
        });

        let whisper_id = AtomicU16::new(0);

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = &mut event_task => {
                    warn!("TS3 event task ended");
                    break;
                }
                _ = &mut routing_task => {
                    warn!("Routing task ended");
                    break;
                }
                Some((opus_data, recipients)) = async_rx.recv() => {
                    if !recipients.is_empty() {
                        let id = whisper_id.fetch_add(1, Ordering::Relaxed);
                        let packet = OutAudio::new(&AudioData::C2SWhisper {
                            id,
                            codec: CodecType::OpusVoice,
                            channels: vec![],
                            clients: recipients,
                            data: &opus_data,
                        });
                        let mut con_guard = con.lock().await;
                        if let Err(e) = con_guard.send_audio(packet) {
                            trace!("Failed to send audio: {:?}", e);
                        } else {
                            trace!("Sent whisper ({} bytes)", opus_data.len());
                        }
                    }
                }
            }
        }

        // Drop mixer input to stop mixer thread
        drop(mixer_input_tx);
        mixer.stop();

        let con = Arc::try_unwrap(con).unwrap_or_else(|_| panic!("Connection still has references"));
        let mut con = con.into_inner();
        con.disconnect(DisconnectOptions::new())?;
        info!("Disconnected cleanly");
        Ok(())
    }

    async fn connect(&self) -> Result<Connection> {
        let identity = if let Some(ref key) = self.config.identity_key {
            Identity::new_from_str(key).context("Invalid identity key")?
        } else {
            Identity::create()
        };

        let mut builder = Connection::build(self.config.address.clone())
            .identity(identity)
            .name(self.config.name.clone())
            .channel(self.config.channel.clone());

        if let Some(ref pw) = self.config.channel_password {
            builder = builder.channel_password(pw.clone());
        }
        if let Some(ref pw) = self.config.server_password {
            builder = builder.password(pw.clone());
        }

        let mut con = builder.connect()?;

        // Wait for first book events (channel list, client list)
        let _ = con
            .events()
            .try_filter(|e| futures::future::ready(matches!(e, StreamItem::BookEvents(_))))
            .next()
            .await
            .context("No book events received")?;

        Ok(con)
    }
}
