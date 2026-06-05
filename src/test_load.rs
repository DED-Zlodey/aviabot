use std::time::Duration;

use crossbeam::channel::Sender;
use tracing::{info, trace};

use crate::audio::mixer::MixerInput;

/// Generate synthetic voice load by encoding silence into Opus packets.
/// Runs in a dedicated thread with precise 20ms timing.
pub fn start_synthetic_load(
    speaker_count: usize,
    base_client_id: u16,
    mixer_input_tx: Sender<MixerInput>,
) {
    std::thread::Builder::new()
        .name("synthetic-load".into())
        .spawn(move || {
            let mut encoder = match opus::Encoder::new(
                48000,
                opus::Channels::Mono,
                opus::Application::Voip,
            ) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!("Failed to create synthetic encoder: {:?}", e);
                    return;
                }
            };

            let mut opus_buf = vec![0u8; 1275];
            let mut lcg_state: u32 = 123456789;
            let client_ids: Vec<u16> = (0..speaker_count as u16)
                .map(|i| base_client_id + i)
                .collect();

            let tick = Duration::from_millis(20);
            let mut next_tick = std::time::Instant::now() + tick;

            info!(
                "Synthetic load started: {} speakers (client ids {}..{})",
                speaker_count,
                base_client_id,
                base_client_id + speaker_count as u16 - 1
            );

            loop {
                // Generate near-silence frame (prevents Opus DTX, stays below AGC noise gate)
                let mut near_silence = [0i16; 960];
                for s in &mut near_silence {
                    lcg_state = lcg_state.wrapping_mul(1664525).wrapping_add(1013904223);
                    let noise = (lcg_state as f32 / u32::MAX as f32) * 2.0 - 1.0;
                    *s = (noise * 10.0) as i16; // ±10 at ±32768 scale = ~-70 dB, well below AGC gate of 400
                }

                for &client_id in &client_ids {
                    match encoder.encode(&near_silence, &mut opus_buf) {
                        Ok(len) => {
                            let packet = opus_buf[..len].to_vec();
                            if mixer_input_tx.try_send((client_id, packet)).is_err() {
                                trace!("Synthetic mixer input full, stopping");
                                return;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Synthetic encode error: {:?}", e);
                        }
                    }
                }

                let now = std::time::Instant::now();
                if now < next_tick {
                    std::thread::sleep(next_tick - now);
                }
                next_tick += tick;
            }
        })
        .expect("Failed to spawn synthetic load thread");
}
