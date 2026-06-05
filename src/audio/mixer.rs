use std::collections::HashSet;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender};
use crossbeam::queue::ArrayQueue;
use rustc_hash::FxHashMap;
use thread_priority::ThreadPriority;
use tracing::{debug, error, info, trace, warn};

use crate::position::{PlayerPosition, PositionSnapshot};

use super::{
    decoder::OpusDecoder,
    effects::{Agc, Biquad, RadioNoise, SidetoneGenerator, SoftClip},
    encoder::OpusEncoder,
    FRAME_MS, FRAME_SAMPLES, SAMPLE_RATE,
};

const MAX_SPEAKER_QUEUE: usize = 8; // 160ms buffer per speaker
const CLEANUP_INTERVAL_TICKS: usize = 250; // ~5 seconds
const INACTIVE_TIMEOUT: Duration = Duration::from_secs(10);

pub type MixerInput = (u16, Vec<u8>); // (client_id, opus_packet)
pub type MixerOutput = (Vec<u8>, Vec<u16>); // (opus_packet, recipient_client_ids)

/// Snapshot of routing data sent periodically from TS3 client thread.
pub struct RoutingSnapshot {
    pub uid_to_client_id: FxHashMap<String, u16>,
    pub positions: PositionSnapshot,
    pub max_distance: f64,
    pub coalition_check: bool,
    pub radio_effects_enabled: bool,
    pub output_gain: f32,
    /// If set, bypass all routing logic and whisper the mix to this client id.
    pub force_whisper_client_id: Option<u16>,
}

struct Speaker {
    decoder: OpusDecoder,
    queue: ArrayQueue<[i16; FRAME_SAMPLES]>,
    last_active: Instant,
    agc: Agc,
    scratch: [i16; FRAME_SAMPLES],
    /// TS3 UID of this speaker, resolved from the TS3 client book.
    uid: Option<String>,
}

impl Speaker {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            decoder: OpusDecoder::new()?,
            queue: ArrayQueue::new(MAX_SPEAKER_QUEUE),
            last_active: Instant::now(),
            agc: Agc::default(),
            scratch: [0i16; FRAME_SAMPLES],
            uid: None,
        })
    }
}

pub struct Mixer {
    handle: Option<JoinHandle<()>>,
}

impl Mixer {
    pub fn start(
        input_rx: Receiver<MixerInput>,
        output_tx: Sender<MixerOutput>,
        routing_rx: Receiver<RoutingSnapshot>,
    ) -> anyhow::Result<Self> {
        let handle = thread::Builder::new()
            .name("audio-mixer".into())
            .spawn(move || {
                if let Err(e) = mixer_loop(input_rx, output_tx, routing_rx) {
                    error!("Mixer loop crashed: {:?}", e);
                }
            })?;

        Ok(Self {
            handle: Some(handle),
        })
    }

    pub fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn mixer_loop(
    input_rx: Receiver<MixerInput>,
    output_tx: Sender<MixerOutput>,
    routing_rx: Receiver<RoutingSnapshot>,
) -> anyhow::Result<()> {
    // Set realtime priority on Windows
    #[cfg(target_os = "windows")]
    {
        let _ = thread_priority::set_current_thread_priority(
            ThreadPriority::Crossplatform(
                thread_priority::ThreadPriorityValue::try_from(99u8).unwrap()
            )
        );
    }

    let tick_duration = Duration::from_millis(FRAME_MS as u64);
    let mut speakers: FxHashMap<u16, Speaker> = FxHashMap::default();
    let mut encoder = OpusEncoder::new()?;
    let mut mix_buf = [0i16; FRAME_SAMPLES];
    let mut fx_buf = [0.0f32; FRAME_SAMPLES];
    let mut enc_buf = [0i16; FRAME_SAMPLES];
    let mut tick_count = 0usize;

    // Voice band-pass: 300 Hz high-pass, 3400 Hz low-pass
    let mut voice_hp = Biquad::new_highpass(SAMPLE_RATE as f32, 300.0);
    let mut voice_lp = Biquad::new_lowpass(SAMPLE_RATE as f32, 3400.0);

    let mut soft_clip = SoftClip::default();
    let mut radio_noise = RadioNoise::new(SAMPLE_RATE as f32);

    // Current routing state
    let mut routing: Option<RoutingSnapshot> = None;

    // Reverse cache: TS3 client id -> UID, refreshed from routing snapshot.
    let mut client_id_to_uid: FxHashMap<u16, String> = FxHashMap::default();

    // Track speakers for sidetone generation (PTT click + tail)
    let mut prev_active_set: HashSet<u16> = HashSet::new();
    let mut sidetone_tracks: FxHashMap<u16, SidetoneGenerator> = FxHashMap::default();

    info!("Audio mixer thread started");

    loop {
        let tick_start = Instant::now();

        // 0. Update routing snapshot if available
        if let Ok(new_routing) = routing_rx.try_recv() {
            // Refresh client_id -> UID reverse cache from uid_to_client_id.
            client_id_to_uid = new_routing
                .uid_to_client_id
                .iter()
                .map(|(uid, &cid)| (cid, uid.clone()))
                .collect();
            routing = Some(new_routing);
        }

        // 1. Drain incoming packets
        loop {
            match input_rx.try_recv() {
                Ok((client_id, packet)) => {
                    // Drop packets from speakers whose UID is not yet resolved.
                    // This matches the C# behavior: unknown speakers are not routed.
                    let speaker_uid = match client_id_to_uid.get(&client_id) {
                        Some(uid) => uid.clone(),
                        None => {
                            trace!("Dropping packet from unresolved client {}", client_id);
                            continue;
                        }
                    };

                    let speaker = match speakers.get_mut(&client_id) {
                        Some(s) => s,
                        None => {
                            match Speaker::new() {
                                Ok(mut s) => {
                                    s.uid = Some(speaker_uid.clone());
                                    speakers.insert(client_id, s);
                                    speakers.get_mut(&client_id).unwrap()
                                }
                                Err(e) => {
                                    error!("Failed to create decoder for client {}: {:?}", client_id, e);
                                    continue;
                                }
                            }
                        }
                    };

                    // Update UID in case it changed.
                    speaker.uid = Some(speaker_uid);

                    if let Some(pcm) = speaker.decoder.decode(&packet) {
                        speaker.scratch.copy_from_slice(pcm);
                        speaker.agc.process(&mut speaker.scratch);

                        let mut frame = [0i16; FRAME_SAMPLES];
                        frame.copy_from_slice(&speaker.scratch);

                        if speaker.queue.push(frame).is_err() {
                            let _ = speaker.queue.pop();
                            let _ = speaker.queue.push(frame);
                        }
                        speaker.last_active = Instant::now();
                    }
                }
                Err(crossbeam::channel::TryRecvError::Empty) => break,
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    info!("Mixer input channel closed, shutting down");
                    return Ok(());
                }
            }
        }

        // 2. Mix
        mix_buf.fill(0);
        let mut active_speakers = 0usize;
        let mut active_set: HashSet<u16> = HashSet::new();
        let mut active_speaker_uids: Vec<(u16, String)> = Vec::new();

        for (client_id, speaker) in speakers.iter_mut() {
            if let Some(pcm) = speaker.queue.pop() {
                for i in 0..FRAME_SAMPLES {
                    mix_buf[i] = mix_buf[i].saturating_add(pcm[i]);
                }
                active_set.insert(*client_id);
                if let Some(uid) = speaker.uid.as_ref() {
                    active_speaker_uids.push((*client_id, uid.clone()));
                }
                active_speakers += 1;
                trace!("Mixed speaker {} (uid={:?})", client_id, speaker.uid);
            }
        }

        // 3. Effects on mix
        let radio_active = active_speakers > 0 || radio_noise.is_active();
        if radio_active {
            let output_gain = routing.as_ref().map(|r| r.output_gain).unwrap_or(1.0);
            let effects_enabled = routing.as_ref().map(|r| r.radio_effects_enabled).unwrap_or(false);

            // Convert mix to normalized f32 for effects
            for i in 0..FRAME_SAMPLES {
                fx_buf[i] = mix_buf[i] as f32 / 32768.0;
            }

            // Band-pass filter (300-3400 Hz) — always applied
            voice_hp.process_frame(&mut fx_buf);
            voice_lp.process_frame(&mut fx_buf);

            if effects_enabled {
                // Soft clip
                soft_clip.process(&mut fx_buf);
                // Radio noise + squelch tail
                radio_noise.process(&mut fx_buf, active_speakers > 0);
            }

            // Apply output gain and clamp to [-1, 1]
            for s in &mut fx_buf {
                *s = (*s * output_gain).clamp(-1.0, 1.0);
            }

            // Convert back to i16 for opus encoder
            for i in 0..FRAME_SAMPLES {
                enc_buf[i] = (fx_buf[i] * 32767.0) as i16;
            }

            // 4. Determine recipients (only non-speaking listeners receive the mix)
            let recipients = if let Some(ref routing) = routing {
                compute_recipients(&active_speaker_uids, &active_set, routing)
            } else {
                // No routing data yet — don't send to anyone to avoid self-echo
                Vec::new()
            };

            if !recipients.is_empty() {
                trace!("Sending mix to {:?} (active speakers: {})", recipients, active_speakers);
                if let Some(encoded) = encoder.encode(&enc_buf) {
                    if output_tx.try_send((encoded.to_vec(), recipients)).is_err() {
                        trace!("Mixer output channel full, dropping frame");
                    }
                }
            }
        }

        // 4. Sidetone: PTT click/tail for the speaking client so they hear radio feedback,
        // but never their own voice.
        let started: Vec<u16> = active_set.difference(&prev_active_set).copied().collect();
        let stopped: Vec<u16> = prev_active_set.difference(&active_set).copied().collect();

        for cid in started {
            let gen = sidetone_tracks
                .entry(cid)
                .or_insert_with(|| SidetoneGenerator::new(SAMPLE_RATE as f32));
            gen.trigger_click();
            debug!("Sidetone click triggered for client {}", cid);
        }
        for cid in stopped {
            let gen = sidetone_tracks
                .entry(cid)
                .or_insert_with(|| SidetoneGenerator::new(SAMPLE_RATE as f32));
            gen.trigger_tail();
            debug!("Sidetone tail triggered for client {}", cid);
        }
        prev_active_set = active_set;

        let mut sidetone_done = Vec::new();
        for (&cid, gen) in sidetone_tracks.iter_mut() {
            let mut frame = [0i16; FRAME_SAMPLES];
            let produced = if gen.next_click_frame(&mut frame) {
                true
            } else {
                gen.next_tail_frame(&mut frame)
            };

            if produced {
                if let Some(encoded) = encoder.encode(&frame) {
                    if output_tx.try_send((encoded.to_vec(), vec![cid])).is_err() {
                        trace!("Sidetone output channel full for {}", cid);
                    }
                }
            } else {
                sidetone_done.push(cid);
            }
        }
        for cid in sidetone_done {
            sidetone_tracks.remove(&cid);
        }

        // 5. Periodic cleanup
        tick_count += 1;
        if tick_count >= CLEANUP_INTERVAL_TICKS {
            tick_count = 0;
            let before = speakers.len();
            speakers.retain(|_, s| s.last_active.elapsed() < INACTIVE_TIMEOUT);
            let after = speakers.len();
            if before != after {
                debug!("Cleaned up {} inactive speakers", before - after);
            }
        }

        // 6. Timing & benchmark
        let work_time = tick_start.elapsed();
        if tick_count % 250 == 0 {
            debug!(
                "Mixer benchmark: {} active speakers, work_time={:?} (budget={:?})",
                active_speakers, work_time, tick_duration
            );
        }
        if work_time > Duration::from_millis(15) {
            warn!(
                "Mixer overrun: work_time={:?} exceeds 15ms budget with {} speakers",
                work_time, active_speakers
            );
        }

        if work_time < tick_duration {
            let remaining = tick_duration - work_time;
            if remaining > Duration::from_millis(2) {
                thread::sleep(remaining - Duration::from_millis(1));
            }
            while Instant::now().duration_since(tick_start) < tick_duration {
                std::hint::spin_loop();
            }
        }
    }
}

/// Compute whisper recipients based on positions and coalition.
/// `active_speaker_uids` contains pairs of (client_id, uid) for speakers who have audio in the current tick.
/// `active_speaker_ids` is used to exclude active speakers from receiving the mixed stream (sidetone only).
fn compute_recipients(
    active_speaker_uids: &[(u16, String)],
    active_speaker_ids: &HashSet<u16>,
    routing: &RoutingSnapshot,
) -> Vec<u16> {
    // Test mode: force whisper to a specific client id regardless of positions.
    if let Some(cid) = routing.force_whisper_client_id {
        debug!("Force whisper mode: routing mix to target client {}", cid);
        return vec![cid];
    }

    let mut recipient_set = HashSet::new();

    trace!(
        "compute_recipients: active_speakers={:?}, uid_to_client_id_len={}",
        active_speaker_uids,
        routing.uid_to_client_id.len()
    );

    // For each active speaker, find who should hear them.
    // Lobby/spectator speakers broadcast to all lobby members of the same coalition.
    // Active speakers whisper to active players within sphere of the same coalition.
    for (_speaker_client_id, speaker_uid) in active_speaker_uids {
        let speaker_pos = match routing.positions.get_by_uid(speaker_uid) {
            Some(pos) => pos,
            None => continue,
        };

        // Validate coalition
        if speaker_pos.country != 101 && speaker_pos.country != 201 {
            continue;
        }

        let candidates: Box<dyn Iterator<Item = &PlayerPosition>> = if speaker_pos.is_lobby_routing() {
            Box::new(routing.positions.lobby_recipients(speaker_pos.country))
        } else {
            Box::new(routing.positions.in_sphere(
                speaker_pos.country,
                speaker_pos.x,
                speaker_pos.y,
                speaker_pos.z,
                routing.max_distance,
            ))
        };

        for candidate in candidates {
            let candidate_uid = match candidate.team_speak_id.as_deref() {
                Some(uid) => uid,
                None => continue,
            };

            // Don't send to the speaker themselves
            if candidate_uid == speaker_uid.as_str() {
                continue;
            }

            // Coalition check
            if routing.coalition_check && candidate.country != speaker_pos.country {
                continue;
            }

            let candidate_cid = match routing.uid_to_client_id.get(candidate_uid) {
                Some(&cid) => cid,
                None => continue,
            };

            // Active speakers get sidetone click/tail instead of the mixed stream
            if active_speaker_ids.contains(&candidate_cid) {
                continue;
            }

            recipient_set.insert(candidate_cid);
        }
    }

    let recipients: Vec<u16> = recipient_set.into_iter().collect();
    trace!("compute_recipients result: {:?}", recipients);
    recipients
}
