use opus::Decoder;
use tracing::debug;

use super::{FRAME_SAMPLES, SAMPLE_RATE};

pub struct OpusDecoder {
    decoder: Decoder,
    scratch: Vec<i16>,
}

impl OpusDecoder {
    pub fn new() -> anyhow::Result<Self> {
        let decoder = Decoder::new(SAMPLE_RATE as u32, opus::Channels::Mono)?;
        Ok(Self {
            decoder,
            scratch: vec![0i16; FRAME_SAMPLES],
        })
    }

    /// Decode Opus packet into internal scratch buffer.
    /// Returns slice of decoded i16 PCM samples (length = FRAME_SAMPLES on success).
    pub fn decode(&mut self, packet: &[u8]) -> Option<&[i16]> {
        if packet.is_empty() {
            // Empty packet = DTX or end-of-stream marker in TS3, ignore
            return None;
        }
        match self.decoder.decode(packet, &mut self.scratch, false) {
            Ok(samples) if samples == FRAME_SAMPLES => Some(&self.scratch[..samples]),
            Ok(samples) => {
                // Frame size mismatch (e.g. DTX/PLC), zero-pad or ignore
                if samples > 0 {
                    for i in samples..FRAME_SAMPLES {
                        self.scratch[i] = 0;
                    }
                    Some(&self.scratch[..FRAME_SAMPLES])
                } else {
                    None
                }
            }
            Err(e) => {
                debug!("Opus decode error: {:?} (packet len={})", e, packet.len());
                None
            }
        }
    }
}
