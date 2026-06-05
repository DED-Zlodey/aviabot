use opus::Encoder;
use tracing::error;

use super::{FRAME_SAMPLES, SAMPLE_RATE};

pub struct OpusEncoder {
    encoder: Encoder,
    scratch: Vec<u8>,
}

impl OpusEncoder {
    pub fn new() -> anyhow::Result<Self> {
        let encoder = Encoder::new(
            SAMPLE_RATE as u32,
            opus::Channels::Mono,
            opus::Application::Voip,
        )?;
        Ok(Self {
            encoder,
            scratch: vec![0u8; 1275], // Max opus packet size
        })
    }

    /// Encode mono i16 PCM into Opus.
    /// Returns encoded slice on success.
    pub fn encode(&mut self, pcm: &[i16]) -> Option<&[u8]> {
        assert_eq!(pcm.len(), FRAME_SAMPLES);
        match self.encoder.encode(pcm, &mut self.scratch) {
            Ok(len) => Some(&self.scratch[..len]),
            Err(e) => {
                error!("Opus encode error: {:?}", e);
                None
            }
        }
    }
}
