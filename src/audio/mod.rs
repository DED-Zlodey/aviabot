pub mod decoder;
pub mod effects;
pub mod encoder;
pub mod mixer;

pub const SAMPLE_RATE: usize = 48000;
pub const FRAME_MS: usize = 20;
pub const FRAME_SAMPLES: usize = SAMPLE_RATE * FRAME_MS / 1000; // 960
