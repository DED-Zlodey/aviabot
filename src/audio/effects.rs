use std::f32::consts::PI;

use super::FRAME_SAMPLES;

// ============================================================================
// Biquad IIR Filter (Direct Form 1, transposed for stability)
// ============================================================================

pub struct Biquad {
    b0: f32, b1: f32, b2: f32,
    a1: f32, a2: f32,
    z1: f32, z2: f32,
}

impl Biquad {
    pub fn new_lowpass(sample_rate: f32, cutoff: f32) -> Self {
        let w0 = 2.0 * PI * cutoff / sample_rate;
        let cosw0 = w0.cos();
        let sinw0 = w0.sin();
        let alpha = sinw0 / std::f32::consts::SQRT_2; // Q = 1/sqrt(2)

        let mut b0 = (1.0 - cosw0) / 2.0;
        let mut b1 = 1.0 - cosw0;
        let mut b2 = (1.0 - cosw0) / 2.0;
        let a0 = 1.0 + alpha;
        let mut a1 = -2.0 * cosw0;
        let mut a2 = 1.0 - alpha;

        b0 /= a0; b1 /= a0; b2 /= a0;
        a1 /= a0; a2 /= a0;

        Self { b0, b1, b2, a1, a2, z1: 0.0, z2: 0.0 }
    }

    pub fn new_highpass(sample_rate: f32, cutoff: f32) -> Self {
        let w0 = 2.0 * PI * cutoff / sample_rate;
        let cosw0 = w0.cos();
        let sinw0 = w0.sin();
        let alpha = sinw0 / std::f32::consts::SQRT_2;

        let mut b0 = (1.0 + cosw0) / 2.0;
        let mut b1 = -(1.0 + cosw0);
        let mut b2 = (1.0 + cosw0) / 2.0;
        let a0 = 1.0 + alpha;
        let mut a1 = -2.0 * cosw0;
        let mut a2 = 1.0 - alpha;

        b0 /= a0; b1 /= a0; b2 /= a0;
        a1 /= a0; a2 /= a0;

        Self { b0, b1, b2, a1, a2, z1: 0.0, z2: 0.0 }
    }

    /// Process a single sample, returns filtered sample.
    #[inline(always)]
    pub fn process_sample(&mut self, input: f32) -> f32 {
        let out = input * self.b0 + self.z1;
        self.z1 = input * self.b1 + self.z2 - self.a1 * out;
        self.z2 = input * self.b2 - self.a2 * out;
        out
    }

    pub fn process_frame(&mut self, buf: &mut [f32]) {
        for s in buf.iter_mut() {
            *s = self.process_sample(*s);
        }
    }
}

// ============================================================================
// Automatic Gain Control (per-speaker, i16 domain)
// ============================================================================

pub struct Agc {
    target_rms: f32,
    current_gain: f32,
    noise_gate: f32,
    attack: f32,
    release: f32,
    min_gain: f32,
    max_gain: f32,
}

impl Default for Agc {
    fn default() -> Self {
        Self {
            target_rms: 4000.0,
            current_gain: 1.0,
            noise_gate: 400.0,
            attack: 0.03,
            release: 0.08,
            min_gain: 0.3,
            max_gain: 6.0,
        }
    }
}

impl Agc {
    pub fn process(&mut self, frame: &mut [i16]) {
        let rms = compute_rms_i16(frame);
        if rms < self.noise_gate {
            // Noise gate: slowly return gain to 1.0 or hold? C# doesn't change gain on silence
            return;
        }
        let target_gain = (self.target_rms / rms).clamp(self.min_gain, self.max_gain);
        let coeff = if target_gain > self.current_gain {
            self.attack
        } else {
            self.release
        };
        self.current_gain += (target_gain - self.current_gain) * coeff;

        for s in frame.iter_mut() {
            let amplified = *s as f32 * self.current_gain;
            *s = amplified.clamp(-32768.0, 32767.0) as i16;
        }
    }
}

#[inline(always)]
fn compute_rms_i16(samples: &[i16]) -> f32 {
    let sum_sq: f32 = samples.iter().map(|s| {
        let v = *s as f32;
        v * v
    }).sum();
    (sum_sq / samples.len() as f32).sqrt()
}

// ============================================================================
// Soft Clip (radio overdrive, normalized f32 domain [-1,1])
// ============================================================================

pub struct SoftClip {
    drive: f32,
    drive_target: f32,
    interpolation: f32,
    rng: fast_rng::Lcg,
    burst_remaining: usize,
}

impl Default for SoftClip {
    fn default() -> Self {
        Self {
            drive: 1.0,
            drive_target: 1.0,
            interpolation: 0.4,
            rng: fast_rng::Lcg::new(12345),
            burst_remaining: 0,
        }
    }
}

impl SoftClip {
    pub fn process(&mut self, buf: &mut [f32]) {
        // Random bursts every ~1-3 seconds (50-150 ticks)
        if self.burst_remaining > 0 {
            self.burst_remaining -= 1;
        } else if self.rng.next_f32() < 0.02 { // 2% chance per tick (~once per second)
            self.drive_target = 1.3 + self.rng.next_f32() * 2.7; // 1.3..4.0
            self.burst_remaining = 2 + (self.rng.next_f32() * 6.0) as usize; // 40-120ms -> 2-6 ticks
        } else {
            self.drive_target = 1.0;
        }

        self.drive += (self.drive_target - self.drive) * self.interpolation;

        if self.drive > 1.05 {
            for s in buf.iter_mut() {
                *s = (*s * self.drive).tanh();
            }
        }
    }
}

// ============================================================================
// Radio Noise (white noise + crackle + squelch tail, normalized f32 domain)
// ============================================================================

pub struct RadioNoise {
    noise_hp: Biquad,
    noise_lp: Biquad,
    lcg: fast_rng::Lcg,
    crackle: f32,
    squelch_tail: f32,
    active: bool,
}

impl RadioNoise {
    pub fn new(sample_rate: f32) -> Self {
        Self {
            noise_hp: Biquad::new_highpass(sample_rate, 800.0),
            noise_lp: Biquad::new_lowpass(sample_rate, 4000.0),
            lcg: fast_rng::Lcg::new(54321),
            crackle: 0.0,
            squelch_tail: 0.0,
            active: false,
        }
    }

    pub fn is_active(&self) -> bool {
        self.active || self.squelch_tail > 0.0
    }

    /// Force a squelch tail (used for sidetone when the local speaker stops talking).
    pub fn force_tail(&mut self) {
        self.active = false;
        self.squelch_tail = 0.03 + self.lcg.next_f32() * 0.04;
    }

    /// Call with `had_speakers` = true if there were active speakers this tick.
    pub fn process(&mut self, buf: &mut [f32], had_speakers: bool) {
        if had_speakers {
            self.active = true;
            self.squelch_tail = 0.0;
        } else if self.active {
            // Speakers just dropped — trigger squelch tail once
            self.squelch_tail = 0.03 + self.lcg.next_f32() * 0.04; // ~1000..2200 in i16 scale
            self.active = false;
        }

        if !had_speakers && self.squelch_tail <= 0.0 {
            return;
        }

        for s in buf.iter_mut() {
            // White noise (normalized to ~200/32768)
            let noise = self.lcg.next_f32() * 2.0 - 1.0;
            let mut noise_sample = noise * 0.0061;

            // Crackle (normalized to ~800/32768)
            if self.lcg.next_f32() < 0.005 {
                self.crackle = (self.lcg.next_f32() * 2.0 - 1.0) * 0.0244;
            }
            self.crackle *= 0.97;
            noise_sample += self.crackle;

            // Squelch tail
            if self.squelch_tail > 0.0 {
                noise_sample += self.squelch_tail;
                self.squelch_tail *= 0.35;
                if self.squelch_tail < 0.00003 {
                    self.squelch_tail = 0.0;
                }
            }

            // Band-pass noise
            noise_sample = self.noise_hp.process_sample(noise_sample);
            noise_sample = self.noise_lp.process_sample(noise_sample);

            *s += noise_sample;
        }
    }
}

// ============================================================================
// Sidetone Generator (PTT click + squelch tail for the speaking client)
// ============================================================================

pub struct SidetoneGenerator {
    click_lcg: fast_rng::Lcg,
    click_env: f32,
    tail_noise: RadioNoise,
}

impl SidetoneGenerator {
    pub fn new(sample_rate: f32) -> Self {
        Self {
            click_lcg: fast_rng::Lcg::new(99911),
            click_env: 0.0,
            tail_noise: RadioNoise::new(sample_rate),
        }
    }

    pub fn trigger_click(&mut self) {
        self.click_env = 1.0;
    }

    pub fn trigger_tail(&mut self) {
        self.tail_noise = RadioNoise::new(48000.0);
        self.tail_noise.force_tail();
    }

    /// Fill `out` with the next click frame. Returns true while click continues.
    pub fn next_click_frame(&mut self, out: &mut [i16]) -> bool {
        if self.click_env < 0.001 {
            return false;
        }
        for s in out.iter_mut() {
            let noise = self.click_lcg.next_f32() * 2.0 - 1.0;
            let sample = noise * self.click_env * 0.20; // ~6500/32768 peak
            *s = (sample.clamp(-1.0, 1.0) * 32767.0) as i16;
            self.click_env *= 0.88;
            if self.click_env < 0.001 {
                self.click_env = 0.0;
                break;
            }
        }
        true
    }

    /// Fill `out` with the next tail frame. Returns true while tail continues.
    pub fn next_tail_frame(&mut self, out: &mut [i16]) -> bool {
        if !self.tail_noise.is_active() {
            return false;
        }
        let mut f32_buf = [0.0f32; FRAME_SAMPLES];
        self.tail_noise.process(&mut f32_buf, false);
        // Boost tail a bit so it's audible as sidetone
        for (i, s) in f32_buf.iter_mut().enumerate() {
            *s = (*s * 2.5).clamp(-1.0, 1.0);
            out[i] = (*s * 32767.0) as i16;
        }
        true
    }
}

// ============================================================================
// Fast RNG (LCG) — zero-cost, no allocations
// ============================================================================

mod fast_rng {
    pub struct Lcg {
        state: u32,
    }

    impl Lcg {
        pub fn new(seed: u32) -> Self {
            Self { state: seed }
        }

        #[inline(always)]
        pub fn next_u32(&mut self) -> u32 {
            self.state = self.state.wrapping_mul(1664525).wrapping_add(1013904223);
            self.state
        }

        #[inline(always)]
        pub fn next_f32(&mut self) -> f32 {
            self.next_u32() as f32 / u32::MAX as f32
        }
    }
}
