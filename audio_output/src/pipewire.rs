#![cfg(all(target_os = "linux", feature = "pipewire"))]

use anyhow::{Result, anyhow};
use crossbeam::queue::ArrayQueue;
use pipewire as pw;
use rubato::{
    Resampler, SincFixedIn, SincInterpolationParameters, SincInterpolationType, WindowFunction,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::thread;
use std::time::Duration;

// FFI bindings for PipeWire thread-safe rate control and stream timing
#[link(name = "pipewire-0.3")]
unsafe extern "C" {
    fn pw_stream_set_control(
        stream: *mut std::ffi::c_void,
        id: u32,
        n_values: u32,
        values: *const f32,
        flags: u32,
    ) -> i32;

    fn pw_thread_loop_lock(loop_: *mut std::ffi::c_void);
    fn pw_thread_loop_unlock(loop_: *mut std::ffi::c_void);

    /// RT-safe.  `time` must point to a zero-initialised PwTime.
    fn pw_stream_get_time(stream: *mut std::ffi::c_void, time: *mut PwTime) -> i32;
}

/// Mirrors `struct spa_fraction` from <spa/utils/defs.h>
#[repr(C)]
struct SpaFraction {
    num: u32,
    denom: u32,
}

/// Mirrors `struct pw_time` from <pipewire/stream.h>.
/// Only the fields up to `queued` are used; later fields (size, avail, …)
/// added in 0.3.50 are covered by zero-initialisation.
#[repr(C)]
#[derive(Default)]
struct PwTime {
    now: i64,
    rate: SpaFraction,
    ticks: u64,
    /// Downstream graph latency in `rate` ticks (frames at `rate.denom` Hz).
    /// Does NOT include queued ring-buffer samples.
    delay: i64,
    queued: u64,
}

impl Default for SpaFraction {
    fn default() -> Self {
        SpaFraction { num: 0, denom: 1 }
    }
}

// SPA control IDs from spa/control/control.h
const SPA_PROP_RATE: u32 = 3;

/// Convert speaker name to PipeWire channel position name
/// PipeWire expects lowercase positions like "FL", "FR", "FC", "LFE", "RL", "RR", etc.
fn to_pipewire_position(name: &str) -> String {
    match name {
        "C" => "FC".to_string(),    // Center → Front-Center
        "BL" => "RL".to_string(),   // Back-Left → Rear-Left
        "BR" => "RR".to_string(),   // Back-Right → Rear-Right
        "BC" => "RC".to_string(),   // Back-Center → Rear-Center
        other => other.to_string(), // FL, FR, LFE, SL, SR, etc. stay as-is
    }
}

// Buffer size: 4 seconds of audio at 48kHz, 16 channels
const BUFFER_SIZE: usize = 48000 * 16 * 4;

/// Runtime configuration for PipeWire buffer sizes and quantum.
///
/// All latency values are in milliseconds and converted to frames at runtime
/// using the actual sample rate, so they work correctly with any sample rate.
///
/// `latency_ms` is both the prefill threshold (playback starts when the buffer
/// reaches this level) and the PI controller target.  Having them identical
/// means the controller starts in its linear regime from the very first frame.
///
/// `max_latency_ms` should be set to at least `2 × latency_ms` to give the
/// ring buffer enough headroom for mpv burst writes without blocking the writer.
#[derive(Debug, Clone)]
pub struct PipewireBufferConfig {
    /// Target latency: prefill threshold AND PI controller setpoint (ms). Default: 500.
    pub latency_ms: u32,
    /// Maximum buffer fill before applying back-pressure (ms). Default: latency_ms × 2.
    pub max_latency_ms: u32,
    /// PipeWire processing quantum in frames. Default: 1024 (~21ms at 48kHz).
    pub quantum_frames: u32,
}

impl Default for PipewireBufferConfig {
    fn default() -> Self {
        let latency_ms = 500;
        Self {
            latency_ms,
            max_latency_ms: latency_ms * 2,
            quantum_frames: 1024,
        }
    }
}

// Proportional gain: rate change per sample of buffer drift.
// Effective value = P_GAIN / 100.  Saturation threshold (where P alone reaches MAX_RATE_ADJUST):
//   drift_sat = MAX_RATE_ADJUST * 100 / P_GAIN
// At 48 kHz, 8ch: 1 ms = 384 samples.  We want saturation at ≈ 260 ms → 100 000 samples.
//   P_GAIN = 0.002 * 100 / 100 000 = 2e-6
const RATE_ADJUST_P_GAIN: f64 = 0.000002;

// Integral gain: corrects steady-state clock mismatch between input and output clocks.
// Keep small enough that a transient burst does not permanently wind up the integrator.
// At anti-windup limit (MAX_INTEGRAL_TERM / I_GAIN = 2000 samples), the I term adds 200 ppm.
const RATE_ADJUST_I_GAIN: f64 = 0.0000001;

// Maximum rate adjustment: ±0.2% (±2000 ppm).
// Real-world clock drift between two independent audio clocks is at most a few hundred ppm.
// Staying below ±0.5% keeps pitch deviation imperceptible (<5 cents).
const MAX_RATE_ADJUST: f64 = 0.002;
// Temporary wider limit used only for large transient errors (seek/pause burst recovery).
const MAX_RATE_ADJUST_FAST: f64 = 0.01;

// Maximum I contribution: 200 ppm.  Keeps the integral from dominating when the buffer
// sits above target for a while (e.g. during initial mpv burst fill).
const MAX_INTEGRAL_TERM: f64 = 0.0002;

// Rubato resampler constants
const RESAMPLER_CHUNK_SIZE: usize = 1024; // Input chunk size for resampler

pub struct PipewireWriter {
    sample_buffer: Arc<ArrayQueue<f32>>,
    sample_rate: u32,
    channel_count: u32,
    /// Pre-computed back-pressure threshold in samples (max_latency_ms → samples).
    max_buffer_samples: usize,
    /// PipeWire quantum latency in ms (pre-computed for use in latency_ms()).
    quantum_ms: f32,
    stream_ready: Arc<AtomicBool>,
    enable_adaptive_resampling: bool,
    /// Current rate-adjust factor applied by the PI controller (f32 bits).
    /// 1.0 = nominal; >1.0 = PipeWire consuming slightly faster; <1.0 = slower.
    /// Only meaningful when adaptive resampling is enabled.
    current_rate_adjust: Arc<AtomicU32>,
    /// Set by request_flush(); the PipeWire callback drains the ring buffer,
    /// resets its state, and clears this flag on the next callback invocation.
    flush_requested: Arc<AtomicBool>,
    /// Downstream graph latency as measured by pw_stream_get_time().delay (f32 ms bits).
    /// Updated every ~100 callbacks once the stream is stable.
    graph_latency_ms_bits: Arc<AtomicU32>,
    /// Configured ring-buffer target latency (from PipewireBufferConfig::latency_ms).
    target_latency_ms: u32,
    _pw_thread: Option<thread::JoinHandle<()>>,
}

impl PipewireWriter {
    pub fn new(
        sample_rate: u32,
        channel_count: u32,
        sink_target: Option<String>,
        enable_adaptive_resampling: bool,
        output_sample_rate: Option<u32>,
        buffer_config: PipewireBufferConfig,
    ) -> Result<Self> {
        Self::new_with_channel_names(
            sample_rate,
            channel_count,
            sink_target,
            None,
            enable_adaptive_resampling,
            output_sample_rate,
            buffer_config,
        )
    }

    pub fn new_with_channel_names(
        sample_rate: u32,
        channel_count: u32,
        sink_target: Option<String>,
        channel_names: Option<Vec<String>>,
        enable_adaptive_resampling: bool,
        output_sample_rate: Option<u32>,
        mut buffer_config: PipewireBufferConfig,
    ) -> Result<Self> {
        // Keep headroom above target, otherwise PI control saturates and the
        // buffer tends to stabilize below setpoint (target at the ceiling).
        if buffer_config.max_latency_ms <= buffer_config.latency_ms {
            let corrected = buffer_config.latency_ms.saturating_mul(2);
            log::warn!(
                "PipeWire max_latency_ms ({}) must be > latency_ms ({}). Auto-correcting to {} ms.",
                buffer_config.max_latency_ms,
                buffer_config.latency_ms,
                corrected
            );
            buffer_config.max_latency_ms = corrected;
        }

        let sample_buffer = Arc::new(ArrayQueue::new(BUFFER_SIZE));
        let buffer_clone = sample_buffer.clone();
        let stream_ready = Arc::new(AtomicBool::new(false));
        let ready_clone = stream_ready.clone();
        let current_rate_adjust = Arc::new(AtomicU32::new(1.0f32.to_bits()));
        let rate_adjust_clone = current_rate_adjust.clone();
        let flush_requested = Arc::new(AtomicBool::new(false));
        let flush_requested_clone = flush_requested.clone();
        let graph_latency_ms_bits = Arc::new(AtomicU32::new(0u32));
        let graph_latency_clone = graph_latency_ms_bits.clone();

        // Capture before moving buffer_config into the thread closure.
        let max_latency_ms = buffer_config.max_latency_ms;
        let target_latency_ms = buffer_config.latency_ms;
        let output_rate_for_quantum = output_sample_rate.unwrap_or(sample_rate);
        let quantum_ms =
            buffer_config.quantum_frames as f32 / output_rate_for_quantum as f32 * 1000.0;

        // Spawn PipeWire thread
        let pw_thread = thread::spawn(move || {
            log::debug!("PipeWire thread started");
            if let Err(e) = run_pipewire_loop(
                buffer_clone,
                sample_rate,
                channel_count,
                ready_clone,
                sink_target,
                channel_names,
                enable_adaptive_resampling,
                output_sample_rate,
                buffer_config,
                rate_adjust_clone,
                flush_requested_clone,
                graph_latency_clone,
            ) {
                log::error!("PipeWire thread error: {}", e);
            }
            log::debug!("PipeWire thread exited");
        });

        // Wait for stream to be ready (with timeout)
        let timeout = Duration::from_secs(3);
        let start = std::time::Instant::now();
        while !stream_ready.load(Ordering::Relaxed) {
            if start.elapsed() > timeout {
                log::warn!("PipeWire stream initialization timeout - continuing anyway");
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }

        log::info!(
            "PipeWire stream initialized: {} Hz, {} channels",
            sample_rate,
            channel_count
        );
        log::info!("Audio streaming to PipeWire is now active");

        if enable_adaptive_resampling {
            log::info!("PipeWire adaptive resampling enabled (PI controller for buffer stability)");
        } else {
            log::info!("PipeWire adaptive resampling disabled (fixed playback rate)");
        }

        let max_buffer_samples =
            (max_latency_ms as usize * sample_rate as usize / 1000) * channel_count as usize;

        Ok(Self {
            sample_buffer,
            sample_rate,
            channel_count,
            max_buffer_samples,
            quantum_ms,
            stream_ready,
            enable_adaptive_resampling,
            current_rate_adjust,
            flush_requested,
            graph_latency_ms_bits,
            target_latency_ms,
            _pw_thread: Some(pw_thread),
        })
    }

    pub fn write_samples(&mut self, samples: &[f32]) -> Result<()> {
        // Check if stream is ready
        if !self.stream_ready.load(Ordering::Relaxed) {
            log::trace!("Stream not ready yet, dropping {} samples", samples.len());
            return Ok(());
        }

        let max_buffer_fill = self.max_buffer_samples;

        let mut sample_idx = 0;
        let mut wait_count = 0;
        let mut last_log_time = std::time::Instant::now();

        while sample_idx < samples.len() {
            // Check buffer fill level
            let buffer_level = self.sample_buffer.len();

            // If buffer is too full, wait for it to drain
            if buffer_level >= max_buffer_fill {
                if wait_count == 0 {
                    log::trace!(
                        "Buffer nearly full ({} samples / {} max), waiting for playback to catch up...",
                        buffer_level,
                        max_buffer_fill
                    );
                }
                wait_count += 1;
                thread::sleep(Duration::from_millis(10));

                // Log periodically while waiting
                if last_log_time.elapsed().as_secs() >= 2 {
                    log::warn!(
                        "Still waiting for buffer to drain: {} samples, waited {}ms",
                        buffer_level,
                        wait_count * 10
                    );
                    last_log_time = std::time::Instant::now();
                }

                // Safety timeout to prevent infinite loop and memory accumulation
                if wait_count > 200 {
                    // 2 seconds max wait instead of 5
                    log::warn!(
                        "Buffer drain timeout after 2s - dropping {} remaining samples to prevent OOM",
                        samples.len() - sample_idx
                    );
                    break;
                }
                continue;
            }

            // Push samples one by one until buffer is full or we've pushed everything
            while sample_idx < samples.len() && self.sample_buffer.len() < max_buffer_fill {
                if self.sample_buffer.push(samples[sample_idx]).is_ok() {
                    sample_idx += 1;
                } else {
                    // Buffer full, will wait on next iteration
                    break;
                }
            }
        }

        // Only log if we had to wait (indicates potential issues)
        if wait_count > 0 {
            log::trace!(
                "Buffer drain wait: {} waits ({}ms), pushed {} samples",
                wait_count,
                wait_count * 10,
                sample_idx
            );
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut last_level = self.sample_buffer.len();
        let mut last_change = start;

        while !self.sample_buffer.is_empty() {
            if start.elapsed() > timeout {
                log::warn!(
                    "Flush timeout - {} samples remaining",
                    self.sample_buffer.len()
                );
                while self.sample_buffer.pop().is_some() {}
                break;
            }
            thread::sleep(Duration::from_millis(10));
            let current = self.sample_buffer.len();
            if current < last_level {
                last_level = current;
                last_change = std::time::Instant::now();
            } else if last_change.elapsed() > Duration::from_millis(500) {
                // Buffer stalled — likely in recovery mode (callback not consuming).
                // Drain immediately rather than spinning until timeout.
                log::debug!("Flush: buffer stalled at {} samples, draining", current);
                while self.sample_buffer.pop().is_some() {}
                break;
            }
        }

        log::debug!("PipeWire buffer flushed");
        Ok(())
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn channel_count(&self) -> u32 {
        self.channel_count
    }

    pub fn buffer_fill_level(&self) -> usize {
        self.sample_buffer.len()
    }

    /// Estimated current end-to-end audio latency in milliseconds.
    ///
    /// Composed of:
    /// - Ring buffer latency: current fill (in frames) / sample_rate
    /// - PipeWire quantum latency: quantum_frames / output_sample_rate
    pub fn latency_ms(&self) -> f32 {
        let fill_frames = self.sample_buffer.len() / self.channel_count as usize;
        let ring_ms = fill_frames as f32 / self.sample_rate as f32 * 1000.0;
        ring_ms + self.quantum_ms
    }

    /// Ring-buffer-only latency in ms (excludes PipeWire graph delay).
    fn ring_latency_ms(&self) -> f32 {
        let fill_frames = self.sample_buffer.len() / self.channel_count as usize;
        fill_frames as f32 / self.sample_rate as f32 * 1000.0
    }

    /// Current rate-adjust factor applied by the PI controller.
    /// Returns `None` when adaptive resampling is disabled.
    /// Value is near 1.0; deviation from 1.0 represents clock drift correction
    /// (e.g. 1.0015 = consuming 0.15 % faster than nominal to drain the buffer).
    pub fn rate_adjust(&self) -> Option<f32> {
        if self.enable_adaptive_resampling {
            Some(f32::from_bits(
                self.current_rate_adjust.load(Ordering::Relaxed),
            ))
        } else {
            None
        }
    }

    /// Downstream graph latency in ms as reported by pw_stream_get_time().delay.
    /// Includes PipeWire graph scheduling and the netjack2 driver quantum.
    /// Returns 0.0 until the stream has been active for ~2 seconds.
    pub fn graph_latency_ms(&self) -> f32 {
        f32::from_bits(self.graph_latency_ms_bits.load(Ordering::Relaxed))
    }

    /// Target audio delay seen by the listener:
    /// configured ring-buffer target + PipeWire graph latency.
    /// Pass the negative of this (in seconds) to mpv's `audio-delay`.
    pub fn total_audio_delay_ms(&self) -> f32 {
        self.target_latency_ms as f32 + self.graph_latency_ms()
    }

    /// Measured total audio delay seen by the listener:
    /// current ring-buffer latency + PipeWire graph latency.
    pub fn measured_audio_delay_ms(&self) -> f32 {
        self.ring_latency_ms() + self.graph_latency_ms()
    }

    /// Signal the PipeWire callback to discard buffered audio and re-enter prefill.
    ///
    /// Call this after a decoder seek/reset so that stale pre-seek audio is not
    /// played out.  The callback drains the ring buffer and resets its state on
    /// the next invocation, then waits for the buffer to refill before resuming.
    pub fn request_flush(&self) {
        self.flush_requested.store(true, Ordering::Relaxed);
        log::debug!("PipeWire flush requested (seek/decoder reset)");
    }
}

impl Drop for PipewireWriter {
    fn drop(&mut self) {
        log::debug!("Dropping PipeWire writer");
        // Discard any remaining samples — flush() was already called by finalize().
        // Calling flush() again here would block for another 500ms–5s if the callback
        // is in recovery mode.  A quick drain + brief pause is sufficient for Drop.
        while self.sample_buffer.pop().is_some() {}
        thread::sleep(Duration::from_millis(100));
    }
}

fn run_pipewire_loop(
    buffer: Arc<ArrayQueue<f32>>,
    sample_rate: u32, // Native sample rate (48000 Hz)
    channel_count: u32,
    stream_ready: Arc<AtomicBool>,
    sink_target: Option<String>,
    channel_names: Option<Vec<String>>,
    enable_adaptive_resampling: bool,
    output_sample_rate: Option<u32>, // Target output rate for upsampling
    buffer_config: PipewireBufferConfig,
    current_rate_adjust: Arc<AtomicU32>,
    flush_requested: Arc<AtomicBool>,
    graph_latency_ms_out: Arc<AtomicU32>,
) -> Result<()> {
    // Determine actual output rate and resampling ratio
    let actual_output_rate = output_sample_rate.unwrap_or(sample_rate);
    let resample_ratio = actual_output_rate as f64 / sample_rate as f64;
    let needs_resampling = resample_ratio != 1.0;

    if needs_resampling {
        log::info!(
            "PipeWire local resampling: {} Hz -> {} Hz (ratio {:.2}x)",
            sample_rate,
            actual_output_rate,
            resample_ratio
        );
    }

    pw::init();

    // Use ThreadLoop for thread-safe rate control
    let main_loop = unsafe {
        pw::thread_loop::ThreadLoop::new(None, None)
            .map_err(|e| anyhow!("Failed to create thread loop: {:?}", e))?
    };

    let context = pw::context::Context::new(&main_loop)
        .map_err(|e| anyhow!("Failed to create context: {:?}", e))?;

    let core = context
        .connect(None)
        .map_err(|e| anyhow!("Failed to connect: {:?}", e))?;

    let mut props = pw::properties::Properties::new();

    // Set node name
    props.insert("node.name", "gsrd-vbap-renderer");
    props.insert("media.name", "VBAP Spatial Audio");

    // Set target sink if specified
    if let Some(ref target) = sink_target {
        props.insert("node.target", target.as_str());
        log::info!("PipeWire output target: {}", target);
    }

    // Set channel names if provided (e.g., "FL,FR,C,LFE,BL,BR")
    // audio.position tells PipeWire the spatial positions of channels
    // Convert to PipeWire standard names (C→FC, BL→RL, BR→RR)
    let positions_string = channel_names.as_ref().map(|names| {
        names
            .iter()
            .map(|n| to_pipewire_position(n))
            .collect::<Vec<_>>()
            .join(",")
    });
    let channels_string = channel_count.to_string();
    if let Some(ref positions) = positions_string {
        props.insert("audio.position", positions.as_str());
        props.insert("audio.channels", channels_string.as_str());
        log::info!("PipeWire channel positions: {}", positions);
    }

    // Force reasonable quantum/buffer sizes to prevent huge latency
    // node.latency specifies target latency in frames/sample_rate format
    let quantum_latency_str = format!("{}/{}", buffer_config.quantum_frames, actual_output_rate);
    props.insert("node.latency", quantum_latency_str.as_str());

    log::debug!(
        "PipeWire stream properties configured: latency={}/{} (~{:.0}ms)",
        buffer_config.quantum_frames,
        actual_output_rate,
        buffer_config.quantum_frames as f64 / actual_output_rate as f64 * 1000.0
    );

    let stream = pw::stream::Stream::new(&core, "gsrd-audio", props)
        .map_err(|e| anyhow!("Failed to create stream: {:?}", e))?;

    // Setup state changed listener
    let ready_for_state = stream_ready.clone();
    let _state_listener = stream
        .add_local_listener_with_user_data(())
        .state_changed(move |_, _, old, new| {
            log::info!("PipeWire stream state changed: {:?} -> {:?}", old, new);
            if new == pw::stream::StreamState::Streaming {
                ready_for_state.store(true, Ordering::Relaxed);
                log::info!("PipeWire stream is now STREAMING");
            }
        })
        .register()
        .map_err(|e| anyhow!("Failed to register state listener: {:?}", e))?;

    // Atomic for adaptive rate matching (stores f32::to_bits(rate))
    // 1.0 = normal speed, >1.0 = faster, <1.0 = slower
    let desired_rate = Arc::new(AtomicU32::new(1.0f32.to_bits()));

    // Initialize Resampler if needed (High quality Sinc)
    let mut resampler_opt = if needs_resampling {
        let params = SincInterpolationParameters {
            sinc_len: 256,
            f_cutoff: 0.95,
            interpolation: SincInterpolationType::Linear,
            oversampling_factor: 256,
            window: WindowFunction::BlackmanHarris2,
        };

        // Rubato expects a relative ratio bound (>= 1.0), not an absolute ratio.
        // With MAX_RATE_ADJUST=0.002, allowed absolute range is:
        //   [base_ratio / 1.002, base_ratio * 1.002]
        let max_resample_ratio_relative = 1.0 + MAX_RATE_ADJUST;
        let max_resample_ratio_abs = resample_ratio * max_resample_ratio_relative;

        log::debug!(
            "Initializing PipeWire resampler: base_ratio={:.4}, max_ratio={:.4}, chunk_size={}",
            resample_ratio,
            max_resample_ratio_abs,
            RESAMPLER_CHUNK_SIZE
        );

        let resampler = SincFixedIn::<f32>::new(
            resample_ratio,
            max_resample_ratio_relative,
            params,
            RESAMPLER_CHUNK_SIZE,
            channel_count as usize,
        )
        .map_err(|e| anyhow!("Failed to create resampler: {:?}", e))?;

        Some(resampler)
    } else {
        None
    };

    // Intermediate buffers for resampling
    let mut resampler_input: Vec<Vec<f32>> =
        vec![vec![0.0; RESAMPLER_CHUNK_SIZE]; channel_count as usize];
    let mut input_frames_collected = 0;
    let mut output_fifo: Vec<f32> =
        Vec::with_capacity(RESAMPLER_CHUNK_SIZE * channel_count as usize * 4);

    // Setup process callback
    let buffer_for_callback = buffer.clone();
    let desired_rate_for_callback = desired_rate.clone();
    let rate_adjust_for_callback = current_rate_adjust.clone();
    let flush_requested_for_callback = flush_requested.clone();
    let graph_latency_for_callback = graph_latency_ms_out.clone();
    let adaptive_resampling_enabled = enable_adaptive_resampling;
    let mut callback_count = 0u64;
    let mut playback_started = false;
    let mut underrun_warned = false;
    let mut accumulated_drift = 0.0f64; // Integral term state
    let mut buffer_recovering = false; // True while waiting for buffer to refill after underrun
    let mut prev_available = 0usize; // Previous callback ring fill (samples)
    let mut non_adaptive_recenter_pending = false;

    // Compute channel-aware buffer thresholds for the callback (ms → frames → samples).
    // IMPORTANT: these thresholds are compared against `buffer_for_callback.len()`, which
    // stores INPUT-domain samples (writer pushes at `sample_rate` before local resampling).
    // Therefore the conversion must use the input sample rate, not `actual_output_rate`.
    // Using output rate here underestimates latency when downsampling (e.g. 96k -> 48k),
    // causing too-low target fill, long-term A/V drift, and instability.
    //
    // latency_ms is used for both the prefill threshold and the PI controller setpoint so
    // that the controller starts in its linear regime from the very first frame.
    let latency_frames = (buffer_config.latency_ms as usize * sample_rate as usize) / 1000;
    let max_buffer_frames = (buffer_config.max_latency_ms as usize * sample_rate as usize) / 1000;
    let min_buffer_fill = latency_frames * channel_count as usize;
    let max_buffer_fill = max_buffer_frames * channel_count as usize;
    let target_buffer_fill = min_buffer_fill; // same as prefill threshold
    let quantum_samples = buffer_config.quantum_frames as usize * channel_count as usize;
    // A sudden drop larger than ~2 callbacks worth of audio is treated as a transport
    // discontinuity (pause/resume/seek burst), not normal clock drift.
    let discontinuity_drop_threshold = quantum_samples.saturating_mul(2);
    // Treat errors above ~120 ms as transient mismatch and allow faster convergence.
    let samples_per_ms = (sample_rate as usize).saturating_mul(channel_count as usize) / 1000;
    let fast_catchup_threshold = 120usize.saturating_mul(samples_per_ms);

    log::info!(
        "PipeWire buffer thresholds ({}ch): latency={}ms max={}ms quantum={}fr | \
         target={} max={} samples",
        channel_count,
        buffer_config.latency_ms,
        buffer_config.max_latency_ms,
        buffer_config.quantum_frames,
        min_buffer_fill,
        max_buffer_fill
    );

    let _listener = stream
        .add_local_listener_with_user_data(())
        .process(move |stream, _| {
            callback_count += 1;

            if let Some(mut buffer) = stream.dequeue_buffer() {
                let datas = buffer.datas_mut();
                if datas.is_empty() {
                    return;
                }

                let data = &mut datas[0];

                // Get the data slice and fill it
                let written = if let Some(slice) = data.data() {
                    let max_samples = slice.len() / 4; // 4 bytes per f32
                    let dest = unsafe {
                        std::slice::from_raw_parts_mut(
                            slice.as_ptr() as *mut f32,
                            max_samples,
                        )
                    };

                    // Ensure frame alignment: PipeWire may provide buffers sized
                    // for the sink's channel count rather than our source's channel count.
                    // We must only write complete frames to avoid channel misalignment.
                    let ch = channel_count as usize;
                    let max_frames = max_samples / ch;
                    let frame_aligned_max = max_frames * ch;

                    if callback_count == 1 {
                        log::info!(
                            "PipeWire callback #1: buffer={} bytes, {} samples, {} channels → {} frames (remainder: {})",
                            slice.len(), max_samples, ch, max_frames, max_samples % ch
                        );
                        if max_samples != frame_aligned_max {
                            log::warn!(
                                "PipeWire buffer NOT frame-aligned! {} samples / {} channels = {} remainder. Sink may have different channel count.",
                                max_samples, ch, max_samples % ch
                            );
                        }
                    }

                    // Seek/decoder-reset flush: discard stale pre-seek audio and
                    // re-enter prefill so post-seek audio starts cleanly.
                    if flush_requested_for_callback.load(Ordering::Relaxed) {
                        while buffer_for_callback.pop().is_some() {}
                        playback_started = false;
                        accumulated_drift = 0.0;
                        buffer_recovering = false;
                        underrun_warned = false;
                        output_fifo.clear();
                        input_frames_collected = 0;
                        flush_requested_for_callback.store(false, Ordering::Relaxed);
                        log::info!("PipeWire: seek flush complete, waiting for buffer prefill");
                    }

                    // Sample downstream graph latency (RT-safe: pw_stream_get_time is RT-safe
                    // inside the process callback). Update every ~100 callbacks to amortise cost.
                    if callback_count % 100 == 50 {
                        let stream_ptr = stream.as_raw_ptr();
                        let mut pw_t = PwTime::default();
                        let ok = unsafe { pw_stream_get_time(stream_ptr as *mut _, &mut pw_t) };
                        if ok == 0 && pw_t.rate.denom > 0 && pw_t.delay > 0 {
                            let delay_ms = pw_t.delay as f32 / pw_t.rate.denom as f32 * 1000.0;
                            graph_latency_for_callback.store(delay_ms.to_bits(), Ordering::Relaxed);
                        }
                    }

                    // Check if we have enough samples to prevent stuttering
                    let available = buffer_for_callback.len();
                    let dropped_since_last = prev_available.saturating_sub(available);

                    // In non-adaptive mode there is no continuous clock correction, so after
                    // pause/resume the ring fill can settle well below target. Detect abrupt
                    // transport drops and force one prefill cycle back to target.
                    if !adaptive_resampling_enabled && playback_started && !buffer_recovering {
                        if dropped_since_last >= discontinuity_drop_threshold {
                            non_adaptive_recenter_pending = true;
                        }
                        if non_adaptive_recenter_pending && available < min_buffer_fill {
                            buffer_recovering = true;
                            accumulated_drift = 0.0;
                            underrun_warned = false;
                            output_fifo.clear();
                            input_frames_collected = 0;
                            non_adaptive_recenter_pending = false;
                            log::info!(
                                "PipeWire non-adaptive recenter: {} < {} samples after discontinuity, refilling",
                                available, min_buffer_fill
                            );
                        }
                    }
                    if available >= min_buffer_fill {
                        non_adaptive_recenter_pending = false;
                    }
                    prev_available = available;

                    // Recovery: when the buffer drops critically during active playback
                    // (e.g. after a pause/resume), output silence without consuming the ring
                    // buffer.  The ring buffer refills at mpv's write speed (~0.5 s) instead
                    // of the PI correction rate (several minutes at 1500 ppm clock drift).
                    if playback_started && available < min_buffer_fill / 2 && !buffer_recovering {
                        buffer_recovering = true;
                        accumulated_drift = 0.0; // prevent PI windup across the gap
                        underrun_warned = false;  // allow underrun log after recovery
                        output_fifo.clear();      // flush stale resampled frames
                        input_frames_collected = 0;
                        log::warn!(
                            "PipeWire buffer critically low ({} < {} samples), entering recovery mode",
                            available, min_buffer_fill / 2
                        );
                    }
                    if buffer_recovering && available >= min_buffer_fill {
                        buffer_recovering = false;
                        log::info!(
                            "PipeWire buffer recovered ({} samples), resuming playback",
                            available
                        );
                    }

                    // Output silence (without consuming ring buffer) while prefilling or recovering
                    if (!playback_started && available < min_buffer_fill) || buffer_recovering {
                        for i in 0..frame_aligned_max {
                            dest[i] = 0.0;
                        }
                        frame_aligned_max
                    } else {
                        if !playback_started {
                            playback_started = true;
                            log::info!("Starting PipeWire playback ({} samples in buffer)", available);
                        }

                        // Path 1: Resampling enabled (local rubato resampling)
                        if let Some(ref mut resampler) = resampler_opt {
                            // Adaptive rate matching with resampler
                            if adaptive_resampling_enabled && callback_count % 10 == 0 {
                                let drift = available as i64 - target_buffer_fill as i64;

                                if drift.abs() > 480 {
                                    accumulated_drift += drift as f64;
                                    let integral_contribution = accumulated_drift * RATE_ADJUST_I_GAIN;
                                    if integral_contribution.abs() > MAX_INTEGRAL_TERM {
                                        accumulated_drift = (MAX_INTEGRAL_TERM / RATE_ADJUST_I_GAIN) * integral_contribution.signum();
                                    }
                                }

                                let p_term = drift as f64 * RATE_ADJUST_P_GAIN / 100.0;
                                let i_term = accumulated_drift * RATE_ADJUST_I_GAIN;
                                // >1.0 means "consume input faster".
                                let max_adjust = if (drift.unsigned_abs() as usize) > fast_catchup_threshold {
                                    MAX_RATE_ADJUST_FAST
                                } else {
                                    MAX_RATE_ADJUST
                                };
                                let consume_adjust =
                                    (1.0 + p_term + i_term).clamp(1.0 - max_adjust, 1.0 + max_adjust);
                                // SincFixedIn ratio is OUTPUT/INPUT.
                                // To consume INPUT faster for a fixed output demand, ratio must decrease.
                                let current_ratio = (resample_ratio / consume_adjust).clamp(
                                    resample_ratio * (1.0 - max_adjust),
                                    resample_ratio * (1.0 + max_adjust)
                                );

                                if let Err(e) = resampler.set_resample_ratio(current_ratio, true) as Result<(), rubato::ResampleError> {
                                    log::warn!("Failed to set resampler ratio: {}", e);
                                }

                                // Store INPUT consumption adjust factor for OSC monitoring.
                                let effective_adjust = consume_adjust as f32;
                                rate_adjust_for_callback.store(effective_adjust.to_bits(), Ordering::Relaxed);

                                if callback_count % 100 == 0 {
                                    log::debug!(
                                        "PipeWire Adaptive: buf={}/{} drift={} ratio={:.6} (base={:.2} P={:.6} I={:.6})",
                                        available, max_buffer_fill, drift, current_ratio, resample_ratio, p_term, i_term
                                    );
                                }
                            }

                            // Feed resampler until output_fifo has enough data
                            let audio_samples_needed = max_samples;

                            while output_fifo.len() < audio_samples_needed {
                                // Fill input buffer
                                while input_frames_collected < RESAMPLER_CHUNK_SIZE {
                                    let mut frame_complete = true;
                                    if buffer_for_callback.len() >= channel_count as usize {
                                        for ch in 0..channel_count as usize {
                                            if let Some(sample_f32) = buffer_for_callback.pop() {
                                                // Already in f32 format [-1.0, 1.0]
                                                resampler_input[ch][input_frames_collected] = sample_f32;
                                            } else {
                                                frame_complete = false;
                                                break;
                                            }
                                        }
                                    } else {
                                        frame_complete = false;
                                    }

                                    if frame_complete {
                                        input_frames_collected += 1;
                                    } else {
                                        break;
                                    }
                                }

                                // Run resampler if input is full
                                if input_frames_collected == RESAMPLER_CHUNK_SIZE {
                                    match resampler.process(&resampler_input, None) {
                                        Ok(output_planar) => {
                                            let output_frames = output_planar[0].len();
                                            for i in 0..output_frames {
                                                for ch in 0..channel_count as usize {
                                                    output_fifo.push(output_planar[ch][i]);
                                                }
                                            }
                                            input_frames_collected = 0;
                                        },
                                        Err(e) => {
                                            log::error!("Resampler error: {}", e);
                                            break;
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }

                            // Fill output buffer from FIFO
                            if output_fifo.len() >= audio_samples_needed {
                                // Copy f32 samples directly from FIFO to PipeWire
                                for i in 0..audio_samples_needed {
                                    dest[i] = output_fifo[i];
                                }
                                output_fifo.drain(0..audio_samples_needed);
                            } else {
                                // Underrun: not enough data in output_fifo
                                if !underrun_warned {
                                    log::warn!("Resampler underrun: only {} samples in FIFO, needed {}", output_fifo.len(), audio_samples_needed);
                                    underrun_warned = true;
                                }
                                for i in 0..max_samples {
                                    dest[i] = 0.0;
                                }
                            }
                            max_samples
                        } else {
                            // Path 2: No resampling (direct copy with optional rate control via PipeWire)
                            // Adaptive rate matching via PipeWire rate control
                            if adaptive_resampling_enabled && callback_count % 10 == 0 {
                                let drift = available as i64 - target_buffer_fill as i64;

                                if drift.abs() > 480 {
                                    accumulated_drift += drift as f64;
                                    let integral_contribution = accumulated_drift * RATE_ADJUST_I_GAIN;
                                    if integral_contribution.abs() > MAX_INTEGRAL_TERM {
                                        accumulated_drift = (MAX_INTEGRAL_TERM / RATE_ADJUST_I_GAIN) * integral_contribution.signum();
                                    }
                                }

                                let p_term = drift as f64 * RATE_ADJUST_P_GAIN / 100.0;
                                let i_term = accumulated_drift * RATE_ADJUST_I_GAIN;
                                // >1.0 means "consume input faster".
                                let max_adjust = if (drift.unsigned_abs() as usize) > fast_catchup_threshold {
                                    MAX_RATE_ADJUST_FAST
                                } else {
                                    MAX_RATE_ADJUST
                                };
                                let consume_adjust =
                                    (1.0 + p_term + i_term).clamp(1.0 - max_adjust, 1.0 + max_adjust);
                                // SPA_PROP_RATE follows output/input semantics too:
                                // to consume input faster, lower the ratio.
                                let pipewire_rate = (1.0 / consume_adjust) as f32;

                                rate_adjust_for_callback
                                    .store((consume_adjust as f32).to_bits(), Ordering::Relaxed);
                                if drift.abs() > 1000 || callback_count % 100 == 0 {
                                    desired_rate_for_callback.store(pipewire_rate.to_bits(), Ordering::Relaxed);
                                }

                                if callback_count % 100 == 0 {
                                    log::debug!(
                                        "Adaptive rate: buf={}/{} drift={} (P={:.6} I={:.6}) -> consume={:.6} pw_rate={:.6}",
                                        available, max_buffer_fill, drift, p_term, i_term, consume_adjust, pipewire_rate
                                    );
                                }
                            }

                            // Read only complete frames to maintain channel alignment
                            let available_frames = available / ch;
                            let frames_to_read = available_frames.min(max_frames);
                            let samples_to_read = frames_to_read * ch;

                            let mut count = 0;
                            while count < samples_to_read {
                                if let Some(sample_f32) = buffer_for_callback.pop() {
                                    dest[count] = sample_f32;
                                    count += 1;
                                } else {
                                    break;
                                }
                            }

                            // Zero-pad the remainder of the buffer
                            while count < max_samples {
                                dest[count] = 0.0;
                                count += 1;
                            }

                            if available < min_buffer_fill && !underrun_warned {
                                log::warn!("Buffer underrun: only {} samples available", available);
                                underrun_warned = true;
                            }

                            max_samples
                        }
                    }
                } else {
                    0
                };

                // Update chunk metadata
                let chunk = data.chunk_mut();
                *chunk.offset_mut() = 0;
                *chunk.size_mut() = (written * 4) as u32;
                *chunk.stride_mut() = 4;
            }
        })
        .register()
        .map_err(|e| anyhow!("Failed to register process listener: {:?}", e))?;

    // Configure audio format
    let mut audio_info = pw::spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(pw::spa::param::audio::AudioFormat::F32LE);
    audio_info.set_rate(actual_output_rate); // Use output rate (may be upsampled)
    audio_info.set_channels(channel_count);

    // Serialize format
    let values: Vec<u8> = pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(pw::spa::pod::Object {
            type_: pw::spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
            id: pw::spa::param::ParamType::EnumFormat.as_raw(),
            properties: audio_info.into(),
        }),
    )
    .map_err(|e| anyhow!("Failed to serialize format: {:?}", e))?
    .0
    .into_inner();

    let param =
        pw::spa::pod::Pod::from_bytes(&values).ok_or_else(|| anyhow!("Failed to create param"))?;

    log::debug!("Connecting PipeWire stream...");

    // Lock for connection (returns RAII guard that auto-unlocks)
    {
        let _lock = main_loop.lock();

        // Connect stream
        stream
            .connect(
                pw::spa::utils::Direction::Output,
                None,
                pw::stream::StreamFlags::AUTOCONNECT
                    | pw::stream::StreamFlags::MAP_BUFFERS
                    | pw::stream::StreamFlags::RT_PROCESS,
                &mut [&param],
            )
            .map_err(|e| anyhow!("Failed to connect stream: {:?}", e))?;
    } // Lock automatically released here

    // Start the thread loop (this spawns the RT thread)
    main_loop.start();

    log::debug!("PipeWire thread loop started");

    // Thread-safe rate control loop (only runs if adaptive resampling is enabled AND no local resampling)
    // When using local rubato resampling, we adjust the resampler ratio instead
    // Periodically check if rate adjustment is needed and apply it with proper locking
    if enable_adaptive_resampling && !needs_resampling {
        let stream_ptr = stream.as_raw_ptr();
        let loop_ptr = main_loop.as_raw_ptr();
        let mut last_applied_rate = 1.0f32;

        loop {
            // Sleep to avoid busy-waiting (check every 50ms)
            thread::sleep(Duration::from_millis(50));

            // Read desired rate from atomic
            let rate_bits = desired_rate.load(Ordering::Relaxed);
            let desired_rate_value = f32::from_bits(rate_bits);

            // Only apply if significantly different from last applied
            if (desired_rate_value - last_applied_rate).abs() > 0.0001 {
                // Lock the thread loop for thread-safe API calls
                unsafe {
                    pw_thread_loop_lock(loop_ptr as *mut _);

                    // Apply rate control
                    let rate = desired_rate_value;
                    let result = pw_stream_set_control(
                        stream_ptr as *mut _,
                        SPA_PROP_RATE,
                        1,
                        &rate as *const f32,
                        0,
                    );

                    pw_thread_loop_unlock(loop_ptr as *mut _);

                    if result == 0 {
                        last_applied_rate = desired_rate_value;
                        log::trace!("Applied rate adjustment: {:.6}", rate);
                    } else {
                        log::warn!("Failed to apply rate control: {}", result);
                    }
                }
            }
        }
    } else {
        // Adaptive resampling disabled - just keep the thread alive at fixed rate (1.0)
        // The stream will play at the nominal sample rate without dynamic adjustments
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }

    // Note: This code is unreachable as the loop runs forever
    // The thread is cleaned up when the program exits
    // In a real implementation, we'd want a shutdown signal
}
