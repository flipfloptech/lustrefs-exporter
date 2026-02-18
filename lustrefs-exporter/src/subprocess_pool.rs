// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//! Persistent REPL subprocess pool for `lctl` and `lnetctl`.
//!
//! Instead of spawning fresh subprocesses per scrape, this module keeps
//! persistent `lctl` / `lnetctl` REPL shells alive and communicates via
//! stdin/stdout pipes. A configurable pool size determines how many
//! concurrent fresh scrapes can run; excess requests coalesce onto the
//! most recently started scrape (FIFO backpressure).
//!
//! Each worker is protected by a watchdog timer that kills and respawns
//! hung processes (e.g. stuck in kernel D-state), and optional RSS
//! monitoring that respawns workers exceeding a memory limit.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, BufReader as AsyncBufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the subprocess pool.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Number of workers per command type (total processes = 2 × pool_size).
    pub pool_size: usize,
    /// Maximum time to wait for a single REPL command response.
    pub watchdog_timeout: Duration,
    /// Maximum RSS (in bytes) for a worker process. 0 = unlimited.
    pub max_worker_rss_bytes: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 2,
            watchdog_timeout: Duration::from_secs(10),
            max_worker_rss_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Exporter-wide statistics (atomic counters)
// ---------------------------------------------------------------------------

/// Atomic counters for exporter telemetry, printed on shutdown.
pub struct ExporterStats {
    pub start_time: Instant,
    pub total_scrapes: AtomicU64,
    pub coalesced_requests: AtomicU64,
    pub watchdog_respawns: AtomicU64,
    pub memory_respawns: AtomicU64,
    /// Peak observed RSS (bytes) across all lctl workers.
    pub peak_lctl_rss_bytes: AtomicU64,
    /// Peak observed RSS (bytes) across all lnetctl workers.
    pub peak_lnetctl_rss_bytes: AtomicU64,

    pub total_lctl_user_cpu_us: AtomicU64,
    pub total_lctl_sys_cpu_us: AtomicU64,
    pub total_lnetctl_user_cpu_us: AtomicU64,
    pub total_lnetctl_sys_cpu_us: AtomicU64,

    pub total_lctl_rss_sum: AtomicU64,
    pub total_lctl_rss_samples: AtomicU64,
    pub total_lnetctl_rss_sum: AtomicU64,
    pub total_lnetctl_rss_samples: AtomicU64,

    /// Instantaneous CPU peak (0-100%, stored as u64 milli-percent e.g. 100000 = 100%)
    pub peak_lctl_cpu_milli: AtomicU64,
    pub peak_lnetctl_cpu_milli: AtomicU64,

    pub total_lctl_cpu_milli_sum: AtomicU64,
    pub total_lnetctl_cpu_milli_sum: AtomicU64,
}

impl ExporterStats {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_scrapes: AtomicU64::new(0),
            coalesced_requests: AtomicU64::new(0),
            watchdog_respawns: AtomicU64::new(0),
            memory_respawns: AtomicU64::new(0),
            peak_lctl_rss_bytes: AtomicU64::new(0),
            peak_lnetctl_rss_bytes: AtomicU64::new(0),

            total_lctl_user_cpu_us: AtomicU64::new(0),
            total_lctl_sys_cpu_us: AtomicU64::new(0),
            total_lnetctl_user_cpu_us: AtomicU64::new(0),
            total_lnetctl_sys_cpu_us: AtomicU64::new(0),

            total_lctl_rss_sum: AtomicU64::new(0),
            total_lctl_rss_samples: AtomicU64::new(0),
            total_lnetctl_rss_sum: AtomicU64::new(0),
            total_lnetctl_rss_samples: AtomicU64::new(0),

            peak_lctl_cpu_milli: AtomicU64::new(0),
            peak_lnetctl_cpu_milli: AtomicU64::new(0),

            total_lctl_cpu_milli_sum: AtomicU64::new(0),
            total_lnetctl_cpu_milli_sum: AtomicU64::new(0),
        }
    }

    fn record_worker_stats(
        &self,
        name: &str,
        rss_bytes: u64,
        utime_delta_us: u64,
        stime_delta_us: u64,
        cpu_milli: u64,
    ) {
        let (rss_peak, rss_sum, rss_samples, u_cpu, s_cpu, cpu_peak, cpu_sum) = if name == "lctl" {
            (
                &self.peak_lctl_rss_bytes,
                &self.total_lctl_rss_sum,
                &self.total_lctl_rss_samples,
                &self.total_lctl_user_cpu_us,
                &self.total_lctl_sys_cpu_us,
                &self.peak_lctl_cpu_milli,
                &self.total_lctl_cpu_milli_sum,
            )
        } else {
            (
                &self.peak_lnetctl_rss_bytes,
                &self.total_lnetctl_rss_sum,
                &self.total_lnetctl_rss_samples,
                &self.total_lnetctl_user_cpu_us,
                &self.total_lnetctl_sys_cpu_us,
                &self.peak_lnetctl_cpu_milli,
                &self.total_lnetctl_cpu_milli_sum,
            )
        };

        rss_peak.fetch_max(rss_bytes, Ordering::Relaxed);
        rss_sum.fetch_add(rss_bytes, Ordering::Relaxed);
        rss_samples.fetch_add(1, Ordering::Relaxed);
        u_cpu.fetch_add(utime_delta_us, Ordering::Relaxed);
        s_cpu.fetch_add(stime_delta_us, Ordering::Relaxed);
        cpu_peak.fetch_max(cpu_milli, Ordering::Relaxed);
        cpu_sum.fetch_add(cpu_milli, Ordering::Relaxed);
    }

    /// Print exit telemetry to stderr.
    pub fn report(&self) {
        let uptime = self.start_time.elapsed();
        let hours = uptime.as_secs() / 3600;
        let mins = (uptime.as_secs() % 3600) / 60;
        let secs = uptime.as_secs() % 60;

        let (user_secs, sys_secs) = read_self_cpu_times();
        let peak_rss_kb = read_self_peak_rss_kb();
        let current_rss_kb = read_self_current_rss_kb();

        let lctl_peak_mb = self.peak_lctl_rss_bytes.load(Ordering::Relaxed) / (1024 * 1024);
        let lnetctl_peak_mb = self.peak_lnetctl_rss_bytes.load(Ordering::Relaxed) / (1024 * 1024);

        let lctl_samples = self.total_lctl_rss_samples.load(Ordering::Relaxed);
        let lctl_avg_mb = if lctl_samples > 0 {
            (self.total_lctl_rss_sum.load(Ordering::Relaxed) / lctl_samples) / (1024 * 1024)
        } else {
            0
        };

        let lnetctl_samples = self.total_lnetctl_rss_samples.load(Ordering::Relaxed);
        let lnetctl_avg_mb = if lnetctl_samples > 0 {
            (self.total_lnetctl_rss_sum.load(Ordering::Relaxed) / lnetctl_samples) / (1024 * 1024)
        } else {
            0
        };

        let lctl_cpu_peak = self.peak_lctl_cpu_milli.load(Ordering::Relaxed) as f64 / 1000.0;
        let lnetctl_cpu_peak = self.peak_lnetctl_cpu_milli.load(Ordering::Relaxed) as f64 / 1000.0;

        let lctl_cpu_avg = if lctl_samples > 0 {
            self.total_lctl_cpu_milli_sum.load(Ordering::Relaxed) as f64 / lctl_samples as f64 / 1000.0
        } else {
            0.0
        };
        let lnetctl_cpu_avg = if lnetctl_samples > 0 {
            self.total_lnetctl_cpu_milli_sum.load(Ordering::Relaxed) as f64 / lnetctl_samples as f64 / 1000.0
        } else {
            0.0
        };

        eprintln!();
        eprintln!("=== Exporter Stats ===");
        eprintln!("Uptime:            {hours}h {mins}m {secs}s");
        eprintln!(
            "Scrapes:           {} ({} coalesced)",
            self.total_scrapes.load(Ordering::Relaxed),
            self.coalesced_requests.load(Ordering::Relaxed)
        );
        eprintln!("RSS (self):        {} MB current, {} MB peak", current_rss_kb / 1024, peak_rss_kb / 1024);
        eprintln!("CPU (self):        {user_secs:.1}s user / {sys_secs:.1}s sys");
        eprintln!(
            "Worker RSS (peak): lctl={lctl_peak_mb} MB, lnetctl={lnetctl_peak_mb} MB"
        );
        eprintln!(
            "Worker RSS (avg):  lctl={lctl_avg_mb} MB, lnetctl={lnetctl_avg_mb} MB"
        );
        eprintln!(
            "Worker CPU (peak): lctl={lctl_cpu_peak:.1}%, lnetctl={lnetctl_cpu_peak:.1}%"
        );
        eprintln!(
            "Worker CPU (avg):  lctl={lctl_cpu_avg:.1}%, lnetctl={lnetctl_cpu_avg:.1}%"
        );
        eprintln!(
            "Worker respawns:   {} watchdog, {} memory",
            self.watchdog_respawns.load(Ordering::Relaxed),
            self.memory_respawns.load(Ordering::Relaxed),
        );
        eprintln!("======================");
    }
}

// ---------------------------------------------------------------------------
// PTY helpers — allocate pseudo-terminals so lctl/lnetctl see isatty()==true
// ---------------------------------------------------------------------------

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use tokio::io::unix::AsyncFd;

/// REPL prompt bytes used to detect end-of-response.
/// With a PTY, the child sees isatty()==true and prints these prompts.
const LCTL_PROMPT: &[u8] = b"lctl > ";
const LNETCTL_PROMPT: &[u8] = b"lnetctl > ";

/// Allocate a PTY master/slave pair with ECHO and OPOST disabled on the slave.
///
/// - **No ECHO**: prevents our commands from being reflected back through
///   the PTY, which would pollute the output stream.
/// - **No OPOST**: prevents `\n` → `\r\n` conversion in output.
fn allocate_pty() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master_raw: libc::c_int = 0;
    let mut slave_raw: libc::c_int = 0;

    if unsafe {
        libc::openpty(
            &mut master_raw,
            &mut slave_raw,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }

    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };

    // Configure the slave terminal
    unsafe {
        let mut termios: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(slave_raw, &mut termios) != 0 {
            return Err(io::Error::last_os_error());
        }
        // Disable echo (all variants) so our commands aren't reflected back
        termios.c_lflag &= !(libc::ECHO | libc::ECHOE | libc::ECHOK | libc::ECHONL);
        // Disable output post-processing (no \n → \r\n conversion)
        termios.c_oflag &= !libc::OPOST;
        if libc::tcsetattr(slave_raw, libc::TCSANOW, &termios) != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    Ok((master, slave))
}

/// Set a file descriptor to non-blocking mode (required for AsyncFd).
fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Raw read(2) wrapper.
fn raw_read(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    let ret =
        unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

/// Raw write(2) wrapper.
fn raw_write(fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    let ret =
        unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

// ---------------------------------------------------------------------------
// REPL Worker — persistent child process with PTY-based stdin/stdout
// ---------------------------------------------------------------------------

/// A persistent REPL subprocess (`lctl` or `lnetctl`) connected via PTY.
///
/// The PTY makes the child believe it's running interactively (isatty()==true),
/// so it prints prompts after each command. We use the prompt as a zero-cost
/// delimiter — no idle timeouts, no artificial delays.
struct ReplWorker {
    child: Child,
    reader: AsyncFd<OwnedFd>,
    writer: AsyncFd<OwnedFd>,
    prompt: &'static [u8],
    name: &'static str,
}

impl ReplWorker {
    /// Spawn a new persistent REPL process via PTY and consume the initial prompt.
    async fn spawn(name: &'static str, prompt: &'static [u8]) -> io::Result<Self> {
        let (master, slave) = allocate_pty()?;

        // Dup slave for stdin (the original becomes stdout)
        let slave_dup = slave.try_clone().map_err(|e| {
            io::Error::new(e.kind(), format!("Failed to dup PTY slave for {name}: {e}"))
        })?;

        let mut child = Command::new(name)
            .stdin(slave_dup)       // child's stdin = PTY slave
            .stdout(slave)          // child's stdout = PTY slave
            .stderr(std::process::Stdio::piped()) // stderr stays separate
            .spawn()?;

        // Drain stderr in background to prevent pipe buffer deadlocks
        if let Some(stderr) = child.stderr.take() {
            let worker_name = name;
            tokio::spawn(async move {
                let mut reader = AsyncBufReader::new(stderr);
                let mut buf = vec![0u8; 1024];
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                                for line in s.lines() {
                                    tracing::debug!("{worker_name} stderr: {line}");
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        // Dup master for separate read/write fds, both set non-blocking
        let master_write = master.try_clone().map_err(|e| {
            io::Error::new(e.kind(), format!("Failed to dup PTY master for {name}: {e}"))
        })?;
        set_nonblocking(&master)?;
        set_nonblocking(&master_write)?;

        let reader = AsyncFd::new(master).map_err(|e| {
            io::Error::new(e.kind(), format!("AsyncFd reader for {name}: {e}"))
        })?;
        let writer = AsyncFd::new(master_write).map_err(|e| {
            io::Error::new(e.kind(), format!("AsyncFd writer for {name}: {e}"))
        })?;

        let mut worker = Self {
            child,
            reader,
            writer,
            prompt,
            name,
        };

        // Consume the initial prompt printed when the REPL starts
        worker.read_until_prompt().await?;
        tracing::debug!("{name}: PTY REPL worker spawned (pid={})", worker.pid().unwrap_or(0));

        Ok(worker)
    }

    /// Write a command to the REPL and read the complete response.
    async fn query(&mut self, command: &str) -> io::Result<Vec<u8>> {
        self.write_all(command.as_bytes()).await?;
        self.write_all(b"\n").await?;

        self.read_until_prompt().await
    }

    /// Write all bytes to the PTY master (async, non-blocking).
    async fn write_all(&self, data: &[u8]) -> io::Result<()> {
        let mut offset = 0;
        while offset < data.len() {
            let mut guard = self.writer.writable().await?;
            match guard.try_io(|inner| raw_write(inner.as_raw_fd(), &data[offset..])) {
                Ok(Ok(n)) => offset += n,
                Ok(Err(e)) => return Err(e),
                Err(_would_block) => continue,
            }
        }
        Ok(())
    }

    /// Read bytes from the PTY master (async, non-blocking).
    async fn read_some(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.reader.readable().await?;
            match guard.try_io(|inner| raw_read(inner.as_raw_fd(), buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Read from PTY until the REPL prompt is detected.
    ///
    /// The prompt (e.g. `lctl > `) appears at the end of stdout after
    /// the command output. This is **deterministic** — no timeouts,
    /// no heuristics. The prompt IS the delimiter.
    async fn read_until_prompt(&mut self) -> io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(64 * 1024);
        let mut buf = [0u8; 8192];

        loop {
            let n = self.read_some(&mut buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{}: REPL process exited unexpectedly", self.name),
                ));
            }
            output.extend_from_slice(&buf[..n]);

            if output.ends_with(self.prompt) {
                // Strip the prompt from the output
                output.truncate(output.len() - self.prompt.len());
                break;
            }
        }

        Ok(output)
    }

    /// Get the child process PID.
    fn pid(&self) -> Option<u32> {
        self.child.id()
    }
}

// ---------------------------------------------------------------------------
// Watchdog Worker — wraps ReplWorker with timeout + RSS monitoring
// ---------------------------------------------------------------------------

/// A REPL worker wrapped with watchdog timeout and memory monitoring.
///
/// - **Watchdog**: if `query()` doesn't complete within the configured
///   timeout, the child is killed and a fresh one spawned.
/// - **RSS check**: after each successful query, reads `/proc/<pid>/status`
///   to check VmRSS. If over the limit, kills and respawns.
pub struct WatchdogWorker {
    worker: ReplWorker,
    config: PoolConfig,
    stats: Arc<ExporterStats>,
}

impl WatchdogWorker {
    /// Send a command to the REPL with watchdog timeout protection.
    ///
    /// On timeout or error, kills the process and spawns a replacement.
    /// Returns `Ok(output)` on success, `Err` on failure (caller should
    /// treat as partial data / skip this data source).
    pub async fn query(&mut self, command: &str) -> io::Result<Vec<u8>> {
        let name = self.worker.name;
        let stats = Arc::clone(&self.stats);
        let pid_opt = self.worker.pid();

        // Spawn sidecar prober to catch peaks and track cumulative CPU
        let prober_stop_tx = pid_opt.map(|pid| {
            let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
            let name_str = name.to_string();
            tokio::spawn(async move {
                let mut last_sample = read_process_stats(pid).ok();
                let mut last_instant = Instant::now();
                let mut interval = tokio::time::interval(Duration::from_millis(50));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        _ = interval.tick() => {
                            if let Ok(current) = read_process_stats(pid) {
                                let now = Instant::now();
                                let delta_time = now.duration_since(last_instant).as_secs_f64();

                                let mut utime_delta_us = 0;
                                let mut stime_delta_us = 0;
                                let mut cpu_milli = 0;

                                if let Some(last) = last_sample {
                                    let utime_delta = current.utime.saturating_sub(last.utime);
                                    let stime_delta = current.stime.saturating_sub(last.stime);

                                    // Convert ticks to microseconds (assuming 100 ticks/sec)
                                    utime_delta_us = utime_delta * 10_000;
                                    stime_delta_us = stime_delta * 10_000;

                                    // CPU % = (delta_ticks / ticks_per_sec) / delta_time
                                    if delta_time > 0.0 {
                                        let total_delta_sec = (utime_delta + stime_delta) as f64 / 100.0;
                                        cpu_milli = (total_delta_sec / delta_time * 100_000.0) as u64;
                                    }
                                }

                                stats.record_worker_stats(
                                    &name_str,
                                    current.rss_bytes,
                                    utime_delta_us,
                                    stime_delta_us,
                                    cpu_milli,
                                );

                                last_sample = Some(current);
                                last_instant = now;
                            } else {
                                break; // Process exited
                            }
                        }
                    }
                }
            });
            stop_tx
        });

        let result = tokio::time::timeout(self.config.watchdog_timeout, self.worker.query(command)).await;

        // Stop prober immediately after query finishes
        if let Some(stop_tx) = prober_stop_tx {
            let _ = stop_tx.send(());
        }

        match result {
            Ok(Ok(output)) => {
                self.check_rss_and_maybe_respawn().await;
                Ok(output)
            }
            Ok(Err(e)) => {
                tracing::warn!("{}: query error: {e}, respawning", self.worker.name);
                self.kill_and_respawn(true).await;
                Err(e)
            }
            Err(_elapsed) => {
                tracing::warn!(
                    "{}: watchdog timeout ({}s), killing and respawning",
                    self.worker.name,
                    self.config.watchdog_timeout.as_secs()
                );
                self.kill_and_respawn(true).await;
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{}: watchdog timeout", self.worker.name),
                ))
            }
        }
    }

    /// Check the worker's RSS and respawn if over the configured limit.
    async fn check_rss_and_maybe_respawn(&mut self) {
        if self.config.max_worker_rss_bytes == 0 {
            return;
        }
        if let Some(pid) = self.worker.pid() {
            if let Ok(rss_bytes) = read_process_rss_bytes(pid) {
                if rss_bytes > self.config.max_worker_rss_bytes {
                    tracing::info!(
                        "{}: RSS {} MB exceeds limit {} MB, respawning",
                        self.worker.name,
                        rss_bytes / (1024 * 1024),
                        self.config.max_worker_rss_bytes / (1024 * 1024),
                    );
                    self.kill_and_respawn(false).await;
                }
            }
        }
    }

    /// Kill the current child and spawn a fresh REPL.
    ///
    /// `is_watchdog` controls which counter gets incremented.
    async fn kill_and_respawn(&mut self, is_watchdog: bool) {
        let _ = self.worker.child.kill().await;

        if is_watchdog {
            self.stats.watchdog_respawns.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.memory_respawns.fetch_add(1, Ordering::Relaxed);
        }

        match ReplWorker::spawn(self.worker.name, self.worker.prompt).await {
            Ok(new_worker) => {
                self.worker = new_worker;
            }
            Err(e) => {
                tracing::error!(
                    "Failed to respawn {} REPL: {e}. Worker is offline until next attempt.",
                    self.worker.name
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Worker Pool — Vec<Mutex<WatchdogWorker>> with try-acquire
// ---------------------------------------------------------------------------

/// A pool of identical REPL workers.
pub struct WorkerPool {
    workers: Vec<Arc<Mutex<WatchdogWorker>>>,
    #[allow(dead_code)]
    name: &'static str,
}

impl WorkerPool {
    /// Create a pool of `pool_size` workers for the given command.
    async fn new(
        name: &'static str,
        prompt: &'static [u8],
        config: &PoolConfig,
        stats: Arc<ExporterStats>,
    ) -> io::Result<Self> {
        let mut workers = Vec::with_capacity(config.pool_size);

        for i in 0..config.pool_size {
            let worker = ReplWorker::spawn(name, prompt).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to spawn {name} worker {i}: {e}"),
                )
            })?;

            workers.push(Arc::new(Mutex::new(WatchdogWorker {
                worker,
                config: config.clone(),
                stats: Arc::clone(&stats),
            })));
        }

        tracing::info!("{name}: spawned {n} REPL workers", n = config.pool_size);
        Ok(Self { workers, name })
    }

    /// Try to acquire a free worker (non-blocking).
    ///
    /// Returns an `OwnedMutexGuard` which is `'static + Send`, allowing
    /// the worker to be moved into spawned tasks and streaming bodies.
    pub fn try_acquire(&self) -> Option<tokio::sync::OwnedMutexGuard<WatchdogWorker>> {
        for mutex in &self.workers {
            if let Ok(guard) = Arc::clone(mutex).try_lock_owned() {
                return Some(guard);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Subprocess Pool — two pools (lctl + lnetctl) + coordination
// ---------------------------------------------------------------------------

/// The top-level subprocess pool holding `lctl` and `lnetctl` worker pools.
///
/// Each scrape claims one worker from each pool. If either pool is
/// exhausted, the request coalesces onto the most recently started scrape.
pub struct SubprocessPool {
    pub lctl_pool: WorkerPool,
    pub lnetctl_pool: WorkerPool,
    pub stats: Arc<ExporterStats>,
}

impl SubprocessPool {
    /// Initialize both pools. Spawns `2 × pool_size` persistent REPL processes.
    pub async fn new(config: &PoolConfig) -> io::Result<Self> {
        let stats = Arc::new(ExporterStats::new());

        let lctl_pool =
            WorkerPool::new("lctl", LCTL_PROMPT, config, Arc::clone(&stats)).await?;
        let lnetctl_pool =
            WorkerPool::new("lnetctl", LNETCTL_PROMPT, config, Arc::clone(&stats)).await?;

        Ok(Self {
            lctl_pool,
            lnetctl_pool,
            stats,
        })
    }

    /// Try to acquire one lctl + one lnetctl worker for a fresh scrape.
    ///
    /// Returns `Some((lctl_guard, lnetctl_guard))` if both pools have a
    /// free worker, otherwise `None` (caller should coalesce).
    pub fn try_acquire_pair(
        &self,
    ) -> Option<(
        tokio::sync::OwnedMutexGuard<WatchdogWorker>,
        tokio::sync::OwnedMutexGuard<WatchdogWorker>,
    )> {
        let lctl = self.lctl_pool.try_acquire()?;
        match self.lnetctl_pool.try_acquire() {
            Some(lnetctl) => Some((lctl, lnetctl)),
            None => {
                // Release lctl — we need both or neither
                drop(lctl);
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// /proc helpers
// ---------------------------------------------------------------------------

/// Read VmRSS (in bytes) for a given PID from `/proc/<pid>/status`.
fn read_process_rss_bytes(pid: u32) -> io::Result<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    for line in status.lines() {
        if let Some(val) = line.strip_prefix("VmRSS:") {
            let val = val.trim();
            // Format is "12345 kB"
            if let Some(kb_str) = val.strip_suffix(" kB").or_else(|| val.strip_suffix("kB")) {
                if let Ok(kb) = kb_str.trim().parse::<u64>() {
                    return Ok(kb * 1024);
                }
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "VmRSS not found in /proc status",
    ))
}

/// Read VmHWM (peak RSS in kB) for the current process from `/proc/self/status`.
fn read_self_peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.strip_prefix("VmHWM:"))
                .and_then(|v| {
                    v.trim()
                        .strip_suffix(" kB")
                        .or_else(|| v.trim().strip_suffix("kB"))
                })
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// Read current VmRSS (in kB) for the current process from `/proc/self/status`.
pub fn read_self_current_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| {
                    v.trim()
                        .strip_suffix(" kB")
                        .or_else(|| v.trim().strip_suffix("kB"))
                })
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// Read user and system CPU time (in seconds) for the current process
/// from `/proc/self/stat`.
fn read_self_cpu_times() -> (f64, f64) {
    let stat = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return (0.0, 0.0),
    };

    let after_comm = match stat.rfind(')') {
        Some(idx) => &stat[idx + 2..],
        None => return (0.0, 0.0),
    };

    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    if fields.len() > 12 {
        let ticks_per_sec = 100.0_f64;
        let utime = fields[11]
            .parse::<u64>()
            .map(|t| t as f64 / ticks_per_sec)
            .unwrap_or(0.0);
        let stime = fields[12]
            .parse::<u64>()
            .map(|t| t as f64 / ticks_per_sec)
            .unwrap_or(0.0);
        (utime, stime)
    } else {
        (0.0, 0.0)
    }
}

struct ProcessSample {
    utime: u64,
    stime: u64,
    rss_bytes: u64,
}

fn read_process_stats(pid: u32) -> io::Result<ProcessSample> {
    let stat_path = format!("/proc/{pid}/stat");
    let status_path = format!("/proc/{pid}/status");

    let stat = std::fs::read_to_string(stat_path)?;
    let status = std::fs::read_to_string(status_path)?;

    // Parse stat for CPU (utime is field 14, stime is field 15)
    let after_comm = stat
        .rfind(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid stat format"))?;
    let fields: Vec<&str> = stat[after_comm + 2..].split_whitespace().collect();
    if fields.len() < 13 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not enough fields in stat",
        ));
    }
    let utime = fields[11]
        .parse::<u64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let stime = fields[12]
        .parse::<u64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Parse status for RSS
    let rss_bytes = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| {
            v.trim()
                .strip_suffix(" kB")
                .or_else(|| v.trim().strip_suffix("kB"))
        })
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0);

    Ok(ProcessSample {
        utime,
        stime,
        rss_bytes,
    })
}
