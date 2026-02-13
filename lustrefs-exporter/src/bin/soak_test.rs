// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//! Soak test tool for lustrefs-exporter.
//!
//! Two modes:
//!   Hard hammer (default): `soak-test --threads 10 --duration 60`
//!   Realistic soak:        `soak-test --threads 10 --duration 300 --max-jitter 10`

use clap::Parser;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Parser)]
#[clap(name = "soak-test", about = "Soak test tool for lustrefs-exporter")]
struct Opts {
    /// Target exporter URL
    #[clap(long, default_value = "http://localhost:32221/metrics")]
    url: String,

    /// Number of concurrent client threads
    #[clap(long, default_value = "4")]
    threads: usize,

    /// Test duration in seconds
    #[clap(long, default_value = "60")]
    duration: u64,

    /// Max random jitter between requests (seconds, float).
    /// 0 = hard hammer (fire as fast as possible).
    #[clap(long, default_value = "0")]
    max_jitter: f64,
}

/// Per-request sample for latency tracking.
struct Sample {
    latency: Duration,
    bytes: usize,
    ok: bool,
}

/// Shared stats collector.
struct Stats {
    samples: Mutex<Vec<Sample>>,
    total: AtomicU64,
    errors: AtomicU64,
    bytes: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::with_capacity(100_000)),
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    async fn record(&self, sample: Sample) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(sample.bytes as u64, Ordering::Relaxed);
        if !sample.ok {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.samples.lock().await.push(sample);
    }

    async fn snapshot(&self) -> Snapshot {
        let samples = self.samples.lock().await;
        let total = self.total.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let total_bytes = self.bytes.load(Ordering::Relaxed);

        if samples.is_empty() {
            return Snapshot {
                total,
                errors,
                total_bytes,
                latency_min: Duration::ZERO,
                latency_p50: Duration::ZERO,
                latency_p95: Duration::ZERO,
                latency_p99: Duration::ZERO,
                latency_max: Duration::ZERO,
                size_min: 0,
                size_avg: 0,
                size_max: 0,
            };
        }

        let mut latencies: Vec<Duration> = samples.iter().map(|s| s.latency).collect();
        latencies.sort();

        let sizes: Vec<usize> = samples.iter().map(|s| s.bytes).collect();
        let size_min = *sizes.iter().min().unwrap_or(&0);
        let size_max = *sizes.iter().max().unwrap_or(&0);
        let size_avg = if sizes.is_empty() {
            0
        } else {
            sizes.iter().sum::<usize>() / sizes.len()
        };

        let pct = |p: f64| -> Duration {
            let idx = ((latencies.len() as f64 * p) as usize).min(latencies.len() - 1);
            latencies[idx]
        };

        Snapshot {
            total,
            errors,
            total_bytes,
            latency_min: latencies[0],
            latency_p50: pct(0.50),
            latency_p95: pct(0.95),
            latency_p99: pct(0.99),
            latency_max: *latencies.last().unwrap_or(&Duration::ZERO),
            size_min,
            size_avg,
            size_max,
        }
    }
}

struct Snapshot {
    total: u64,
    errors: u64,
    total_bytes: u64,
    latency_min: Duration,
    latency_p50: Duration,
    latency_p95: Duration,
    latency_p99: Duration,
    latency_max: Duration,
    size_min: usize,
    size_avg: usize,
    size_max: usize,
}

impl std::fmt::Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "  requests: {} total, {} errors ({:.1}%)",
            self.total,
            self.errors,
            if self.total > 0 {
                self.errors as f64 / self.total as f64 * 100.0
            } else {
                0.0
            }
        )?;
        writeln!(
            f,
            "  latency:  min={:.1}ms  p50={:.1}ms  p95={:.1}ms  p99={:.1}ms  max={:.1}ms",
            self.latency_min.as_secs_f64() * 1000.0,
            self.latency_p50.as_secs_f64() * 1000.0,
            self.latency_p95.as_secs_f64() * 1000.0,
            self.latency_p99.as_secs_f64() * 1000.0,
            self.latency_max.as_secs_f64() * 1000.0,
        )?;
        write!(
            f,
            "  response: min={}B  avg={}B  max={}B  total={:.1}MB",
            self.size_min,
            self.size_avg,
            self.size_max,
            self.total_bytes as f64 / 1_048_576.0,
        )
    }
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

#[tokio::main]
async fn main() {
    let opts = Opts::parse();
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Duration::from_secs(opts.duration);
    let start = Instant::now();

    let mode = if opts.max_jitter > 0.0 {
        format!("realistic (jitter 0–{:.1}s)", opts.max_jitter)
    } else {
        "hard hammer".to_string()
    };

    println!("╭─────────────────────────────────────────╮");
    println!("│          lustrefs-exporter soak          │");
    println!("╰─────────────────────────────────────────╯");
    println!();
    println!("  target:   {}", opts.url);
    println!("  threads:  {}", opts.threads);
    println!("  duration: {}", format_duration(deadline));
    println!("  mode:     {mode}");
    println!();

    // Spawn worker tasks
    let mut handles = Vec::with_capacity(opts.threads);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(opts.threads)
        .build()
        .expect("Failed to build HTTP client");

    for id in 0..opts.threads {
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        let url = opts.url.clone();
        let max_jitter = opts.max_jitter;
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();

            while !stop.load(Ordering::Relaxed) {
                let req_start = Instant::now();
                let result = client.get(&url).send().await;

                let (ok, bytes) = match result {
                    Ok(resp) => {
                        let status_ok = resp.status().is_success();
                        let body = resp.bytes().await.unwrap_or_default();
                        (status_ok, body.len())
                    }
                    Err(e) => {
                        if !stop.load(Ordering::Relaxed) {
                            eprintln!("[thread {id}] error: {e}");
                        }
                        (false, 0)
                    }
                };

                let latency = req_start.elapsed();
                stats.record(Sample { latency, bytes, ok }).await;

                // Jitter delay (0 = no delay, hard hammer)
                if max_jitter > 0.0 {
                    let jitter_secs = rng.gen_range(0.0..max_jitter);
                    tokio::time::sleep(Duration::from_secs_f64(jitter_secs)).await;
                }
            }
        }));
    }

    // Periodic reporting
    let stats_reporter = Arc::clone(&stats);
    let stop_reporter = Arc::clone(&stop);
    let reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await; // skip immediate tick
        let mut last_total = 0u64;
        let mut last_time = Instant::now();

        while !stop_reporter.load(Ordering::Relaxed) {
            interval.tick().await;
            if stop_reporter.load(Ordering::Relaxed) {
                break;
            }
            let snap = stats_reporter.snapshot().await;
            let elapsed = start.elapsed();
            let delta_reqs = snap.total - last_total;
            let delta_secs = last_time.elapsed().as_secs_f64();
            let rps = if delta_secs > 0.0 {
                delta_reqs as f64 / delta_secs
            } else {
                0.0
            };

            println!(
                "── {: >6} ── {:.0} req/s ── p50={:.1}ms ── p99={:.1}ms ── {} reqs ({} err) ──",
                format_duration(elapsed),
                rps,
                snap.latency_p50.as_secs_f64() * 1000.0,
                snap.latency_p99.as_secs_f64() * 1000.0,
                snap.total,
                snap.errors,
            );

            last_total = snap.total;
            last_time = Instant::now();
        }
    });

    // Wait for duration
    tokio::time::sleep(deadline).await;
    stop.store(true, Ordering::Relaxed);

    // Wait for workers to finish
    for h in handles {
        let _ = h.await;
    }
    reporter.abort();

    // Final report
    let snap = stats.snapshot().await;
    let elapsed = start.elapsed();
    let rps = snap.total as f64 / elapsed.as_secs_f64();

    println!();
    println!("╭─────────────────────────────────────────╮");
    println!("│              Final Results               │");
    println!("╰─────────────────────────────────────────╯");
    println!();
    println!("  elapsed:  {}", format_duration(elapsed));
    println!("  rps:      {rps:.1}");
    println!("{snap}");
    println!();
}
