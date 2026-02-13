// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::Parser;
use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio::sync::{Mutex, mpsc};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(name = "lustrefs-loadtest")]
#[command(about = "HTTP load-testing and fixture generation tool for lustrefs-exporter")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Run an HTTP load-test (fixed request count, as fast as possible)
    Run {
        /// Target URL to benchmark (e.g. http://host:32221/metrics)
        url: String,

        /// Number of concurrent workers (tokio tasks)
        #[arg(short = 'c', long = "concurrency", default_value_t = 10)]
        concurrency: usize,

        /// Total number of requests to issue
        #[arg(short = 'n', long = "requests", default_value_t = 100)]
        requests: usize,

        /// Accept invalid TLS certificates (self-signed, expired, wrong host)
        #[arg(short = 'k', long = "insecure")]
        insecure: bool,

        /// Request timeout in seconds
        #[arg(long = "timeout", default_value_t = 30)]
        timeout: u64,
    },
    /// Duration-based soak test with optional jitter
    ///
    /// Without --max-jitter: hard hammer (fire as fast as possible).
    /// With --max-jitter 10: each thread waits a random 0–10s between requests,
    /// simulating realistic client behavior that stresses cache expiry.
    Soak {
        /// Target URL (e.g. http://host:32221/metrics)
        #[arg(default_value = "http://localhost:32221/metrics")]
        url: String,

        /// Number of concurrent client threads
        #[arg(short = 'c', long = "threads", default_value_t = 4)]
        threads: usize,

        /// Test duration in seconds
        #[arg(short = 'd', long = "duration", default_value_t = 60)]
        duration: u64,

        /// Max random jitter between requests (seconds, float).
        /// 0 = hard hammer (fire as fast as possible).
        #[arg(short = 'j', long = "max-jitter", default_value_t = 0.0)]
        max_jitter: f64,

        /// Accept invalid TLS certificates
        #[arg(short = 'k', long = "insecure")]
        insecure: bool,

        /// Request timeout in seconds
        #[arg(long = "timeout", default_value_t = 120)]
        timeout: u64,
    },
    /// Generate a massive synthetic Lustre procfs fixture for stress-testing
    Fixture {
        /// Directory where the fixture tree will be created
        #[arg(short = 'o', long = "output-dir")]
        output_dir: std::path::PathBuf,

        /// Number of jobs per target
        #[arg(short = 'j', long = "jobs", default_value_t = 1000)]
        num_jobs: usize,

        /// Number of OST targets
        #[arg(long = "osts", default_value_t = 2)]
        num_osts: usize,

        /// Number of MDT targets
        #[arg(long = "mdts", default_value_t = 1)]
        num_mdts: usize,
    },
}

// ---------------------------------------------------------------------------
// Per-request result (used by `run`)
// ---------------------------------------------------------------------------

struct RequestResult {
    ttfb: Duration,
    total: Duration,
    status: u16,
    is_error: bool,
}

// ---------------------------------------------------------------------------
// Load-test engine (fixed request count)
// ---------------------------------------------------------------------------

async fn run_loadtest(
    url: &str,
    concurrency: usize,
    requests: usize,
    insecure: bool,
    timeout: u64,
) -> Result<Vec<RequestResult>> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(timeout))
        .pool_max_idle_per_host(concurrency)
        .build()?;

    let client = Arc::new(client);
    let url: Arc<str> = Arc::from(url);

    let (tx, mut rx) = mpsc::channel::<RequestResult>(requests);

    let mut handles = Vec::with_capacity(concurrency);
    let base_per_worker = requests / concurrency;
    let remainder = requests % concurrency;

    for worker_id in 0..concurrency {
        // Spread the remainder across the first `remainder` workers.
        let count = base_per_worker + if worker_id < remainder { 1 } else { 0 };
        let client = Arc::clone(&client);
        let url = Arc::clone(&url);
        let tx = tx.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..count {
                let t0 = Instant::now();

                // send() returns when the response headers arrive → TTFB
                let response = match client.get(url.as_ref()).send().await {
                    Ok(resp) => resp,
                    Err(_e) => {
                        let total = t0.elapsed();
                        let _ = tx
                            .send(RequestResult {
                                ttfb: total,
                                total,
                                status: 0,
                                is_error: true,
                            })
                            .await;
                        continue;
                    }
                };

                let ttfb = t0.elapsed();
                let status = response.status().as_u16();

                // Consume the full body so we measure total transfer time.
                let body_ok = response.bytes().await.is_ok();
                let total = t0.elapsed();

                let is_error = !body_ok || !(200..300).contains(&status);

                let _ = tx
                    .send(RequestResult {
                        ttfb,
                        total,
                        status,
                        is_error,
                    })
                    .await;
            }
        }));
    }

    // Drop the original sender so the channel closes once all workers finish.
    drop(tx);

    let mut results = Vec::with_capacity(requests);
    while let Some(res) = rx.recv().await {
        results.push(res);
    }

    // Wait for all worker tasks to complete (should already be done).
    for handle in handles {
        handle.await?;
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Soak test engine (duration-based with optional jitter)
// ---------------------------------------------------------------------------

struct SoakSample {
    ttfb: Duration,
    total: Duration,
    bytes: usize,
    ok: bool,
}

struct SoakStats {
    samples: Mutex<Vec<SoakSample>>,
    total: AtomicU64,
    errors: AtomicU64,
    bytes: AtomicU64,
}

impl SoakStats {
    fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::with_capacity(100_000)),
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    async fn record(&self, sample: SoakSample) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(sample.bytes as u64, Ordering::Relaxed);
        if !sample.ok {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.samples.lock().await.push(sample);
    }

    async fn snapshot(&self) -> SoakSnapshot {
        let samples = self.samples.lock().await;
        let total = self.total.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let total_bytes = self.bytes.load(Ordering::Relaxed);

        if samples.is_empty() {
            return SoakSnapshot {
                total,
                errors,
                total_bytes,
                ttfb_min: Duration::ZERO,
                ttfb_p50: Duration::ZERO,
                ttfb_p95: Duration::ZERO,
                ttfb_p99: Duration::ZERO,
                ttfb_max: Duration::ZERO,
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

        let mut ttfbs: Vec<Duration> = samples.iter().map(|s| s.ttfb).collect();
        ttfbs.sort();

        let mut latencies: Vec<Duration> = samples.iter().map(|s| s.total).collect();
        latencies.sort();

        let sizes: Vec<usize> = samples.iter().map(|s| s.bytes).collect();
        let size_min = sizes.iter().copied().min().unwrap_or(0);
        let size_max = sizes.iter().copied().max().unwrap_or(0);
        let size_avg = if sizes.is_empty() {
            0
        } else {
            sizes.iter().sum::<usize>() / sizes.len()
        };

        let pct = |sorted: &[Duration], p: f64| -> Duration {
            let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
            sorted[idx]
        };

        SoakSnapshot {
            total,
            errors,
            total_bytes,
            ttfb_min: ttfbs[0],
            ttfb_p50: pct(&ttfbs, 0.50),
            ttfb_p95: pct(&ttfbs, 0.95),
            ttfb_p99: pct(&ttfbs, 0.99),
            ttfb_max: *ttfbs.last().unwrap_or(&Duration::ZERO),
            latency_min: latencies[0],
            latency_p50: pct(&latencies, 0.50),
            latency_p95: pct(&latencies, 0.95),
            latency_p99: pct(&latencies, 0.99),
            latency_max: *latencies.last().unwrap_or(&Duration::ZERO),
            size_min,
            size_avg,
            size_max,
        }
    }
}

struct SoakSnapshot {
    total: u64,
    errors: u64,
    total_bytes: u64,
    ttfb_min: Duration,
    ttfb_p50: Duration,
    ttfb_p95: Duration,
    ttfb_p99: Duration,
    ttfb_max: Duration,
    latency_min: Duration,
    latency_p50: Duration,
    latency_p95: Duration,
    latency_p99: Duration,
    latency_max: Duration,
    size_min: usize,
    size_avg: usize,
    size_max: usize,
}

async fn run_soak(
    url: &str,
    threads: usize,
    duration_secs: u64,
    max_jitter: f64,
    insecure: bool,
    timeout: u64,
) {
    let stats = Arc::new(SoakStats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Duration::from_secs(duration_secs);
    let start = Instant::now();

    let mode = if max_jitter > 0.0 {
        format!("realistic (jitter 0\u{2013}{:.1}s)", max_jitter)
    } else {
        "hard hammer".to_string()
    };

    println!("\u{256d}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256e}");
    println!("\u{2502}          lustrefs-exporter soak          \u{2502}");
    println!("\u{2570}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256f}");
    println!();
    println!("  target:   {url}");
    println!("  threads:  {threads}");
    println!("  duration: {}", format_duration(deadline));
    println!("  mode:     {mode}");
    println!();

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(timeout))
        .pool_max_idle_per_host(threads)
        .build()
        .expect("Failed to build HTTP client");

    // Spawn worker tasks
    let mut handles = Vec::with_capacity(threads);

    for id in 0..threads {
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        let url = url.to_string();
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();

            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let result = client.get(&url).send().await;

                let (ok, bytes, ttfb) = match result {
                    Ok(resp) => {
                        let ttfb = t0.elapsed();
                        let status_ok = resp.status().is_success();
                        let body = resp.bytes().await.unwrap_or_default();
                        (status_ok, body.len(), ttfb)
                    }
                    Err(e) => {
                        if !stop.load(Ordering::Relaxed) {
                            eprintln!("[thread {id}] error: {e}");
                        }
                        (false, 0, t0.elapsed())
                    }
                };

                let total = t0.elapsed();
                stats
                    .record(SoakSample {
                        ttfb,
                        total,
                        bytes,
                        ok,
                    })
                    .await;

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
                "\u{2500}\u{2500} {: >6} \u{2500}\u{2500} {:.0} req/s \u{2500}\u{2500} ttfb p50={:.1}ms p99={:.1}ms \u{2500}\u{2500} total p50={:.1}ms p99={:.1}ms \u{2500}\u{2500} {} reqs ({} err) \u{2500}\u{2500}",
                format_duration(elapsed),
                rps,
                snap.ttfb_p50.as_secs_f64() * 1000.0,
                snap.ttfb_p99.as_secs_f64() * 1000.0,
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
    println!("\u{256d}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256e}");
    println!("\u{2502}              Final Results               \u{2502}");
    println!("\u{2570}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256f}");
    println!();
    println!("  elapsed:  {}", format_duration(elapsed));
    println!("  rps:      {rps:.1}");
    println!(
        "  requests: {} total, {} errors ({:.1}%)",
        snap.total,
        snap.errors,
        if snap.total > 0 {
            snap.errors as f64 / snap.total as f64 * 100.0
        } else {
            0.0
        }
    );
    println!();
    println!("  --- TTFB (ms) ---");
    println!(
        "  min={:.1}  p50={:.1}  p95={:.1}  p99={:.1}  max={:.1}",
        snap.ttfb_min.as_secs_f64() * 1000.0,
        snap.ttfb_p50.as_secs_f64() * 1000.0,
        snap.ttfb_p95.as_secs_f64() * 1000.0,
        snap.ttfb_p99.as_secs_f64() * 1000.0,
        snap.ttfb_max.as_secs_f64() * 1000.0,
    );
    println!();
    println!("  --- Total Latency (ms) ---");
    println!(
        "  min={:.1}  p50={:.1}  p95={:.1}  p99={:.1}  max={:.1}",
        snap.latency_min.as_secs_f64() * 1000.0,
        snap.latency_p50.as_secs_f64() * 1000.0,
        snap.latency_p95.as_secs_f64() * 1000.0,
        snap.latency_p99.as_secs_f64() * 1000.0,
        snap.latency_max.as_secs_f64() * 1000.0,
    );
    println!();
    println!(
        "  response: min={}B  avg={}B  max={}B  total={:.1}MB",
        snap.size_min,
        snap.size_avg,
        snap.size_max,
        snap.total_bytes as f64 / 1_048_576.0,
    );
    println!();
}

// ---------------------------------------------------------------------------
// Fixture generation
// ---------------------------------------------------------------------------

fn generate_fixture(
    output_dir: &std::path::Path,
    num_jobs: usize,
    num_osts: usize,
    num_mdts: usize,
) -> Result<()> {
    use std::fs;
    use std::io::Write;

    println!("Generating fixture in {}...", output_dir.display());

    // Create OST fixtures
    for i in 0..num_osts {
        let ost_name = format!("ds{:03}-OST{:04}", i / 100, i % 100);
        let ost_dir = output_dir.join(format!("osd-ldiskfs/{}", ost_name));
        fs::create_dir_all(&ost_dir)?;

        let mut f = fs::File::create(ost_dir.join("job_stats"))?;
        writeln!(f, "obdfilter.{}.job_stats=", ost_name)?;
        writeln!(f, "job_stats:")?;

        for j in 0..num_jobs {
            let job_id = format!("job_{}", j);
            writeln!(f, "- job_id:          \"{}\"", job_id)?;
            writeln!(f, "  snapshot_time:   1720516680")?;
            writeln!(f, "  read_bytes:      {{ samples:          100, unit: bytes, min:     4096, max:   475136, sum:          5468160, sumsq:      1071040692224 }}")?;
            writeln!(f, "  write_bytes:     {{ samples:          100, unit: bytes, min:     4096, max:   475136, sum:          5468160, sumsq:      1071040692224 }}")?;
            writeln!(f, "  getattr:         {{ samples:           10, unit: usecs, min:       10, max:      100, sum:             1000, sumsq:            100000 }}")?;
        }
    }

    // Create MDT fixtures
    for i in 0..num_mdts {
        let mdt_name = format!("ds{:03}-MDT{:04}", i / 100, i % 100);
        let mdt_dir = output_dir.join(format!("mdt/{}", mdt_name));
        fs::create_dir_all(&mdt_dir)?;

        let mut f = fs::File::create(mdt_dir.join("job_stats"))?;
        writeln!(f, "mdt.{}.job_stats=", mdt_name)?;
        writeln!(f, "job_stats:")?;

        for j in 0..num_jobs {
            let job_id = format!("job_{}", j);
            writeln!(f, "- job_id:          \"{}\"", job_id)?;
            writeln!(f, "  snapshot_time:   1720516680")?;
            writeln!(f, "  open:            {{ samples:          100, unit: usecs, min:       10, max:      100, sum:             1000, sumsq:            100000 }}")?;
            writeln!(f, "  close:           {{ samples:          100, unit: usecs, min:       10, max:      100, sum:             1000, sumsq:            100000 }}")?;
        }
    }

    println!("Fixture generation complete.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Statistics & Reporting (for `run` subcommand)
// ---------------------------------------------------------------------------

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64) * pct / 100.0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn print_report(results: &[RequestResult], wall_clock: Duration) {
    let total = results.len();
    let success_count = results.iter().filter(|r| !r.is_error).count();
    let fail_count = total - success_count;

    let rps = if wall_clock.as_secs_f64() > 0.0 {
        total as f64 / wall_clock.as_secs_f64()
    } else {
        0.0
    };

    // Collect latency values (in ms) for successful requests only.
    let mut ttfb_ms: Vec<f64> = results
        .iter()
        .filter(|r| !r.is_error)
        .map(|r| r.ttfb.as_secs_f64() * 1000.0)
        .collect();

    let mut total_ms: Vec<f64> = results
        .iter()
        .filter(|r| !r.is_error)
        .map(|r| r.total.as_secs_f64() * 1000.0)
        .collect();

    ttfb_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    total_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!("\n--- Results ---");
    println!("Wall Clock:        {wall_clock:.2?}");
    println!("Requests/sec:      {rps:.2}");
    println!();
    println!("Total Requests:    {total}");
    println!("  Successful:      {success_count}");
    println!("  Failed:          {fail_count}");

    // HTTP status code distribution
    let mut status_counts: HashMap<u16, usize> = HashMap::new();
    for r in results {
        *status_counts.entry(r.status).or_insert(0) += 1;
    }
    let mut statuses: Vec<_> = status_counts.into_iter().collect();
    statuses.sort_by_key(|&(code, _)| code);
    println!();
    println!("--- Status Codes ---");
    for (code, count) in &statuses {
        let label = if *code == 0 {
            "ERR".to_string()
        } else {
            code.to_string()
        };
        println!("  {label:<6} {count}");
    }

    if !ttfb_ms.is_empty() {
        let min = ttfb_ms[0];
        let max = ttfb_ms[ttfb_ms.len() - 1];
        let avg = ttfb_ms.iter().sum::<f64>() / ttfb_ms.len() as f64;

        println!();
        println!("--- TTFB (ms) ---");
        println!("  Min:    {min:.2}");
        println!("  Max:    {max:.2}");
        println!("  Avg:    {avg:.2}");
        println!("  p50:    {:.2}", percentile(&ttfb_ms, 50.0));
        println!("  p90:    {:.2}", percentile(&ttfb_ms, 90.0));
        println!("  p99:    {:.2}", percentile(&ttfb_ms, 99.0));
    }

    if !total_ms.is_empty() {
        let min = total_ms[0];
        let max = total_ms[total_ms.len() - 1];
        let avg = total_ms.iter().sum::<f64>() / total_ms.len() as f64;

        println!();
        println!("--- Total Time (ms) ---");
        println!("  Min:    {min:.2}");
        println!("  Max:    {max:.2}");
        println!("  Avg:    {avg:.2}");
        println!("  p50:    {:.2}", percentile(&total_ms, 50.0));
        println!("  p90:    {:.2}", percentile(&total_ms, 90.0));
        println!("  p99:    {:.2}", percentile(&total_ms, 99.0));
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

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            url,
            concurrency,
            requests,
            insecure,
            timeout,
        } => {
            if concurrency == 0 {
                bail!("--concurrency must be at least 1");
            }
            if requests == 0 {
                bail!("--requests must be at least 1");
            }

            println!(
                "Benchmarking {} with {} concurrent workers ({} total requests)...",
                url, concurrency, requests,
            );

            let start = Instant::now();
            let results = run_loadtest(&url, concurrency, requests, insecure, timeout).await?;
            let wall_clock = start.elapsed();

            print_report(&results, wall_clock);
        }
        Commands::Soak {
            url,
            threads,
            duration,
            max_jitter,
            insecure,
            timeout,
        } => {
            if threads == 0 {
                bail!("--threads must be at least 1");
            }

            run_soak(&url, threads, duration, max_jitter, insecure, timeout).await;
        }
        Commands::Fixture {
            output_dir,
            num_jobs,
            num_osts,
            num_mdts,
        } => {
            generate_fixture(&output_dir, num_jobs, num_osts, num_mdts)?;
        }
    }

    Ok(())
}
