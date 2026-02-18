// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    Error,
    jobstats::{JobstatMetrics, jobstats_stream},
    metrics::{self, Metrics},
    subprocess_pool::{SubprocessPool, ExporterStats},
};
use std::sync::atomic::Ordering;
use axum::{
    BoxError, Router,
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{Query, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use lustre_collector::{parse_lctl_output, parse_lnetctl_output, parse_lnetctl_stats, parser};
use prometheus_client::{encoding::text::encode, registry::Registry};
use serde::Deserialize;
use std::{
    borrow::Cow,
    io,
    sync::Arc,
    time::Duration,
};
use tokio::{process::Command, sync::Mutex};
use tower::{
    ServiceBuilder, limit::GlobalConcurrencyLimitLayer, load_shed::LoadShedLayer,
    timeout::TimeoutLayer,
};
use tower_http::compression::CompressionLayer;

#[derive(Debug, Deserialize)]
pub struct Params {
    // Only enable jobstats if "jobstats=true"
    #[serde(default)]
    jobstats: bool,
}

const TIMEOUT_DURATION_SECS: u64 = 120;
const DEFAULT_SUBPROCESS_TIMEOUT_SECS: u64 = 30;
const DEFAULT_CACHE_TTL_SECS: u64 = 1;

/// Configuration passed from CLI to the application.
#[derive(Clone, Debug)]
pub struct AppConfig {
    pub subprocess_timeout: Duration,
    pub cache_ttl: Duration,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            subprocess_timeout: Duration::from_secs(DEFAULT_SUBPROCESS_TIMEOUT_SECS),
            cache_ttl: Duration::from_secs(DEFAULT_CACHE_TTL_SECS),
        }
    }
}

/// The current state of an in-progress or cached scrape.
#[derive(Debug, Clone)]
pub enum ScrapeState {
    /// Scrape just started, no data yet.
    Started,
    /// Core metrics (lctl core + lnetctl) are ready.
    CoreReady(Arc<str>),
    /// Full scrape (including jobstats) is complete.
    Complete {
        core: Arc<str>,
        jobstats: Option<Arc<str>>,
    },
    /// Scrape failed.
    Failed,
}

/// A snapshot of the scrape progress.
#[derive(Debug, Clone)]
pub struct ScrapeSnapshot {
    pub state: ScrapeState,
}

/// Type used for coalescing concurrent requests.

/// FIFO backpressure coordinator.
///
/// When all pool workers are busy, incoming requests coalesce onto the
/// **most recently started** scrape via its watch channel.
struct ScrapeCoordinator {
    /// The latest active or recently completed scrape.
    latest_watch: Option<tokio::sync::watch::Receiver<ScrapeSnapshot>>,
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    /// Persistent REPL subprocess pool. `None` in test mode (falls back to
    /// per-request subprocess spawning).
    pool: Option<Arc<SubprocessPool>>,
    /// Per-request fallback: shared metrics/registry for subprocess mode.
    core_registry: Arc<Registry>,
    jobstats_registry: Arc<Registry>,
    metrics: Arc<Metrics>,
    jobstat_metrics: Arc<JobstatMetrics>,
    /// Backpressure coordinator for pool mode.
    coordinator: Arc<Mutex<ScrapeCoordinator>>,
    /// Exporter stats (shared between pool and legacy mode).
    pool_stats: Arc<ExporterStats>,
    config: AppConfig,
}

/// Create an app without a pool (subprocess-per-request, used by tests).
pub fn app() -> Router {
    app_with_config(AppConfig::default())
}

/// Create an app without a pool (subprocess-per-request mode).
pub fn app_with_config(config: AppConfig) -> Router {
    let mut core_registry = Registry::default();
    let mut jobstats_registry = Registry::default();
    let metrics = Metrics::default();
    let jobstat_metrics = JobstatMetrics::default();

    jobstat_metrics.register_metric(&mut jobstats_registry);
    metrics.register_metric(&mut core_registry);

    tracing::info!(
        subprocess_timeout_secs = config.subprocess_timeout.as_secs(),
        cache_ttl_secs = config.cache_ttl.as_secs(),
        "Exporter configuration (subprocess-per-request mode)"
    );

    let state = AppState {
        pool: None,
        core_registry: Arc::new(core_registry),
        jobstats_registry: Arc::new(jobstats_registry),
        metrics: Arc::new(metrics),
        jobstat_metrics: Arc::new(jobstat_metrics),
        coordinator: Arc::new(Mutex::new(ScrapeCoordinator {
            latest_watch: None,
        })),
        pool_stats: Arc::new(ExporterStats::new()),
        config,
    };

    let load_shedder = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_error))
        .layer(LoadShedLayer::new())
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(
            TIMEOUT_DURATION_SECS,
        )))
        .layer(GlobalConcurrencyLimitLayer::new(10))
        .layer(CompressionLayer::new());

    Router::new()
        .route("/metrics", get(scrape))
        .layer(load_shedder)
        .with_state(state)
}

/// Create an app with a persistent REPL subprocess pool (production mode).
///
/// Pool workers self-regulate via try-acquire. Excess requests coalesce
/// onto the most recently started scrape — no load shedding, no 503s.
pub fn app_with_pool(pool: Arc<SubprocessPool>, config: AppConfig) -> Router {
    // These are only used as fallback; pool mode creates per-scrape instances.
    let core_registry = Registry::default();
    let jobstats_registry = Registry::default();
    let metrics = Metrics::default();
    let jobstat_metrics = JobstatMetrics::default();

    tracing::info!(
        cache_ttl_secs = config.cache_ttl.as_secs(),
        "Exporter configuration (REPL pool mode)"
    );

    let state = AppState {
        pool: Some(pool.clone()),
        core_registry: Arc::new(core_registry),
        jobstats_registry: Arc::new(jobstats_registry),
        metrics: Arc::new(metrics),
        jobstat_metrics: Arc::new(jobstat_metrics),
        coordinator: Arc::new(Mutex::new(ScrapeCoordinator {
            latest_watch: None,
        })),
        pool_stats: Arc::clone(&pool.stats),
        config,
    };

    // Pool mode: no load shedder or concurrency limit — the pool self-regulates.
    // Keep compression and a generous timeout.
    let middleware = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_error))
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(
            TIMEOUT_DURATION_SECS,
        )))
        .layer(CompressionLayer::new());

    Router::new()
        .route("/metrics", get(scrape))
        .layer(middleware)
        .with_state(state)
}

pub async fn handle_error(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        return (StatusCode::REQUEST_TIMEOUT, Cow::from("request timed out"));
    }

    if error.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::from("service is overloaded, try again later"),
        );
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("Unhandled internal error: {error}")),
    )
}

pub fn jobstats_metrics_cmd() -> std::process::Command {
    let mut cmd = std::process::Command::new("lctl");

    cmd.arg("get_param")
        .args(["obdfilter.*OST*.job_stats", "mdt.*.job_stats"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    cmd
}

pub fn lustre_metrics_output() -> Command {
    let mut cmd = Command::new("lctl");

    cmd.arg("get_param")
        .args(parser::params())
        .kill_on_drop(true);

    cmd
}

pub fn net_show_output() -> Command {
    let mut cmd = Command::new("lnetctl");

    cmd.args(["net", "show", "-v", "4"]).kill_on_drop(true);

    cmd
}

pub fn lnet_stats_output() -> Command {
    let mut cmd = Command::new("lnetctl");

    cmd.args(["stats", "show"]).kill_on_drop(true);

    cmd
}


/// Perform a fresh scrape by running subprocesses and encoding metrics.
///
/// Returns `(core_metrics, optional_jobstats)` as `Arc<str>` for zero-copy caching.
async fn perform_scrape(
    state: &AppState,
    include_jobstats: bool,
) -> (Arc<str>, Option<Arc<str>>) {
    let subprocess_timeout = state.config.subprocess_timeout;

    // 1. Start Jobstats (if enabled) in background immediately
    let jobstats_future = if include_jobstats {
        let jobstat_metrics = state.jobstat_metrics.clone();
        let jobstats_registry = state.jobstats_registry.clone();

        Some(tokio::spawn(async move {
            let mut buffer = String::new();
            match tokio::time::timeout(subprocess_timeout, async {
                let child = tokio::task::spawn_blocking(move || {
                    let child = jobstats_metrics_cmd().spawn()?;
                    Ok::<_, Error>(child)
                })
                .await??;

                let mut child = child;
                let reader = io::BufReader::with_capacity(
                    128 * 1_024,
                    child.stdout.take().ok_or(io::Error::new(
                        io::ErrorKind::NotFound,
                        "stdout missing for lctl jobstats call.",
                    ))?,
                );

                let reader_stderr = io::BufReader::new(child.stderr.take().ok_or(
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "stderr missing for lctl jobstats call.",
                    ),
                )?);

                tokio::task::spawn(async move {
                    use std::io::BufRead;
                    for line in reader_stderr.lines().map_while(Result::ok) {
                        tracing::debug!("stderr: {line}");
                    }
                });

                tokio::task::spawn_blocking(move || {
                    if let Err(e) = child.wait() {
                        tracing::debug!("Unexpected error when waiting for child: {e}");
                    }
                });

                let jobstat_clone = (*jobstat_metrics).clone();
                let handle = jobstats_stream(reader, jobstat_clone);
                handle.await?;

                encode(&mut buffer, &jobstats_registry)?;

                Ok::<_, Error>(())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("lctl jobstats failed: {e}"),
                Err(_) => tracing::warn!("lctl jobstats timed out"),
            };
            buffer
        }))
    } else {
        None
    };

    // 2. Run Core Metrics (lctl, lnetctl) in parallel
    let run_lustre_metrics = async {
        match tokio::time::timeout(subprocess_timeout, lustre_metrics_output().output()).await {
            Ok(Ok(output)) => match parse_lctl_output(&output.stdout) {
                Ok(records) => Ok::<_, Error>(records),
                Err(e) => {
                    tracing::warn!("Failed to parse lctl output: {e}");
                    Ok(vec![])
                }
            },
            Ok(Err(e)) => {
                tracing::warn!("lctl get_param failed: {e}");
                Ok(vec![])
            }
            Err(_) => {
                tracing::warn!("lctl get_param timed out");
                Ok(vec![])
            }
        }
    };

    let run_net_show = async {
        match tokio::time::timeout(subprocess_timeout, net_show_output().output()).await {
            Ok(Ok(output)) => match parse_lnetctl_output(&output.stdout) {
                Ok(records) => Ok::<_, Error>(records),
                Err(e) => {
                    tracing::warn!("Failed to parse lnetctl net show output: {e}");
                    Ok(vec![])
                }
            },
            Ok(Err(e)) => {
                tracing::warn!("lnetctl net show failed: {e}");
                Ok(vec![])
            }
            Err(_) => {
                tracing::warn!("lnetctl net show timed out");
                Ok(vec![])
            }
        }
    };

    let run_lnet_stats = async {
        match tokio::time::timeout(subprocess_timeout, lnet_stats_output().output()).await {
            Ok(Ok(output)) => match parse_lnetctl_stats(&output.stdout) {
                Ok(records) => Ok::<_, Error>(records),
                Err(e) => {
                    tracing::warn!("Failed to parse lnetctl stats output: {e}");
                    Ok(vec![])
                }
            },
            Ok(Err(e)) => {
                tracing::warn!("lnetctl stats show failed: {e}");
                Ok(vec![])
            }
            Err(_) => {
                tracing::warn!("lnetctl stats show timed out");
                Ok(vec![])
            }
        }
    };

    // Execute core metrics in parallel
    let (lustre_records, net_records, stats_records) =
        tokio::join!(run_lustre_metrics, run_net_show, run_lnet_stats);

    // Aggregate and encode core metrics
    let mut output = vec![];
    if let Ok(mut records) = lustre_records {
        output.append(&mut records);
    }
    if let Ok(mut records) = net_records {
        output.append(&mut records);
    }
    if let Ok(mut records) = stats_records {
        output.append(&mut records);
    }

    metrics::build_lustre_stats(&output, &state.metrics);

    let mut core_buffer = String::new();
    if let Err(e) = encode(&mut core_buffer, &state.core_registry) {
        tracing::warn!("Failed to encode core metrics: {e}");
    }

    state.metrics.clear();

    // 3. Await Jobstats
    let jobstats_buffer = if let Some(fut) = jobstats_future {
        let result = fut.await.unwrap_or_default();
        state.jobstat_metrics.clear();
        if result.is_empty() {
            None
        } else {
            Some(Arc::from(result.as_str()))
        }
    } else {
        None
    };

    let core_arc: Arc<str> = Arc::from(core_buffer.as_str());

    (core_arc, jobstats_buffer)
}

// ---------------------------------------------------------------------------
// Pooled scrape — uses persistent REPL workers, streams core before jobstats
// ---------------------------------------------------------------------------

/// Stream a scrape from a watch receiver.
///
/// This handles both primary and coalesced requests by following the
/// `ScrapeState` machine:
/// 1. Wait for `CoreReady` -> Yield core metrics immediately.
/// 2. Wait for `Complete` -> Yield jobstats (if requested).
fn build_streaming_response_from_watch(
    mut rx: tokio::sync::watch::Receiver<ScrapeSnapshot>,
    include_jobstats: bool,
) -> Response {
    let stream = async_stream::stream! {
        // Phase 1: Wait for Core
        let core_data;
        loop {
            let snap = rx.borrow().clone();
            match snap.state {
                ScrapeState::CoreReady(core) => {
                    core_data = Some(core);
                    break;
                }
                ScrapeState::Complete { core, .. } => {
                    core_data = Some(core);
                    break;
                }
                ScrapeState::Failed => {
                    yield Err(Error::Other("Scrape failed during core phase".into()));
                    return;
                }
                ScrapeState::Started => {
                    if rx.changed().await.is_err() {
                        yield Err(Error::Other("Scrape task died".into()));
                        return;
                    }
                }
            }
        }

        let core = core_data.unwrap();
        let core_bytes = if include_jobstats && core.ends_with("# EOF\n") {
            // Strip EOF if we're expecting jobstats to follow
            Bytes::copy_from_slice(&core.as_bytes()[..core.len() - 6])
        } else {
            Bytes::copy_from_slice(core.as_bytes())
        };
        yield Ok::<Bytes, Error>(core_bytes);

        if !include_jobstats {
            return;
        }

        // Phase 2: Wait for Jobstats (if requested)
        loop {
            let snap = rx.borrow().clone();
            match snap.state {
                ScrapeState::Complete { jobstats, .. } => {
                    if let Some(js) = jobstats {
                        yield Ok::<Bytes, Error>(Bytes::copy_from_slice(js.as_bytes()));
                    } else {
                        // Success but no jobstats (or empty) - still need local EOF
                        yield Ok::<Bytes, Error>(Bytes::from_static(b"# EOF\n"));
                    }
                    return;
                }
                ScrapeState::CoreReady(_) | ScrapeState::Started => {
                    if rx.changed().await.is_err() {
                        // Scrape died or was replaced
                        yield Ok::<Bytes, Error>(Bytes::from_static(b"# EOF\n"));
                        return;
                    }
                }
                ScrapeState::Failed => {
                    yield Ok::<Bytes, Error>(Bytes::from_static(b"# EOF\n"));
                    return;
                }
            }
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from_stream(stream))
        .unwrap()
}


/// Encode raw lctl/lnetctl output into core metrics (OpenMetrics text).
fn encode_core(
    core_output: io::Result<Vec<u8>>,
    net_output: io::Result<Vec<u8>>,
    stats_output: io::Result<Vec<u8>>,
) -> Arc<str> {
    let mut core_registry = Registry::default();
    let core_metrics = Metrics::default();
    core_metrics.register_metric(&mut core_registry);

    let mut output = vec![];

    match core_output {
        Ok(data) => match parse_lctl_output(&data) {
            Ok(records) => output.extend(records),
            Err(e) => tracing::warn!("Failed to parse lctl core output: {e}"),
        },
        Err(e) => tracing::warn!("lctl core query failed: {e}"),
    }

    match net_output {
        Ok(data) => match parse_lnetctl_output(&data) {
            Ok(records) => output.extend(records),
            Err(e) => tracing::warn!("Failed to parse lnetctl net show output: {e}"),
        },
        Err(e) => tracing::warn!("lnetctl net show query failed: {e}"),
    }

    match stats_output {
        Ok(data) => match parse_lnetctl_stats(&data) {
            Ok(records) => output.extend(records),
            Err(e) => tracing::warn!("Failed to parse lnetctl stats show output: {e}"),
        },
        Err(e) => tracing::warn!("lnetctl stats show query failed: {e}"),
    }

    metrics::build_lustre_stats(&output, &core_metrics);

    let mut core_buffer = String::new();
    if let Err(e) = encode(&mut core_buffer, &core_registry) {
        tracing::warn!("Failed to encode core metrics: {e}");
    }

    Arc::from(core_buffer.as_str())
}

/// Encode raw jobstats data into OpenMetrics text.
async fn encode_jobstats_data(data: Vec<u8>) -> Option<Arc<str>> {
    let mut jobstats_registry = Registry::default();
    let jobstat_metrics = JobstatMetrics::default();
    jobstat_metrics.register_metric(&mut jobstats_registry);

    let cursor = std::io::Cursor::new(data);
    let reader = std::io::BufReader::new(cursor);
    let handle = jobstats_stream(reader, jobstat_metrics);
    let _returned_metrics = handle.await.unwrap_or_default();

    let mut js_buffer = String::new();
    if let Err(e) = encode(&mut js_buffer, &jobstats_registry) {
        tracing::warn!("Failed to encode jobstats: {e}");
    }

    if js_buffer.is_empty() {
        None
    } else {
        Some(Arc::from(js_buffer.as_str()))
    }
}

// ---------------------------------------------------------------------------
// Scrape endpoint — pool mode with coalesce, or legacy subprocess mode
// ---------------------------------------------------------------------------

/// Main metrics scraping endpoint handler.
///
/// **Pool mode** (production): try to acquire workers for a fresh scrape.
/// If all workers are busy, coalesce onto the most recently started scrape.
///
/// **Subprocess mode** (tests/fallback): single scrape with coalescing,
/// spawning fresh `lctl`/`lnetctl` processes per request.
pub async fn scrape(
    State(state): State<AppState>,
    params: Query<Params>,
) -> impl IntoResponse {
    if let Some(ref pool) = state.pool {
        // ── Pool mode ──────────────────────────────────────────────
        scrape_pooled(&state, pool, params.jobstats).await
    } else {
        // ── Legacy subprocess mode (tests) ─────────────────────────
        scrape_subprocess(&state, params.jobstats).await
    }
}

/// Pool-mode scrape: try-acquire workers or coalesce onto latest active scrape.
///
/// **Streaming Coalescing Architecture**:
/// 1. If workers are available, start a new `watch`-based scrape.
/// 2. If workers are busy, join the `latest_watch` from the coordinator.
/// 3. Both paths use `build_streaming_response_from_watch` which ensures
///    CORE metrics are streamed as soon as any worker pair produces them.
async fn scrape_pooled(
    state: &AppState,
    pool: &SubprocessPool,
    include_jobstats: bool,
) -> Response {
    // 1. Try to join an existing active scrape first
    {
        let coord = state.coordinator.lock().await;
        if let Some(rx) = &coord.latest_watch {
            let state_to_join = rx.borrow().state.clone();
            match state_to_join {
                ScrapeState::Started | ScrapeState::CoreReady(_) | ScrapeState::Complete { .. } => {
                    let rx_clone = rx.clone();
                    pool.stats.coalesced_requests.fetch_add(1, Ordering::Relaxed);
                    drop(coord);
                    return build_streaming_response_from_watch(rx_clone, include_jobstats);
                }
                ScrapeState::Failed => { /* Try to start a new one instead */ }
            }
        }
    }

    // 2. Try to acquire a worker pair for a new scrape
    if let Some((mut lctl, mut lnetctl)) = pool.try_acquire_pair() {
        let (tx, rx) = tokio::sync::watch::channel(ScrapeSnapshot {
            state: ScrapeState::Started,
        });

        // Register this as the latest active scrape
        {
            let mut coord = state.coordinator.lock().await;
            coord.latest_watch = Some(rx.clone());
        }

        pool.stats
            .total_scrapes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let core_params = parser::params();
        let lctl_core_cmd = format!("get_param {}", core_params.join(" "));

        // Driver Task: Runs the scrape and updates the watch channel
        tokio::spawn(async move {
            // Phase 1: Core (lctl core + lnetctl)
            let lctl_core_fut = lctl.query(&lctl_core_cmd);
            let lnetctl_fut = async {
                let net = lnetctl.query("net show -v 4").await;
                let stats = lnetctl.query("stats show").await;
                (net, stats)
            };

            let (core_result, (net_result, stats_result)) = tokio::join!(lctl_core_fut, lnetctl_fut);
            drop(lnetctl); // Free lnetctl worker immediately

            let core_arc = encode_core(core_result, net_result, stats_result);

            if !include_jobstats {
                // Done.
                let _ = tx.send(ScrapeSnapshot {
                    state: ScrapeState::Complete {
                        core: core_arc,
                        jobstats: None,
                    },
                });
                return;
            }

            // Signal that Core is ready so all waiters can start streaming
            let _ = tx.send(ScrapeSnapshot {
                state: ScrapeState::CoreReady(Arc::clone(&core_arc)),
            });

            // Phase 2: Jobstats
            let jobstats_result = lctl
                .query("get_param obdfilter.*OST*.job_stats mdt.*.job_stats")
                .await;
            drop(lctl); // Free lctl worker

            let jobstats_arc = match jobstats_result {
                Ok(data) => encode_jobstats_data(data).await,
                Err(e) => {
                    tracing::warn!("lctl jobstats query failed: {e}");
                    None
                }
            };

            let _ = tx.send(ScrapeSnapshot {
                state: ScrapeState::Complete {
                    core: core_arc,
                    jobstats: jobstats_arc,
                },
            });
        });

        return build_streaming_response_from_watch(rx, include_jobstats);
    }

    // 3. Fallback: All workers busy, but no active scrape found (rare race)
    // Coalesce onto whatever is in the coordinator even if it's Finished.
    let rx = {
        let coord = state.coordinator.lock().await;
        coord.latest_watch.clone()
    };

    if let Some(rx) = rx {
        build_streaming_response_from_watch(rx, include_jobstats)
    } else {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from("Service Unavailable: Workers busy and no active scrape"))
            .unwrap()
    }
}

/// Legacy subprocess-mode scrape with coalescing (used by tests).
async fn scrape_subprocess(state: &AppState, include_jobstats: bool) -> Response {
    // 1. Try to join an existing active scrape
    {
        let coord = state.coordinator.lock().await;
        if let Some(rx) = &coord.latest_watch {
            let rx_clone = rx.clone();
            state.pool_stats.coalesced_requests.fetch_add(1, Ordering::Relaxed);
            drop(coord);
            return build_streaming_response_from_watch(rx_clone, include_jobstats);
        }
    }

    // 2. Start a new scrape
    let (tx, rx) = tokio::sync::watch::channel(ScrapeSnapshot {
        state: ScrapeState::Started,
    });

    {
        let mut coord = state.coordinator.lock().await;
        coord.latest_watch = Some(rx.clone());
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        let (core, jobstats) = perform_scrape(&state_clone, include_jobstats).await;
        
        // Signal completion
        let _ = tx.send(ScrapeSnapshot {
            state: ScrapeState::Complete {
                core,
                jobstats,
            },
        });

        // Clear coordinator
        {
            let mut coord = state_clone.coordinator.lock().await;
            coord.latest_watch = None;
        }
    });

    build_streaming_response_from_watch(rx, include_jobstats)
}

#[cfg(test)]
mod tests {
    use crate::routes::{
        jobstats_metrics_cmd, lnet_stats_output, lustre_metrics_output, net_show_output,
    };
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::Request,
    };
    use commandeer_test::commandeer;
    use serial_test::serial;
    use std::{
        io::{self, BufReader, Read},
    };
    use tokio::task::JoinSet;
    use tower::ServiceExt as _;

    /// Create a new Axum app with the provided state and a Request
    /// to scrape the metrics endpoint.
    fn get_app() -> (Request<Body>, Router) {
        let app = crate::routes::app();

        let request = Request::builder()
            .uri("/metrics?jobstats=true")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        (request, app)
    }

    #[commandeer(Replay, "lctl", "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_metrics_endpoint_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let (request, app) = get_app();

        let resp = app.oneshot(request).await.unwrap();

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let original_body_str = std::str::from_utf8(&body).unwrap();

        let (request, app) = get_app();

        let resp = app.oneshot(request).await.unwrap();

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();

        assert_eq!(original_body_str, body_str);

        insta::assert_snapshot!(original_body_str);

        Ok(())
    }

    #[commandeer(Replay, "lctl", "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_app_function() {
        let (request, app) = get_app();

        let response = app.oneshot(request).await.unwrap();

        assert!(response.status().is_success())
    }

    #[commandeer(Replay, "lctl", "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_app_routes() {
        let app = crate::routes::app();

        // Test that the /metrics route exists
        let request = Request::builder()
            .uri("/metrics")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert!(response.status().is_success())
    }

    #[commandeer(Replay, "lctl", "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_concurrent_requests() {
        let app = crate::routes::app();

        // Test that concurrency limiting works by sending multiple requests
        // This test verifies the load_shed layer is applied
        let mut handles = JoinSet::new();

        // Send 15 requests (more than the 10 limit)
        for _ in 0..15 {
            let app = app.clone();

            handles.spawn(async move {
                let request = Request::builder()
                    .uri("/metrics")
                    .method("GET")
                    .body(Body::empty())
                    .unwrap();

                app.oneshot(request).await
            });
        }

        // Wait for all requests to complete
        let result = handles
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        // Some requests should succeed or fail based on system state,
        // but none should panic
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_error() {
        use crate::routes::handle_error;
        use axum::{BoxError, http::StatusCode, response::IntoResponse};

        // Test timeout error
        let timeout_error = Box::new(tower::timeout::error::Elapsed::new()) as BoxError;
        let response = handle_error(timeout_error).await.into_response();

        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();

        assert_eq!(body_str, "request timed out");

        // Test overloaded error
        let overloaded_error = Box::new(tower::load_shed::error::Overloaded::new()) as BoxError;
        let response = handle_error(overloaded_error).await.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();

        assert_eq!(body_str, "service is overloaded, try again later");

        // Test generic/unhandled error
        let generic_error = Box::new(std::io::Error::other("some random error")) as BoxError;

        let response = handle_error(generic_error).await.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();

        assert!(body_str.starts_with("Unhandled internal error:"));
    }

    #[commandeer(Replay, "lctl")]
    #[test]
    #[serial]
    fn test_jobstats_metrics_cmd_with_mock() {
        let mut child = jobstats_metrics_cmd()
            .spawn()
            .expect("Failed to spawn child.");

        let mut reader = BufReader::with_capacity(
            128 * 1_024,
            child
                .stdout
                .take()
                .ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    "stdout missing for lctl jobstats call.",
                ))
                .unwrap(),
        );

        let mut buff = String::new();
        reader.read_to_string(&mut buff).unwrap();

        child.wait().expect("Failed to wait for child process");

        insta::assert_snapshot!(buff);
    }

    #[commandeer(Replay, "lctl")]
    #[tokio::test]
    #[serial]
    async fn test_lustre_metrics_output_with_mock() {
        let output = lustre_metrics_output().output().await.unwrap();

        insta::assert_snapshot!(String::from_utf8(output.stdout).unwrap());
    }

    #[commandeer(Replay, "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_net_show_output_with_mock() {
        let output = net_show_output().output().await.unwrap();

        insta::assert_snapshot!(String::from_utf8(output.stdout).unwrap());
    }

    #[commandeer(Replay, "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_lnet_stats_output_with_mock() {
        let output = lnet_stats_output().output().await.unwrap();

        insta::assert_snapshot!(String::from_utf8(output.stdout).unwrap());
    }

    #[commandeer(Replay, "lctl", "lnetctl")]
    #[tokio::test]
    #[serial]
    async fn test_jobstats_with_stderr_output() -> Result<(), Box<dyn std::error::Error>> {
        let (request, app) = get_app();

        let resp = app.oneshot(request).await.unwrap();

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let original_body_str = std::str::from_utf8(&body).unwrap();

        insta::assert_snapshot!(original_body_str);

        Ok(())
    }
}
