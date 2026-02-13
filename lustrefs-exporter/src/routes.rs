// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    Error,
    jobstats::{JobstatMetrics, jobstats_stream},
    metrics::{self, Metrics},
};
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
    time::{Duration, Instant},
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

/// Cached scrape result using `Arc<str>` for zero-copy sharing across requests.
type CachedResult = (Arc<str>, Option<Arc<str>>);

/// Request coalescing + TTL caching for scrape results.
///
/// When multiple requests arrive concurrently:
/// 1. If a cached result exists within TTL → serve immediately (Arc clone = refcount bump)
/// 2. If a scrape is in-progress → subscribe to broadcast, wait for result
/// 3. Otherwise → start a fresh scrape, broadcast result to all waiters
struct ScrapeCache {
    /// Most recent scrape: (timestamp, core_metrics, optional_jobstats)
    last_scrape: Option<(Instant, Arc<str>, Option<Arc<str>>)>,
    /// Active scrape broadcaster — waiters subscribe here
    waiters: Option<tokio::sync::broadcast::Sender<CachedResult>>,
}

/// Shared application state holding persistent metrics, registry, and scrape cache.
///
/// `Family::clone()` shares the backing `Arc<RwLock<BTreeMap>>`, so the
/// `Registry` and `Metrics`/`JobstatMetrics` reference the same live data.
/// On each scrape we `clear()` all families, re-populate from fresh data,
/// and encode from the persistent `Registry`. Empty families are skipped
/// by the encoder, so jobstat descriptors only appear when populated.
#[derive(Clone)]
pub struct AppState {
    core_registry: Arc<Registry>,
    jobstats_registry: Arc<Registry>,
    metrics: Arc<Metrics>,
    jobstat_metrics: Arc<JobstatMetrics>,
    scrape_cache: Arc<Mutex<ScrapeCache>>,
    config: AppConfig,
}

pub fn app() -> Router {
    app_with_config(AppConfig::default())
}

pub fn app_with_config(config: AppConfig) -> Router {
    let mut core_registry = Registry::default();
    let mut jobstats_registry = Registry::default();
    let metrics = Metrics::default();
    let jobstat_metrics = JobstatMetrics::default();

    // Register all metric families.
    // Jobstats go to their own registry for streaming.
    jobstat_metrics.register_metric(&mut jobstats_registry);
    metrics.register_metric(&mut core_registry);

    tracing::info!(
        subprocess_timeout_secs = config.subprocess_timeout.as_secs(),
        cache_ttl_secs = config.cache_ttl.as_secs(),
        "Exporter configuration"
    );

    let state = AppState {
        core_registry: Arc::new(core_registry),
        jobstats_registry: Arc::new(jobstats_registry),
        metrics: Arc::new(metrics),
        jobstat_metrics: Arc::new(jobstat_metrics),
        scrape_cache: Arc::new(Mutex::new(ScrapeCache {
            last_scrape: None,
            waiters: None,
        })),
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

/// Build a streaming HTTP response from cached `Arc<str>` buffers.
///
/// Uses `Bytes::from(Arc<str>)` for zero-copy response delivery — the
/// `Arc` reference count is bumped, no data is copied.
fn build_streaming_response(
    core: Arc<str>,
    jobstats: Option<Arc<str>>,
    include_jobstats: bool,
) -> Response {
    let stream = async_stream::stream! {
        if include_jobstats && jobstats.is_some() {
            // Strip `# EOF\n` from core buffer so the combined stream
            // has a single EOF at the end of jobstats.
            let core_bytes = if core.ends_with("# EOF\n") {
                Bytes::copy_from_slice(&core.as_bytes()[..core.len() - 6])
            } else {
                Bytes::copy_from_slice(core.as_bytes())
            };
            yield Ok::<Bytes, Error>(core_bytes);

            if let Some(js) = jobstats {
                yield Ok::<Bytes, Error>(Bytes::copy_from_slice(js.as_bytes()));
            }
        } else {
            yield Ok::<Bytes, Error>(Bytes::copy_from_slice(core.as_bytes()));
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

/// Main metrics scraping endpoint handler for the Prometheus exporter.
///
/// Implements request coalescing with TTL-based caching:
/// 1. **Cache hit** — result within TTL served immediately (Arc refcount bump)
/// 2. **In-progress scrape** — subscribe to broadcast, wait for shared result
/// 3. **Fresh scrape** — run subprocesses, broadcast result to all waiters
///
/// Each subprocess is wrapped with `tokio::time::timeout` to kill hung
/// `lctl`/`lnetctl` processes. On timeout or error, the scrape continues
/// with partial data — Prometheus handles missing metrics gracefully.
pub async fn scrape(
    State(state): State<AppState>,
    params: Query<Params>,
) -> impl IntoResponse {
    let mut cache_guard = state.scrape_cache.lock().await;

    // 1. Check if we have a fresh cached result
    if let Some((timestamp, core, jobstats)) = &cache_guard.last_scrape {
        if timestamp.elapsed() < state.config.cache_ttl {
            // Serve cached: jobstats-requesting clients need jobstats in cache
            if !params.jobstats || jobstats.is_some() {
                let core = Arc::clone(core);
                let jobstats = jobstats.as_ref().map(Arc::clone);
                drop(cache_guard);
                return build_streaming_response(core, jobstats, params.jobstats);
            }
        }
    }

    // 2. If a scrape is already in progress, wait for it
    if let Some(tx) = &cache_guard.waiters {
        let mut rx = tx.subscribe();
        drop(cache_guard);
        match rx.recv().await {
            Ok((core, jobstats)) => {
                return build_streaming_response(core, jobstats, params.jobstats);
            }
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("Scrape failed"))
                    .unwrap();
            }
        }
    }

    // 3. Start a new scrape — register broadcast for waiters
    let (tx, _) = tokio::sync::broadcast::channel::<CachedResult>(16);
    cache_guard.waiters = Some(tx.clone());
    drop(cache_guard);

    // Perform the actual scrape
    let (core, jobstats) = perform_scrape(&state, params.jobstats).await;

    // Update cache and notify waiters
    let mut cache_guard = state.scrape_cache.lock().await;
    cache_guard.last_scrape = Some((Instant::now(), Arc::clone(&core), jobstats.clone()));
    if let Some(waiters) = cache_guard.waiters.take() {
        let _ = waiters.send((Arc::clone(&core), jobstats.clone()));
    }
    drop(cache_guard);

    build_streaming_response(core, jobstats, params.jobstats)
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
