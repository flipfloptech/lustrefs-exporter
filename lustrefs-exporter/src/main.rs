// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use clap::Parser;
use lustrefs_exporter::{
    Error, dump_stats,
    routes::{AppConfig, app_with_pool},
    subprocess_pool::{PoolConfig, SubprocessPool},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

const LUSTREFS_EXPORTER_PORT: &str = "32221";

#[derive(Debug, Parser)]
pub struct CommandOpts {
    /// Port that exporter will listen to
    #[clap(short, long, env = "LUSTREFS_EXPORTER_PORT", default_value = LUSTREFS_EXPORTER_PORT)]
    pub port: u16,

    /// Timeout in seconds for each subprocess (lctl, lnetctl). If a subprocess
    /// does not complete within this duration it is killed and the scrape
    /// continues with partial data.
    #[clap(long, env = "LUSTREFS_EXPORTER_SUBPROCESS_TIMEOUT", default_value = "30")]
    pub subprocess_timeout_secs: u64,

    /// Cache TTL in seconds. Concurrent requests within this window share
    /// a single scrape result instead of spawning new subprocesses.
    #[clap(long, env = "LUSTREFS_EXPORTER_CACHE_TTL", default_value = "1")]
    pub cache_ttl_secs: u64,

    /// Number of persistent REPL workers per command type (lctl, lnetctl).
    /// Total persistent processes = 2 × pool_size. Each scrape claims one
    /// worker from each pool. Excess requests coalesce onto the most
    /// recently started scrape.
    #[clap(long, env = "LUSTREFS_EXPORTER_POOL_SIZE", default_value = "2")]
    pub pool_size: usize,

    /// Watchdog timeout in seconds for REPL worker I/O. If a worker
    /// does not produce output within this duration, it is killed and
    /// a fresh replacement is spawned.
    #[clap(long, env = "LUSTREFS_EXPORTER_WATCHDOG_TIMEOUT", default_value = "10")]
    pub watchdog_timeout_secs: u64,

    /// Maximum RSS (in MB) for a REPL worker process. If a worker's
    /// memory exceeds this limit after a query, it is killed and
    /// respawned. Set to 0 to disable (useful for baselining).
    #[clap(long, env = "LUSTREFS_EXPORTER_MAX_WORKER_RSS", default_value = "0")]
    pub max_worker_rss_mb: u64,

    /// Dump stats as raw string and exit
    #[clap(long, hide = true)]
    dump: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let opts = CommandOpts::parse();

    if opts.dump {
        dump_stats().await?;
    } else {
        let addr = SocketAddr::from(([0, 0, 0, 0], opts.port));

        let config = AppConfig {
            subprocess_timeout: Duration::from_secs(opts.subprocess_timeout_secs),
            cache_ttl: Duration::from_secs(opts.cache_ttl_secs),
        };

        let pool_config = PoolConfig {
            pool_size: opts.pool_size,
            watchdog_timeout: Duration::from_secs(opts.watchdog_timeout_secs),
            max_worker_rss_bytes: opts.max_worker_rss_mb * 1024 * 1024,
        };

        tracing::info!(
            pool_size = pool_config.pool_size,
            watchdog_timeout_secs = pool_config.watchdog_timeout.as_secs(),
            max_worker_rss_mb = opts.max_worker_rss_mb,
            "Initializing REPL subprocess pool"
        );

        let pool = Arc::new(SubprocessPool::new(&pool_config).await?);
        let stats = pool.stats.clone();

        let app = app_with_pool(Arc::clone(&pool), config);

        tracing::info!("Listening on http://{addr}/metrics");

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", opts.port)).await?;

        // Run server until shutdown signal
        let server = axum::serve(listener, app);

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    tracing::error!("Server error: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received shutdown signal");
            }
        }

        // Print exit telemetry (peak worker RSS tracked atomically during runtime)
        stats.report();
    }

    Ok(())
}
