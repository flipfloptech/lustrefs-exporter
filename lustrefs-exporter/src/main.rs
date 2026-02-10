// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use clap::Parser;
use lustrefs_exporter::{Error, dump_stats, routes::{AppConfig, app_with_config}};
use std::{net::SocketAddr, time::Duration};

const LUSTREFS_EXPORTER_PORT: &str = "32221";

#[derive(Debug, Parser)]
pub struct CommandOpts {
    /// Port that exporter will listen to
    #[clap(short, long, env = "LUSTREFS_EXPORTER_PORT", default_value = LUSTREFS_EXPORTER_PORT)]
    pub port: u16,

    /// Response cache TTL in seconds. Concurrent scrapes within this window
    /// are served from cache, preventing duplicate subprocess invocations.
    /// The write-lock coalescing ensures only one scrape runs at a time
    /// regardless of TTL; this value controls post-scrape staleness tolerance.
    #[clap(long, env = "LUSTREFS_EXPORTER_CACHE_TTL", default_value = "1")]
    pub cache_ttl_secs: u64,

    /// Timeout in seconds for each subprocess (lctl, lnetctl). If a subprocess
    /// does not complete within this duration it is killed and the scrape
    /// continues with partial data.
    #[clap(long, env = "LUSTREFS_EXPORTER_SUBPROCESS_TIMEOUT", default_value = "30")]
    pub subprocess_timeout_secs: u64,

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

        tracing::info!("Listening on http://{addr}/metrics");

        let config = AppConfig {
            cache_ttl: Duration::from_secs(opts.cache_ttl_secs),
            subprocess_timeout: Duration::from_secs(opts.subprocess_timeout_secs),
        };

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", opts.port)).await?;

        axum::serve(listener, app_with_config(config)).await?;
    }

    Ok(())
}
