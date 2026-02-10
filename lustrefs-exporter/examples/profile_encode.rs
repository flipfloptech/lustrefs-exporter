// Phase-instrumented profiling binary.
// Run with: cargo run --release --example profile_encode

use lustre_collector::{Record, parse_lctl_output, parse_lnetctl_output, parse_lnetctl_stats};
use lustrefs_exporter::metrics::{Metrics, build_lustre_stats};
use prometheus_client::{encoding::text::encode, registry::Registry};
use std::time::Instant;

fn generate_records() -> Vec<Record> {
    let mut records = Vec::new();

    let lustre_metrics = include_str!(
        "../../lustre-collector/src/fixtures/valid/lustre-2.14.0_ddn133/2.14.0_ddn133_quota.txt"
    );
    let mut lustre_metrics_records =
        parse_lctl_output(lustre_metrics.as_bytes()).expect("Failed to parse lustre metrics");
    records.append(&mut lustre_metrics_records);

    let net_show = include_bytes!("../fixtures/lnetctl_net_show.txt");
    let mut net_show_records =
        parse_lnetctl_output(net_show).expect("Failed to parse lnetctl net show");
    records.append(&mut net_show_records);

    let net_stats = include_bytes!("../fixtures/lnetctl_stats.txt");
    let mut net_stats_records =
        parse_lnetctl_stats(net_stats).expect("Failed to parse lnetctl stats");
    records.append(&mut net_stats_records);

    records
}

fn main() {
    let records = generate_records();
    eprintln!("Loaded {} records", records.len());

    let mut registry = Registry::default();
    let metrics = Metrics::default();
    metrics.register_metric(&mut registry);

    let iters = 100;
    let mut build_total = std::time::Duration::ZERO;
    let mut encode_total = std::time::Duration::ZERO;
    let mut clear_total = std::time::Duration::ZERO;
    let mut alloc_total = std::time::Duration::ZERO;

    for i in 0..iters {
        // Phase 1: Build (populate families from records)
        let t = Instant::now();
        build_lustre_stats(&records, &metrics);
        build_total += t.elapsed();

        // Phase 2: Allocate buffer
        let t = Instant::now();
        let mut buffer = String::new();
        alloc_total += t.elapsed();

        // Phase 3: Encode
        let t = Instant::now();
        encode(&mut buffer, &registry).expect("Failed to encode");
        encode_total += t.elapsed();

        if i == 0 {
            eprintln!("Output size: {} bytes ({} lines)", buffer.len(), buffer.lines().count());
        }

        // Phase 4: Clear (what happens after encoding)
        let t = Instant::now();
        metrics.clear();
        clear_total += t.elapsed();
    }

    let total = build_total + encode_total + clear_total + alloc_total;
    eprintln!("\n--- Phase breakdown ({iters} iterations) ---");
    eprintln!("Build:   {:>8.1?}/iter  ({:.1}%)", build_total / iters, build_total.as_nanos() as f64 / total.as_nanos() as f64 * 100.0);
    eprintln!("Encode:  {:>8.1?}/iter  ({:.1}%)", encode_total / iters, encode_total.as_nanos() as f64 / total.as_nanos() as f64 * 100.0);
    eprintln!("Clear:   {:>8.1?}/iter  ({:.1}%)", clear_total / iters, clear_total.as_nanos() as f64 / total.as_nanos() as f64 * 100.0);
    eprintln!("Alloc:   {:>8.1?}/iter  ({:.1}%)", alloc_total / iters, alloc_total.as_nanos() as f64 / total.as_nanos() as f64 * 100.0);
    eprintln!("Total:   {:>8.1?}/iter", total / iters);
}
