// Copyright (c) 2025 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{Family, LabelProm};
use lustre_collector::TargetVariant;
use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};
use std::{
    io::BufRead,
    sync::atomic::AtomicU64,
};
use tokio::task::JoinHandle;

#[derive(Debug)]
enum State {
    Empty,
    Target(String),
    TargetJob(String),
}

#[derive(Clone, Debug, Default)]
pub struct JobstatMetrics {
    read_samples_total: Family<Counter<u64>>,
    read_minimum_size_bytes: Family<Gauge<u64, AtomicU64>>,
    read_maximum_size_bytes: Family<Counter<u64>>,
    read_bytes_total: Family<Counter<u64>>,
    write_samples_total: Family<Counter<u64>>,
    write_minimum_size_bytes: Family<Gauge<u64, AtomicU64>>,
    write_maximum_size_bytes: Family<Counter<u64>>,
    write_bytes_total: Family<Counter<u64>>,
    stats_total: Family<Counter<u64>>,
    target_info: Family<Gauge<u64, AtomicU64>>,
}

impl JobstatMetrics {
    pub fn clear(&self) {
        self.read_samples_total.clear();
        self.read_minimum_size_bytes.clear();
        self.read_maximum_size_bytes.clear();
        self.read_bytes_total.clear();
        self.write_samples_total.clear();
        self.write_minimum_size_bytes.clear();
        self.write_maximum_size_bytes.clear();
        self.write_bytes_total.clear();
        self.stats_total.clear();
        self.target_info.clear();
    }

    pub fn register_metric(&self, registry: &mut Registry) {
        registry.register(
            "lustre_job_read_samples",
            "Total number of reads that have been recorded",
            self.read_samples_total.clone(),
        );

        registry.register(
            "lustre_job_read_minimum_size_bytes",
            "The minimum read size in bytes",
            self.read_minimum_size_bytes.clone(),
        );

        registry.register_without_auto_suffix(
            "lustre_job_read_maximum_size_bytes",
            "The maximum read size in bytes",
            self.read_maximum_size_bytes.clone(),
        );

        registry.register(
            "lustre_job_read_bytes",
            "The total number of bytes that have been read",
            self.read_bytes_total.clone(),
        );

        registry.register(
            "lustre_job_write_samples",
            "Total number of writes that have been recorded",
            self.write_samples_total.clone(),
        );

        registry.register(
            "lustre_job_write_minimum_size_bytes",
            "The minimum write size in bytes",
            self.write_minimum_size_bytes.clone(),
        );

        registry.register_without_auto_suffix(
            "lustre_job_write_maximum_size_bytes",
            "The maximum write size in bytes",
            self.write_maximum_size_bytes.clone(),
        );

        registry.register(
            "lustre_job_write_bytes",
            "The total number of bytes that have been written",
            self.write_bytes_total.clone(),
        );

        registry.register(
            "lustre_job_stats",
            "Number of operations the filesystem has performed, recorded by jobstats",
            self.stats_total.clone(),
        );

        registry.register("target_info", "Target metadata", self.target_info.clone());
    }
}

/// Parse a target header line like `obdfilter.ds002-OST0000.job_stats=`
/// into `(kind, target_name)`. Returns None if not a valid target line.
///
/// Manual parsing replaces regex: split on first `.` to get kind prefix,
/// then find `.job_stats=` suffix to extract the target name between them.
fn parse_target(line: &str) -> Option<(TargetVariant, &str)> {
    let (prefix, rest) = line.split_once('.')?;
    let kind = match prefix {
        "obdfilter" => TargetVariant::Ost,
        "mdt" => TargetVariant::Mdt,
        _ => return None,
    };
    // Handle both formats:
    //   obdfilter.ds002-OST0000.job_stats=          (normal)
    //   obdfilter.es01a-OST0001.job_stats=job_stats: (empty target, concatenated)
    let target = rest
        .strip_suffix(".job_stats=job_stats:")
        .or_else(|| rest.strip_suffix(".job_stats="))?;
    Some((kind, target))
}

/// Parse a stat line like `  read_bytes:  { samples:  0, unit: bytes, min:  0, max:  0, sum:  0, sumsq:  0 }`
/// into `(stat_name, samples, min, max, sum)`.
///
/// Uses manual `split_once` + positional field extraction — no regex.
/// Fields are always in order: samples, unit, min, max, sum, sumsq.
fn parse_stat_line(line: &str) -> Option<(&str, u64, u64, u64, u64)> {
    let trimmed = line.trim_start();

    // Split "stat_name: { samples: ..."
    let (stat_name, rest) = trimmed.split_once(':')?;
    let rest = rest.trim_start();

    // Skip the opening `{`
    let rest = rest.strip_prefix('{')?;

    // Extract fields positionally: "samples: N, unit: X, min: N, max: N, sum: N, sumsq: N }"
    let mut fields = rest.split(',');

    // Field 1: " samples: N"
    let samples_field = fields.next()?;
    let samples: u64 = samples_field
        .trim()
        .strip_prefix("samples:")?
        .trim()
        .parse()
        .ok()?;

    // Field 2: " unit: X" — skip
    let _unit = fields.next()?;

    // Field 3: " min: N"
    let min_field = fields.next()?;
    let min: u64 = min_field
        .trim()
        .strip_prefix("min:")?
        .trim()
        .parse()
        .ok()?;

    // Field 4: " max: N"
    let max_field = fields.next()?;
    let max: u64 = max_field
        .trim()
        .strip_prefix("max:")?
        .trim()
        .parse()
        .ok()?;

    // Field 5: " sum: N"
    let sum_field = fields.next()?;
    let sum: u64 = sum_field
        .trim()
        .strip_prefix("sum:")?
        .trim()
        .parse()
        .ok()?;

    Some((stat_name, samples, min, max, sum))
}

/// Build a reusable label vec for a single job.
///
/// Contains all 4 label entries including a placeholder for `operation`.
/// The caller mutates `labels[2].1` in-place for each stat line via
/// `record_stat`, reusing the String's heap allocation — zero allocs per stat.
fn make_job_labels(
    component: &'static str,
    jobid: &str,
    target: &str,
) -> Vec<(&'static str, String)> {
    // Sorted alphabetically by key. Operation at index 2.
    vec![
        ("component", component.to_string()),
        ("jobid", jobid.to_string()),
        ("operation", String::new()), // placeholder — mutated in-place per stat
        ("target", target.to_string()),
    ]
}

/// Record a single parsed stat into the appropriate metric family.
///
/// Mutates `labels[2].1` (the operation field) in-place — no heap
/// allocation since we reuse the existing String buffer via
/// `clear()` + `push_str()`.
fn record_stat(
    jobstats: &mut JobstatMetrics,
    kind: TargetVariant,
    labels: &mut Vec<(&'static str, String)>,
    stat_name: &str,
    samples: u64,
    min: u64,
    max: u64,
    sum: u64,
) {
    // Map stat name to a static operation string.
    let operation: &'static str = match stat_name {
        "read_bytes" => "read_bytes",
        "write_bytes" => "write_bytes",
        "read" => "read",
        "write" => "write",
        "prealloc" => "prealloc",
        "getattr" => "getattr",
        "setattr" => "setattr",
        "punch" => "punch",
        "sync" => "sync",
        "destroy" => "destroy",
        "create" => "create",
        "statfs" => "statfs",
        "get_info" => "get_info",
        "set_info" => "set_info",
        "quotactl" => "quotactl",
        "open" => "open",
        "close" => "close",
        "mknod" => "mknod",
        "link" => "link",
        "unlink" => "unlink",
        "mkdir" => "mkdir",
        "rmdir" => "rmdir",
        "rename" => "rename",
        "getxattr" => "getxattr",
        "setxattr" => "setxattr",
        "samedir_rename" => "samedir_rename",
        "parallel_rename_file" => "parallel_rename_file",
        "parallel_rename_dir" => "parallel_rename_dir",
        "crossdir_rename" => "crossdir_rename",
        "migrate" => "migrate",
        _ => {
            if kind == TargetVariant::Ost {
                tracing::debug!("Unhandled OST jobstats stats: {stat_name}");
            } else {
                tracing::debug!("Unhandled MDT jobstats stats: {stat_name}");
            }
            return;
        }
    };

    // Mutate operation in-place — reuses existing String buffer.
    labels[2].1.clear();
    labels[2].1.push_str(operation);

    if kind == TargetVariant::Ost {
        match stat_name {
            "read_bytes" => {
                jobstats
                    .read_samples_total
                    .get_or_create(labels)
                    .inc_by(samples);
                jobstats
                    .read_minimum_size_bytes
                    .get_or_create(labels)
                    .set(min);
                jobstats
                    .read_maximum_size_bytes
                    .get_or_create(labels)
                    .inc_by(max);
                jobstats
                    .read_bytes_total
                    .get_or_create(labels)
                    .inc_by(sum);
            }
            "write_bytes" => {
                jobstats
                    .write_samples_total
                    .get_or_create(labels)
                    .inc_by(samples);
                jobstats
                    .write_minimum_size_bytes
                    .get_or_create(labels)
                    .set(min);
                jobstats
                    .write_maximum_size_bytes
                    .get_or_create(labels)
                    .inc_by(max);
                jobstats
                    .write_bytes_total
                    .get_or_create(labels)
                    .inc_by(sum);
            }
            "getattr" | "setattr" | "punch" | "sync" | "destroy" | "create" | "statfs"
            | "get_info" | "set_info" | "quotactl" | "read" | "write" | "prealloc" => {
                jobstats.stats_total.get_or_create(labels).inc_by(samples);
            }
            _ => {}
        }
    } else if kind == TargetVariant::Mdt {
        match stat_name {
            "open" | "close" | "mknod" | "link" | "unlink" | "mkdir" | "rmdir" | "rename"
            | "getattr" | "setattr" | "getxattr" | "setxattr" | "statfs" | "sync"
            | "samedir_rename" | "parallel_rename_file" | "parallel_rename_dir"
            | "crossdir_rename" | "read" | "write" | "read_bytes" | "write_bytes" | "punch"
            | "migrate" => {
                jobstats.stats_total.get_or_create(labels).inc_by(samples);
            }
            _ => {}
        }
    }
}

pub fn jobstats_stream<R: BufRead + std::marker::Send + 'static>(
    f: R,
    mut jobstats: JobstatMetrics,
) -> JoinHandle<JobstatMetrics> {
    tokio::spawn(async move {
        let mut state = State::Empty;
        // Track current target's parsed kind and name to avoid re-parsing
        let mut current_kind = TargetVariant::Ost;
        let mut current_component: &'static str = "ost";
        let mut current_target = String::new();
        // Pre-built labels for the current job — rebuilt only on job_id change
        let mut current_labels: Vec<(&'static str, String)> = Vec::new();

        for line in f.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    tracing::debug!("Unexpected error reading jobstats line: {e}");
                    return jobstats;
                }
            };

            // Skip metadata lines early
            if line == "job_stats:"
                || line.starts_with("  start_time:")
                || line.starts_with("  elapsed_time:")
                || line.starts_with("  snapshot_time:")
            {
                continue;
            }

            match &state {
                State::Empty | State::Target(_) => {
                    if line.starts_with("obdfilter") || line.starts_with("mdt.") {
                        // Parse target header
                        if let Some((kind, target)) = parse_target(&line) {
                            current_kind = kind;
                            current_component = kind.to_prom_label();
                            current_target = target.to_string();
                            state = State::Target(line);
                        } else {
                            tracing::debug!("Failed to parse target line: {line}");
                            return jobstats;
                        }
                    } else if line.starts_with("- job_id:") {
                        // Job line while in target state
                        let jobid = line
                            .trim_start_matches("- job_id:")
                            .trim()
                            .trim_matches('"');
                        current_labels =
                            make_job_labels(current_component, jobid, &current_target);
                        let target_line = match &state {
                            State::Target(t) => t.clone(),
                            _ => continue,
                        };
                        state = State::TargetJob(target_line);
                    } else {
                        tracing::debug!("Unexpected line: {line}, state: {state:?}");
                        return jobstats;
                    }
                }
                State::TargetJob(_) => {
                    if line.starts_with("  ") {
                        // Stat line: parse and record inline — no Vec collection
                        if let Some((stat_name, samples, min, max, sum)) =
                            parse_stat_line(&line)
                        {
                            record_stat(
                                &mut jobstats,
                                current_kind,
                                &mut current_labels,
                                stat_name,
                                samples,
                                min,
                                max,
                                sum,
                            );
                        } else {
                            tracing::debug!("Failed to parse stat line: {line}");
                        }
                    } else if line.starts_with("- job_id:") {
                        // New job in same target — rebuild labels
                        let jobid = line
                            .trim_start_matches("- job_id:")
                            .trim()
                            .trim_matches('"');
                        current_labels =
                            make_job_labels(current_component, jobid, &current_target);
                        let target_line = match &state {
                            State::TargetJob(t) => t.clone(),
                            _ => continue,
                        };
                        state = State::TargetJob(target_line);
                    } else if line.starts_with("obdfilter") || line.starts_with("mdt.") {
                        // New target
                        if let Some((kind, target)) = parse_target(&line) {
                            current_kind = kind;
                            current_component = kind.to_prom_label();
                            current_target = target.to_string();
                            state = State::Target(line);
                        } else {
                            tracing::debug!("Failed to parse target line: {line}");
                            return jobstats;
                        }
                    } else {
                        tracing::debug!("Unexpected line: {line}, state: {state:?}");
                        return jobstats;
                    }
                }
            }
        }

        jobstats
    })
}

#[cfg(test)]
pub mod tests {
    use prometheus_client::{encoding::text::encode, registry::Registry};

    use crate::{
        jobstats::{self, JobstatMetrics},
        tests::{
            compare_metrics, get_scrape, historical_snapshot_path, read_metrics_from_snapshot,
        },
    };
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    async fn stream_jobstats<R: BufRead + std::marker::Send + 'static>(f: R) -> String {
        let mut registry = Registry::default();
        let metrics = JobstatMetrics::default();

        let stream = BufReader::with_capacity(128 * 1_024, f);

        let jobstats = jobstats::jobstats_stream(stream, metrics).await.unwrap();

        jobstats.register_metric(&mut registry);

        let mut buffer = String::new();

        encode(&mut buffer, &registry).unwrap();

        buffer
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_larger_yaml() {
        let f = BufReader::new(File::open("fixtures/jobstats_only/ds86.txt").unwrap());

        let buffer = stream_jobstats(f).await;

        assert_eq!(buffer.lines().count(), 3881470);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_large_yaml() {
        let f = BufReader::new(File::open("fixtures/jobstats_only/co-vm03.txt").unwrap());

        let buffer = stream_jobstats(f).await;

        assert_eq!(
            buffer.lines().count(),
            (4 + // 4 metrics per read_bytes
                4 + // 4 metrics per write_bytes
                13) // 13 metrics for recognized OST operations
                * 49167 // 49167 jobs
                + 2 * 9 // HELP and TYPE lines
                + 1 // # EOF
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_new_yaml() {
        let f = BufReader::new(File::open("fixtures/jobstats_only/2.14.0_162.txt").unwrap());

        let buffer = stream_jobstats(f).await;

        assert_eq!(
            buffer.lines().count(),
            (4 + // 4 metrics per read_bytes
                4 + // 4 metrics per write_bytes
                13) // 13 metrics for recognized OST operations
                * 16 // 16 jobs
                + 2 * 9 // HELP and TYPE lines
                + 1 // # EOF
        );
    }

    fn create_job_template(job_id: &str) -> String {
        format!(
            r#"- job_id:          "{}"
  snapshot_time:   1720516680
  read_bytes:      {{ samples:           0, unit: bytes, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  write_bytes:     {{ samples:          52, unit: bytes, min:     4096, max:   475136, sum:          5468160, sumsq:      1071040692224 }}
  read:            {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  write:           {{ samples:          52, unit: usecs, min:       12, max:    40081, sum:           692342, sumsq:        17432258604 }}
  getattr:         {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  setattr:         {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  punch:           {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  sync:            {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  destroy:         {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  create:          {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  statfs:          {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  get_info:        {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  set_info:        {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  quotactl:        {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}
  prealloc:        {{ samples:           0, unit: usecs, min:        0, max:        0, sum:                0, sumsq:                  0 }}"#,
            job_id
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_synthetic_yaml() -> Result<(), Box<dyn std::error::Error>> {
        // Make the string static so it lives through the entire test
        let input_10_jobs = format!(
            r#"obdfilter.ds002-OST0000.job_stats=
job_stats:
{}"#,
            (0..10)
                .map(|i| create_job_template(&i.to_string()))
                .collect::<Vec<_>>()
                .join("\n")
        );

        // Convert to bytes and then to cursor to avoid borrowing issues
        let bytes = input_10_jobs.into_bytes();

        let buffer = stream_jobstats(BufReader::with_capacity(
            128 * 1_024,
            std::io::Cursor::new(bytes),
        ))
        .await;

        assert_eq!(
            buffer.lines().count(),
            (4 + // 4 metrics per read_bytes
                4 + // 4 metrics per write_bytes
                13) // 13 metrics for recognized OST operations
                * 10 // 10 jobs
                    + 2 * 9 // HELP and TYPE lines
                    + 1 // # EOF
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_some_empty() {
        let f = BufReader::new(File::open("fixtures/jobstats_only/some_empty.txt").unwrap());

        let buffer = stream_jobstats(f).await;

        assert_eq!(
            buffer.lines().count(),
            (4 + // 4 metrics per read_bytes
                4 + // 4 metrics per write_bytes
                13) // 13 metrics for recognized OST operations
                + 2 * 9 // HELP and TYPE lines
                + 1 // # EOF
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parse_2_14_0_164_jobstats_otel() {
        let f = BufReader::new(File::open("fixtures/jobstats_only/2.14.0_164.txt").unwrap());

        let stats = stream_jobstats(f).await;

        insta::assert_snapshot!(stats);

        let current = get_scrape(stats);

        let previous = read_metrics_from_snapshot(&historical_snapshot_path(
            "lustrefs_exporter__jobstats__tests__parse_2_14_0_164_jobstats.histsnap",
        ));

        compare_metrics(&current, &previous);
    }
}
