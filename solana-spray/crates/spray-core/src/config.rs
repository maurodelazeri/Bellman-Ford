//! Environment-driven configuration.
//!
//! Everything the benchmark sweeps lives here so a run is fully described by
//! its environment, and the harness can record that environment alongside the
//! numbers it produced.

use std::time::Duration;

use crate::events::{SinkConfig, SinkKind};
use crate::lifecycle::JournalMode;
use crate::solana::ClusterConfig;
use crate::types::Region;

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub region: Region,
    pub journal_mode: JournalMode,
    /// Bind address of the Restate service endpoint.
    pub bind: String,
    /// Bind address of the side HTTP server that exposes counters.
    pub stats_bind: String,
    pub cluster: ClusterConfig,
    pub sink: SinkConfig,
    /// Emit an event for every stage, not just the durable ones.
    pub emit_all_stages: bool,
    /// Put the first TPU burst on the wire before the first journal append.
    /// Trades nothing (sends are idempotent) for one durable append of latency
    /// on the path that actually matters.
    pub fast_path_first_send: bool,
    /// Use a durable Restate timer for the post-landing commitment wait instead
    /// of an in-process sleep. Costs one journal append plus a partition timer
    /// per transaction; exposed so the cost can be measured rather than argued
    /// about.
    pub durable_commitment_wait: bool,
    /// Per-handler inactivity timeout advertised to Restate. Must exceed the
    /// longest durable step, which for `lean` is the entire spray phase.
    pub inactivity_timeout: Duration,
    pub abort_timeout: Duration,
    /// Retention for journals / workflow completions. The point of the whole
    /// exercise: Restate is a working set, not an archive.
    pub journal_retention: Duration,
    pub workflow_retention: Duration,
    /// Tokio worker threads for the service process.
    pub worker_threads: usize,
}

impl AppConfig {
    pub fn from_env() -> AppConfig {
        let sink_kind = match env_str("SPRAY_SINK", "null").as_str() {
            "file" => SinkKind::File,
            _ => SinkKind::Null,
        };
        AppConfig {
            region: Region::parse(&env_str("SPRAY_REGION", "us")).unwrap_or(Region::Us),
            journal_mode: JournalMode::parse(&env_str("SPRAY_JOURNAL_MODE", "lean"))
                .unwrap_or(JournalMode::Lean),
            bind: env_str("SPRAY_BIND", "0.0.0.0:9080"),
            stats_bind: env_str("SPRAY_STATS_BIND", "0.0.0.0:9081"),
            cluster: ClusterConfig {
                slot_ms: env_u64("SPRAY_SLOT_MS", 400),
                num_leaders: env_u64("SPRAY_NUM_LEADERS", 1_500) as u32,
                real_udp: env_bool("SPRAY_REAL_UDP", false),
                p_fast_land: env_u64("SPRAY_P_FAST_LAND", 820) as u32,
                p_slow_land: env_u64("SPRAY_P_SLOW_LAND", 90) as u32,
                p_chain_failure: env_u64("SPRAY_P_CHAIN_FAIL", 30) as u32,
                fast_land_max_slots: env_u64("SPRAY_FAST_LAND_SLOTS", 4),
                slow_land_max_slots: env_u64("SPRAY_SLOW_LAND_SLOTS", 60),
            },
            sink: SinkConfig {
                kind: sink_kind,
                path: env_str("SPRAY_SINK_PATH", "/var/spray/events"),
                queue_capacity: env_u64("SPRAY_SINK_QUEUE", 262_144) as usize,
                batch_size: env_u64("SPRAY_SINK_BATCH", 2_048) as usize,
                batch_linger_ms: env_u64("SPRAY_SINK_LINGER_MS", 20),
                writers: env_u64("SPRAY_SINK_WRITERS", 2) as usize,
            },
            emit_all_stages: env_bool("SPRAY_EMIT_ALL_STAGES", true),
            fast_path_first_send: env_bool("SPRAY_FAST_PATH", true),
            durable_commitment_wait: env_bool("SPRAY_DURABLE_COMMITMENT_WAIT", false),
            inactivity_timeout: Duration::from_secs(env_u64("SPRAY_INACTIVITY_TIMEOUT_S", 300)),
            abort_timeout: Duration::from_secs(env_u64("SPRAY_ABORT_TIMEOUT_S", 600)),
            journal_retention: Duration::from_secs(env_u64("SPRAY_JOURNAL_RETENTION_S", 300)),
            workflow_retention: Duration::from_secs(env_u64("SPRAY_WORKFLOW_RETENTION_S", 300)),
            worker_threads: env_u64(
                "SPRAY_WORKER_THREADS",
                std::thread::available_parallelism()
                    .map(|n| n.get() as u64)
                    .unwrap_or(4),
            ) as usize,
        }
    }

    /// One-line summary recorded in benchmark output.
    pub fn summary(&self) -> String {
        format!(
            "region={} journal_mode={} sink={:?} slot_ms={} fast_path={} emit_all_stages={} durable_commitment_wait={} journal_retention={}s workers={}",
            self.region.as_str(),
            self.journal_mode.as_str(),
            self.sink.kind,
            self.cluster.slot_ms,
            self.fast_path_first_send,
            self.emit_all_stages,
            self.durable_commitment_wait,
            self.journal_retention.as_secs(),
            self.worker_threads,
        )
    }
}
