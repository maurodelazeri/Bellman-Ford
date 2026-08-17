//! Lifecycle event emission.
//!
//! This is the ClickHouse/Redis half of the design. The rule that makes it fast
//! *and* complete:
//!
//! > Emitting an event is never a durable step. Every event is a pure function
//! > of `(tx_id, stage)`, carries a deterministic `seq`, and is therefore
//! > perfectly idempotent. The sink deduplicates on `(tx_id, seq)`.
//!
//! That is what buys the completeness guarantee for free. If the process dies
//! mid-lifecycle, Restate replays the handler from its last durable step; the
//! replay re-emits the same events with the same `seq` values, and the sink
//! collapses the duplicates. You get a gapless trail without paying a journal
//! append per event.
//!
//! The emit path itself is a non-blocking push into a bounded ring. If the sink
//! backs up we drop and count, rather than stalling a transaction that has 400
//! milliseconds to reach a leader.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::solana::now_micros;
use crate::types::{Region, Stage};

/// One row as it lands in Kafka -> ClickHouse and on the Redis push stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub tx_id: String,
    /// Deterministic per stage. Together with `tx_id` this is the dedup key.
    pub seq: u32,
    pub stage: Stage,
    pub region: Region,
    pub slot: u64,
    pub ts_micros: u64,
    /// Restate invocation id, so an operator can jump from a ClickHouse row
    /// straight to the journal in the Restate UI while it is still retained.
    pub invocation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Set on the final event of a transaction. The verifier keys off this.
    pub terminal: bool,
    /// How many durable journal steps had been taken when this was emitted.
    pub journal_steps: u32,
}

/// Where events go once they leave the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    /// Count only. Use when measuring peak Restate throughput, so the
    /// measurement is not confounded by local disk.
    Null,
    /// Append JSON lines to a file. Stands in for the Kafka producer and gives
    /// the verifier something to check completeness against.
    File,
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub kind: SinkKind,
    pub path: String,
    /// Bounded queue depth. Beyond this we shed load and count it.
    pub queue_capacity: usize,
    /// Flush when this many events are buffered...
    pub batch_size: usize,
    /// ...or this much time has passed, whichever comes first.
    pub batch_linger_ms: u64,
    /// Number of writer tasks. Each owns its own file shard.
    pub writers: usize,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            kind: SinkKind::Null,
            path: "events".to_string(),
            queue_capacity: 262_144,
            batch_size: 1_024,
            batch_linger_ms: 20,
            writers: 2,
        }
    }
}

#[derive(Default)]
pub struct SinkStats {
    pub emitted: AtomicU64,
    pub dropped: AtomicU64,
    pub written: AtomicU64,
    pub batches: AtomicU64,
    pub write_micros: AtomicU64,
}

impl SinkStats {
    pub fn snapshot(&self) -> SinkSnapshot {
        SinkSnapshot {
            emitted: self.emitted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            write_micros: self.write_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SinkSnapshot {
    pub emitted: u64,
    pub dropped: u64,
    pub written: u64,
    pub batches: u64,
    pub write_micros: u64,
}

/// Handle used from the hot path. Cloning is cheap; emitting never awaits.
pub struct EventSink {
    txs: Vec<mpsc::Sender<LifecycleEvent>>,
    pub stats: Arc<SinkStats>,
    kind: SinkKind,
}

impl EventSink {
    pub fn start(config: SinkConfig) -> Arc<Self> {
        let stats = Arc::new(SinkStats::default());
        let mut txs = Vec::with_capacity(config.writers.max(1));

        if config.kind == SinkKind::Null {
            // Still go through a channel and a drain task so the benchmark pays
            // the same queueing cost it would with a real producer attached.
            for _ in 0..config.writers.max(1) {
                let (tx, mut rx) = mpsc::channel::<LifecycleEvent>(config.queue_capacity);
                let stats = Arc::clone(&stats);
                tokio::spawn(async move {
                    let mut buf = Vec::with_capacity(1024);
                    while rx.recv_many(&mut buf, 1024).await > 0 {
                        stats.written.fetch_add(buf.len() as u64, Ordering::Relaxed);
                        stats.batches.fetch_add(1, Ordering::Relaxed);
                        buf.clear();
                    }
                });
                txs.push(tx);
            }
        } else {
            for shard in 0..config.writers.max(1) {
                let (tx, rx) = mpsc::channel::<LifecycleEvent>(config.queue_capacity);
                let stats = Arc::clone(&stats);
                let cfg = config.clone();
                tokio::spawn(async move {
                    if let Err(e) = file_writer(shard, rx, cfg, stats).await {
                        tracing::error!("event sink writer {shard} died: {e}");
                    }
                });
                txs.push(tx);
            }
        }

        Arc::new(EventSink {
            txs,
            stats,
            kind: config.kind,
        })
    }

    pub fn kind(&self) -> SinkKind {
        self.kind
    }

    /// Push an event. Never blocks, never awaits, never fails the caller.
    #[inline]
    pub fn emit(&self, ev: LifecycleEvent) {
        self.stats.emitted.fetch_add(1, Ordering::Relaxed);
        // Shard by tx so one transaction's events keep their relative order in
        // one file, which makes the trail readable by eye as well as by tool.
        let idx = (fnv(&ev.tx_id) as usize) % self.txs.len();
        if self.txs[idx].try_send(ev).is_err() {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn fnv(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

async fn file_writer(
    shard: usize,
    mut rx: mpsc::Receiver<LifecycleEvent>,
    cfg: SinkConfig,
    stats: Arc<SinkStats>,
) -> anyhow::Result<()> {
    let path = format!("{}.{shard}.jsonl", cfg.path);
    if let Some(dir) = std::path::Path::new(&path).parent() {
        if !dir.as_os_str().is_empty() {
            tokio::fs::create_dir_all(dir).await.ok();
        }
    }
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    let mut file = tokio::io::BufWriter::with_capacity(1 << 20, file);

    let mut batch: Vec<LifecycleEvent> = Vec::with_capacity(cfg.batch_size);
    let linger = Duration::from_millis(cfg.batch_linger_ms);
    let mut scratch = Vec::with_capacity(1 << 20);

    loop {
        let n = tokio::select! {
            n = rx.recv_many(&mut batch, cfg.batch_size) => n,
            _ = tokio::time::sleep(linger), if !batch.is_empty() => 0,
        };
        if n == 0 && batch.is_empty() {
            // Channel closed and nothing pending.
            break;
        }
        let t0 = std::time::Instant::now();
        scratch.clear();
        for ev in &batch {
            serde_json::to_writer(&mut scratch, ev)?;
            scratch.push(b'\n');
        }
        file.write_all(&scratch).await?;
        file.flush().await?;
        stats
            .write_micros
            .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
        stats.written.fetch_add(batch.len() as u64, Ordering::Relaxed);
        stats.batches.fetch_add(1, Ordering::Relaxed);
        batch.clear();
    }
    file.flush().await?;
    Ok(())
}

/// Builds events with the deterministic sequence numbers the sink dedups on.
pub struct EventTrail {
    pub tx_id: String,
    pub region: Region,
    pub invocation_id: String,
}

impl EventTrail {
    /// `seq` is derived from the stage, not from a counter, so replays produce
    /// byte-identical keys. `Stage` is a closed, ordered enum precisely so this
    /// mapping is stable.
    #[inline]
    pub fn event(
        &self,
        stage: Stage,
        slot: u64,
        journal_steps: u32,
        detail: Option<String>,
    ) -> LifecycleEvent {
        LifecycleEvent {
            tx_id: self.tx_id.clone(),
            seq: stage as u32,
            stage,
            region: self.region,
            slot,
            ts_micros: now_micros(),
            invocation_id: self.invocation_id.clone(),
            detail,
            terminal: stage == Stage::Settled,
            journal_steps,
        }
    }
}
