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
//!
//! ## The tail is the exception, and it is not optional
//!
//! Idempotent re-emission covers everything *except the last events of a
//! transaction*. Replay only re-emits for an invocation that runs again; one
//! that had already completed never will. So an event still sitting in the
//! writer's buffer when the process is killed is gone for good — measured, not
//! theorised: a SIGKILL run lost the terminal events of the ~13 transactions
//! that had completed within the linger window.
//!
//! The fix costs no journal appends. Restate does not consider an invocation
//! complete until its handler returns, so flushing the sink *before* returning
//! makes the tail at-least-once: either the flush lands, or the handler never
//! returned and Restate replays and re-emits. See [`EventSink::emit_durable`].

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
    /// Durable emits the sink could not confirm. Each one failed its handler so
    /// Restate would retry; a non-zero value means the sink is the bottleneck.
    pub unconfirmed: AtomicU64,
}

impl SinkStats {
    pub fn snapshot(&self) -> SinkSnapshot {
        SinkSnapshot {
            emitted: self.emitted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            write_micros: self.write_micros.load(Ordering::Relaxed),
            unconfirmed: self.unconfirmed.load(Ordering::Relaxed),
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
    pub unconfirmed: u64,
}

/// One queued event, optionally carrying an ack the writer resolves once the
/// batch containing it has reached the sink.
struct SinkMsg {
    ev: LifecycleEvent,
    ack: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Raised when a durable emit could not be confirmed in time. Returning this
/// from a handler is deliberate: Restate retries the invocation, the trail is
/// re-emitted, and the dedup key keeps it harmless.
#[derive(Debug)]
pub struct SinkTimeout;

impl std::fmt::Display for SinkTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "event sink did not confirm the terminal event in time")
    }
}

impl std::error::Error for SinkTimeout {}

/// Handle used from the hot path. Cloning is cheap; emitting never awaits.
pub struct EventSink {
    txs: Vec<mpsc::Sender<SinkMsg>>,
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
                let (tx, mut rx) = mpsc::channel::<SinkMsg>(config.queue_capacity);
                let stats = Arc::clone(&stats);
                tokio::spawn(async move {
                    let mut buf: Vec<SinkMsg> = Vec::with_capacity(1024);
                    while rx.recv_many(&mut buf, 1024).await > 0 {
                        stats.written.fetch_add(buf.len() as u64, Ordering::Relaxed);
                        stats.batches.fetch_add(1, Ordering::Relaxed);
                        // The null sink discards, so there is nothing to make
                        // durable; acking on receipt is the honest behaviour.
                        // It follows that the null sink cannot provide the
                        // crash guarantee — only the file sink can.
                        for m in buf.drain(..) {
                            if let Some(ack) = m.ack {
                                let _ = ack.send(());
                            }
                        }
                    }
                });
                txs.push(tx);
            }
        } else {
            for shard in 0..config.writers.max(1) {
                let (tx, rx) = mpsc::channel::<SinkMsg>(config.queue_capacity);
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
        if self.txs[self.shard_of(&ev.tx_id)]
            .try_send(SinkMsg { ev, ack: None })
            .is_err()
        {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Push an event and wait until it has actually reached the sink.
    ///
    /// Call this for the last event of a transaction, before returning from the
    /// handler. Because a transaction's events are all on one shard, flushing
    /// that shard flushes the whole trail, so one await at the end covers
    /// everything the transaction emitted.
    ///
    /// This is off the critical path: by the time it runs the transaction has
    /// already landed or expired, and no leader is waiting on us.
    pub async fn emit_durable(
        &self,
        ev: LifecycleEvent,
        timeout: Duration,
    ) -> Result<(), SinkTimeout> {
        self.stats.emitted.fetch_add(1, Ordering::Relaxed);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let idx = self.shard_of(&ev.tx_id);
        if self.txs[idx]
            .send(SinkMsg {
                ev,
                ack: Some(ack_tx),
            })
            .await
            .is_err()
        {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(SinkTimeout);
        }
        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(())) => Ok(()),
            // Either the deadline passed or the writer died. Both mean we
            // cannot claim the trail is complete, so say so and let Restate
            // retry rather than quietly returning a truncated record.
            _ => {
                self.stats.unconfirmed.fetch_add(1, Ordering::Relaxed);
                Err(SinkTimeout)
            }
        }
    }

    /// Shard by tx so one transaction's events keep their relative order in one
    /// file, which makes the trail readable by eye as well as by tool.
    #[inline]
    fn shard_of(&self, tx_id: &str) -> usize {
        (fnv(tx_id) as usize) % self.txs.len()
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
    mut rx: mpsc::Receiver<SinkMsg>,
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

    let mut batch: Vec<SinkMsg> = Vec::with_capacity(cfg.batch_size);
    let linger = Duration::from_millis(cfg.batch_linger_ms);
    let mut scratch = Vec::with_capacity(1 << 20);

    loop {
        // Block until there is something to write, then spend up to `linger`
        // gathering more.
        //
        // `recv_many` returns as soon as *one* message is available, so waiting
        // on it alone yields a "batch" of one under light load: the linger has
        // to be an explicit deadline around the gather, or the setting does
        // nothing at all.
        match rx.recv().await {
            Some(m) => batch.push(m),
            None => break, // channel closed
        }
        let deadline = tokio::time::Instant::now() + linger;
        while batch.len() < cfg.batch_size {
            // A pending ack means a handler is blocked on this batch reaching
            // disk. Stop gathering and write: the linger is a throughput
            // optimisation and must never hold a caller hostage.
            if batch.iter().any(|m| m.ack.is_some()) {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let want = cfg.batch_size - batch.len();
            tokio::select! {
                n = rx.recv_many(&mut batch, want) => { if n == 0 { break } }
                _ = tokio::time::sleep(remaining) => break,
            }
        }
        let t0 = std::time::Instant::now();
        scratch.clear();
        for m in &batch {
            serde_json::to_writer(&mut scratch, &m.ev)?;
            scratch.push(b'\n');
        }
        file.write_all(&scratch).await?;
        file.flush().await?;
        // Only now is the batch genuinely out of this process's memory, so this
        // is the earliest point an ack may be released.
        for m in batch.iter_mut() {
            if let Some(ack) = m.ack.take() {
                let _ = ack.send(());
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Region;

    fn ev(tx: &str, stage: Stage) -> LifecycleEvent {
        EventTrail {
            tx_id: tx.to_string(),
            region: Region::Us,
            invocation_id: "inv".to_string(),
        }
        .event(stage, 1, 0, None)
    }

    /// The guarantee the terminal event depends on: once `emit_durable`
    /// resolves, everything that transaction emitted is on disk — not merely
    /// queued. Without this, a SIGKILL right after the handler returns loses
    /// the tail of a trail that will never be replayed.
    #[tokio::test]
    async fn emit_durable_resolves_only_after_the_trail_is_on_disk() {
        let dir = std::env::temp_dir().join(format!("spray-sink-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let prefix = dir.join("events").to_string_lossy().to_string();

        let sink = EventSink::start(SinkConfig {
            kind: SinkKind::File,
            path: prefix.clone(),
            // A linger far longer than the test: if the ack were resolved on
            // enqueue rather than on flush, the file would still be empty.
            batch_linger_ms: 60_000,
            batch_size: 4,
            writers: 1,
            queue_capacity: 1024,
        });

        sink.emit(ev("tx-1", Stage::Accepted));
        sink.emit(ev("tx-1", Stage::Validated));
        sink.emit(ev("tx-1", Stage::Spraying));
        sink.emit_durable(ev("tx-1", Stage::Settled), Duration::from_secs(10))
            .await
            .expect("sink should confirm the terminal event");

        let written = std::fs::read_to_string(format!("{prefix}.0.jsonl")).unwrap();
        let stages: Vec<Stage> = written
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<LifecycleEvent>(l).unwrap().stage)
            .collect();
        assert_eq!(
            stages,
            vec![
                Stage::Accepted,
                Stage::Validated,
                Stage::Spraying,
                Stage::Settled
            ],
            "the whole trail must be durable once the terminal event is confirmed"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The window `emit_durable` exists to close: a plain `emit` leaves the
    /// event in the writer's buffer, where a SIGKILL destroys it. That is
    /// harmless for an invocation that will run again — replay re-emits — but
    /// unrecoverable for one that has already completed, because nothing will
    /// ever emit those events again.
    #[tokio::test]
    async fn plain_emit_leaves_events_in_memory_until_the_batch_flushes() {
        let dir = std::env::temp_dir().join(format!("spray-window-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let prefix = dir.join("events").to_string_lossy().to_string();
        let path = format!("{prefix}.0.jsonl");

        let sink = EventSink::start(SinkConfig {
            kind: SinkKind::File,
            path: prefix.clone(),
            batch_linger_ms: 60_000,
            batch_size: 4096,
            writers: 1,
            queue_capacity: 1024,
        });

        sink.emit(ev("tx-2", Stage::Confirmed));
        sink.emit(ev("tx-2", Stage::Settled));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            on_disk.is_empty(),
            "events are still buffered, not durable; killing the process here \
             would lose them: {on_disk:?}"
        );

        // The same events become durable only once something forces the flush.
        sink.emit_durable(ev("tx-2", Stage::Accepted), Duration::from_secs(10))
            .await
            .unwrap();
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert_eq!(on_disk.lines().count(), 3);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Stage-derived sequence numbers are what let the sink deduplicate replays.
    /// If two emissions of the same stage disagreed, dedup would silently drop
    /// real information instead of a duplicate.
    #[test]
    fn event_keys_are_stable_across_replays() {
        let a = ev("tx-9", Stage::Confirmed);
        let b = ev("tx-9", Stage::Confirmed);
        assert_eq!((a.tx_id, a.seq, a.stage), (b.tx_id, b.seq, b.stage));
        assert_ne!(ev("tx-9", Stage::Expired).seq, a.seq);
    }
}
