//! The workload itself: spray a signed transaction at upcoming leaders until it
//! lands or its blockhash expires.
//!
//! Nothing in this module knows about Restate. That separation is the whole
//! design: the spray loop is a plain async function that the Restate handler
//! wraps in one durable step. Restate decides *when* to run it and remembers
//! *that it ran*; it is not consulted 1200 times per transaction while it runs.
//!
//! The loop is safe to re-execute from scratch after a crash because a TPU send
//! is idempotent by construction — the transaction is already signed, and the
//! cluster deduplicates by signature. That is the property that lets us take
//! the journal out of the inner loop without losing correctness.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::events::{EventSink, EventTrail};
use crate::solana::{Cluster, LandingNotice};
use crate::types::{Outcome, SprayReport, Stage, SubmitRequest};

/// How many durable checkpoints a lifecycle takes.
///
/// This is the single most important knob in the whole experiment. Every
/// durable step is a replicated, fsync'd append to Restate's log, so the
/// journal step count per transaction — not the transaction's two-minute
/// duration — is what determines how many transactions a Restate cluster can
/// carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// Three durable steps: validate, spray-to-terminal, settle. A crash
    /// re-sprays from the beginning, which is harmless and cheap.
    Lean,
    /// Checkpoint at each phase boundary, plus one mid-spray checkpoint per
    /// `checkpoint_slots`. Bounds re-spray work after a crash.
    Standard,
    /// A checkpoint every spray window plus per-stage events journaled. Models
    /// the "journal everything" instinct so its cost is visible.
    Verbose,
}

impl JournalMode {
    pub fn parse(s: &str) -> Option<JournalMode> {
        match s {
            "lean" => Some(JournalMode::Lean),
            "standard" => Some(JournalMode::Standard),
            "verbose" => Some(JournalMode::Verbose),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JournalMode::Lean => "lean",
            JournalMode::Standard => "standard",
            JournalMode::Verbose => "verbose",
        }
    }

    /// Slots of spraying between mid-flight checkpoints. `None` means "one
    /// window, run it to the end".
    pub fn checkpoint_slots(self) -> Option<u64> {
        match self {
            JournalMode::Lean => None,
            JournalMode::Standard => Some(30), // ~12s at 400ms slots
            JournalMode::Verbose => Some(4),   // one leader rotation
        }
    }
}

/// Durable outcome of the `validate` step.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Validation {
    pub submit_slot: u64,
    pub expiry_slot: u64,
}

/// Cumulative spray counters carried across checkpoint boundaries.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SprayProgress {
    pub sends: u32,
    pub leaders_hit: u32,
    pub last_slot: u64,
    pub elapsed_ms: u64,
}

/// What one spray window concluded.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowOutcome {
    /// Reached a terminal state; the lifecycle can move on.
    Terminal(SprayReport),
    /// Window elapsed with the transaction still in flight.
    Pending(SprayProgress),
}

pub struct SprayEngine {
    pub cluster: Arc<Cluster>,
    pub sink: Arc<EventSink>,
}

impl SprayEngine {
    pub fn new(cluster: Arc<Cluster>, sink: Arc<EventSink>) -> Arc<Self> {
        Arc::new(SprayEngine { cluster, sink })
    }

    /// Compute the deadline for a submission. Pure, so it is stable on replay
    /// once `submit_slot` has been journaled.
    pub fn validate(&self, req: &SubmitRequest) -> Result<Validation, String> {
        if req.tx_id.is_empty() {
            return Err("empty signature".to_string());
        }
        if req.payload.is_empty() {
            return Err("empty transaction payload".to_string());
        }
        if req.validity_slots == 0 {
            return Err("validity_slots must be positive".to_string());
        }
        let submit_slot = self.cluster.current_slot();
        // The lease already started ticking when the client fetched the
        // blockhash, so what is left is the validity window minus its age.
        let remaining = req.validity_slots.saturating_sub(req.blockhash_age_slots);
        if remaining == 0 {
            return Err("blockhash already expired at submission".to_string());
        }
        Ok(Validation {
            submit_slot,
            expiry_slot: submit_slot + remaining,
        })
    }

    /// Fire one burst at the upcoming leaders immediately, before anything has
    /// been journaled.
    ///
    /// This is the fast path, and it is the single biggest latency win
    /// available. Journaling the deadline first costs a durable append —
    /// milliseconds — before the first packet reaches a leader, and a Solana
    /// slot is only 400ms. Since a signed transaction is idempotent on chain,
    /// there is nothing to protect: the worst case if the process dies right
    /// after this burst is that a leader saw a transaction we then re-send.
    ///
    /// Correctness is unaffected because the burst commits us to nothing. The
    /// deadline is still pinned durably a moment later, and if the invocation
    /// never resumes, the transaction simply expires on chain by itself — which
    /// is exactly what it would have done had we never sent it.
    pub async fn first_burst(&self, req: &SubmitRequest) -> u32 {
        let slot = self.cluster.current_slot();
        let payload = req.payload.as_bytes();
        let mut sends = 0;
        for ahead in 0..=req.spray.leaders_ahead as u64 {
            let leader = self
                .cluster
                .schedule
                .leader_for_slot(slot + ahead * crate::solana::SLOTS_PER_LEADER);
            self.cluster.tpu.send(leader, payload).await;
            sends += 1;
        }
        sends
    }

    /// Spray at leaders from now until either a terminal state or `until_slot`.
    ///
    /// This is the hot path. It runs entirely inside the service process: no
    /// Restate interaction, no journal appends, no network round trips to the
    /// Restate server. The only shared state it touches is an atomic counter
    /// per send and one oneshot registration on the confirmation feed.
    pub async fn spray_window(
        &self,
        req: &SubmitRequest,
        v: Validation,
        until_slot: u64,
        mut progress: SprayProgress,
        trail: &EventTrail,
        journal_steps: u32,
        emit_window_events: bool,
    ) -> WindowOutcome {
        let started = Instant::now();
        let carried_ms = progress.elapsed_ms;
        let fate = self.cluster.config.fate(&req.tx_id);
        let current = self.cluster.current_slot();

        // Subscribe before the first send, so a landing cannot slip between the
        // send and the subscription.
        let sub = self
            .cluster
            .feed
            .register(&req.tx_id, v.submit_slot, fate, current);

        let payload = req.payload.as_bytes();
        let mut tick =
            tokio::time::interval(Duration::from_millis(req.spray.interval_ms.max(1)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut landing: Option<LandingNotice> = None;
        let mut last_leader: Option<u32> = None;

        // A subscription that resolves is the only success path; everything
        // else is a deadline. Race the two.
        let wait = async {
            match sub {
                Some(s) => s.recv().await,
                // Fated never to land: park forever and let the deadline win.
                None => std::future::pending().await,
            }
        };
        tokio::pin!(wait);

        loop {
            let slot = self.cluster.current_slot();
            let deadline_slot = until_slot.min(v.expiry_slot);
            if slot > deadline_slot {
                break;
            }
            if progress.sends >= req.spray.max_sends {
                break;
            }

            // Hit the current leader and the next few. Sending the same packet
            // to several upcoming leaders is what "spray" means: whoever is
            // producing when the packet arrives can include it.
            for ahead in 0..=req.spray.leaders_ahead as u64 {
                let leader = self
                    .cluster
                    .schedule
                    .leader_for_slot(slot + ahead * crate::solana::SLOTS_PER_LEADER);
                if last_leader != Some(leader.0) {
                    progress.leaders_hit += 1;
                    last_leader = Some(leader.0);
                }
                self.cluster.tpu.send(leader, payload).await;
                progress.sends += 1;
            }
            progress.last_slot = slot;

            tokio::select! {
                biased;
                notice = &mut wait => {
                    landing = notice;
                    break;
                }
                _ = tick.tick() => {}
            }
        }

        progress.elapsed_ms = carried_ms + started.elapsed().as_millis() as u64;
        let slot_now = self.cluster.current_slot();

        if let Some(notice) = landing {
            if emit_window_events {
                self.sink.emit(trail.event(
                    Stage::Landed,
                    notice.slot,
                    journal_steps,
                    Some(format!("sends={}", progress.sends)),
                ));
            }
            // Wait out the commitment lag. This is real time on the wire, not a
            // durable timer: at Solana's cadence `confirmed` is one slot behind
            // `processed`, and burning a journal append on a 400ms wait would
            // cost more than the wait itself.
            let lag = req.commitment.slots_after_landing();
            if lag > 0 {
                let target = notice.slot + lag;
                let now = self.cluster.current_slot();
                if target > now {
                    let ms = (target - now) * self.cluster.clock.slot_ms();
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
            }
            let outcome = match notice.outcome {
                Outcome::FailedOnChain => Outcome::FailedOnChain,
                _ => Outcome::Confirmed,
            };
            return WindowOutcome::Terminal(SprayReport {
                outcome,
                landed_slot: Some(notice.slot),
                sends: progress.sends,
                leaders_hit: progress.leaders_hit,
                elapsed_ms: carried_ms + started.elapsed().as_millis() as u64,
                final_slot: self.cluster.current_slot(),
            });
        }

        // No landing. Either the blockhash is dead, or this window just ended.
        if slot_now > v.expiry_slot {
            return WindowOutcome::Terminal(SprayReport {
                outcome: Outcome::Expired,
                landed_slot: None,
                sends: progress.sends,
                leaders_hit: progress.leaders_hit,
                elapsed_ms: progress.elapsed_ms,
                final_slot: slot_now,
            });
        }
        if progress.sends >= req.spray.max_sends {
            return WindowOutcome::Terminal(SprayReport {
                outcome: Outcome::Dropped,
                landed_slot: None,
                sends: progress.sends,
                leaders_hit: progress.leaders_hit,
                elapsed_ms: progress.elapsed_ms,
                final_slot: slot_now,
            });
        }
        WindowOutcome::Pending(progress)
    }
}

/// Deliver the user's webhook. Runs inside a durable step with its own retry
/// policy, because unlike a TPU send this one is *not* idempotent from the
/// receiver's point of view and we want at-least-once with a bounded cost.
pub async fn deliver_webhook(url: &str, body: &[u8]) -> Result<u16, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // A deliberately minimal HTTP/1.1 client: the webhook target is a customer
    // endpoint, and pulling a full client stack in here would put TLS and
    // connection-pool behaviour into the middle of a latency measurement.
    let rest = url.strip_prefix("http://").ok_or("only http:// supported")?;
    let (host_port, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let mut stream = tokio::net::TcpStream::connect(host_port)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stream.set_nodelay(true).ok();

    let mut req = Vec::with_capacity(body.len() + 160);
    req.extend_from_slice(
        format!(
            "POST {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    req.extend_from_slice(body);
    stream
        .write_all(&req)
        .await
        .map_err(|e| format!("write: {e}"))?;

    let mut resp = Vec::with_capacity(256);
    let mut buf = [0u8; 512];
    loop {
        let n = stream.read(&mut buf).await.map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        resp.extend_from_slice(&buf[..n]);
        if resp.len() >= 16 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&resp);
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("bad status line: {head:?}"))?;
    if (200..300).contains(&status) {
        Ok(status)
    } else {
        Err(format!("webhook returned {status}"))
    }
}
