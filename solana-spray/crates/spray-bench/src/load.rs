//! Open-loop load generator.
//!
//! Open-loop matters. A closed-loop generator (N workers each submitting, then
//! waiting) silently throttles itself when the system under test slows down, so
//! it reports a healthy latency for a system that is actually falling over.
//! Here the schedule is fixed in advance by wall clock: if Restate cannot keep
//! up, the queue depth and the ack latency both grow, and we see it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use spray_core::solana::now_micros;
use spray_core::types::{
    Commitment, Region, SprayPolicy, SubmitRequest, Webhook,
};
use tokio::sync::Mutex;

use crate::ingress::IngressClient;

#[derive(Clone)]
pub struct LoadConfig {
    pub target: Target,
    pub rate: u64,
    pub duration: Duration,
    pub spike_rate: u64,
    pub spike_secs: u64,
    pub spike_every: u64,
    pub webhook_pct: u32,
    pub webhook_url: String,
    pub validity_slots: u64,
    pub blockhash_age_slots: u64,
    pub commitment: Commitment,
    pub spray: SprayPolicy,
    pub payload_bytes: usize,
    pub regions: Vec<Region>,
    pub max_in_flight_submits: usize,
    pub run_id: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Target {
    /// Keyed workflow, deduplicated by Restate on the signature.
    Workflow,
    /// Plain service, deduplicated by an idempotency key header.
    Service,
}

impl Target {
    pub fn parse(s: &str) -> Option<Target> {
        match s {
            "workflow" => Some(Target::Workflow),
            "service" => Some(Target::Service),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Target::Workflow => "workflow",
            Target::Service => "service",
        }
    }
}

#[derive(Default)]
pub struct LoadStats {
    pub scheduled: AtomicU64,
    pub submitted: AtomicU64,
    pub acked: AtomicU64,
    pub errors: AtomicU64,
    pub rejected: AtomicU64,
    /// Submissions the pacer could not issue on time because the in-flight cap
    /// was saturated. Non-zero means the generator itself was backpressured,
    /// which is the honest signal that the target could not absorb the rate.
    pub backpressured: AtomicU64,
    pub in_flight_submits: AtomicU64,
}

pub struct LoadResult {
    pub stats: Arc<LoadStats>,
    pub ack_hist: Histogram<u64>,
    pub achieved_rate: f64,
    pub wall: Duration,
    pub status_codes: Vec<(u16, u64)>,
}

fn make_payload(n: usize) -> String {
    // Stand-in for a signed, serialized v0 transaction. Size matters: it is
    // what gets copied into every TPU send and, at 10k in flight, into every
    // journal entry that carries the request.
    let mut s = String::with_capacity(n);
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz123456789";
    for i in 0..n {
        s.push(ALPHABET[i % ALPHABET.len()] as char);
    }
    s
}

pub async fn run(client: IngressClient, cfg: LoadConfig) -> anyhow::Result<LoadResult> {
    let stats = Arc::new(LoadStats::default());
    let hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?,
    ));
    let codes = Arc::new(Mutex::new(std::collections::BTreeMap::<u16, u64>::new()));
    let payload = Arc::new(make_payload(cfg.payload_bytes));
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.max_in_flight_submits));

    let start = Instant::now();
    let mut seq: u64 = 0;
    // Pace in 1ms slices: fine enough that a 10k/s target does not arrive in
    // visible bursts, coarse enough that the pacer itself costs nothing.
    //
    // The debt is integrated against the *wall clock*, not accumulated one
    // slice at a time. If an iteration overruns its slice — which it will, once
    // the box is saturated — a per-slice accumulator silently under-delivers
    // and the run reports a rate nobody asked for. Integrating real elapsed
    // time makes the generator catch back up, and if it genuinely cannot, that
    // shows up as backpressure instead of as a quietly lowered target.
    let slice = Duration::from_millis(1);
    let mut next_wake = start;
    let mut carry: f64 = 0.0;
    let mut last_tick = start;

    while start.elapsed() < cfg.duration {
        next_wake += slice;
        let now = Instant::now();
        if next_wake > now {
            tokio::time::sleep(next_wake - now).await;
        } else {
            // Behind schedule: do not try to sleep into the past, and do not
            // let `next_wake` drift arbitrarily far back.
            next_wake = Instant::now();
        }

        let elapsed_s = start.elapsed().as_secs();
        // `spike_rate == 0` means "no spiking" — not "spike down to nothing".
        let in_spike = cfg.spike_rate > 0
            && cfg.spike_every > 0
            && cfg.spike_secs > 0
            && (elapsed_s % cfg.spike_every) < cfg.spike_secs;
        let rate = if in_spike { cfg.spike_rate } else { cfg.rate };

        let now = Instant::now();
        let dt = now.duration_since(last_tick).as_secs_f64();
        last_tick = now;
        carry += rate as f64 * dt;
        let n = carry.floor() as u64;
        carry -= n as f64;

        for _ in 0..n {
            seq += 1;
            stats.scheduled.fetch_add(1, Ordering::Relaxed);

            let permit = match Arc::clone(&sem).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    stats.backpressured.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let region = cfg.regions[(seq as usize) % cfg.regions.len()];
            let tx_id = format!("{}-{:09}", cfg.run_id, seq);
            let webhook = if cfg.webhook_pct > 0 && (seq % 100) < cfg.webhook_pct as u64 {
                Some(Webhook {
                    url: cfg.webhook_url.clone(),
                    max_attempts: 3,
                })
            } else {
                None
            };

            let req = SubmitRequest {
                tx_id: tx_id.clone(),
                payload: (*payload).clone(),
                region,
                blockhash_age_slots: cfg.blockhash_age_slots,
                validity_slots: cfg.validity_slots,
                commitment: cfg.commitment,
                spray: cfg.spray,
                webhook,
                bundle: None,
                client_ts_micros: now_micros(),
            };

            let body = Bytes::from(serde_json::to_vec(&req)?);
            let path = match cfg.target {
                Target::Workflow => format!("/TxWorkflow/{tx_id}/run/send"),
                Target::Service => "/TxService/submit/send".to_string(),
            };
            let idem = match cfg.target {
                Target::Workflow => None,
                Target::Service => Some(tx_id.clone()),
            };

            let client = client.clone();
            let stats = Arc::clone(&stats);
            let hist = Arc::clone(&hist);
            let codes = Arc::clone(&codes);
            tokio::spawn(async move {
                let _permit = permit;
                stats.in_flight_submits.fetch_add(1, Ordering::Relaxed);
                stats.submitted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = client.post(&path, body, idem.as_deref()).await;
                let micros = t0.elapsed().as_micros() as u64;
                stats.in_flight_submits.fetch_sub(1, Ordering::Relaxed);
                match res {
                    Ok((status, _)) => {
                        hist.lock().await.record(micros.max(1)).ok();
                        *codes.lock().await.entry(status.as_u16()).or_insert(0) += 1;
                        if status.is_success() {
                            stats.acked.fetch_add(1, Ordering::Relaxed);
                        } else {
                            stats.rejected.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    }

    // Let the last wave of submissions ack before reporting.
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while stats.in_flight_submits.load(Ordering::Relaxed) > 0 && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let wall = start.elapsed();
    let ack_hist = hist.lock().await.clone();
    let status_codes = codes.lock().await.iter().map(|(k, v)| (*k, *v)).collect();
    let achieved_rate = stats.acked.load(Ordering::Relaxed) as f64 / wall.as_secs_f64();

    Ok(LoadResult {
        stats,
        ack_hist,
        achieved_rate,
        wall,
        status_codes,
    })
}
