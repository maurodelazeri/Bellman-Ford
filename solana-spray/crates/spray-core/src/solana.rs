//! A cheap but structurally faithful stand-in for a Solana cluster.
//!
//! The point of this module is to be *honest about where CPU goes*. A benchmark
//! of Restate is worthless if the mock cluster eats the machine, so every piece
//! here is O(1) per transaction and shares work globally:
//!
//! * one slot clock for the whole process, not a timer per transaction;
//! * one leader schedule computed arithmetically, no allocation;
//! * one confirmation feed task that resolves *all* pending transactions,
//!   modelling a Geyser/`blockSubscribe` stream rather than per-signature
//!   `getSignatureStatuses` polling (which is what you would actually build at
//!   this transaction rate anyway);
//! * TPU sends are counted, and optionally really put on a UDP socket, so the
//!   syscall cost of spraying is visible in the numbers instead of hidden.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::oneshot;

use crate::types::Outcome;

/// Mainnet slot time. Overridable because leader rotation cadence is the main
/// driver of how long a lifecycle lasts.
pub const DEFAULT_SLOT_MS: u64 = 400;
/// Consecutive slots each leader owns on mainnet.
pub const SLOTS_PER_LEADER: u64 = 4;

pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Identifies one validator in the (fake) leader schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaderId(pub u32);

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub slot_ms: u64,
    pub num_leaders: u32,
    /// Send real UDP datagrams at a local sink instead of only counting them.
    pub real_udp: bool,
    /// Probability weights for the landing model, in per-mille.
    pub p_fast_land: u32,
    pub p_slow_land: u32,
    pub p_chain_failure: u32,
    /// Everything left over expires.
    pub fast_land_max_slots: u64,
    pub slow_land_max_slots: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            slot_ms: DEFAULT_SLOT_MS,
            num_leaders: 1_500,
            real_udp: false,
            // Roughly a healthy-network profile: most transactions land within a
            // couple of leader rotations, a tail drags on, a few percent fail on
            // chain, the remainder never land and expire.
            p_fast_land: 820,
            p_slow_land: 90,
            p_chain_failure: 30,
            fast_land_max_slots: 4,
            slow_land_max_slots: 60,
        }
    }
}

/// Deterministic 64-bit mix, so a transaction's fate is a pure function of its
/// id. Replays after a crash therefore produce the same outcome, which is what
/// makes the completeness verifier meaningful.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    splitmix64(h)
}

/// The fate the cluster has decided for a transaction, resolved lazily.
#[derive(Debug, Clone, Copy)]
pub struct Fate {
    pub outcome: Outcome,
    /// Slot it lands in, relative to the submission slot. `None` = never lands.
    pub land_after_slots: Option<u64>,
}

impl ClusterConfig {
    /// Decide, deterministically, what will happen to this transaction.
    pub fn fate(&self, tx_id: &str) -> Fate {
        let h = hash_str(tx_id);
        let roll = (h % 1000) as u32;
        let spread = splitmix64(h);

        if roll < self.p_fast_land {
            Fate {
                outcome: Outcome::Confirmed,
                land_after_slots: Some(spread % self.fast_land_max_slots.max(1)),
            }
        } else if roll < self.p_fast_land + self.p_slow_land {
            let lo = self.fast_land_max_slots;
            let hi = self.slow_land_max_slots.max(lo + 1);
            Fate {
                outcome: Outcome::Confirmed,
                land_after_slots: Some(lo + spread % (hi - lo)),
            }
        } else if roll < self.p_fast_land + self.p_slow_land + self.p_chain_failure {
            Fate {
                outcome: Outcome::FailedOnChain,
                land_after_slots: Some(spread % self.fast_land_max_slots.max(1)),
            }
        } else {
            Fate {
                outcome: Outcome::Expired,
                land_after_slots: None,
            }
        }
    }
}

/// Monotonic slot clock shared by the whole process.
pub struct SlotClock {
    origin: Instant,
    slot_ms: u64,
    /// Slot number the clock started at, so slots look like mainnet values.
    base_slot: u64,
}

impl SlotClock {
    pub fn new(slot_ms: u64, base_slot: u64) -> Self {
        SlotClock {
            origin: Instant::now(),
            slot_ms,
            base_slot,
        }
    }

    #[inline]
    pub fn current_slot(&self) -> u64 {
        self.base_slot + (self.origin.elapsed().as_millis() as u64) / self.slot_ms
    }

    #[inline]
    pub fn slot_ms(&self) -> u64 {
        self.slot_ms
    }

    /// Wall-clock instant a given slot begins at.
    pub fn slot_start(&self, slot: u64) -> Instant {
        self.origin + Duration::from_millis(slot.saturating_sub(self.base_slot) * self.slot_ms)
    }
}

/// Arithmetic leader schedule: no allocation, no lookup table, matching the
/// "leader owns four consecutive slots" structure of the real one.
pub struct LeaderSchedule {
    num_leaders: u32,
}

impl LeaderSchedule {
    pub fn new(num_leaders: u32) -> Self {
        LeaderSchedule { num_leaders }
    }

    #[inline]
    pub fn leader_for_slot(&self, slot: u64) -> LeaderId {
        let rotation = slot / SLOTS_PER_LEADER;
        LeaderId((splitmix64(rotation) % self.num_leaders as u64) as u32)
    }
}

/// Sends transaction packets at leaders. Either counts them (default) or really
/// writes UDP datagrams so the syscall cost shows up in the profile.
pub struct TpuClient {
    sends: AtomicU64,
    bytes: AtomicU64,
    socket: Option<Arc<tokio::net::UdpSocket>>,
    sink_addr: std::net::SocketAddr,
}

impl TpuClient {
    pub async fn new(real_udp: bool) -> anyhow::Result<Self> {
        let (socket, sink_addr) = if real_udp {
            // A local blackhole socket stands in for the validator's TPU port.
            let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
            let sink_addr = sink.local_addr()?;
            // Drain the sink so the kernel buffer does not fill and start
            // reporting errors that would distort the measurement.
            tokio::spawn(async move {
                let mut buf = vec![0u8; 2048];
                while sink.recv_from(&mut buf).await.is_ok() {}
            });
            let tx = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
            (Some(Arc::new(tx)), sink_addr)
        } else {
            (None, "127.0.0.1:1".parse().unwrap())
        };
        Ok(TpuClient {
            sends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            socket,
            sink_addr,
        })
    }

    /// Fire one packet at one leader. Never fails the caller: a dropped UDP
    /// packet is a normal, expected event in a sprayer.
    #[inline]
    pub async fn send(&self, _leader: LeaderId, payload: &[u8]) {
        self.sends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
        if let Some(sock) = &self.socket {
            let _ = sock.send_to(payload, self.sink_addr).await;
        }
    }

    pub fn total_sends(&self) -> u64 {
        self.sends.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Registration handed back when a transaction subscribes to the feed.
pub struct LandingSubscription {
    rx: oneshot::Receiver<LandingNotice>,
    tx_id: String,
    feed: Arc<ConfirmationFeed>,
}

#[derive(Debug, Clone, Copy)]
pub struct LandingNotice {
    pub slot: u64,
    pub outcome: Outcome,
}

impl LandingSubscription {
    /// Wait for the cluster to report this transaction landing. Resolves only
    /// when the feed fires; callers race it against their own deadline.
    pub async fn recv(self) -> Option<LandingNotice> {
        // Deregistration on drop is handled below; take the receiver out first.
        let LandingSubscription { rx, tx_id, feed } = self;
        match rx.await {
            Ok(n) => Some(n),
            Err(_) => {
                feed.deregister(&tx_id);
                None
            }
        }
    }
}

struct PendingLanding {
    tx_id: String,
    outcome: Outcome,
    waiter: oneshot::Sender<LandingNotice>,
}

/// One task drives landing notifications for every in-flight transaction.
///
/// Transactions are bucketed by the slot they land in, so each tick only walks
/// the transactions that are actually due. This keeps the mock at O(landings)
/// per slot rather than O(in-flight) per poll.
pub struct ConfirmationFeed {
    by_slot: Mutex<HashMap<u64, Vec<PendingLanding>>>,
    index: Mutex<HashMap<String, u64>>,
    landings: AtomicU64,
}

impl ConfirmationFeed {
    pub fn new() -> Arc<Self> {
        Arc::new(ConfirmationFeed {
            by_slot: Mutex::new(HashMap::new()),
            index: Mutex::new(HashMap::new()),
            landings: AtomicU64::new(0),
        })
    }

    /// Start the background ticker that publishes landings as slots advance.
    pub fn spawn(self: &Arc<Self>, clock: Arc<SlotClock>) {
        let feed = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(clock.slot_ms() / 4));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut published_through = clock.current_slot();
            loop {
                ticker.tick().await;
                let now = clock.current_slot();
                while published_through <= now {
                    feed.publish_slot(published_through);
                    published_through += 1;
                }
            }
        });
    }

    fn publish_slot(&self, slot: u64) {
        let due = {
            let mut by_slot = self.by_slot.lock().unwrap();
            by_slot.remove(&slot)
        };
        let Some(due) = due else { return };
        let mut index = self.index.lock().unwrap();
        for p in due {
            index.remove(&p.tx_id);
            self.landings.fetch_add(1, Ordering::Relaxed);
            let _ = p.waiter.send(LandingNotice {
                slot,
                outcome: p.outcome,
            });
        }
    }

    /// Register interest in a transaction landing. Returns `None` if the
    /// transaction is fated never to land, so the caller can skip the wait
    /// entirely and just run out its deadline.
    ///
    /// Registration is idempotent per `tx_id` and tolerates a landing slot that
    /// is already in the past: after a crash the handler replays and
    /// re-subscribes with the original submission slot, and must still be told
    /// that the transaction landed while it was down.
    pub fn register(
        self: &Arc<Self>,
        tx_id: &str,
        submit_slot: u64,
        fate: Fate,
        current_slot: u64,
    ) -> Option<LandingSubscription> {
        let land_after = fate.land_after_slots?;
        let land_slot = submit_slot + land_after;
        let (tx, rx) = oneshot::channel();

        if land_slot <= current_slot {
            // Already happened. Resolve straight away rather than waiting for a
            // tick that will never come.
            self.landings.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(LandingNotice {
                slot: land_slot,
                outcome: fate.outcome,
            });
            return Some(LandingSubscription {
                rx,
                tx_id: tx_id.to_string(),
                feed: Arc::clone(self),
            });
        }

        // Drop any stale registration from a previous attempt of this handler.
        self.deregister(tx_id);
        {
            let mut by_slot = self.by_slot.lock().unwrap();
            by_slot.entry(land_slot).or_default().push(PendingLanding {
                tx_id: tx_id.to_string(),
                outcome: fate.outcome,
                waiter: tx,
            });
        }
        {
            let mut index = self.index.lock().unwrap();
            index.insert(tx_id.to_string(), land_slot);
        }
        Some(LandingSubscription {
            rx,
            tx_id: tx_id.to_string(),
            feed: Arc::clone(self),
        })
    }

    fn deregister(&self, tx_id: &str) {
        let slot = {
            let mut index = self.index.lock().unwrap();
            index.remove(tx_id)
        };
        let Some(slot) = slot else { return };
        let mut by_slot = self.by_slot.lock().unwrap();
        if let Some(bucket) = by_slot.get_mut(&slot) {
            bucket.retain(|p| p.tx_id != tx_id);
            if bucket.is_empty() {
                by_slot.remove(&slot);
            }
        }
    }

    pub fn total_landings(&self) -> u64 {
        self.landings.load(Ordering::Relaxed)
    }

    pub fn pending(&self) -> usize {
        self.index.lock().unwrap().len()
    }
}

/// Everything the service needs to talk to the "cluster".
pub struct Cluster {
    pub config: ClusterConfig,
    pub clock: Arc<SlotClock>,
    pub schedule: LeaderSchedule,
    pub tpu: TpuClient,
    pub feed: Arc<ConfirmationFeed>,
}

impl Cluster {
    pub async fn start(config: ClusterConfig) -> anyhow::Result<Arc<Self>> {
        let clock = Arc::new(SlotClock::new(config.slot_ms, 300_000_000));
        let feed = ConfirmationFeed::new();
        feed.spawn(Arc::clone(&clock));
        let tpu = TpuClient::new(config.real_udp).await?;
        Ok(Arc::new(Cluster {
            schedule: LeaderSchedule::new(config.num_leaders),
            config,
            clock,
            tpu,
            feed,
        }))
    }

    #[inline]
    pub fn current_slot(&self) -> u64 {
        self.clock.current_slot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fate_is_deterministic() {
        let cfg = ClusterConfig::default();
        for i in 0..1000 {
            let id = format!("tx-{i}");
            let a = cfg.fate(&id);
            let b = cfg.fate(&id);
            assert_eq!(a.outcome, b.outcome);
            assert_eq!(a.land_after_slots, b.land_after_slots);
        }
    }

    #[test]
    fn fate_distribution_is_roughly_configured() {
        let cfg = ClusterConfig::default();
        let mut confirmed = 0;
        let mut expired = 0;
        let n = 20_000;
        for i in 0..n {
            match cfg.fate(&format!("sig{i}")).outcome {
                Outcome::Confirmed => confirmed += 1,
                Outcome::Expired => expired += 1,
                _ => {}
            }
        }
        // 82% + 9% land, 6% expire.
        assert!(confirmed > n * 85 / 100, "confirmed={confirmed}");
        assert!(expired > n * 3 / 100, "expired={expired}");
    }

    #[test]
    fn leader_schedule_holds_four_slots() {
        let s = LeaderSchedule::new(1500);
        assert_eq!(s.leader_for_slot(100), s.leader_for_slot(103));
        // Rotation boundary at a multiple of SLOTS_PER_LEADER.
        assert_ne!(s.leader_for_slot(100), s.leader_for_slot(104));
    }
}
