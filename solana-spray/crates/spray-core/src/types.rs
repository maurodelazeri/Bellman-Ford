//! Wire types for a transaction submission and the lifecycle stages every
//! submission walks through.
//!
//! The stage list is deliberately closed and totally ordered: the web UI, the
//! ClickHouse schema and the completeness verifier all key off `Stage as u8`.
//! A transaction's event trail is complete iff its emitted stages are a
//! contiguous prefix-compatible walk that ends in [`Stage::Settled`].

use serde::{Deserialize, Serialize};

/// Where the submission is being processed. One Restate deployment per region
/// in the "regional" topology, one shared deployment in the "global" topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Region {
    Us,
    Eu,
    Asia,
}

impl Region {
    pub fn as_str(self) -> &'static str {
        match self {
            Region::Us => "us",
            Region::Eu => "eu",
            Region::Asia => "asia",
        }
    }

    pub fn parse(s: &str) -> Option<Region> {
        match s {
            "us" => Some(Region::Us),
            "eu" => Some(Region::Eu),
            "asia" => Some(Region::Asia),
            _ => None,
        }
    }

    pub const ALL: [Region; 3] = [Region::Us, Region::Eu, Region::Asia];
}

/// Commitment the submitter wants to reach before we call the transaction done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

impl Commitment {
    /// Extra slots we must observe on top of the landing slot before the
    /// transaction counts as having reached this commitment.
    pub fn slots_after_landing(self) -> u64 {
        match self {
            Commitment::Processed => 0,
            Commitment::Confirmed => 1,
            Commitment::Finalized => 31,
        }
    }
}

/// How aggressively to spray the signed transaction at upcoming leaders.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SprayPolicy {
    /// How many leaders ahead of the current one to hit on every tick.
    /// Real-world values are 2-4; Jito/Helius-style senders use 2.
    pub leaders_ahead: u8,
    /// Interval between spray ticks, in milliseconds. Solana slots are ~400ms,
    /// so 100-200ms means every leader gets several shots.
    pub interval_ms: u64,
    /// Hard cap on TPU sends, as a runaway guard.
    pub max_sends: u32,
}

impl Default for SprayPolicy {
    fn default() -> Self {
        SprayPolicy {
            leaders_ahead: 2,
            interval_ms: 100,
            max_sends: 4_000,
        }
    }
}

/// Optional webhook fired once the transaction reaches a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub url: String,
    /// Give up after this many attempts and record `webhook_failed` rather than
    /// blocking the record from settling.
    pub max_attempts: u32,
}

/// Membership of a transaction in a user-defined bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRef {
    pub bundle_id: String,
    pub index: u32,
}

/// How a bundle's members relate to each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BundleMode {
    /// Run member N+1 only if member N landed successfully. Aborts the tail on
    /// the first failure.
    Sequential,
    /// Fire every member at once, regardless of individual outcomes.
    AllAtOnce,
}

/// A single signed transaction handed to us by a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitRequest {
    /// Base58 signature; doubles as the Restate idempotency key / workflow key.
    pub tx_id: String,
    /// The signed, serialized transaction. We never decode or re-sign it.
    pub payload: String,
    pub region: Region,
    /// How many slots old the recent blockhash already was when the client
    /// signed. Expressed as an age rather than an absolute slot on purpose: the
    /// client and the sprayer do not share a slot clock, and making the client
    /// guess ours is how you get transactions that expire on arrival.
    #[serde(default)]
    pub blockhash_age_slots: u64,
    /// Blockhash validity in slots. Solana's real value is 150 (~60s); we let
    /// callers stretch it so we can exercise the full two-minute lifecycle.
    pub validity_slots: u64,
    pub commitment: Commitment,
    pub spray: SprayPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<Webhook>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle: Option<BundleRef>,
    /// Client-side submit timestamp in microseconds since the epoch, used to
    /// compute true end-to-end latency without a clock round trip.
    #[serde(default)]
    pub client_ts_micros: u64,
}

/// A bundle of transactions submitted as one unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRequest {
    pub bundle_id: String,
    pub mode: BundleMode,
    pub members: Vec<SubmitRequest>,
}

/// The closed set of lifecycle stages. Ordering matters: the verifier requires
/// the emitted `seq` values to be strictly increasing, and terminal records to
/// end at `Settled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Stage {
    /// Accepted at the edge, durably recorded, not yet validated.
    Accepted = 0,
    /// Structurally valid, expiry deadline computed.
    Validated = 1,
    /// Actively spraying at leaders.
    Spraying = 2,
    /// Observed in a block (any commitment).
    Landed = 3,
    /// Reached the commitment the submitter asked for.
    Confirmed = 4,
    /// Blockhash deadline passed without ever landing.
    Expired = 5,
    /// Landed but the runtime returned an error.
    FailedOnChain = 6,
    /// Neither landed nor expired: we lost track of it and gave up.
    Dropped = 7,
    /// Webhook delivery attempted (success or permanent failure).
    WebhookSettled = 8,
    /// Record closed. Always the last event for a transaction.
    Settled = 9,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Accepted => "accepted",
            Stage::Validated => "validated",
            Stage::Spraying => "spraying",
            Stage::Landed => "landed",
            Stage::Confirmed => "confirmed",
            Stage::Expired => "expired",
            Stage::FailedOnChain => "failed_on_chain",
            Stage::Dropped => "dropped",
            Stage::WebhookSettled => "webhook_settled",
            Stage::Settled => "settled",
        }
    }

    /// True for the stages that close out the on-chain part of the lifecycle.
    pub fn is_chain_terminal(self) -> bool {
        matches!(
            self,
            Stage::Confirmed | Stage::Expired | Stage::FailedOnChain | Stage::Dropped
        )
    }
}

/// What actually happened to the transaction on chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Confirmed,
    Expired,
    FailedOnChain,
    Dropped,
}

impl Outcome {
    pub fn stage(self) -> Stage {
        match self {
            Outcome::Confirmed => Stage::Confirmed,
            Outcome::Expired => Stage::Expired,
            Outcome::FailedOnChain => Stage::FailedOnChain,
            Outcome::Dropped => Stage::Dropped,
        }
    }

    pub fn landed(self) -> bool {
        matches!(self, Outcome::Confirmed)
    }
}

/// Result of the spray phase, journaled as one unit in `lean` mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SprayReport {
    pub outcome: Outcome,
    /// Slot the transaction landed in, if it landed at all.
    pub landed_slot: Option<u64>,
    /// TPU packets actually put on the wire.
    pub sends: u32,
    /// Leaders touched.
    pub leaders_hit: u32,
    /// Wall-clock duration of the spray phase.
    pub elapsed_ms: u64,
    /// Slot the spray phase gave up at.
    pub final_slot: u64,
}

/// The value a completed transaction lifecycle returns to its caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxResult {
    pub tx_id: String,
    pub outcome: Outcome,
    pub landed_slot: Option<u64>,
    pub sends: u32,
    /// Number of durable journal steps this lifecycle actually took. Reported
    /// so benchmarks can correlate throughput against journal pressure.
    pub journal_steps: u32,
    pub total_ms: u64,
    pub webhook_delivered: Option<bool>,
}

/// Result of running a bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleResult {
    pub bundle_id: String,
    pub mode: BundleMode,
    /// Populated for sequential bundles, where the orchestrator waits for each
    /// member before deciding whether to run the next.
    pub members: Vec<TxResult>,
    /// Populated for all-at-once bundles: dispatch is one-way, so the bundle
    /// reports which signatures it handed off, and each member's own trail
    /// carries the outcome.
    pub dispatched: Vec<String>,
    /// Index of the member whose failure stopped a sequential bundle.
    pub aborted_after: Option<u32>,
}
