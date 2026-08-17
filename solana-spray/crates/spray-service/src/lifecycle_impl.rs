//! Where the durability boundary is drawn.
//!
//! One shared implementation backs both service shapes we benchmark
//! (`TxWorkflow`, a keyed Restate workflow, and `TxService`, a plain service
//! deduplicated by idempotency key), so the two differ only in the Restate
//! primitive, never in the workload.
//!
//! ## The rule
//!
//! A `ctx.run` is a replicated, fsync'd append to Restate's log. It costs
//! roughly a millisecond and it is the resource that runs out first. So a step
//! becomes durable only when re-executing it would be *wrong*, not merely
//! wasteful:
//!
//! | Action | Durable? | Why |
//! |---|---|---|
//! | Compute the expiry deadline | yes | `submit_slot` must not move on replay, or the deadline shifts under us |
//! | Spray at leaders for two minutes | no | a signed transaction is idempotent on chain; re-spraying after a crash is correct and free |
//! | Emit a lifecycle event | no | events carry a deterministic `(tx_id, seq)`; the sink dedups |
//! | Wait out commitment lag | no (configurable) | it is a 400ms wall-clock wait; a durable timer costs more than the wait |
//! | Record the spray outcome | yes | this is the fact everything downstream branches on |
//! | Deliver the webhook | yes | not idempotent at the receiver; needs at-least-once with bounded retries |
//!
//! `JournalMode` moves the spray phase across that line so the cost of being
//! wrong about it can be measured instead of argued about.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use restate_sdk::prelude::*;
use spray_core::events::{EventSink, EventTrail};
use spray_core::hist::LogHistogram;
use spray_core::solana::now_micros;
use spray_core::lifecycle::{
    deliver_webhook, JournalMode, SprayEngine, SprayProgress, Validation, WindowOutcome,
};
use spray_core::types::{Outcome, SprayReport, Stage, SubmitRequest, TxResult};
use spray_core::AppConfig;

/// Counters exposed on the side HTTP port. These are what tell us whether the
/// system is keeping up, and how much work replay is costing us.
#[derive(Default)]
pub struct ServiceStats {
    /// Handler bodies entered. Includes replays.
    pub handler_entries: AtomicU64,
    /// Lifecycles that reached `Settled`.
    pub completed: AtomicU64,
    /// `ctx.run` closures that actually executed (as opposed to being replayed
    /// from the journal). `handler_entries - lifecycles_started` is replay.
    pub durable_steps_executed: AtomicU64,
    /// Durable steps recorded across all completed lifecycles.
    pub durable_steps_journaled: AtomicU64,
    pub in_flight: AtomicU64,
    pub landed: AtomicU64,
    pub expired: AtomicU64,
    pub failed_on_chain: AtomicU64,
    pub dropped: AtomicU64,
    pub webhooks_ok: AtomicU64,
    pub webhooks_failed: AtomicU64,
    pub bundles_completed: AtomicU64,
    /// Sum of end-to-end lifecycle latency, for a cheap running mean.
    pub total_latency_micros: AtomicU64,

    /// **The number that decides whether this architecture is viable.**
    /// Client submit -> first TPU packet on the wire. A Solana slot is 400ms,
    /// so every millisecond here is a quarter of a percent of a leader's turn.
    pub time_to_first_send: LogHistogram,
    /// Client submit -> handler body entered. Everything Restate does before
    /// our code runs: ingress, durable append, partition routing, invoker
    /// dispatch.
    pub time_to_handler: LogHistogram,
    /// Duration of the `validate` durable step, i.e. the cost of one journal
    /// append as observed from inside a handler.
    pub durable_step_latency: LogHistogram,
}

pub struct Shared {
    pub engine: Arc<SprayEngine>,
    pub sink: Arc<EventSink>,
    pub cfg: AppConfig,
    pub stats: Arc<ServiceStats>,
}

/// The lifecycle body is identical for both service shapes, but Rust cannot
/// currently express "generic over any Restate context" here: the SDK's handler
/// macros require the handler future to be `Send` for *any* lifetime, and a
/// generic `C: ContextSideEffects<'ctx>` bound cannot satisfy that
/// higher-ranked requirement (rust-lang/rust#100013). So the body is written
/// once and instantiated per concrete context type.
macro_rules! impl_lifecycle {
    ($ctx:ty, $run:ident, $spray:ident) => {
impl Shared {

    /// Run one transaction's full lifecycle.
    ///
    /// Two lifetimes, not one: `'ctx` is the lifetime Restate's internal context
    /// is borrowed for, `'a` is our own borrow of the handler's local context
    /// value. Collapsing them would require the handler's local to live as long
    /// as the invocation, which it does not.
    pub async fn $run(
        &self,
        ctx: &$ctx,
        req: SubmitRequest,
    ) -> Result<TxResult, HandlerError> {
        let stats = &self.stats;
        stats.handler_entries.fetch_add(1, Ordering::Relaxed);

        let trail = EventTrail {
            tx_id: req.tx_id.clone(),
            region: req.region,
            invocation_id: ctx.invocation_id().to_string(),
        };
        let emit_all = self.cfg.emit_all_stages;
        let mut journal_steps: u32 = 0;

        if req.client_ts_micros > 0 {
            stats
                .time_to_handler
                .record(now_micros().saturating_sub(req.client_ts_micros));
        }

        // ---- Fast path: packets before paperwork ---------------------------
        // Nothing durable has happened yet, and that is deliberate. The first
        // burst is idempotent, so there is no state to protect, and the leader
        // for the current slot stops caring in 400ms. Journaling first would
        // spend a chunk of that window on an fsync.
        if self.cfg.fast_path_first_send {
            self.engine.first_burst(&req).await;
            if req.client_ts_micros > 0 {
                stats
                    .time_to_first_send
                    .record(now_micros().saturating_sub(req.client_ts_micros));
            }
        }

        // The invocation is durable from the moment Restate accepted it, so the
        // acceptance event is truthful before we do any work.
        if emit_all {
            self.sink
                .emit(trail.event(Stage::Accepted, 0, journal_steps, None));
        }

        // ---- Durable step 1: pin the deadline -------------------------------
        // `submit_slot` is wall-clock-derived and must survive replay, otherwise
        // a crash at minute one would silently grant the transaction a fresh
        // two-minute lease.
        let engine = Arc::clone(&self.engine);
        let vreq = req.clone();
        let stats_c = Arc::clone(stats);
        let step_t0 = std::time::Instant::now();
        let validation: Validation = ctx
            .run(move || async move {
                stats_c.durable_steps_executed.fetch_add(1, Ordering::Relaxed);
                match engine.validate(&vreq) {
                    Ok(v) => Ok(Json(v)),
                    Err(e) => Err(TerminalError::new(e).into()),
                }
            })
            .name("validate")
            .await?
            .into_inner();
        journal_steps += 1;
        stats
            .durable_step_latency
            .record(step_t0.elapsed().as_micros() as u64);

        // Without the fast path, the first packet only goes out once the spray
        // step starts — one durable append later. Record it there so the two
        // configurations are measured the same way.
        if !self.cfg.fast_path_first_send && req.client_ts_micros > 0 {
            self.engine.first_burst(&req).await;
            stats
                .time_to_first_send
                .record(now_micros().saturating_sub(req.client_ts_micros));
        }

        if emit_all {
            self.sink.emit(trail.event(
                Stage::Validated,
                validation.submit_slot,
                journal_steps,
                Some(format!("expiry_slot={}", validation.expiry_slot)),
            ));
            self.sink.emit(trail.event(
                Stage::Spraying,
                validation.submit_slot,
                journal_steps,
                None,
            ));
        }

        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        let report = self
            .$spray(ctx, &req, validation, &trail, &mut journal_steps)
            .await;
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        let report = report?;

        match report.outcome {
            Outcome::Confirmed => stats.landed.fetch_add(1, Ordering::Relaxed),
            Outcome::Expired => stats.expired.fetch_add(1, Ordering::Relaxed),
            Outcome::FailedOnChain => stats.failed_on_chain.fetch_add(1, Ordering::Relaxed),
            Outcome::Dropped => stats.dropped.fetch_add(1, Ordering::Relaxed),
        };

        if emit_all {
            self.sink.emit(trail.event(
                report.outcome.stage(),
                report.landed_slot.unwrap_or(report.final_slot),
                journal_steps,
                Some(format!(
                    "sends={} leaders={} ms={}",
                    report.sends, report.leaders_hit, report.elapsed_ms
                )),
            ));
        }

        // ---- Durable step: the webhook, only if one was asked for -----------
        let webhook_delivered = if let Some(hook) = req.webhook.clone() {
            let body = serde_json::to_vec(&serde_json::json!({
                "tx_id": req.tx_id,
                "outcome": report.outcome,
                "landed_slot": report.landed_slot,
                "sends": report.sends,
            }))
            .unwrap_or_default();

            let stats_c = Arc::clone(stats);
            let url = hook.url.clone();
            let res: Result<bool, TerminalError> = ctx
                .run(move || async move {
                    stats_c.durable_steps_executed.fetch_add(1, Ordering::Relaxed);
                    match deliver_webhook(&url, &body).await {
                        Ok(_) => Ok(true),
                        // A non-2xx or a connection error is retryable; the
                        // run's own policy bounds it and turns exhaustion into
                        // a recorded failure rather than wedging the record.
                        Err(e) => Err(HandlerError::from(e)),
                    }
                })
                .name("webhook")
                .retry_policy(
                    RunRetryPolicy::new()
                        .initial_delay(Duration::from_millis(200))
                        .exponentiation_factor(2.0)
                        .max_delay(Duration::from_secs(5))
                        .max_attempts(hook.max_attempts.max(1)),
                )
                .await;
            journal_steps += 1;

            let ok = res.is_ok();
            if ok {
                stats.webhooks_ok.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.webhooks_failed.fetch_add(1, Ordering::Relaxed);
            }
            if emit_all {
                self.sink.emit(trail.event(
                    Stage::WebhookSettled,
                    report.final_slot,
                    journal_steps,
                    Some(if ok { "delivered" } else { "gave_up" }.to_string()),
                ));
            }
            Some(ok)
        } else {
            None
        };

        // The settle event closes the record, and it is the one emission that
        // must be confirmed before we return.
        //
        // Everything earlier is covered by replay: if this invocation runs
        // again, it re-emits the whole trail with identical keys. The tail has
        // no such cover, because a completed invocation never runs again — so
        // an event still buffered in the writer when the process dies would be
        // lost for good. Restate does not mark the invocation complete until
        // this handler returns, so waiting here converts that at-most-once tail
        // into at-least-once, and costs no journal append.
        //
        // On timeout we fail rather than return a truncated record: Restate
        // retries, the trail is re-emitted, and the dedup key absorbs it.
        let settled = trail.event(
            Stage::Settled,
            report.final_slot,
            journal_steps,
            Some(format!("outcome={:?}", report.outcome)),
        );
        if self.cfg.durable_settle {
            self.sink
                .emit_durable(settled, self.cfg.settle_confirm_timeout)
                .await
                .map_err(HandlerError::from)?;
        } else {
            self.sink.emit(settled);
        }

        let total_ms = report.elapsed_ms;
        stats.completed.fetch_add(1, Ordering::Relaxed);
        stats
            .durable_steps_journaled
            .fetch_add(journal_steps as u64, Ordering::Relaxed);
        if req.client_ts_micros > 0 {
            let e2e = spray_core::solana::now_micros().saturating_sub(req.client_ts_micros);
            stats.total_latency_micros.fetch_add(e2e, Ordering::Relaxed);
        }

        Ok(TxResult {
            tx_id: req.tx_id,
            outcome: report.outcome,
            landed_slot: report.landed_slot,
            sends: report.sends,
            journal_steps,
            total_ms,
            webhook_delivered,
        })
    }

    /// The spray phase, split into as many durable windows as `JournalMode`
    /// asks for.
    async fn $spray(
        &self,
        ctx: &$ctx,
        req: &SubmitRequest,
        validation: Validation,
        trail: &EventTrail,
        journal_steps: &mut u32,
    ) -> Result<SprayReport, HandlerError> {
        let checkpoint = self.cfg.journal_mode.checkpoint_slots();
        let mut progress = SprayProgress::default();
        let mut window_start = validation.submit_slot;
        let mut window_idx: u32 = 0;

        loop {
            let until = match checkpoint {
                // One window covering the whole blockhash lease.
                None => validation.expiry_slot,
                Some(slots) => (window_start + slots).min(validation.expiry_slot),
            };

            let engine = Arc::clone(&self.engine);
            let req_c = req.clone();
            let trail_c = EventTrail {
                tx_id: trail.tx_id.clone(),
                region: trail.region,
                invocation_id: trail.invocation_id.clone(),
            };
            let stats_c = Arc::clone(&self.stats);
            let steps_now = *journal_steps;
            let emit_window = self.cfg.emit_all_stages
                && self.cfg.journal_mode == JournalMode::Verbose;

            let outcome: WindowOutcome = ctx
                .run(move || async move {
                    stats_c.durable_steps_executed.fetch_add(1, Ordering::Relaxed);
                    let out = engine
                        .spray_window(
                            &req_c,
                            validation,
                            until,
                            progress,
                            &trail_c,
                            steps_now,
                            emit_window,
                        )
                        .await;
                    Ok(Json(out))
                })
                .name(if window_idx == 0 {
                    "spray"
                } else {
                    "spray_continue"
                })
                .await?
                .into_inner();
            *journal_steps += 1;
            window_idx += 1;

            match outcome {
                WindowOutcome::Terminal(report) => {
                    // Optionally pay for a durable timer on the commitment wait,
                    // so its cost shows up in the comparison.
                    if self.cfg.durable_commitment_wait && report.outcome == Outcome::Confirmed {
                        let lag = req.commitment.slots_after_landing();
                        if lag > 0 {
                            ctx.sleep(Duration::from_millis(lag * self.cfg.cluster.slot_ms))
                                .await?;
                            *journal_steps += 1;
                        }
                    }
                    return Ok(report);
                }
                WindowOutcome::Pending(p) => {
                    progress = p;
                    window_start = until;
                    if window_start >= validation.expiry_slot {
                        // Deadline reached without a landing.
                        return Ok(SprayReport {
                            outcome: Outcome::Expired,
                            landed_slot: None,
                            sends: progress.sends,
                            leaders_hit: progress.leaders_hit,
                            elapsed_ms: progress.elapsed_ms,
                            final_slot: progress.last_slot,
                        });
                    }
                }
            }
        }
    }

}
    };
}

impl_lifecycle!(Context<'_>, run_lifecycle_service, spray_phase_service);
impl_lifecycle!(WorkflowContext<'_>, run_lifecycle_workflow, spray_phase_workflow);
