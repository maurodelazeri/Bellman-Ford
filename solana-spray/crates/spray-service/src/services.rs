//! The Restate-facing surface: two shapes of the same lifecycle, plus bundles.
//!
//! We register both shapes so a benchmark can compare them directly:
//!
//! * [`TxWorkflow`] — a keyed workflow. Restate deduplicates by key, keeps K/V
//!   state, and exposes shared handlers other callers can hit while the run is
//!   in flight. That is what powers a "what is my transaction doing right now"
//!   UI without a database. It costs state writes and a completion record that
//!   lingers for the retention period.
//! * [`TxService`] — a plain service. Deduplication comes from the caller's
//!   idempotency key instead, there is no K/V state, and the journal is
//!   discardable the moment the invocation ends. Cheapest possible shape.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use restate_sdk::prelude::*;
use spray_core::types::{BundleMode, BundleRequest, BundleResult, SubmitRequest, TxResult};

use crate::lifecycle_impl::Shared;

/// Keyed workflow: one instance per transaction signature.
pub struct TxWorkflow(pub Arc<Shared>);

#[restate_sdk::workflow]
impl TxWorkflow {
    /// Drive the transaction from acceptance to settlement. Runs exactly once
    /// per signature: a duplicate submission attaches to the running instance
    /// instead of double-spraying.
    #[handler]
    async fn run(
        &self,
        ctx: WorkflowContext<'_>,
        req: Json<SubmitRequest>,
    ) -> Result<Json<TxResult>, HandlerError> {
        let req = req.into_inner();

        // A tiny amount of K/V state so the shared `status` handler can answer
        // without touching the journal. This is the state a live UI reads.
        ctx.set("tx_id", req.tx_id.clone());
        ctx.set("region", req.region.as_str().to_string());

        let res = self.0.run_lifecycle_workflow(&ctx, req).await?;
        ctx.set("outcome", format!("{:?}", res.outcome));
        Ok(Json(res))
    }

    /// Read the live state of an in-flight transaction. Concurrent with `run`,
    /// so a status poll never blocks the spray.
    #[handler(journal_retention = "5m")]
    async fn status(
        &self,
        ctx: SharedWorkflowContext<'_>,
    ) -> Result<Json<serde_json::Value>, HandlerError> {
        let tx_id: Option<String> = ctx.get("tx_id").await?;
        let region: Option<String> = ctx.get("region").await?;
        let outcome: Option<String> = ctx.get("outcome").await?;
        Ok(Json(serde_json::json!({
            "key": ctx.key(),
            "tx_id": tx_id,
            "region": region,
            "outcome": outcome,
            "state": if outcome.is_some() { "settled" } else if tx_id.is_some() { "in_flight" } else { "unknown" },
        })))
    }
}

/// Plain service: same lifecycle, no keying, no retained state.
pub struct TxService(pub Arc<Shared>);

#[restate_sdk::service]
impl TxService {
    /// Submit a transaction. Callers pass an `idempotency-key` header equal to
    /// the signature to get the same dedup guarantee the workflow gives for
    /// free, without paying for a retained completion record.
    #[handler]
    async fn submit(
        &self,
        ctx: Context<'_>,
        req: Json<SubmitRequest>,
    ) -> Result<Json<TxResult>, HandlerError> {
        let res = self.0.run_lifecycle_service(&ctx, req.into_inner()).await?;
        Ok(Json(res))
    }
}

/// User-defined bundles. Solana has no native multi-transaction atomicity
/// outside Jito, so "bundle" here means a caller-specified ordering policy that
/// we enforce.
pub struct BundleService(pub Arc<Shared>);

#[restate_sdk::service]
impl BundleService {
    #[handler]
    async fn execute(
        &self,
        ctx: Context<'_>,
        req: Json<BundleRequest>,
    ) -> Result<Json<BundleResult>, HandlerError> {
        let req = req.into_inner();
        let mut members = Vec::with_capacity(req.members.len());
        let mut dispatched = Vec::new();
        let mut aborted_after = None;

        match req.mode {
            // "Run this, and only if it lands, run the next one." Each member is
            // a durable call: Restate remembers which members already ran, so a
            // crash mid-bundle resumes at the right member rather than
            // re-broadcasting the ones that already landed.
            BundleMode::Sequential => {
                for (i, m) in req.members.into_iter().enumerate() {
                    let tx_id = m.tx_id.clone();
                    let res: TxResult = ctx
                        .workflow_client::<TxWorkflowClient>(tx_id)
                        .run(Json(m))
                        .call()
                        .await?
                        .into_inner();
                    let landed = res.outcome.landed();
                    members.push(res);
                    if !landed {
                        aborted_after = Some(i as u32);
                        break;
                    }
                }
            }
            // "Just run these three, whatever happens." Dispatch is one-way, so
            // the bundle handler completes in microseconds and each member owns
            // its own lifecycle and its own event trail from then on.
            BundleMode::AllAtOnce => {
                for m in req.members.into_iter() {
                    let tx_id = m.tx_id.clone();
                    ctx.workflow_client::<TxWorkflowClient>(tx_id.clone())
                        .run(Json(m))
                        .send();
                    dispatched.push(tx_id);
                }
            }
        }

        self.0.stats.bundles_completed.fetch_add(1, Ordering::Relaxed);
        Ok(Json(BundleResult {
            bundle_id: req.bundle_id,
            mode: req.mode,
            members,
            dispatched,
            aborted_after,
        }))
    }
}
