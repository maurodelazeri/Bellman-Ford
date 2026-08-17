//! A side HTTP port exposing counters.
//!
//! Deliberately separate from the Restate endpoint: the benchmark harness polls
//! it once a second, and we do not want that traffic sharing a connection pool
//! (or a measurement) with the invoker.

use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use crate::lifecycle_impl::Shared;

pub async fn serve(shared: Arc<Shared>, bind: String) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("stats server listening on {bind}");
    loop {
        let (stream, _) = listener.accept().await?;
        let shared = Arc::clone(&shared);
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                let shared = Arc::clone(&shared);
                async move { Ok::<_, Infallible>(handle(&shared, req.uri().path())) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });
    }
}

fn handle(shared: &Shared, path: &str) -> Response<Full<Bytes>> {
    match path {
        "/stats" => json(StatusCode::OK, snapshot(shared)),
        "/health" => json(StatusCode::OK, serde_json::json!({"ok": true})),
        "/config" => json(
            StatusCode::OK,
            serde_json::json!({ "summary": shared.cfg.summary() }),
        ),
        _ => json(StatusCode::NOT_FOUND, serde_json::json!({"error":"no route"})),
    }
}

fn json(status: StatusCode, v: serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(v.to_string())))
        .unwrap()
}

pub fn snapshot(shared: &Shared) -> serde_json::Value {
    let s = &shared.stats;
    let sink = shared.sink.stats.snapshot();
    let completed = s.completed.load(Ordering::Relaxed);
    let entries = s.handler_entries.load(Ordering::Relaxed);
    let journaled = s.durable_steps_journaled.load(Ordering::Relaxed);
    let total_lat = s.total_latency_micros.load(Ordering::Relaxed);

    serde_json::json!({
        "config": shared.cfg.summary(),
        "journal_mode": shared.cfg.journal_mode.as_str(),
        "handler_entries": entries,
        "completed": completed,
        "in_flight": s.in_flight.load(Ordering::Relaxed),
        // Handler bodies entered beyond the number of lifecycles that finished.
        // A large gap means Restate is replaying a lot, which is the signal that
        // the journal mode is too lean for the failure rate being seen.
        "durable_steps_executed": s.durable_steps_executed.load(Ordering::Relaxed),
        "durable_steps_journaled": journaled,
        "journal_steps_per_tx": if completed > 0 { journaled as f64 / completed as f64 } else { 0.0 },
        "mean_e2e_ms": if completed > 0 { (total_lat as f64 / completed as f64) / 1000.0 } else { 0.0 },
        "outcomes": {
            "landed": s.landed.load(Ordering::Relaxed),
            "expired": s.expired.load(Ordering::Relaxed),
            "failed_on_chain": s.failed_on_chain.load(Ordering::Relaxed),
            "dropped": s.dropped.load(Ordering::Relaxed),
        },
        "webhooks": {
            "ok": s.webhooks_ok.load(Ordering::Relaxed),
            "failed": s.webhooks_failed.load(Ordering::Relaxed),
        },
        "bundles_completed": s.bundles_completed.load(Ordering::Relaxed),
        // The latency triple that says where the time goes: what Restate costs
        // before our code runs, what one durable append costs, and what the
        // combination means for the only deadline that is real — the leader's.
        "latency": {
            "time_to_handler": s.time_to_handler.to_json(),
            "time_to_first_send": s.time_to_first_send.to_json(),
            "durable_step": s.durable_step_latency.to_json(),
        },
        "cluster": {
            "slot": shared.engine.cluster.current_slot(),
            "tpu_sends": shared.engine.cluster.tpu.total_sends(),
            "tpu_bytes": shared.engine.cluster.tpu.total_bytes(),
            "landings": shared.engine.cluster.feed.total_landings(),
            "feed_pending": shared.engine.cluster.feed.pending(),
        },
        "sink": {
            "emitted": sink.emitted,
            "written": sink.written,
            "dropped": sink.dropped,
            "batches": sink.batches,
            "write_micros": sink.write_micros,
            "unconfirmed": sink.unconfirmed,
        },
    })
}
