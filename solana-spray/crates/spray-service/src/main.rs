//! Restate service endpoint for the Solana transaction sprayer.

mod lifecycle_impl;
mod services;
mod stats_server;

use std::sync::Arc;

use restate_sdk::prelude::*;
use spray_core::events::EventSink;
use spray_core::lifecycle::SprayEngine;
use spray_core::solana::Cluster;
use spray_core::AppConfig;

use lifecycle_impl::{ServiceStats, Shared};
use services::{BundleService, TxService, TxWorkflow};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,spray_service=info".into()),
        )
        .init();

    let cfg = AppConfig::from_env();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.worker_threads)
        .enable_all()
        .build()?;
    rt.block_on(run(cfg))
}

async fn run(cfg: AppConfig) -> anyhow::Result<()> {
    tracing::info!("starting spray-service: {}", cfg.summary());

    let cluster = Cluster::start(cfg.cluster.clone()).await?;
    let sink = EventSink::start(cfg.sink.clone());
    let engine = SprayEngine::new(cluster, Arc::clone(&sink));
    let shared = Arc::new(Shared {
        engine,
        sink,
        stats: Arc::new(ServiceStats::default()),
        cfg: cfg.clone(),
    });

    {
        let shared = Arc::clone(&shared);
        let bind = cfg.stats_bind.clone();
        tokio::spawn(async move {
            if let Err(e) = stats_server::serve(shared, bind).await {
                tracing::error!("stats server failed: {e}");
            }
        });
    }

    // Options are set here rather than as macro attributes so a benchmark sweep
    // can move them without recompiling.
    //
    // `inactivity_timeout` is the one that bites: in `lean` mode a single
    // durable step spans the entire two-minute spray, and Restate's default
    // one-minute inactivity timeout would ask the handler to suspend right in
    // the middle of it.
    let workflow_opts = ServiceOptions::new()
        .inactivity_timeout(cfg.inactivity_timeout)
        .abort_timeout(cfg.abort_timeout)
        .journal_retention(cfg.journal_retention)
        .idempotency_retention(cfg.journal_retention)
        .handler(
            "run",
            HandlerOptions::new()
                .inactivity_timeout(cfg.inactivity_timeout)
                .abort_timeout(cfg.abort_timeout)
                .journal_retention(cfg.journal_retention)
                // The knob that keeps Restate a working set rather than an
                // archive: once a transaction has settled and its events are in
                // ClickHouse, Restate has no reason to remember it.
                .workflow_retention(cfg.workflow_retention),
        );

    let service_opts = ServiceOptions::new()
        .inactivity_timeout(cfg.inactivity_timeout)
        .abort_timeout(cfg.abort_timeout)
        .journal_retention(cfg.journal_retention)
        .idempotency_retention(cfg.journal_retention);

    let endpoint = Endpoint::builder()
        .bind(
            TxWorkflow(Arc::clone(&shared))
                .into_service_definition()
                .options(workflow_opts),
        )
        .bind(
            TxService(Arc::clone(&shared))
                .into_service_definition()
                .options(service_opts.clone()),
        )
        .bind(
            BundleService(Arc::clone(&shared))
                .into_service_definition()
                .options(service_opts),
        )
        .build();

    let addr = cfg.bind.parse()?;
    tracing::info!("restate endpoint listening on {addr}");
    HttpServer::new(endpoint).listen_and_serve(addr).await;
    Ok(())
}
