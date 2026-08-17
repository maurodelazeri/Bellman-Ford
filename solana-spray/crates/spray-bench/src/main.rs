//! Benchmark harness: load generator, drain watcher, completeness verifier and
//! a webhook sink, in one binary.

mod ingress;
mod load;
mod verify;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use spray_core::types::{Commitment, Region, SprayPolicy};

use ingress::{http1_get, IngressClient};
use load::{LoadConfig, Target};

#[derive(Parser)]
#[command(name = "spray-bench", about = "Load, drain and verify the Restate sprayer")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Drive load at the Restate ingress and report throughput and latency.
    Load(LoadArgs),
    /// Check event-trail completeness for a finished run.
    Verify {
        /// Sink path prefix, matching SPRAY_SINK_PATH.
        #[arg(long, default_value = ".run/service/events")]
        prefix: String,
    },
    /// Serve a webhook endpoint that 200s everything, for the callback path.
    WebhookSink {
        #[arg(long, default_value = "0.0.0.0:9091")]
        bind: String,
    },
    /// Poll the service stats port until every submitted lifecycle settles.
    Drain {
        #[arg(long, default_value = "127.0.0.1:9081")]
        stats: String,
        #[arg(long)]
        expect: u64,
        #[arg(long, default_value_t = 300)]
        timeout_s: u64,
    },
}

#[derive(Parser, Clone)]
struct LoadArgs {
    #[arg(long, default_value = "127.0.0.1:8080")]
    ingress: String,
    #[arg(long, default_value = "127.0.0.1:9081")]
    stats: String,
    /// Restate admin port, scraped for server-side counters.
    #[arg(long, default_value = "127.0.0.1:9070")]
    admin: String,
    /// `workflow` (keyed, dedup by Restate) or `service` (dedup by idempotency key).
    #[arg(long, default_value = "workflow")]
    target: String,
    /// Steady-state submissions per second.
    #[arg(long, default_value_t = 500)]
    rate: u64,
    #[arg(long, default_value_t = 60)]
    duration_s: u64,
    /// Rate during spikes. 0 disables spiking.
    #[arg(long, default_value_t = 0)]
    spike_rate: u64,
    #[arg(long, default_value_t = 10)]
    spike_secs: u64,
    #[arg(long, default_value_t = 30)]
    spike_every: u64,
    /// Percentage of submissions carrying a webhook callback.
    #[arg(long, default_value_t = 0)]
    webhook_pct: u32,
    #[arg(long, default_value = "http://127.0.0.1:9091/hook")]
    webhook_url: String,
    /// Blockhash lease in slots. 150 is Solana's real value (~60s at 400ms).
    #[arg(long, default_value_t = 150)]
    validity_slots: u64,
    /// Age of the blockhash at signing time, in slots. Real clients submit with
    /// a blockhash that is already a few slots stale.
    #[arg(long, default_value_t = 5)]
    blockhash_age_slots: u64,
    #[arg(long, default_value = "confirmed")]
    commitment: String,
    #[arg(long, default_value_t = 2)]
    leaders_ahead: u8,
    #[arg(long, default_value_t = 100)]
    spray_interval_ms: u64,
    /// Serialized transaction size in bytes. Real v0 transactions run 200-1232.
    #[arg(long, default_value_t = 512)]
    payload_bytes: usize,
    /// HTTP/2 connections to the ingress. Requests multiplex over these.
    #[arg(long, default_value_t = 8)]
    connections: usize,
    #[arg(long, default_value_t = 20_000)]
    max_in_flight_submits: usize,
    /// Wait for all lifecycles to settle after the load phase ends.
    #[arg(long, default_value_t = true)]
    drain: bool,
    #[arg(long, default_value_t = 420)]
    drain_timeout_s: u64,
    #[arg(long, default_value = "us,eu,asia")]
    regions: String,
    #[arg(long)]
    out: Option<String>,
    #[arg(long, default_value = "run")]
    run_id: String,
    /// Also verify the event trail at the end (requires SPRAY_SINK=file).
    #[arg(long)]
    verify_prefix: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::env::var("BENCH_THREADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        )
        .enable_all()
        .build()?;
    rt.block_on(async move {
        match cli.cmd {
            Cmd::Load(args) => run_load(args).await,
            Cmd::Verify { prefix } => {
                let r = verify::verify(&prefix)?;
                println!("{}", serde_json::to_string_pretty(&r.to_json())?);
                if !r.ok() {
                    std::process::exit(2);
                }
                Ok(())
            }
            Cmd::WebhookSink { bind } => webhook_sink(bind).await,
            Cmd::Drain {
                stats,
                expect,
                timeout_s,
            } => {
                let done = drain(&stats, expect, Duration::from_secs(timeout_s), None).await?;
                println!("{}", serde_json::to_string_pretty(&done)?);
                Ok(())
            }
        }
    })
}

/// A running sample of the system under load.
#[derive(Clone, serde::Serialize)]
struct Sample {
    t_s: f64,
    completed: u64,
    in_flight: u64,
    tpu_sends: u64,
    sink_emitted: u64,
    sink_dropped: u64,
    handler_entries: u64,
    durable_steps_executed: u64,
}

async fn scrape(stats_addr: &str) -> Option<serde_json::Value> {
    let body = http1_get(stats_addr, "/stats").await.ok()?;
    serde_json::from_slice(&body).ok()
}

fn sample_from(v: &serde_json::Value, t: f64) -> Sample {
    let g = |p: &[&str]| -> u64 {
        let mut cur = v;
        for k in p {
            cur = match cur.get(k) {
                Some(x) => x,
                None => return 0,
            };
        }
        cur.as_u64().unwrap_or(0)
    };
    Sample {
        t_s: t,
        completed: g(&["completed"]),
        in_flight: g(&["in_flight"]),
        tpu_sends: g(&["cluster", "tpu_sends"]),
        sink_emitted: g(&["sink", "emitted"]),
        sink_dropped: g(&["sink", "dropped"]),
        handler_entries: g(&["handler_entries"]),
        durable_steps_executed: g(&["durable_steps_executed"]),
    }
}

async fn run_load(args: LoadArgs) -> anyhow::Result<()> {
    let target = Target::parse(&args.target)
        .ok_or_else(|| anyhow::anyhow!("--target must be workflow|service"))?;
    let commitment = match args.commitment.as_str() {
        "processed" => Commitment::Processed,
        "finalized" => Commitment::Finalized,
        _ => Commitment::Confirmed,
    };
    let regions: Vec<Region> = args
        .regions
        .split(',')
        .filter_map(|s| Region::parse(s.trim()))
        .collect();
    anyhow::ensure!(!regions.is_empty(), "--regions produced no valid regions");

    let service_cfg = scrape(&args.stats)
        .await
        .and_then(|v| v.get("config").and_then(|c| c.as_str().map(String::from)))
        .unwrap_or_else(|| "<service stats unavailable>".to_string());

    let baseline = scrape(&args.stats).await;
    let baseline_completed = baseline
        .as_ref()
        .and_then(|v| v.get("completed").and_then(|c| c.as_u64()))
        .unwrap_or(0);

    println!("== spray-bench ==");
    println!("service: {service_cfg}");
    println!(
        "target={} rate={}/s spike={}/s duration={}s validity_slots={} payload={}B conns={}",
        target.label(),
        args.rate,
        args.spike_rate,
        args.duration_s,
        args.validity_slots,
        args.payload_bytes,
        args.connections
    );

    let client = IngressClient::connect(&args.ingress, args.connections).await?;

    // Sample the service while the load runs, so the report can show whether
    // throughput held or decayed rather than just an average.
    let samples = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Sample>::new()));
    let sampler = {
        let samples = std::sync::Arc::clone(&samples);
        let stats_addr = args.stats.clone();
        let t0 = Instant::now();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            loop {
                tick.tick().await;
                if let Some(v) = scrape(&stats_addr).await {
                    samples
                        .lock()
                        .await
                        .push(sample_from(&v, t0.elapsed().as_secs_f64()));
                }
            }
        })
    };

    let cfg = LoadConfig {
        target,
        rate: args.rate,
        duration: Duration::from_secs(args.duration_s),
        spike_rate: args.spike_rate,
        spike_secs: args.spike_secs,
        spike_every: args.spike_every,
        webhook_pct: args.webhook_pct,
        webhook_url: args.webhook_url.clone(),
        validity_slots: args.validity_slots,
        blockhash_age_slots: args.blockhash_age_slots,
        commitment,
        spray: SprayPolicy {
            leaders_ahead: args.leaders_ahead,
            interval_ms: args.spray_interval_ms,
            max_sends: 8_000,
        },
        payload_bytes: args.payload_bytes,
        regions,
        max_in_flight_submits: args.max_in_flight_submits,
        run_id: args.run_id.clone(),
    };

    let result = load::run(client, cfg).await?;
    let s = &result.stats;

    println!("\n-- submission phase ({:.1}s) --", result.wall.as_secs_f64());
    println!(
        "scheduled={} submitted={} acked={} rejected={} errors={} backpressured={}",
        s.scheduled.load(Ordering::Relaxed),
        s.submitted.load(Ordering::Relaxed),
        s.acked.load(Ordering::Relaxed),
        s.rejected.load(Ordering::Relaxed),
        s.errors.load(Ordering::Relaxed),
        s.backpressured.load(Ordering::Relaxed),
    );
    println!("achieved submit rate: {:.0}/s", result.achieved_rate);
    let h = &result.ack_hist;
    println!(
        "ingress ack latency (us): p50={} p90={} p99={} p999={} max={}",
        h.value_at_quantile(0.50),
        h.value_at_quantile(0.90),
        h.value_at_quantile(0.99),
        h.value_at_quantile(0.999),
        h.max()
    );
    println!("status codes: {:?}", result.status_codes);

    let expect = baseline_completed + s.acked.load(Ordering::Relaxed);
    let drain_report = if args.drain {
        println!("\n-- drain phase (waiting for lifecycles to settle) --");
        Some(
            drain(
                &args.stats,
                expect,
                Duration::from_secs(args.drain_timeout_s),
                Some(std::sync::Arc::clone(&samples)),
            )
            .await?,
        )
    } else {
        None
    };

    sampler.abort();
    let final_stats = scrape(&args.stats).await;

    let verify_report = match &args.verify_prefix {
        Some(p) => {
            println!("\n-- verifying event trails --");
            let r = verify::verify(p)?;
            println!("{}", serde_json::to_string_pretty(&r.to_json())?);
            Some(r.to_json())
        }
        None => None,
    };

    if let Some(fs) = &final_stats {
        println!("\n-- service final --");
        println!("{}", serde_json::to_string_pretty(fs)?);
    }

    if let Some(out) = &args.out {
        let samples = samples.lock().await.clone();
        let report = serde_json::json!({
            "target": target.label(),
            "service_config": service_cfg,
            "load": {
                "rate": args.rate,
                "spike_rate": args.spike_rate,
                "duration_s": args.duration_s,
                "connections": args.connections,
                "payload_bytes": args.payload_bytes,
                "validity_slots": args.validity_slots,
                "webhook_pct": args.webhook_pct,
                "scheduled": s.scheduled.load(Ordering::Relaxed),
                "acked": s.acked.load(Ordering::Relaxed),
                "rejected": s.rejected.load(Ordering::Relaxed),
                "errors": s.errors.load(Ordering::Relaxed),
                "backpressured": s.backpressured.load(Ordering::Relaxed),
                "achieved_rate": result.achieved_rate,
                "wall_s": result.wall.as_secs_f64(),
                "ack_latency_us": {
                    "p50": h.value_at_quantile(0.50),
                    "p90": h.value_at_quantile(0.90),
                    "p99": h.value_at_quantile(0.99),
                    "p999": h.value_at_quantile(0.999),
                    "max": h.max(),
                },
                "status_codes": result.status_codes,
            },
            "drain": drain_report,
            "final_service_stats": final_stats,
            "verify": verify_report,
            "samples": samples,
        });
        std::fs::write(out, serde_json::to_string_pretty(&report)?)?;
        println!("\nwrote {out}");
    }

    Ok(())
}

/// Wait for the service to report `expect` completed lifecycles.
async fn drain(
    stats_addr: &str,
    expect: u64,
    timeout: Duration,
    samples: Option<std::sync::Arc<tokio::sync::Mutex<Vec<Sample>>>>,
) -> anyhow::Result<serde_json::Value> {
    let t0 = Instant::now();
    let mut last_completed = 0u64;
    let mut last_progress = Instant::now();
    let mut peak_settle_rate = 0f64;
    // Seed from the current reading, not from zero: otherwise the first
    // interval reports the whole backlog as if it settled in one second.
    let mut prev = (
        Instant::now(),
        scrape(stats_addr)
            .await
            .and_then(|v| v.get("completed").and_then(|c| c.as_u64()))
            .unwrap_or(0),
    );

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(v) = scrape(stats_addr).await else {
            if t0.elapsed() > timeout {
                anyhow::bail!("stats endpoint unreachable during drain");
            }
            continue;
        };
        let completed = v.get("completed").and_then(|c| c.as_u64()).unwrap_or(0);
        let in_flight = v.get("in_flight").and_then(|c| c.as_u64()).unwrap_or(0);

        let dt = prev.0.elapsed().as_secs_f64();
        if dt >= 1.0 {
            let rate = (completed.saturating_sub(prev.1)) as f64 / dt;
            peak_settle_rate = peak_settle_rate.max(rate);
            prev = (Instant::now(), completed);
            if let Some(s) = &samples {
                s.lock().await.push(sample_from(&v, t0.elapsed().as_secs_f64()));
            }
            println!(
                "  t={:>5.0}s completed={completed}/{expect} in_flight={in_flight} settle_rate={rate:.0}/s",
                t0.elapsed().as_secs_f64()
            );
        }

        if completed > last_completed {
            last_completed = completed;
            last_progress = Instant::now();
        }
        if completed >= expect {
            return Ok(serde_json::json!({
                "drained": true,
                "completed": completed,
                "expected": expect,
                "drain_s": t0.elapsed().as_secs_f64(),
                "peak_settle_rate": peak_settle_rate,
            }));
        }
        if t0.elapsed() > timeout {
            return Ok(serde_json::json!({
                "drained": false,
                "reason": "timeout",
                "completed": completed,
                "expected": expect,
                "in_flight": in_flight,
                "stalled_for_s": last_progress.elapsed().as_secs_f64(),
                "drain_s": t0.elapsed().as_secs_f64(),
                "peak_settle_rate": peak_settle_rate,
            }));
        }
    }
}

/// Minimal webhook receiver: accept, count, 200.
async fn webhook_sink(bind: String) -> anyhow::Result<()> {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    let count = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("webhook sink listening on {bind}");
    {
        let count = Arc::clone(&count);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                println!("webhooks received: {}", count.load(Ordering::Relaxed));
            }
        });
    }
    loop {
        let (mut stream, _) = listener.accept().await?;
        let count = Arc::clone(&count);
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 8192];
            // Read one request; the sender closes after the response.
            let _ = stream.read(&mut buf).await;
            count.fetch_add(1, Ordering::Relaxed);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await;
            let _ = stream.shutdown().await;
        });
    }
}
