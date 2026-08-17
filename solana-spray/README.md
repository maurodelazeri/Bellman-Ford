# solana-spray — Restate as the durability layer for a Solana transaction sender

A working Rust application, plus the benchmark harness used to answer one
question: **can Restate carry a low-latency Solana transaction sprayer, where
thousands of transactions are simultaneously alive, each with its own two-minute
lifecycle, and none may end up with a half-written event trail?**

Short answer from the runs in this repo: **yes, and the constraint is not what
you would expect.** Concurrency is nearly free. The cost is durable journal
appends per second, and the design controls that directly.

---

## What it does

A user hands us a signed transaction. We spray it at the current leader and the
next few, over and over, until it lands or its blockhash dies. Along the way
every transaction walks the same closed stage machine, and each stage is pushed
to Kafka/Redis so ClickHouse and the web UI can see it:

```
Accepted → Validated → Spraying → ┬─ Confirmed ──┐
                                  ├─ Expired ────┤→ [WebhookSettled] → Settled
                                  ├─ FailedOnChain
                                  └─ Dropped ────┘
```

Also implemented: user-defined bundles (`sequential` — run the next only if this
one lands; `all-at-once` — run them all regardless), optional per-transaction
webhooks with bounded retries, and a live `status` handler a UI can poll without
touching a database.

## Layout

```
crates/spray-core/      workload, free of any Restate dependency
  types.rs              wire types + the closed stage machine
  solana.rs             mock cluster: slot clock, leader schedule, TPU, confirmation feed
  lifecycle.rs          the hot spray loop and the fast path
  events.rs             lifecycle events + batched sink (stands in for Kafka)
  hist.rs               lock-free latency histogram
crates/spray-service/   the Restate endpoint
  lifecycle_impl.rs     ← where the durability boundary is drawn
  services.rs           TxWorkflow / TxService / BundleService
crates/spray-bench/     open-loop load generator, drain watcher, trail verifier
config/restate-bench.toml   tuned server profile, every setting explained
scripts/                bootstrap / up / sweep / crash-test / retention-test / bundle-test
results/                raw JSON from the runs quoted below
vendor/                 downloaded restate-server (gitignored)
.run/                   all runtime state: RocksDB, event files, logs (gitignored)
```

## Running it

Self-contained: `bootstrap.sh` downloads `restate-server` into `vendor/`, and
all runtime state (Restate's RocksDB, event files, logs) lives in `.run/`.
Nothing is installed system-wide and nothing is written outside this directory,
so `rm -rf .run vendor target` puts the machine back exactly as it was.

```bash
./scripts/bootstrap.sh             # fetch restate-server into vendor/
cargo build --release
./scripts/up.sh                    # restate-server + service + register

./scripts/sweep.sh my-run SPRAY_JOURNAL_MODE=lean -- --rate 1000 --duration-s 40
./scripts/crash-test.sh 400 24 2 5 # SIGKILL mid-flight, verify trails
./scripts/retention-test.sh 60     # prove Restate forgets settled work
./scripts/bundle-test.sh
python3 scripts/report.py
```

`bootstrap.sh` prefers the official release tarball and falls back to the
container image if GitHub releases are unreachable.

---

## The central design decision

A `ctx.run` is a replicated, fsync'd append to Restate's log. **Measured here:
~190µs of CPU and 2–7ms of latency each.** It is the resource that runs out
first, so a step becomes durable only when re-executing it would be *wrong*, not
merely wasteful.

| Action | Durable? | Why |
|---|---|---|
| Compute the expiry deadline | **yes** | `submit_slot` must not move on replay, or a crash silently grants a fresh lease |
| Spray at leaders for two minutes | **no** | a signed transaction is idempotent on chain; re-spraying after a crash is correct and free |
| Emit a lifecycle event | **no** | events carry a deterministic `(tx_id, seq)`; the sink dedups |
| Confirm the *final* event reached the sink | **yes**, but not journaled | replay covers every event except the last; see below |
| Wait out commitment lag | **no** | it is a 400ms wall-clock wait; a durable timer costs more than the wait |
| Record the spray outcome | **yes** | this is the fact everything downstream branches on |
| Deliver the webhook | **yes** | not idempotent at the receiver; needs at-least-once, bounded |

That is **2 journal steps per transaction**, whether the transaction lives 400ms
or two minutes.

### The event trail is complete *because* emission is not journaled

Every event's key is a pure function of `(tx_id, stage)`. When the service dies
and Restate replays the handler, the trail is re-emitted with byte-identical
keys and the sink collapses the duplicates. The completeness guarantee comes
from determinism, not from durability — so it costs nothing.

The verifier checks exactly this and reports duplicates separately from gaps.

### ...except the tail, which is a real but narrow window

That argument has a hole: **replay only covers an invocation that runs again.**
One that has already completed never will, so any of its events still sitting in
the writer's buffer when the process is killed are gone for good.

The window is demonstrated, in isolation and end to end:

* a unit test shows a plain `emit` leaves the event in memory, unwritten, until
  the batch flushes;
* with the sink batching over a 2s window and a SIGKILL mid-flight, one
  transaction in 8,000 lost its terminal *and* settled events, leaving a trail
  that stopped at `spraying` — the exact failure this exercise exists to
  prevent. Re-running with the fix on: 8,000 / 8,000.

The fix costs no journal appends. Restate does not mark an invocation complete
until its handler returns, so the handler waits for the sink to confirm the
`Settled` event *before* returning. Either the flush lands, or the handler never
returned and Restate replays and re-emits. On timeout the handler fails
deliberately, so Restate retries rather than letting a truncated record stand. A
transaction's events all hash to one sink shard, so one await at the end covers
its whole trail, and it is off the critical path — by then the transaction has
already landed or expired.

`SPRAY_DURABLE_SETTLE=0` restores the broken behaviour so the failure can be
reproduced rather than taken on faith.

#### Two corrections worth recording

I originally reported this section as "12,000 / 12,000 gapless, which proves the
trade is sound." Two things were wrong with that.

**The first failure I saw had a different cause than I assigned it.** A crash run
showed 13 transactions stuck at `spraying`, and I attributed it to buffer loss.
It was almost certainly the *verifier running too early*: the old wait loop
stopped after a single five-second quiet poll with `in_flight == 0`, which is
exactly the state that holds while Restate is backing off before re-dispatching
after a kill. The loop now requires several consecutive quiet polls *and* zero
non-completed invocations according to Restate itself, which is the only
authority on the question. Six subsequent runs with the durable settle disabled
never reproduced the 13.

**The buffer window was smaller than the config claimed.** `batch_linger_ms`
did nothing: the writer waited on `recv_many`, which returns as soon as *one*
message is available, so every "batch" was whatever happened to be queued at
that instant. The knob is now an explicit deadline around the gather, which is
both what the setting always claimed and what makes the durability window real
enough to test. That is why the loss only reproduces after fixing the batching:
before, the exposure was microseconds.

Net: the tail window is real and worth closing, and it is cheap to close. But it
was not the cause of the one failure I originally blamed it for.

### Fast path: packets before paperwork

The first TPU burst goes out *before* the first journal append. There is no
state to protect — the burst commits us to nothing, and the leader for the
current slot stops caring in 400ms. If the process dies immediately after, the
transaction expires on chain by itself, exactly as if we had never sent it.

Measured: **time-to-first-packet mean 2.45ms with the fast path, 6.84ms
without** — a durable append saved off the only deadline that is real.

---

## Results

Environment: **4 vCPU, 15GB RAM**, single machine running `restate-server`
1.7.3, the service, *and* the load generator together. Slot time 400ms,
blockhash lease 150 slots (~60s), 512-byte payloads, ~91% land / 6% expire /
3% fail on chain.

Because all three processes share four cores, absolute throughput here is a
floor, not a ceiling. CPU is attributed per process so the numbers extrapolate.

### 1. Concurrency is nearly free — this is the headline

| Test | Concurrent in-flight | Ingress ack p50 | Time-to-first-packet p50 | Outcome |
|---|---|---|---|---|
| `conc-allexpire` (every tx runs the full 60s) | **17,500** | 1.6ms | 4.1ms | 27,000/27,000 settled |
| `spike-3k` (300/s baseline, 3,000/s spikes) | 9,354 | 36ms | 66ms | 108,003/108,003 settled, 0 errors |

**17,500 simultaneously active workflows on four cores**, with submission
latency still at 1.6ms. Well past the 5–10K target.

The reason: in `lean` mode a transaction that is merely spraying costs Restate
*nothing*. It is not journaling, not being polled, not holding a timer. It costs
the service process one tokio task. Restate only does work at the two journal
steps.

**So "5–10K workflows alive at once" is the wrong thing to worry about. The
number that sizes the cluster is journal appends per second.**

### 2. Throughput and where the knee is

| Rate | Achieved | Ack p50 | Ack p99 | TTFP p50 | Durable step (mean) | Peak in-flight | Errors |
|---|---|---|---|---|---|---|---|
| 500/s | 500/s | 1.7ms | 17ms | 4.1ms | 5.5ms | 2,151 | 0 |
| 1,000/s | 999/s | 2.9ms | 25ms | 4.1ms | 7.3ms | 4,404 | 0 |
| 2,000/s | 1,997/s | 16.6ms | 132ms | 32.8ms | 36.9ms | 8,540 | 0 |

The knee on this box is between 1,000 and 2,000 tx/s — and it is a *CPU* knee,
not a Restate limit. At 2,000/s the three processes consumed 4.1 cores of a
4-core machine. Nothing was dropped or rejected at any rate; the system degrades
by queueing, which is the right failure mode.

### 3. What durability costs, in CPU

| Journal mode | Steps/tx | restate-server CPU | Cost per extra step |
|---|---|---|---|
| `lean` | 2.00 | 96.8s / 40k tx | — |
| `standard` | 2.29 | 104.0s / 40k tx | |
| `verbose` | 4.79 | 118.0s / 40k tx | **≈190µs CPU per journal append** |

Idle Restate costs 0.03 cores. Per-transaction cost *falls* as load rises
(3.17 → 2.45 → 1.46 millicore-s/tx from 500 → 2,000/s) because log-append and
command batching kick in — Restate gets more efficient under pressure.

**Sizing rule from these numbers:** `cores ≈ (tx/s × steps/tx × 0.19ms) +
(tx/s × 0.6ms service) + ~1 core overhead`. For 10,000 tx/s at 2 steps each:
**≈ 4 cores of Restate + ≈ 6 cores of service ≈ 10–12 cores.** One reasonably
sized machine per region.

### 4. Crash recovery — the requirement that matters

Every run below SIGKILLs the service mid-flight, restarts it, and then verifies
every transaction's event trail. "Gaps" counts trails missing a mandatory stage;
"dups" counts events re-emitted by replay, which is the expected and desired
signature of the design.

| Run | Submitted | Kills | Downtime | Complete trails | Gaps | Dups | Inconsistent |
|---|---|---|---|---|---|---|---|
| default settings | 9,600 | 2 | 5s | **9,600** | **0** | 7,647 | **0** |
| default settings ×3 earlier trials | 9,600 each | 2 | 5s | **9,600** each | **0** | ~7,300 | **0** |
| **75s outage** | 12,000 | 1 | **75s** | **12,000** | **0** | 3,104 | **0** |
| 40,000 tx, no crash | 40,001 | 0 | — | **40,001** | **0** | 0 | 0 |
| 2s sink batching, **`DURABLE_SETTLE=0`** | 8,000 | 1 | 5s | 7,999 | **1** | 2,378 | 0 |
| 2s sink batching, `DURABLE_SETTLE=1` | 8,000 | 1 | 5s | **8,000** | **0** | 2,393 | 0 |

The last two rows are the controlled pair described above: with the terminal
event unconfirmed, one transaction in 8,000 lost its outcome and settled events
and was left stopped at `spraying`. With the confirmation in place, none were.

The 75-second outage is the scenario from the brief verbatim: the system goes
dark, transactions expire on chain while it is down, and on restart they must
still walk to a final state rather than vanish. Expiry rose from 6.2% to 7.2% —
exactly the transactions that would have landed during the outage — and every
one produced a full trail ending in `settled`.

### 5. Retention — and a trap worth knowing about

Handler-level `journal_retention` / `workflow_retention` do what they claim, but
**only if you also lower `worker.cleanup-interval`, which defaults to one hour.**

With the default, 4,000 completed invocations were still sitting in Restate 5
minutes after settling with a 60s retention configured — no expiry at all. With
`cleanup-interval = "1m"`, the same 4,000 drained to **zero** within the
retention window.

Note on disk: RocksDB preallocates ~481MB regardless, so at this scale the
working set is invisible against that floor. The invocation count is the
meaningful signal.

### 6. Service shape, webhooks, bundles

- **`TxService` (plain service + idempotency key) vs `TxWorkflow` (keyed
  workflow):** service is ~5% cheaper (3.03 vs 3.17 millicore-s/tx) and ~0.5ms
  faster at the ingress. The workflow buys you Restate-side dedup on the
  signature and a live `status` handler. **The workflow is worth it** — 5% is
  not the reason to give up a free status API.
- **Webhooks:** 3,000/3,000 delivered at 20% attach rate; adds exactly 1 journal
  step to the transactions that use one (2.0 → 2.2 steps/tx average).
- **Sink batching:** fixing `batch_linger_ms` to be a real deadline took the
  file sink from 2.3 to 8.3 events per write, and cut its CPU roughly threefold
  (13.8s → 4.7s of write time per 200k events) with no drops and no unconfirmed
  terminal events.
- **Bundles:** sequential correctly aborts the tail on the first failure
  (`aborted_after: 1`); all-at-once dispatches in **32ms** and lets each member
  own its own lifecycle and trail.

### 7. Restate server tuning that mattered

Found by measurement, folded into `config/restate-bench.toml`:

| Setting | Default | Here | Effect |
|---|---|---|---|
| `worker.invoker.concurrent-invocations-limit` | 1,000 | 40,000 | **Critical.** At 1,000 everything past the cap queues, which looks exactly like "Restate is slow" while Restate is idle |
| `worker.invoker.inactivity-timeout` | 1m | 5m | `lean` mode has one durable step spanning the whole spray; under 2m Restate asks the handler to suspend mid-spray |
| `worker.cleanup-interval` | 1h | 1m | Without this, retention silently does nothing |
| `bifrost.local.writer-batch-commit-duration` | 0s | 0s (do **not** set) | Setting it to 1ms added **~2ms to every submission** for no throughput gain |
| `bifrost.default-provider` | replicated | local | Single node only; no measurable latency difference in these runs |

Latency floor decomposition at 20/s on an idle box: **2.4ms** end-to-end for a
durable `/send`, of which ~0.6ms is the WAL fsync (raw disk fsync p50 is 187µs)
and the rest is Restate's ingress → partition → log → ack pipeline.

---

## What this says about the architecture

**Restate is fast enough, and the ingress hop is not where your latency budget
goes.** 2.4ms to durably accept a transaction, 2.45ms to first packet on the
wire — against a 400ms slot. That is under 1% of a leader's turn.

**Do not put Restate on the spray loop.** Every design that journals per attempt
dies immediately: 1,200 ticks × 3 sends per transaction at 5,000 concurrent is
millions of appends per second. Journal the *decisions*, not the *attempts*.

**On one-global vs per-region:** these numbers are all single-region. The case
for per-region Restate is not throughput — it is that a durable append is one
fsync plus one network round trip, and a cross-continent round trip is 80–150ms.
That is 20–40 Solana slots per journal step. A global Restate orchestrating
US/EU/Asia would put a quarter-second on the critical path of every durable step.
**Run Restate in-region, next to the sprayer.** If a global view is needed, it
belongs in ClickHouse, which is already downstream and already off the critical
path.

**On the 5K–10K concurrency worry:** it is not the binding constraint. Provision
for journal appends per second, not for live workflows.

## Caveats — what these numbers are not

- Four cores, shared with the load generator. The 2,000/s knee is this box, not
  Restate.
- Single Restate node, `local` loglet, replication 1. A production cluster
  replicates appends, which adds a network round trip per journal step. Re-run
  the sweep with `default-provider = "replicated"` and real replication before
  sizing anything.
- The Solana cluster is mocked. TPU sends are counted (or written to a local UDP
  socket with `SPRAY_REAL_UDP=1`); no QUIC handshakes, no real leader schedule,
  no stake-weighted QoS. The mock is deliberately cheap so the measurement is of
  Restate, not of the mock.
- The event sink writes JSONL to local disk, not to a real Kafka broker.
  Producer latency and broker backpressure are not modelled.
- No cross-region test was run at all. The latency argument above is arithmetic
  from known RTTs, not a measurement.
