#!/usr/bin/env python3
"""Render one row per benchmark result file.

Reads `results/*.json` written by `spray-bench load` plus the matching
`*.cpu.json` written by `sweep.sh`, and prints a comparison table.
"""
import json
import os
import sys

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "results"
ONLY = sys.argv[2:] if len(sys.argv) > 2 else None


def load(name):
    with open(os.path.join(RESULTS, name)) as f:
        return json.load(f)


def get(d, *path, default=0):
    for k in path:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


rows = []
for fn in sorted(os.listdir(RESULTS)):
    if not fn.endswith(".json") or fn.endswith((".cpu.json", ".verify.json")):
        continue
    name = fn[:-5]
    if ONLY and name not in ONLY:
        continue
    d = load(fn)
    fs = d.get("final_service_stats") or {}
    cpu = {}
    cpu_path = os.path.join(RESULTS, name + ".cpu.json")
    if os.path.exists(cpu_path):
        with open(cpu_path) as f:
            cpu = json.load(f)
    samples = d.get("samples") or []
    peak_inflight = max([s["in_flight"] for s in samples], default=0)

    rows.append(
        {
            "name": name,
            "target": d.get("target", ""),
            "rate": get(d, "load", "rate"),
            "achieved": round(get(d, "load", "achieved_rate")),
            "acked": get(d, "load", "acked"),
            "errors": get(d, "load", "errors") + get(d, "load", "rejected"),
            "backpressured": get(d, "load", "backpressured"),
            "ack_p50": get(d, "load", "ack_latency_us", "p50"),
            "ack_p99": get(d, "load", "ack_latency_us", "p99"),
            "ttfs_p50": get(fs, "latency", "time_to_first_send", "p50_us"),
            "ttfs_p99": get(fs, "latency", "time_to_first_send", "p99_us"),
            "step_mean": get(fs, "latency", "durable_step", "mean_us"),
            "jsteps": get(fs, "journal_steps_per_tx"),
            "peak_inflight": peak_inflight,
            "completed": get(fs, "completed"),
            "drained": get(d, "drain", "drained", default=None),
            "peak_settle": round(get(d, "drain", "peak_settle_rate")),
            "sink_dropped": get(fs, "sink", "dropped"),
            "restate_mc": get(cpu, "restate-server", "millicores_per_tx"),
            "service_mc": get(cpu, "spray-service", "millicores_per_tx"),
            "bench_mc": get(cpu, "spray-bench", "millicores_per_tx"),
            "verify_ok": get(d, "verify", "ok", default=None),
        }
    )

hdr = [
    ("name", 22), ("target", 9), ("rate", 6), ("achv", 6), ("err", 5),
    ("bp", 6), ("ackp50", 8), ("ackp99", 9), ("ttfsp50", 9), ("ttfsp99", 10),
    ("step_us", 9), ("jstp", 6), ("peakIF", 8), ("done", 8), ("setl/s", 8),
    ("drop", 6), ("mc/tx", 7), ("ok", 5),
]
print("".join(h.ljust(w) for h, w in hdr))
print("-" * sum(w for _, w in hdr))
for r in rows:
    total_mc = round((r["restate_mc"] or 0) + (r["service_mc"] or 0), 3)
    cells = [
        r["name"], r["target"], r["rate"], r["achieved"], r["errors"],
        r["backpressured"], r["ack_p50"], r["ack_p99"], r["ttfs_p50"],
        r["ttfs_p99"], round(r["step_mean"]), round(r["jsteps"], 2), r["peak_inflight"],
        r["completed"], r["peak_settle"], r["sink_dropped"], total_mc,
        "-" if r["verify_ok"] is None else ("yes" if r["verify_ok"] else "NO"),
    ]
    print("".join(str(c).ljust(w) for c, (_, w) in zip(cells, hdr)))
