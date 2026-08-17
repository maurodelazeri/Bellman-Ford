#!/usr/bin/env python3
"""Attribute CPU seconds to restate-server, spray-service and spray-bench.

The three processes share a small machine, so a raw throughput number says
nothing about what the *service* would do on dedicated hardware. Measuring CPU
per process, and dividing by transactions completed, gives a figure that
extrapolates: "N millicores per transaction" is portable in a way that
"X transactions per second on this box" is not.

Usage:
  cpu.py start <statefile>
  cpu.py stop  <statefile> <completed_tx>
"""
import json
import os
import sys

CLK_TCK = os.sysconf("SC_CLK_TCK")
NAMES = ("restate-server", "spray-service", "spray-bench")


def procs():
    """Map process name -> (utime+stime) ticks, summed over matching pids."""
    out = {n: 0 for n in NAMES}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\x00", b" ").decode("utf8", "replace")
            with open(f"/proc/{pid}/stat") as f:
                stat = f.read()
        except OSError:
            continue
        for name in NAMES:
            if name in cmd:
                # utime and stime are fields 14 and 15, after the comm field
                # which may itself contain spaces inside parentheses.
                tail = stat[stat.rfind(")") + 2 :].split()
                out[name] += int(tail[11]) + int(tail[12])
                break
    return out


def main():
    mode, statefile = sys.argv[1], sys.argv[2]
    if mode == "start":
        with open(statefile, "w") as f:
            json.dump({"ticks": procs()}, f)
        return

    completed = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    with open(statefile) as f:
        before = json.load(f)["ticks"]
    after = procs()
    report = {}
    for name in NAMES:
        secs = (after[name] - before[name]) / CLK_TCK
        entry = {"cpu_seconds": round(secs, 2)}
        if completed:
            # Millicores held for one second per transaction; multiply by target
            # throughput to size a machine.
            entry["millicores_per_tx"] = round(secs * 1000.0 / completed, 4)
        report[name] = entry
    report["completed_tx"] = completed
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
