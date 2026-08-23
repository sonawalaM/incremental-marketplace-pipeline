"""One-command pipeline runner.

    python run.py                       # default interval, all steps
    python run.py --fresh               # tear down volumes and start clean
    python run.py --steps bronze,normalize
    python run.py --interval-start 2026-08-03T00:00:00+00:00 --interval-end 2026-08-04T00:00:00+00:00

Runs on the HOST (it drives `docker compose`), not inside the jobs container.

Writes two files:
  .runs/latest.log      full transcript, UTF-8, ANSI stripped
  .runs/summary.json    per-step status + every METRIC line the jobs emitted

The JSON is the point. Reviewing a run should mean reading a structured summary, not
scrolling several hundred lines of Spark warnings looking for a row count.

Note on encoding: PowerShell's `Out-File` writes UTF-16LE, which reads as a binary blob to
most tooling. This writes UTF-8 explicitly so the logs are readable everywhere.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

RUNS_DIR = Path(".runs")
ANSI = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

DEFAULT_START = "2026-08-02T00:00:00+00:00"
DEFAULT_END = "2026-08-03T00:00:00+00:00"

SEED_ARGS = ["--start", "2026-08-01", "--days", "7", "--orders", "5000", "--seed", "42"]


def run(cmd: list[str], log: list[str], timeout: int = 1800) -> tuple[int, str]:
    """Run a command, stream nothing, capture everything. Returns (exit_code, output)."""
    header = f"\n$ {' '.join(cmd)}\n"
    print(header.rstrip(), flush=True)
    log.append(header)
    started = time.monotonic()
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=timeout)
        out = (p.stdout + p.stderr).decode("utf-8", errors="replace")
        code = p.returncode
    except subprocess.TimeoutExpired:
        out, code = f"TIMEOUT after {timeout}s", 124
    except FileNotFoundError as e:
        out, code = f"COMMAND NOT FOUND: {e}", 127
    out = ANSI.sub("", out)
    log.append(out)
    log.append(f"[exit {code} in {time.monotonic() - started:.1f}s]\n")
    return code, out


def metrics_from(output: str) -> list[dict]:
    found = []
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("METRIC "):
            try:
                found.append(json.loads(line[len("METRIC "):]))
            except json.JSONDecodeError:
                pass
    return found


def wait_for_db(log: list[str], attempts: int = 30) -> bool:
    for _ in range(attempts):
        code, _ = run(["docker", "compose", "exec", "-T", "source-db",
                       "pg_isready", "-U", "pipeline", "-d", "marketplace"], log, timeout=30)
        if code == 0:
            return True
        time.sleep(2)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-start", default=DEFAULT_START)
    ap.add_argument("--interval-end", default=DEFAULT_END)
    ap.add_argument("--steps", default="up,seed,build,bronze,normalize,tests")
    ap.add_argument("--fresh", action="store_true",
                    help="docker compose down -v first, and delete the lake")
    args = ap.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    log: list[str] = []
    results: list[dict] = []
    all_metrics: list[dict] = []
    RUNS_DIR.mkdir(exist_ok=True)

    started_at = datetime.now(timezone.utc).isoformat()
    log.append(f"run started {started_at}\ninterval [{args.interval_start}, {args.interval_end})\n")

    if args.fresh:
        run(["docker", "compose", "down", "-v"], log)
        lake = Path("data")
        if lake.exists():
            import shutil
            shutil.rmtree(lake, ignore_errors=True)
            log.append("removed ./data (lake reset)\n")

    jobs = ["docker", "compose", "run", "--rm", "jobs"]
    interval = ["--interval-start", args.interval_start, "--interval-end", args.interval_end]

    plan = {
        "up": (["docker", "compose", "up", "-d", "source-db"], 300),
        "build": (["docker", "compose", "build", "jobs"], 1800),
        "seed": (jobs + ["-m", "src.generator.seed"] + SEED_ARGS, 600),
        "bronze": (jobs + ["-m", "src.ingestion.bronze"] + interval, 900),
        "normalize": (jobs + ["-m", "src.transform.normalize"] + interval, 900),
        "tests": (jobs + ["-m", "pytest", "tests/", "-q"], 900),
    }

    for step in steps:
        if step not in plan:
            results.append({"step": step, "status": "unknown", "exit_code": None})
            continue

        cmd, timeout = plan[step]
        code, out = run(cmd, log, timeout=timeout)

        if step == "up" and code == 0:
            if not wait_for_db(log):
                code = 1
                log.append("source-db never became ready\n")

        ms = metrics_from(out)
        all_metrics.extend(ms)
        results.append({
            "step": step,
            "status": "ok" if code == 0 else "FAILED",
            "exit_code": code,
            "metrics": ms,
            "tail": [l for l in out.splitlines() if l.strip()][-12:] if code != 0 else [],
        })

        if code != 0:
            log.append(f"STOPPING — step '{step}' failed\n")
            break

    summary = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "interval": {"start": args.interval_start, "end": args.interval_end},
        "fresh": args.fresh,
        "overall": "ok" if all(r["status"] == "ok" for r in results) else "FAILED",
        "steps": results,
        "metrics": all_metrics,
    }

    (RUNS_DIR / "latest.log").write_text("".join(log), encoding="utf-8")
    (RUNS_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    for r in results:
        mark = "OK  " if r["status"] == "ok" else "FAIL"
        print(f"  {mark}  {r['step']}")
    print(f"  overall: {summary['overall']}")
    print(f"  -> {RUNS_DIR / 'latest.log'}")
    print(f"  -> {RUNS_DIR / 'summary.json'}")
    print("=" * 60)

    return 0 if summary["overall"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
