"""One-command pipeline runner.

    python run.py                       # default interval, all steps
    python run.py --fresh               # tear down volumes and start clean
    python run.py --steps bronze,normalize
    python run.py --verbose             # every line, unfiltered
    python run.py --interval-start 2026-08-03T00:00:00+00:00 --interval-end 2026-08-04T00:00:00+00:00

Runs on the HOST (it drives `docker compose`), not inside the jobs container.

Two audiences, two outputs, and they are deliberately not the same thing:

  console            what is happening RIGHT NOW — streamed line by line as the step runs,
                     Spark's several hundred WARN lines filtered out, and a heartbeat while
                     a step is silent
  .runs/latest.log   every byte, unfiltered, UTF-8, ANSI stripped
  .runs/summary.json per-step status, durations, and every METRIC line the jobs emitted

The first of those is the one that was missing. A build step that prints nothing for six
minutes is indistinguishable from a hang, and "it looks stuck" is not a diagnosis anyone can
act on.

Note on encoding: PowerShell's `Out-File` writes UTF-16LE, which reads as a binary blob to most
tooling. This writes UTF-8 explicitly so the logs are readable everywhere.
"""
from __future__ import annotations

import argparse
import json
import platform
import queue
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

RUNS_DIR = Path(".runs")
ANSI = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
IS_WINDOWS = platform.system() == "Windows"

DEFAULT_START = "2026-08-02T00:00:00+00:00"
DEFAULT_END = "2026-08-03T00:00:00+00:00"

SEED_ARGS = ["--start", "2026-08-01", "--days", "7", "--orders", "5000", "--seed", "42"]

HEARTBEAT_SECONDS = 20      # how long a step may say nothing before we say something for it

# ---------------------------------------------------------------------------------------
# Console filtering.
#
# Spark is loud: native-hadoop warnings, illegal-reflective-access warnings, ivy resolution,
# a WARN for every configuration it disagrees with. None of it is about this pipeline. It all
# goes to the log file; only these lines get to interrupt the reader.
# ---------------------------------------------------------------------------------------

# Our own jobs log as: "2026-08-24 09:31:47,123 INFO  bronze | read 348 rows"
OUR_LOG = re.compile(r"^\d{4}-\d{2}-\d{2} [\d:,]+ (?:INFO|WARNING|ERROR)\s+\S+ \| ")

ALWAYS_SHOW = re.compile(
    r"^METRIC |Traceback|^\s*File \"|Error|ERROR|Exception|assert|FAILED|failed|"
    r"\d+ (?:passed|failed|error)|^\[\+\]|Container |Network |Volume |^#\d+ |"
    r"GATE (?:PASSED|FAILED)"
)
NEVER_SHOW = re.compile(
    r"NativeCodeLoader|illegal reflective|Illegal reflective|^WARNING: |"
    r"Setting default log level|^:: |^\tconfs:|^\tfound |^\tdownloading |"
    r"^\s*$|log4j|SLF4J|Unable to load native|"
    r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d WARN"          # Spark's own WARN format
)


def show_on_console(line: str) -> bool:
    """Decide whether one captured line earns a place on the console.

    Order matters: a METRIC line is never noise. The log file keeps everything either way, so
    the cost of hiding a line is one `type .runs\\latest.log`, and the cost of showing every
    line is that nobody reads any of them.
    """
    if line.startswith("METRIC "):
        return True
    if NEVER_SHOW.search(line):
        return False
    return bool(OUR_LOG.match(line) or ALWAYS_SHOW.search(line))


def render(line: str) -> str:
    """Trim the parts of a line the reader already knows."""
    if line.startswith("METRIC "):
        try:
            fields = json.loads(line[len("METRIC "):])
        except json.JSONDecodeError:
            return "      " + line
        job = fields.pop("job", "")
        body = "  ".join(f"{k}={v}" for k, v in fields.items())
        return f"      METRIC {job:10s} {body}"
    if OUR_LOG.match(line):                      # drop the timestamp; elapsed is printed anyway
        return "      . " + line.split(" | ", 1)[-1]
    return "      " + line.strip()


def fmt(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


# ---------------------------------------------------------------------------------------
# Running a command with its output visible
# ---------------------------------------------------------------------------------------

def _pump(stream, q: "queue.Queue") -> None:
    for raw in iter(stream.readline, b""):
        q.put(raw)
    stream.close()
    q.put(None)


def run(cmd: list[str], log: list[str], timeout: int = 1800,
        quiet: bool = False, verbose: bool = False) -> tuple[int, str]:
    """Run a command, streaming its output as it arrives. Returns (exit_code, full_output).

    `quiet=True` captures without printing — for the probes (`docker info`, `pg_isready`)
    whose output is only interesting when they fail.

    Output is read on a background thread so the main loop can emit a heartbeat while the
    child is silent. Without that, `docker compose build` looks identical to a deadlock.
    """
    header = f"\n$ {' '.join(cmd)}\n"
    log.append(header)
    if not quiet:
        print(f"      $ {' '.join(cmd)}", flush=True)

    started = time.monotonic()
    captured: list[str] = []
    try:
        # No bufsize=1 here: line buffering is not supported on a binary stream, and Python
        # warns about it. The default buffer is right anyway -- readline() on a BufferedReader
        # returns as soon as it sees a newline, it does not wait for the buffer to fill.
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except FileNotFoundError as e:
        out = f"COMMAND NOT FOUND: {e}"
        log.append(out + "\n[exit 127 in 0.0s]\n")
        return 127, out

    q: "queue.Queue" = queue.Queue()
    threading.Thread(target=_pump, args=(p.stdout, q), daemon=True).start()

    hidden = 0
    last_spoke = started
    while True:
        elapsed = time.monotonic() - started
        if elapsed > timeout:
            p.kill()
            captured.append(f"TIMEOUT after {timeout}s")
            break
        try:
            raw = q.get(timeout=1.0)
        except queue.Empty:
            # nothing arrived this second — say so, but only every HEARTBEAT_SECONDS
            if not quiet and time.monotonic() - last_spoke > HEARTBEAT_SECONDS:
                note = f", {hidden} lines hidden" if hidden else ""
                print(f"      ... still running, {fmt(elapsed)}{note}", flush=True)
                last_spoke = time.monotonic()
            continue
        if raw is None:
            break
        line = ANSI.sub("", raw.decode("utf-8", errors="replace").rstrip("\r\n"))
        captured.append(line)
        if quiet:
            continue
        if verbose or show_on_console(line):
            print(render(line), flush=True)
            last_spoke = time.monotonic()
        else:
            hidden += 1

    code = p.wait()
    out = "\n".join(captured)
    log.append(out)
    log.append(f"\n[exit {code} in {time.monotonic() - started:.1f}s]\n")
    return code, out


# ---------------------------------------------------------------------------------------
# Docker preflight
# ---------------------------------------------------------------------------------------

def daemon_is_up(exit_code: int, output: str) -> bool:
    """Decide from `docker info --format {{.ServerVersion}}` whether the daemon is reachable.

    **The exit code is not evidence.** On Windows the Docker CLI exits 0 with Docker Desktop
    stopped — it writes the connect error to stderr and returns success anyway:

        $ docker info --format {{.ServerVersion}}
        error during connect: ... open //./pipe/dockerDesktopLinuxEngine: The system
        cannot find the file specified.
        [exit 0]

    So the check is on the *output*: a reachable daemon prints a version and nothing else.
    Kept as a pure function of (exit_code, output) so the Windows behaviour above is pinned by
    a test rather than by whichever machine happened to run it.
    """
    if exit_code != 0:
        return False
    first = next((l.strip() for l in output.splitlines() if l.strip()), "")
    return re.match(r"^\d+\.\d+", first) is not None


def docker_is_running(log: list[str]) -> tuple[bool, str]:
    """Check the daemon once, up front, and hand back the raw output for the diagnosis."""
    code, out = run(["docker", "info", "--format", "{{.ServerVersion}}"], log,
                    timeout=30, quiet=True)
    return daemon_is_up(code, out), out.strip()


def diagnose_docker(log: list[str], probe_output: str) -> list[str]:
    """Work out WHICH way Docker is unavailable, rather than guessing on the reader's behalf.

    Four distinct causes, four different fixes. Printing "start Docker Desktop" when the real
    problem is that the CLI is pointed at Windows containers costs the reader an afternoon.
    """
    findings: list[str] = []

    if "COMMAND NOT FOUND" in probe_output:
        findings.append("`docker` is not on PATH — Docker Desktop is not installed, or its CLI "
                        "was never added to PATH.")
        return findings

    _, ctx = run(["docker", "context", "ls"], log, timeout=20, quiet=True)
    current = next((l for l in ctx.splitlines() if "*" in l), "")
    if current:
        findings.append(f"active docker context: {' '.join(current.split())}")

    if not IS_WINDOWS:
        sock = Path("/var/run/docker.sock")
        findings.append(f"/var/run/docker.sock: {'present' if sock.exists() else 'MISSING'}")
        findings.append("-> Start the Docker daemon (`sudo systemctl start docker`), or add your "
                        "user to the `docker` group if it is running but unreachable.")
        return findings

    _, pipe = run(["powershell", "-NoProfile", "-Command",
                   r"Test-Path \\.\pipe\dockerDesktopLinuxEngine"], log, timeout=20, quiet=True)
    linux_pipe = "True" in pipe

    _, proc = run(["powershell", "-NoProfile", "-Command",
                   "@(Get-Process 'Docker Desktop' -ErrorAction SilentlyContinue).Count"],
                  log, timeout=20, quiet=True)
    tail = proc.strip().splitlines()[-1].strip() if proc.strip() else "0"
    launched = tail.isdigit() and int(tail) > 0

    findings.append(f"Docker Desktop process: {'running' if launched else 'NOT running'}")
    findings.append("Linux engine pipe //./pipe/dockerDesktopLinuxEngine: "
                    f"{'present' if linux_pipe else 'MISSING'}")

    if not launched:
        findings.append("-> Docker Desktop is not started. Launch it and wait for the whale icon "
                        "in the tray to stop animating.")
    elif linux_pipe:
        findings.append("-> The pipe exists but the daemon did not answer — it is probably still "
                        "starting. Wait 30s and re-run.")
    elif "desktop-windows" in ctx:
        findings.append("-> Docker Desktop is in WINDOWS CONTAINERS mode. Right-click the tray "
                        "icon -> 'Switch to Linux containers'. This repo needs Linux containers.")
    else:
        _, wsl = run(["wsl", "-l", "-v"], log, timeout=20, quiet=True)
        wsl = wsl.replace("\x00", "")               # wsl.exe writes UTF-16; strip the nulls
        distro = [" ".join(l.split()) for l in wsl.splitlines() if "docker-desktop" in l]
        if not distro:
            findings.append("-> No docker-desktop WSL distro found. Docker Desktop's WSL2 backend "
                            "is not installed, or is disabled in Settings -> General.")
        else:
            findings.append("WSL: " + " / ".join(distro))
            if not any("Running" in d for d in distro):
                findings.append("-> The docker-desktop WSL distro is stopped. Run `wsl --shutdown`, "
                                "then restart Docker Desktop.")
    return findings


def wait_for_db(log: list[str], attempts: int = 30) -> bool:
    print("      . waiting for postgres to answer pg_isready", flush=True)
    started = time.monotonic()
    for _ in range(attempts):
        code, _ = run(["docker", "compose", "exec", "-T", "source-db",
                       "pg_isready", "-U", "pipeline", "-d", "marketplace"],
                      log, timeout=30, quiet=True)
        if code == 0:
            print(f"      . postgres ready after {fmt(time.monotonic() - started)}", flush=True)
            return True
        time.sleep(2)
    print(f"      . postgres never answered after {attempts} attempts", flush=True)
    return False


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


# ---------------------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-start", default=DEFAULT_START)
    ap.add_argument("--interval-end", default=DEFAULT_END)
    ap.add_argument("--steps", default="up,seed,build,bronze,normalize,silver,tests")
    ap.add_argument("--fresh", action="store_true",
                    help="docker compose down -v first, and delete the lake")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="print every line, including Spark's warnings")
    args = ap.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    log: list[str] = []
    results: list[dict] = []
    all_metrics: list[dict] = []
    RUNS_DIR.mkdir(exist_ok=True)

    started_at = datetime.now(timezone.utc).isoformat()
    run_started = time.monotonic()
    log.append(f"run started {started_at}\ninterval [{args.interval_start}, {args.interval_end})\n")

    def write_outputs(summary: dict) -> None:
        (RUNS_DIR / "latest.log").write_text("".join(log), encoding="utf-8")
        (RUNS_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\nincremental-marketplace-pipeline . {started_at[:19].replace('T', ' ')} UTC")
    print(f"interval  [{args.interval_start}, {args.interval_end})")
    print(f"steps     {' -> '.join(steps)}" + ("   (fresh)" if args.fresh else ""))
    print(f"log       {RUNS_DIR / 'latest.log'}\n")

    # Preflight BEFORE --fresh: on a machine with Docker down, the old order deleted the lake
    # and only then discovered it could not rebuild it.
    if steps:
        print("preflight checking the docker daemon", flush=True)
        up, probe = docker_is_running(log)
        if not up:
            findings = diagnose_docker(log, probe)
            print("\n" + "=" * 76)
            print("  BLOCKED  Docker cannot be reached. Every step here shells out to")
            print("           `docker compose`, so nothing ran and nothing was deleted.")
            print("\n  what `docker info` said:")
            for l in (probe.splitlines() or ["(no output)"])[:3]:
                print(f"    {l[:112]}")
            print("\n  what that means here:")
            for f in findings:
                print(f"    {f}")
            print("=" * 76 + "\n")
            log.append("\n".join(["BLOCKED - docker unreachable", *findings]) + "\n")
            write_outputs({"started_at": started_at, "overall": "BLOCKED",
                           "reason": "docker daemon unreachable",
                           "diagnosis": findings,
                           "probe_output": probe.splitlines()[:3],
                           "steps": [], "metrics": []})
            return 2
        print(f"preflight docker {probe} reachable\n", flush=True)

    if args.fresh:
        print("fresh     tearing down containers, volumes and the lake", flush=True)
        run(["docker", "compose", "down", "-v"], log, quiet=True)
        lake = Path("data")
        if lake.exists():
            import shutil
            shutil.rmtree(lake, ignore_errors=True)
            log.append("removed ./data (lake reset)\n")
            print("fresh     removed ./data\n", flush=True)

    jobs = ["docker", "compose", "run", "--rm", "jobs"]
    interval = ["--interval-start", args.interval_start, "--interval-end", args.interval_end]

    plan = {
        "up": (["docker", "compose", "up", "-d", "source-db"], 300),
        "build": (["docker", "compose", "build", "jobs"], 1800),
        "seed": (jobs + ["-m", "src.generator.seed"] + SEED_ARGS, 600),
        "bronze": (jobs + ["-m", "src.ingestion.bronze"] + interval, 900),
        "normalize": (jobs + ["-m", "src.transform.normalize"] + interval, 900),
        "silver": (jobs + ["-m", "src.ingestion.silver"] + interval, 900),
        # The gate manages its own lake and its own intervals — it is a full 7-day forward
        # pass plus a replay, not a single-interval job.
        "gate": (jobs + ["-m", "src.gate"], 1800),
        "tests": (jobs + ["-m", "pytest", "tests/", "-q"], 900),
    }

    for i, step in enumerate(steps, 1):
        if step not in plan:
            print(f"[{i}/{len(steps)}] {step:10s} unknown step - skipped\n")
            results.append({"step": step, "status": "unknown", "exit_code": None})
            continue

        cmd, timeout = plan[step]
        print(f"[{i}/{len(steps)}] {step:10s} (timeout {fmt(timeout)})", flush=True)
        step_started = time.monotonic()
        code, out = run(cmd, log, timeout=timeout, verbose=args.verbose)

        if step == "up" and code == 0 and not wait_for_db(log):
            code = 1
            log.append("source-db never became ready\n")

        ms = metrics_from(out)
        all_metrics.extend(ms)
        tail = [l for l in out.splitlines() if l.strip()][-15:] if code != 0 else []
        results.append({
            "step": step,
            "status": "ok" if code == 0 else "FAILED",
            "exit_code": code,
            "seconds": round(time.monotonic() - step_started, 1),
            "metrics": ms,
            "tail": tail,
        })

        took = fmt(time.monotonic() - step_started)
        if code == 0:
            print(f"      ok   {step} in {took}\n", flush=True)
        else:
            print(f"      FAIL {step} in {took}, exit {code}")
            print("      last lines before it stopped:")
            for l in tail[-10:]:
                print(f"        {l[:140]}")
            print(flush=True)
            log.append(f"STOPPING - step '{step}' failed\n")
            break

    summary = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "seconds": round(time.monotonic() - run_started, 1),
        "interval": {"start": args.interval_start, "end": args.interval_end},
        "fresh": args.fresh,
        "overall": "ok" if results and all(r["status"] == "ok" for r in results) else "FAILED",
        "steps": results,
        "metrics": all_metrics,
    }
    write_outputs(summary)

    print("=" * 76)
    for r in results:
        mark = "ok  " if r["status"] == "ok" else "FAIL"
        print(f"  {mark}  {r['step']:10s} {r.get('seconds', 0):>7.1f}s")
    print(f"  overall: {summary['overall']} in {fmt(summary['seconds'])}")
    print(f"  -> {RUNS_DIR / 'latest.log'}   (everything, unfiltered)")
    print(f"  -> {RUNS_DIR / 'summary.json'} (per-step status + all METRIC lines)")
    print("=" * 76)

    return 0 if summary["overall"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
