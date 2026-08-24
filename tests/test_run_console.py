"""What the runner puts on the console, and what it keeps to the log file.

The filter is the whole point of the console view, so it is pinned here rather than left to
whatever a given Spark version happens to print. Every string below is copied verbatim from a
real run — Spark's warnings, docker compose's progress lines, our own job logs.

The rule being tested: **the log file keeps everything; the console keeps what a human needs
to know the run is alive and going the right way.** Hiding a line costs one `type
.runs/latest.log`. Showing every line costs the reader's attention, permanently.

Stdlib only, so this runs anywhere `run.py` does.
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from run import fmt, render, show_on_console  # noqa: E402

# --- verbatim Spark / JVM noise -------------------------------------------------------
SPARK_NOISE = [
    "25/08/24 09:31:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your "
    "platform... using builtin-java classes where applicable",
    "Setting default log level to \"WARN\".",
    "WARNING: An illegal reflective access operation has occurred",
    ":: loading settings :: url = jar:file:/opt/spark/jars/ivy-2.5.1.jar!/org/apache/ivy/core/"
    "settings/ivysettings.xml",
    "\tconfs: [default]",
    "\tfound io.delta#delta-spark_2.13;4.0.0 in central",
    "",
    "   ",
]

# --- verbatim lines a human actually needs ---------------------------------------------
OUR_LOGS = [
    "2026-08-24 09:31:47,123 INFO  bronze | shopify_orders | read 348 rows, slice fingerprint a1b2c3d4",
    "2026-08-24 09:31:48,004 WARNING silver | 0 rows in interval - nothing written",
    "2026-08-24 09:31:49,900 ERROR gate | assertion failed: revenue moved on replay",
]
DOCKER_PROGRESS = [
    "[+] Running 1/1",
    " Container mkt-source-db  Started",
    " Network incremental-marketplace-pipeline_default  Created",
    "#8 [4/9] RUN apt-get install -y openjdk-21-jdk-headless",
]
FAILURES = [
    "Traceback (most recent call last):",
    '  File "/app/src/gate.py", line 210, in <module>',
    "AssertionError: naive table did not break",
    "1 failed, 26 passed in 19.33s",
    "GATE PASSED",
]
METRICS = [
    'METRIC {"job": "silver", "rows_in": 981, "rows_after_dedup": 828, "duplicate_keys": 0}',
]


def test_spark_noise_never_reaches_the_console():
    for line in SPARK_NOISE:
        assert show_on_console(line) is False, line


def test_our_own_job_logs_always_reach_the_console():
    for line in OUR_LOGS:
        assert show_on_console(line) is True, line


def test_docker_progress_reaches_the_console():
    """During a six-minute image build these lines are the only proof of life."""
    for line in DOCKER_PROGRESS:
        assert show_on_console(line) is True, line


def test_failures_always_reach_the_console():
    for line in FAILURES:
        assert show_on_console(line) is True, line


def test_metric_lines_are_never_filtered():
    """A METRIC line is the run's actual result. It outranks every noise rule."""
    for line in METRICS:
        assert show_on_console(line) is True, line


def test_metric_lines_render_as_readable_key_values():
    out = render(METRICS[0])
    assert "silver" in out
    assert "rows_in=981" in out
    assert "duplicate_keys=0" in out
    assert "{" not in out, "raw JSON should not survive to the console"


def test_our_log_lines_drop_the_timestamp():
    out = render(OUR_LOGS[0])
    assert "2026-08-24" not in out
    assert "read 348 rows" in out


def test_fmt_is_readable_at_both_ends():
    assert fmt(9) == "9s"
    assert fmt(59) == "59s"
    assert fmt(60) == "1m00s"
    assert fmt(372) == "6m12s"
