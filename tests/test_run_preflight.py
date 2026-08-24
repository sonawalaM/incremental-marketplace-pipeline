"""The Docker preflight in `run.py`.

Pinned by a test because the obvious implementation is wrong on the one platform this repo is
developed on. `docker info` **exits 0 on Windows when Docker Desktop is stopped** — it writes
the connect error to stderr and returns success anyway. A preflight that trusts the exit code
therefore passes, and the reader gets the named-pipe error the preflight existed to prevent.

The strings below are copied verbatim from real runs, not paraphrased. Stdlib only, so this
runs anywhere `run.py` does.
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from run import daemon_is_up  # noqa: E402

# Verbatim, from .runs/latest.log on a Windows host with Docker Desktop stopped.
WINDOWS_DAEMON_DOWN = (
    'error during connect: Get "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/v1.46/info": '
    "open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified."
)
LINUX_DAEMON_DOWN = (
    "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. "
    "Is the docker daemon running?"
)


def test_windows_reports_success_while_the_daemon_is_down():
    """The whole reason this function exists. Exit code 0, daemon unreachable."""
    assert daemon_is_up(0, WINDOWS_DAEMON_DOWN) is False


def test_linux_daemon_down_is_rejected():
    assert daemon_is_up(1, LINUX_DAEMON_DOWN) is False


def test_docker_not_installed_is_rejected():
    assert daemon_is_up(127, "COMMAND NOT FOUND: [Errno 2] No such file or directory: 'docker'") is False


def test_a_reachable_daemon_is_accepted():
    assert daemon_is_up(0, "28.1.1\n") is True


def test_leading_blank_lines_do_not_hide_the_version():
    assert daemon_is_up(0, "\n\n  28.1.1  \n") is True


def test_empty_output_is_not_a_version():
    """A silent success is not evidence either — the daemon must actually name itself."""
    assert daemon_is_up(0, "") is False
    assert daemon_is_up(0, "   \n\n") is False
