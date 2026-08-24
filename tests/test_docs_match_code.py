"""The README is a deliverable. This stops it drifting from the code.

Documentation rots silently: a command gets renamed, a metric's definition changes, a file is
deleted, and the README keeps confidently describing the old thing. These checks make that a
test failure instead of something a reader discovers.

Stdlib only -- no Spark, no Delta, no Docker. Runs in milliseconds.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = (ROOT / "README.md").read_text(encoding="utf-8")
RUN_PY = (ROOT / "run.py").read_text(encoding="utf-8")
SOURCES = {p.relative_to(ROOT).as_posix(): p.read_text(encoding="utf-8")
           for p in (ROOT / "src").rglob("*.py")}
ALL_SRC = "\n".join(SOURCES.values())


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def test_every_documented_step_exists_in_run_py():
    documented = set()
    for group in re.findall(r"--steps ([\w,]+)", README):
        documented |= set(group.split(","))
    available = set(re.findall(r'"(\w+)": \(', RUN_PY))
    assert documented, "README documents no steps -- did the commands table disappear?"
    assert documented <= available, f"README documents steps run.py does not have: {documented - available}"


def test_every_documented_module_exists():
    for mod in re.findall(r"jobs -m ([\w.]+)", README):
        assert (ROOT / (mod.replace(".", "/") + ".py")).exists(), f"README references missing module {mod}"


def test_python_snippets_in_readme_appear_in_the_source():
    """A reader who greps the repo for a snippet must find it. Paraphrased code is a lie."""
    for block in re.findall(r"```python\n(.*?)```", README, re.S):
        for line in block.splitlines():
            code = line.split("#")[0].strip()
            if len(code) < 25 or code.startswith(("import ", "from ")):
                continue
            assert _norm(code) in _norm(ALL_SRC), f"README snippet not found verbatim in src/: {code}"


def test_documented_paths_exist():
    for path in re.findall(r"`(src/[\w/]+\.py|sql/[\w.]+\.sql|tests/[\w/]*)`", README):
        assert (ROOT / path).exists(), f"README references missing path: {path}"


def test_readme_does_not_reference_deleted_artefacts():
    """demo.py and the Makefile were removed in Aug 2026. They must not creep back into the docs."""
    for gone in ("demo.py", "demo-broken", "demo-fixed", "make up", "make seed", "Makefile"):
        assert gone not in README, f"README references removed artefact: {gone}"


def test_revenue_is_defined_once_and_used_everywhere():
    """The single most load-bearing definition in the project.

    Amounts do not change between versions of an order -- only the status does. If any consumer
    summed gross_amount_usd instead of net_amount_usd, reverting a refund would leave the number
    identical and the replay demonstration would prove nothing while appearing to pass.
    """
    assert "net_amount_usd" in SOURCES["src/transform/normalize.py"], "net_amount_usd must be produced by normalize"
    for consumer in ("src/gate.py", "src/ingestion/silver.py"):
        assert 'F.sum("net_amount_usd")' in SOURCES[consumer], f"{consumer} must sum net_amount_usd"
        assert 'F.sum("gross_amount_usd")' not in SOURCES[consumer], f"{consumer} must not treat gross as revenue"
    assert "net_amount_usd" in README, "README must document what revenue means"


def test_the_gate_can_actually_fail():
    """Both halves of the demonstration are asserted: the guarded table must survive a replay,
    and the naive one must not. A gate that only checks the happy path proves nothing."""
    gate = SOURCES["src/gate.py"]
    assert "backdated_orders_present" in gate
    assert "naive_broke" in gate, "gate must fail if the naive table does NOT break"
    assert "GUARDED_PATH" in gate and "NAIVE_PATH" in gate, "gate must build both tables"


def test_the_guard_is_a_single_named_condition():
    silver = SOURCES["src/ingestion/silver.py"]
    assert 'RESTATEMENT_GUARD = "s.updated_at_utc > t.updated_at_utc"' in silver
    assert "guarded: bool = True" in silver, "naive mode must be one flag, not a forked code path"


def test_no_job_reads_a_clock_for_its_bounds():
    """Interval bounds are arguments. A job that can invent its own bounds cannot be replayed."""
    for name in ("src/ingestion/bronze.py", "src/ingestion/silver.py"):
        src = SOURCES[name]
        assert "datetime.now(" not in src, f"{name} must not read wall clock for bounds"
        assert "--interval-start" in src and "required=True" in src
