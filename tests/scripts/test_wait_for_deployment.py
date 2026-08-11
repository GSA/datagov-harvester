"""Tests for bin/wait_for_deployment.sh.

Every deploy pushes with `--strategy rolling --no-wait`, so a job succeeding does not
mean the new droplet is running. This script is the gate that makes it true, and the
property worth pinning is that it does not return early -- a reindex started against
the previous droplet would write documents in the old shape and still pass both
verification gates, because `compare` never inspects document shape.
"""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "wait_for_deployment.sh"

APP = "datagov-harvest"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_wait(tmp_path, *arguments, active_counts=(0,), quiet_seconds="0"):
    """Run the script against a fake `cf`.

    ``active_counts`` is the number of ACTIVE deployments each successive
    ``cf curl`` call reports, so a test can describe a rollout draining over
    several polls. The last value repeats once the list is exhausted.
    """
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()
    counts_file = tmp_path / "counts"
    counts_file.write_text("\n".join(str(c) for c in active_counts))
    index_file = tmp_path / "index"
    index_file.write_text("0")

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"

case "$1" in
  app)
    echo "fake-app-guid"
    ;;
  curl)
    # Serve the next value from the counts list, repeating the last one.
    # `awk END{print NR}` rather than `wc -l`: wc pads its output with spaces
    # (breaking the numeric compare) and undercounts a file whose last line has
    # no trailing newline, which is exactly what "\\n".join writes.
    idx=$(cat "$CF_INDEX_FILE")
    total=$(awk 'END{print NR}' "$CF_COUNTS_FILE")
    line=$(( idx + 1 ))
    if [[ $line -gt $total ]]; then line=$total; fi
    count=$(sed -n "${line}p" "$CF_COUNTS_FILE")
    echo $(( idx + 1 )) > "$CF_INDEX_FILE"
    resources=""
    for (( i = 0; i < count; i++ )); do
      resources="${resources}{},"
    done
    resources="${resources%,}"
    printf '{"resources":[%s]}\\n' "$resources"
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CF_CALLS_FILE": str(calls_file),
        "CF_COUNTS_FILE": str(counts_file),
        "CF_INDEX_FILE": str(index_file),
        # Keep the polling instant; the timing is not what these tests pin.
        "DEPLOYMENT_POLL_SECONDS": "0",
        "DEPLOYMENT_QUIET_SECONDS": quiet_seconds,
    }
    result = subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=60,
    )
    return result, calls_file.read_text()


def test_returns_once_no_deployment_is_active(tmp_path):
    result, calls = _run_wait(tmp_path, APP)

    assert result.returncode == 0, result.stderr
    assert "no active deployment" in result.stdout
    # Scoped to this app: a concurrent proxy or catalog deploy must not block it.
    assert "app_guids=fake-app-guid" in calls
    assert "status_values=ACTIVE" in calls


def test_waits_for_an_in_flight_rollout_to_drain(tmp_path):
    """The whole point: return only after the rollout finishes."""
    result, _ = _run_wait(tmp_path, APP, active_counts=(2, 1, 0))

    assert result.returncode == 0, result.stderr
    assert "waiting for 2 active deployment(s)" in result.stdout
    assert "waiting for 1 active deployment(s)" in result.stdout


def test_times_out_rather_than_returning_on_a_stuck_deployment(tmp_path):
    """Failing here is correct: a caller must not run a task against unknown code."""
    result, _ = _run_wait(tmp_path, APP, "1", active_counts=(1,))

    assert result.returncode == 1
    assert "Timed out" in result.stderr
    # The message has to say why it matters, not just that it timed out.
    assert "previous droplet" in result.stderr


def test_a_momentary_gap_does_not_count_as_drained(tmp_path):
    """CF reports no ACTIVE deployment between superseding one and starting its
    replacement -- exactly what a restart cron colliding with a deploy looks like.
    Returning in that gap would hand the caller the old droplet, so the quiet
    period must see the zero persist."""
    # Deployments never drain here (the last count repeats), and the quiet period is
    # longer than the 1s timeout, so the only way to exit 0 would be to accept that
    # first transient zero. Timing out is the correct, safe outcome.
    result, _ = _run_wait(tmp_path, APP, "1", active_counts=(0, 1), quiet_seconds="600")

    assert result.returncode == 1
    assert "waiting for 1 active deployment(s)" in result.stdout


def test_requires_an_app_name(tmp_path):
    result, _ = _run_wait(tmp_path)

    assert result.returncode != 0
    assert "Usage" in result.stderr
