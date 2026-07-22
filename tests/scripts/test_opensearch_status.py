import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "opensearch_status.sh"
DEFAULT_PAYLOAD = REPO_ROOT / "scripts" / "opensearch_status.py"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_wrapper(tmp_path, *arguments):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    streamed_payload = tmp_path / "streamed.py"
    ssh_arguments = tmp_path / "ssh-arguments"

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

case "$1" in
  target)
    echo "fake target"
    ;;
  ssh)
    printf '%s\n' "$*" > "$CF_SSH_ARGUMENTS"
    cat > "$CF_STREAMED_PAYLOAD"
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
        "CF_APP_INSTANCE": "3",
        "CF_SSH_ARGUMENTS": str(ssh_arguments),
        "CF_STREAMED_PAYLOAD": str(streamed_payload),
    }
    result = subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )
    return result, streamed_payload, ssh_arguments


def test_wrapper_streams_default_python_payload(tmp_path):
    result, streamed_payload, ssh_arguments = _run_wrapper(
        tmp_path,
        "custom-harvester",
    )

    assert result.returncode == 0
    assert streamed_payload.read_text() == DEFAULT_PAYLOAD.read_text()
    assert "ssh custom-harvester -i 3 -T -c" in ssh_arguments.read_text()


def test_wrapper_accepts_custom_python_payload(tmp_path):
    custom_payload = tmp_path / "custom_status.py"
    custom_payload.write_text("print('custom status')\n")

    result, streamed_payload, _ = _run_wrapper(
        tmp_path,
        "custom-harvester",
        str(custom_payload),
    )

    assert result.returncode == 0
    assert streamed_payload.read_text() == custom_payload.read_text()
