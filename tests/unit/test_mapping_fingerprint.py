import subprocess
import sys
from pathlib import Path

from search.config import SETTINGS
from search.fingerprint import mapping_fingerprint
from search.mappings import MAPPINGS

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_fingerprint_is_stable_across_calls():
    assert mapping_fingerprint() == mapping_fingerprint()


def test_fingerprint_ignores_key_order():
    reordered = {"properties": dict(reversed(list(MAPPINGS["properties"].items())))}

    assert mapping_fingerprint(mappings=reordered) == mapping_fingerprint()


def test_fingerprint_changes_when_a_field_type_changes():
    changed = {
        "properties": {**MAPPINGS["properties"], "slug": {"type": "text"}},
    }

    assert mapping_fingerprint(mappings=changed) != mapping_fingerprint()


def test_fingerprint_changes_when_a_field_is_added():
    changed = {
        "properties": {**MAPPINGS["properties"], "brand_new": {"type": "keyword"}},
    }

    assert mapping_fingerprint(mappings=changed) != mapping_fingerprint()


def test_fingerprint_changes_when_an_analyzer_changes():
    changed = {
        **SETTINGS,
        "analysis": {
            **SETTINGS["analysis"],
            "analyzer": {"datagov_text": {"type": "standard"}},
        },
    }

    assert mapping_fingerprint(settings=changed) != mapping_fingerprint()


def test_module_entrypoint_prints_the_fingerprint_without_app_dependencies():
    """CI computes this on a bare runner, so importing must not need the app."""
    result = subprocess.run(
        [sys.executable, "-m", "search.fingerprint"],
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == mapping_fingerprint()
