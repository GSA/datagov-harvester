import json
from pathlib import Path

PROXY = Path("proxy")


def test_waf_block_uses_uri_error_page_so_posts_get_429():
    config = (PROXY / "nginx-malicious.conf").read_text()

    assert "error_page 429 $waf_block_page;" in config
    assert "@malicious" not in config


def test_block_page_is_exempt_from_the_block_it_serves():
    maps = (PROXY / "nginx-maps.conf").read_text()

    assert 'map "$http_x_amzn_waf_detection:$uri" $waf_block {' in maps
    assert "volatile;" in maps
    assert '"~^[^:]*:/malicious\\.(html|json)$"  0;' in maps


def test_api_paths_get_json_block_page():
    maps = (PROXY / "nginx-maps.conf").read_text()

    assert "~^/api/  /malicious.json;" in maps
    body = json.loads((PROXY / "public" / "malicious.json").read_text())
    assert isinstance(body["error"], str)


def test_block_does_not_reveal_the_rate_limit_window():
    config = (PROXY / "nginx-malicious.conf").read_text()

    assert "Retry-After" not in config
