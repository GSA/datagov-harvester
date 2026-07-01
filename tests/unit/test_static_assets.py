from pathlib import Path
from unittest.mock import patch

import pytest

from app import create_app
from app.static_assets import (
    DEFAULT_ASSET_VERSION,
    compute_static_asset_version,
    get_asset_version,
    static_url,
    unversion_static_filename,
    versioned_static_filename,
)


@pytest.fixture
def app():
    with patch("app.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True})
    return app


@pytest.fixture
def dbapp(app):
    yield app


@pytest.fixture
def default_function_fixture():
    yield


def test_get_asset_version_uses_env_var(monkeypatch):
    monkeypatch.setenv("ASSET_VERSION", "abc1234")
    assert get_asset_version() == "abc1234"


def test_get_asset_version_hashes_static_asset_contents(monkeypatch, tmp_path):
    monkeypatch.delenv("ASSET_VERSION", raising=False)
    asset = tmp_path / "js" / "app.js"
    asset.parent.mkdir()
    asset.write_text("one")

    first_version = get_asset_version(tmp_path)
    asset.write_text("two")
    second_version = get_asset_version(tmp_path)

    assert first_version != DEFAULT_ASSET_VERSION
    assert first_version != second_version
    assert len(first_version) == 7
    assert len(second_version) == 7


def test_get_asset_version_uses_dev_without_static_files(monkeypatch, tmp_path):
    monkeypatch.delenv("ASSET_VERSION", raising=False)

    assert get_asset_version(tmp_path) == DEFAULT_ASSET_VERSION


@pytest.mark.parametrize("asset_version", ["../secret", "abc/123", "abc.123", ""])
def test_static_url_rejects_unsafe_asset_version(app, asset_version):
    app.config["ASSET_VERSION"] = asset_version

    with app.test_request_context("/"), pytest.raises(ValueError):
        static_url("js/filter.js")


def test_get_asset_version_rejects_unsafe_env_var(monkeypatch):
    monkeypatch.setenv("ASSET_VERSION", "../secret")

    with pytest.raises(ValueError):
        get_asset_version()


def test_static_url_embeds_version_in_filename(app):
    app.config["ASSET_VERSION"] = "8eb9d3e"
    with app.test_request_context("/"):
        url = static_url("js/filter.js")

    assert url.endswith("/js/filter.8eb9d3e.js")
    assert "?v=" not in url


def test_static_url_preserves_existing_filename_dots(app):
    app.config["ASSET_VERSION"] = "8eb9d3e"
    with app.test_request_context("/"):
        url = static_url("assets/htmx/htmx.min.js")

    assert url.endswith("/assets/htmx/htmx.min.8eb9d3e.js")


def test_versioned_static_filename_maps_back_to_on_disk_filename():
    filename = "assets/htmx/htmx.min.js"

    versioned_filename = versioned_static_filename(filename, "8eb9d3e")

    assert versioned_filename == "assets/htmx/htmx.min.8eb9d3e.js"
    assert unversion_static_filename(versioned_filename, "8eb9d3e") == filename
    assert unversion_static_filename(filename, "8eb9d3e") == filename


def test_compute_static_asset_version_ignores_unserved_build_dependencies(tmp_path):
    asset = tmp_path / "js" / "app.js"
    dependency = tmp_path / "node_modules" / "dependency.js"
    asset.parent.mkdir()
    dependency.parent.mkdir()
    asset.write_text("asset")
    dependency.write_text("one")

    first_version = compute_static_asset_version(tmp_path)
    dependency.write_text("two")

    assert compute_static_asset_version(tmp_path) == first_version


def test_versioned_static_asset_request_serves_on_disk_file(app):
    app.config["ASSET_VERSION"] = "8eb9d3e"

    response = app.test_client().get("/js/datetime.8eb9d3e.js")

    assert response.status_code == 200
    assert b"document" in response.data
    assert Path(app.static_folder, "js", "datetime.js").exists()
