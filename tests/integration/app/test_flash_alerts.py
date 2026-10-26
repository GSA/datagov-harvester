"""Flash and page-level alerts use one color per outcome.

success -> usa-alert--success (green), warning -> usa-alert--warning (yellow),
error -> usa-alert--error (red). See app/templates/components/alert.j2.
"""

import ast
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from harvester.utils.reharvest_links import create_reharvest_token

APP_DIR = Path(__file__).parents[3] / "app"
FLASH_CATEGORIES = {"success", "warning", "error", "info"}
ALERT_COLORS = {
    "usa-alert--success",
    "usa-alert--warning",
    "usa-alert--error",
    "usa-alert--info",
}


def notification_classes(html):
    """Return the color class of each flashed alert on the page."""
    region = BeautifulSoup(html, "html.parser").find(
        attrs={"aria-label": "Notifications"}
    )
    return [
        next(c for c in alert["class"] if c in ALERT_COLORS)
        for alert in region.select(".usa-alert")
    ]


def page_alert_class(html):
    alert = BeautifulSoup(html, "html.parser").select_one("main .usa-alert")
    return next(c for c in alert["class"] if c in ALERT_COLORS)


@pytest.fixture
def admin_client(app, client, monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    with client.session_transaction() as sess:
        sess["user"] = "tester@gsa.gov"
    return client


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        ("success", "usa-alert--success"),
        ("warning", "usa-alert--warning"),
        ("error", "usa-alert--error"),
        ("info", "usa-alert--info"),
        # Flask's default category, and anything unrecognised, is neutral
        # rather than looking like a warning.
        ("message", "usa-alert--info"),
        ("danger", "usa-alert--info"),
    ],
)
def test_alert_class_mapping(app, category, expected):
    macro = app.jinja_env.get_template("components/alert.j2").module
    assert str(macro.alert_class(category)) == expected


def test_every_flash_call_has_a_known_category():
    """Guard against categoryless flash() calls that would render as info."""
    problems = []
    for path in APP_DIR.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "flash"
            ):
                continue
            where = f"{path.relative_to(APP_DIR.parent)}:{node.lineno}"
            args = list(node.args) + [
                kw.value for kw in node.keywords if kw.arg == "category"
            ]
            if len(args) < 2:
                problems.append(f"{where} has no category")
                continue
            category = args[1]
            literals = (
                [category.body, category.orelse]
                if isinstance(category, ast.IfExp)
                else [category]
            )
            for literal in literals:
                if (
                    isinstance(literal, ast.Constant)
                    and literal.value not in FLASH_CATEGORIES
                ):
                    problems.append(f"{where} uses unknown category {literal.value!r}")
    assert problems == []


class TestFlashColors:
    def test_success_is_green(
        self, admin_client, interface_no_jobs, source_data_dcatus
    ):
        source_id = source_data_dcatus["id"]
        response = admin_client.post(
            f"/harvest_source/{source_id}",
            data={"lock": "Lock"},
            follow_redirects=True,
        )
        assert notification_classes(response.text) == ["usa-alert--success"]
        assert f"Harvest source {source_id} locked." in response.text

    def test_nothing_changed_is_yellow(
        self, admin_client, interface_no_jobs, source_data_dcatus
    ):
        source_id = source_data_dcatus["id"]
        queued = interface_no_jobs.add_harvest_job(
            {"harvest_source_id": source_id, "status": "new", "job_type": "harvest"}
        )
        response = admin_client.post(
            f"/harvest_source/{source_id}",
            data={"harvest": "Harvest"},
            follow_redirects=True,
        )
        assert notification_classes(response.text) == ["usa-alert--warning"]
        assert f"Job {queued.id} already queued." in response.text

    def test_refused_action_is_red(
        self, admin_client, interface_no_jobs, organization_data
    ):
        response = admin_client.post(
            f"/organization/{organization_data['id']}",
            data={"delete": "Delete"},
            follow_redirects=True,
        )
        assert notification_classes(response.text) == ["usa-alert--error"]
        assert "please delete those first" in response.text

    def test_locked_source_harvest_is_red(
        self, admin_client, interface_no_jobs, source_data_dcatus
    ):
        source_id = source_data_dcatus["id"]
        interface_no_jobs.set_harvest_source_lock(source_id, True)
        response = admin_client.post(
            f"/harvest_source/{source_id}",
            data={"harvest": "Harvest"},
            follow_redirects=True,
        )
        assert notification_classes(response.text) == ["usa-alert--error"]
        assert "is locked" in response.text

    def test_validation_error_is_red(self, admin_client, interface):
        response = admin_client.post(
            "/organization/add",
            data={
                "name": "Bad URL Org",
                "slug": "bad-url-org",
                "logo": "https://example.com/logo.png",
                "code_repo_url": "ftp://github.com/test",
            },
        )
        assert notification_classes(response.text) == ["usa-alert--error"]
        assert "URL must start with http" in response.text

    def test_success_with_warning_shows_green_then_yellow(
        self, admin_client, interface
    ):
        response = admin_client.post(
            "/organization/add",
            data={
                "name": "Conflict Org",
                "slug": "conflict-org",
                "logo": "https://example.com/logo.png",
                "organization_type": "Federal Government",
                "code_repo_url": "https://github.com/test",
                "code_repo_exempt": "y",
            },
            follow_redirects=True,
        )
        assert notification_classes(response.text) == [
            "usa-alert--success",
            "usa-alert--warning",
        ]

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (202, "usa-alert--success"),
            (404, "usa-alert--error"),
            (409, "usa-alert--error"),
            (500, "usa-alert--error"),
        ],
    )
    def test_source_delete_color_follows_status(
        self, admin_client, interface_no_jobs, source_data_dcatus, status, expected
    ):
        with patch(
            "app.main.harvest_sources.enqueue_harvest_source_delete",
            return_value=("delete result", status),
        ):
            response = admin_client.post(
                f"/harvest_source/{source_data_dcatus['id']}",
                data={"delete": "Delete"},
                follow_redirects=True,
            )
        assert notification_classes(response.text) == [expected]

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (200, "usa-alert--success"),
            (500, "usa-alert--warning"),
        ],
    )
    def test_add_source_color_follows_scheduling(
        self,
        admin_client,
        interface_no_jobs,
        organization_data,
        source_data_dcatus,
        status,
        expected,
    ):
        source = interface_no_jobs.get_harvest_source(source_data_dcatus["id"])
        with patch(
            "app.main.harvest_sources.create_harvest_source",
            return_value=(source, "create result", status),
        ):
            response = admin_client.post(
                "/harvest_source/add",
                data={
                    "organization_id": organization_data["id"],
                    "name": "New Source",
                    "url": "https://example.gov/new/data.json",
                    "notification_emails": "a@example.gov",
                    "frequency": "manual",
                    "schema_type": "dcatus1.1: federal",
                    "source_type": "document",
                    "notification_frequency": "always",
                    "send_report_email": "False",
                },
                follow_redirects=True,
            )
        assert notification_classes(response.text) == [expected]


class TestRefreshPageColors:
    def make_token(self, app, interface_no_jobs, source_data_dcatus):
        job = interface_no_jobs.add_harvest_job(
            {
                "harvest_source_id": source_data_dcatus["id"],
                "status": "complete",
                "job_type": "harvest",
                "date_created": datetime.now() - timedelta(minutes=1),
                "date_finished": datetime.now(),
            }
        )
        return create_reharvest_token(
            app.config["SECRET_KEY"],
            "person@gsa.gov",
            source_data_dcatus["id"],
            job.id,
        )

    def test_invalid_link_is_red(self, client):
        response = client.get("/harvest_source/refresh?token=nope")
        assert response.status_code == 400
        assert page_alert_class(response.text) == "usa-alert--error"

    def test_stale_link_is_red(
        self, app, client, interface_no_jobs, source_data_dcatus
    ):
        token = self.make_token(app, interface_no_jobs, source_data_dcatus)
        interface_no_jobs.add_harvest_job(
            {
                "harvest_source_id": source_data_dcatus["id"],
                "status": "new",
                "job_type": "harvest",
                "date_created": datetime.now(),
            }
        )
        response = client.get(f"/harvest_source/refresh?token={token}")
        assert response.status_code == 410
        assert page_alert_class(response.text) == "usa-alert--error"

    @pytest.mark.parametrize(
        ("result", "expected"),
        [
            (("Updated job to in_progress", "success"), "usa-alert--success"),
            (("Can't trigger harvest.", "warning"), "usa-alert--warning"),
            (("Can't trigger harvest.", "error"), "usa-alert--error"),
        ],
    )
    def test_requested_harvest_uses_start_job_category(
        self,
        app,
        client,
        monkeypatch,
        interface_no_jobs,
        source_data_dcatus,
        result,
        expected,
    ):
        monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
        token = self.make_token(app, interface_no_jobs, source_data_dcatus)
        with patch("app.deps.load_manager.start_job", return_value=result):
            response = client.post(
                "/harvest_source/refresh",
                data={"token": token, "refresh": "Refresh now"},
            )
        assert response.status_code == 200
        assert page_alert_class(response.text) == expected

    def test_already_queued_is_yellow(
        self, app, client, monkeypatch, interface_no_jobs, source_data_dcatus
    ):
        monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
        token = self.make_token(app, interface_no_jobs, source_data_dcatus)
        # A job queued between the link check and the enqueue (e.g. by the
        # scheduler) is reported as the active job.
        queued = SimpleNamespace(id="queued-job")
        with patch(
            "app.deps.db.enqueue_harvest_job_if_idle", return_value=(None, queued)
        ):
            response = client.post(
                "/harvest_source/refresh",
                data={"token": token, "refresh": "Refresh now"},
            )
        assert response.status_code == 409
        assert "Job queued-job is already queued or running." in response.text
        assert page_alert_class(response.text) == "usa-alert--warning"
