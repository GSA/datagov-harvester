"""End-to-end attribution of API requests to individual API clients.

These go through the real routes, auth decorator, mutation logging and
database. Only synthetic keys are used.
"""

import hashlib
import json
import logging

import pytest

from app.api_credentials import parse_api_clients
from database.models import HarvestSource

CLIENT_A_KEY = "synthetic-client-a-key-0123456789abcdef"
CLIENT_B_KEY = "synthetic-client-b-key-fedcba9876543210"
CLIENT_A_OLD_KEY = "synthetic-client-a-old-key-5555555555"


def digest_of(key: str) -> str:
    return "sha256:" + hashlib.sha256(key.encode()).hexdigest()


def configure_clients(app, monkeypatch, config: dict):
    monkeypatch.setitem(
        app.config,
        "API_CLIENTS",
        parse_api_clients(json.dumps(config), legacy_token=app.config["API_TOKEN"]),
    )


@pytest.fixture
def api_clients(app, monkeypatch):
    configure_clients(
        app,
        monkeypatch,
        {"client-a": digest_of(CLIENT_A_KEY), "client-b": digest_of(CLIENT_B_KEY)},
    )


@pytest.fixture
def audit_log(caplog):
    caplog.set_level(logging.INFO, logger="harvest_admin")
    return caplog


def org_payload(slug):
    return {"name": f"Org {slug}", "logo": "https://example.com/l.png", "slug": slug}


def audit_lines(caplog, prefix="Audit "):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith(prefix)]


def login_lines(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith("event=user_login")
    ]


def assert_no_secrets(text, *keys):
    for key in keys:
        assert key not in text
        assert digest_of(key)[7:] not in text


@pytest.mark.usefixtures("api_clients")
class TestDistinctClientAttribution:
    def test_two_clients_authenticate_with_distinct_actors(self, client, audit_log):
        res_a = client.post(
            "/api/v1/organization/add",
            json=org_payload("client-a-org"),
            headers={"X-API-Key": CLIENT_A_KEY},
        )
        res_b = client.post(
            "/api/v1/organization/add",
            json=org_payload("client-b-org"),
            headers={"X-API-Key": CLIENT_B_KEY},
        )

        assert res_a.status_code == 201
        assert res_b.status_code == 201

        logins = login_lines(audit_log)
        assert len(logins) == 2
        assert "method=api_client user=api_client:client-a client_id=client-a" in (
            logins[0]
        )
        assert "method=api_client user=api_client:client-b client_id=client-b" in (
            logins[1]
        )

        audits = audit_lines(audit_log, "Audit create organization")
        assert len(audits) == 2
        assert "auth_type=api_client" in audits[0]
        assert "user=api_client:client-a" in audits[0]
        assert "organization_slug=client-a-org" in audits[0]
        assert "user=api_client:client-b" in audits[1]
        assert "organization_slug=client-b-org" in audits[1]
        assert_no_secrets(audit_log.text, CLIENT_A_KEY, CLIENT_B_KEY)

    def test_create_update_archive_are_attributed(
        self,
        client,
        interface_no_jobs,
        organization_data,
        source_data_dcatus,
        audit_log,
    ):
        res = client.post(
            f"/api/v1/organization/edit/{organization_data['id']}",
            json={"name": "Renamed by client B"},
            headers={"X-API-Key": CLIENT_B_KEY},
        )
        assert res.status_code == 200
        edit = audit_lines(audit_log, "Audit edit organization")
        assert len(edit) == 1
        assert "user=api_client:client-b" in edit[0]
        assert "auth_type=api_client" in edit[0]

        res = client.delete(
            f"/api/v1/harvest_source/{source_data_dcatus['id']}",
            headers={"X-API-Key": CLIENT_A_KEY},
        )
        assert res.status_code == 200, res.get_json()
        archive = audit_lines(audit_log, "Audit archive harvest_source")
        assert len(archive) == 1
        assert "user=api_client:client-a" in archive[0]
        assert f"harvest_source_id={source_data_dcatus['id']}" in archive[0]

        # Archive attribution persisted in the database uses the client actor.
        source = interface_no_jobs.db.get(HarvestSource, source_data_dcatus["id"])
        interface_no_jobs.db.refresh(source)
        assert source.is_archived is True
        assert source.archived_by == "api_client:client-a"

    def test_identity_does_not_leak_between_requests(
        self, client, organization_data, interface, audit_log
    ):
        interface.add_organization(organization_data)
        org_id = organization_data["id"]
        for key, name in [
            (CLIENT_A_KEY, "A1"),
            (CLIENT_B_KEY, "B1"),
            (CLIENT_A_KEY, "A2"),
        ]:
            res = client.post(
                f"/api/v1/organization/edit/{org_id}",
                json={"name": name},
                headers={"X-API-Key": key},
            )
            assert res.status_code == 200

        users = [
            line.split("user=")[1].split()[0]
            for line in audit_lines(audit_log, "Audit edit organization")
        ]
        assert users == [
            "api_client:client-a",
            "api_client:client-b",
            "api_client:client-a",
        ]

        # A later anonymous request does not inherit any earlier identity.
        res = client.post(
            f"/api/v1/organization/edit/{org_id}",
            json={"name": "anon"},
        )
        assert res.status_code == 401

    @pytest.mark.parametrize(
        "spoof_headers",
        [
            {"X-API-Client": "client-b"},
            {"X-Client-Id": "client-b"},
            {"X-Forwarded-User": "admin@gsa.gov"},
            {"X-Remote-User": "admin@gsa.gov"},
            {"X-User": "api_client:client-b"},
        ],
    )
    def test_caller_supplied_identity_headers_are_ignored(
        self, client, spoof_headers, audit_log
    ):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("spoof-org"),
            headers={"X-API-Key": CLIENT_A_KEY, **spoof_headers},
        )
        assert res.status_code == 201
        audit = audit_lines(audit_log, "Audit create organization")
        assert len(audit) == 1
        assert "user=api_client:client-a" in audit[0]
        assert "client-b" not in audit_log.text
        assert "admin@gsa.gov" not in audit_log.text

    def test_identity_headers_without_a_key_do_not_authenticate(self, client):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("no-key-org"),
            headers={"X-API-Client": "client-a"},
        )
        assert res.status_code == 401
        assert res.data.decode() == "error: X-API-Key header missing"


@pytest.mark.usefixtures("api_clients")
class TestRejection:
    @pytest.mark.parametrize(
        "bad_key",
        [
            "not-a-real-key",
            digest_of(CLIENT_A_KEY),
            digest_of(CLIENT_A_KEY)[7:],
            CLIENT_A_KEY + "x",
        ],
    )
    def test_invalid_keys_are_rejected(self, client, bad_key, audit_log):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("bad-key-org"),
            headers={"X-API-Key": bad_key},
        )
        assert res.status_code == 401
        assert res.data.decode() == "error: Unauthorized"
        assert audit_lines(audit_log) == []
        assert bad_key not in audit_log.text
        assert "client-a" not in audit_log.text
        assert_no_secrets(audit_log.text, CLIENT_A_KEY)

    def test_missing_key_keeps_existing_semantics(self, client):
        res = client.post("/api/v1/organization/add", json=org_payload("m-org"))
        assert res.status_code == 401
        assert res.data.decode() == "error: X-API-Key header missing"

    def test_empty_key_keeps_existing_semantics(self, client):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("e-org"),
            headers={"X-API-Key": ""},
        )
        assert res.status_code == 401
        assert res.data.decode() == "error: Unauthorized"

    def test_invalid_key_does_not_fall_back_to_session(self, client, audit_log):
        with client.session_transaction() as sess:
            sess["user"] = "session-user@gsa.gov"

        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("fallback-org"),
            headers={"X-API-Key": "not-a-real-key"},
        )
        assert res.status_code == 401
        assert audit_lines(audit_log) == []

    def test_valid_key_wins_over_session_for_attribution(self, client, audit_log):
        with client.session_transaction() as sess:
            sess["user"] = "session-user@gsa.gov"

        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("key-and-session-org"),
            headers={"X-API-Key": CLIENT_B_KEY},
        )
        assert res.status_code == 201
        audit = audit_lines(audit_log, "Audit create organization")
        assert "user=api_client:client-b" in audit[0]
        assert "session-user@gsa.gov" not in audit[0]

    def test_log_injection_through_key_header_is_not_logged(self, client, audit_log):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("inject-org"),
            headers={"X-API-Key": "bogus user=api_client:client-a auth_type=x"},
        )
        assert res.status_code == 401
        assert "user=api_client:client-a" not in audit_log.text


class TestBackwardCompatibility:
    def test_legacy_shared_token_still_works_alongside_clients(
        self, app, client, api_clients, audit_log
    ):
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("legacy-org"),
            headers={"X-API-Key": app.config["API_TOKEN"]},
        )
        assert res.status_code == 201
        assert "method=api_token user=<api_token>" in login_lines(audit_log)[0]
        audit = audit_lines(audit_log, "Audit create organization")
        assert "user=<api_token>" in audit[0]
        assert "auth_type=api_token" in audit[0]
        assert app.config["API_TOKEN"] not in audit_log.text

    def test_legacy_shared_token_works_with_no_clients_configured(
        self, app, client, monkeypatch, audit_log
    ):
        configure_clients(app, monkeypatch, {})
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("legacy-only-org"),
            headers={"X-API-Key": app.config["API_TOKEN"]},
        )
        assert res.status_code == 201
        assert "user=<api_token>" in audit_lines(audit_log)[0]

    def test_session_auth_unchanged_when_clients_configured(
        self,
        app,
        client,
        api_clients,
        organization_data,
        interface,
        audit_log,
        monkeypatch,
    ):
        monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
        interface.add_organization(organization_data)
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        res = client.post(
            f"/organization/edit/{organization_data['id']}",
            data={
                "name": "Updated Org",
                "logo": "https://example.com/newlogo.png",
                "organization_type": "City Government",
                "description": "New description",
                "slug": "updated-slug",
            },
        )
        assert res.status_code == 302
        audit = audit_lines(audit_log, "Audit edit organization")
        assert "user=tester@gsa.gov" in audit[0]
        assert "auth_type=session" in audit[0]

    def test_anonymous_browser_still_redirected_to_login(self, client, api_clients):
        res = client.get("/organization/add")
        assert res.status_code == 302
        assert res.location.endswith("/login")

    def test_public_read_only_api_needs_no_key(
        self, client, api_clients, interface_no_jobs, organization_data
    ):
        res = client.get("/api/v1/organization_list/")
        assert res.status_code == 200
        assert any(org["id"] == organization_data["id"] for org in res.get_json())

        res = client.get(f"/api/v1/organization/{organization_data['id']}")
        assert res.status_code == 200


class TestRotationAndRevocation:
    def test_rotation_overlap_then_revoke_old_key(
        self, app, client, monkeypatch, audit_log
    ):
        configure_clients(
            app,
            monkeypatch,
            {"client-a": [digest_of(CLIENT_A_KEY), digest_of(CLIENT_A_OLD_KEY)]},
        )
        for slug, key in [
            ("old-key-org", CLIENT_A_OLD_KEY),
            ("new-key-org", CLIENT_A_KEY),
        ]:
            res = client.post(
                "/api/v1/organization/add",
                json=org_payload(slug),
                headers={"X-API-Key": key},
            )
            assert res.status_code == 201
        audits = audit_lines(audit_log, "Audit create organization")
        assert all("user=api_client:client-a" in line for line in audits)

        # Drop the old key: it stops working, the new one keeps working.
        configure_clients(app, monkeypatch, {"client-a": digest_of(CLIENT_A_KEY)})
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("old-key-org-2"),
            headers={"X-API-Key": CLIENT_A_OLD_KEY},
        )
        assert res.status_code == 401
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("new-key-org-2"),
            headers={"X-API-Key": CLIENT_A_KEY},
        )
        assert res.status_code == 201

    def test_revoked_client_is_rejected(self, app, client, monkeypatch):
        configure_clients(
            app,
            monkeypatch,
            {"client-a": digest_of(CLIENT_A_KEY), "client-b": digest_of(CLIENT_B_KEY)},
        )
        configure_clients(app, monkeypatch, {"client-b": digest_of(CLIENT_B_KEY)})
        res = client.post(
            "/api/v1/organization/add",
            json=org_payload("revoked-org"),
            headers={"X-API-Key": CLIENT_A_KEY},
        )
        assert res.status_code == 401


@pytest.mark.usefixtures("api_clients")
class TestHarvestSourceMutations:
    def test_api_edit_lock_and_unarchive_are_attributed(
        self, client, interface_no_jobs, source_data_dcatus, audit_log
    ):
        source_id = source_data_dcatus["id"]
        res = client.post(
            f"/api/v1/harvest_source/edit/{source_id}",
            json={"name": "Renamed by client B", "is_locked": True},
            headers={"X-API-Key": CLIENT_B_KEY},
        )
        assert res.status_code == 200, res.get_json()
        lock = audit_lines(audit_log, "Audit lock harvest_source")
        edit = audit_lines(audit_log, "Audit edit harvest_source")
        assert len(lock) == 1 and len(edit) == 1
        for line in (lock[0], edit[0]):
            assert "user=api_client:client-b" in line
            assert "auth_type=api_client" in line
            assert f"harvest_source_id={source_id}" in line

        res = client.delete(
            f"/api/v1/harvest_source/{source_id}", headers={"X-API-Key": CLIENT_A_KEY}
        )
        assert res.status_code == 200, res.get_json()
        res = client.post(
            f"/api/v1/harvest_source/{source_id}/unarchive",
            headers={"X-API-Key": CLIENT_B_KEY},
        )
        assert res.status_code == 200, res.get_json()
        unarchive = audit_lines(audit_log, "Audit unarchive harvest_source")
        assert len(unarchive) == 1
        assert "user=api_client:client-b" in unarchive[0]


class TestAuthorizationBoundaries:
    """A named key grants exactly what the shared token grants: nothing more."""

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("GET", "/organization/add"),
            ("GET", "/harvest_source/add"),
            ("GET", "/harvest_source/edit/{source_id}"),
            ("GET", "/organization/edit/{org_id}"),
            ("GET", "/api/v1/organization_list/"),
            ("GET", "/api/v1/harvest_source/{source_id}"),
            ("POST", "/api/v1/organization/edit/not-a-uuid"),
        ],
    )
    def test_named_key_matches_legacy_token_access(
        self,
        app,
        client,
        api_clients,
        interface_no_jobs,
        organization_data,
        source_data_dcatus,
        method,
        path,
    ):
        url = path.format(
            source_id=source_data_dcatus["id"], org_id=organization_data["id"]
        )
        statuses = {}
        for label, key in [
            ("legacy", app.config["API_TOKEN"]),
            ("named", CLIENT_A_KEY),
        ]:
            res = client.open(url, method=method, headers={"X-API-Key": key}, json={})
            statuses[label] = res.status_code
        assert statuses["named"] == statuses["legacy"]

    def test_api_keys_never_create_a_browser_session(self, app, client, api_clients):
        # Routes that check session["user"] directly (e.g. dataset slug edits)
        # therefore stay closed to every API key, named or shared.
        for key in (app.config["API_TOKEN"], CLIENT_A_KEY):
            res = client.get("/organization/add", headers={"X-API-Key": key})
            assert res.status_code == 200
            with client.session_transaction() as sess:
                assert "user" not in sess


class TestConfigLoadedByCreateApp:
    """Keys come from the environment when the app is created, as on deploy."""

    @pytest.fixture
    def make_app(self, monkeypatch, dbapp):
        from unittest.mock import patch

        from app import create_app

        created = []

        def _make(config: dict):
            monkeypatch.setenv("HARVEST_API_CLIENTS", json.dumps(config))
            with patch("app.deps.load_manager.start", lambda: True):
                fresh = create_app()
            fresh.config.update({"TESTING": True})
            created.append(fresh)
            return fresh

        yield _make
        from database.models import db

        for fresh in created:
            with fresh.app_context():
                db.session.remove()
                db.engine.dispose()

    def test_create_app_config_authenticates_and_attributes(self, make_app, audit_log):
        fresh = make_app({"client-a": digest_of(CLIENT_A_KEY)})
        res = fresh.test_client().post(
            "/api/v1/organization/add",
            json=org_payload("from-create-app"),
            headers={"X-API-Key": CLIENT_A_KEY},
        )
        assert res.status_code == 201
        audit = audit_lines(audit_log, "Audit create organization")
        assert "user=api_client:client-a" in audit[0]

    def test_changes_apply_only_to_instances_created_after_them(
        self, make_app, monkeypatch
    ):
        # Instance started before revocation keeps the old configuration until
        # it is restarted, as during a rolling restart.
        old_instance = make_app(
            {"client-a": digest_of(CLIENT_A_KEY), "client-b": digest_of(CLIENT_B_KEY)}
        )
        new_instance = make_app({"client-b": digest_of(CLIENT_B_KEY)})

        def add(app, slug):
            return (
                app.test_client()
                .post(
                    "/api/v1/organization/add",
                    json=org_payload(slug),
                    headers={"X-API-Key": CLIENT_A_KEY},
                )
                .status_code
            )

        assert add(new_instance, "revoked-on-new") == 401
        assert add(old_instance, "still-valid-on-old") == 201
