from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup
from sqlalchemy.orm import sessionmaker

from database.interface import HarvesterDBInterface, StaleReharvestLinkError
from database.models import HarvestJob
from database.models import HarvestSource as SourceModel
from database.models import Organization, db
from harvester.harvest import HarvestSource
from harvester.lib.load_manager import LoadManager
from harvester.utils.reharvest_links import (
    create_reharvest_token,
    read_reharvest_token,
)


def finished_job(interface_no_jobs, source_data_dcatus):
    return interface_no_jobs.add_harvest_job(
        {
            "harvest_source_id": source_data_dcatus["id"],
            "status": "complete",
            "job_type": "harvest",
            "date_created": datetime.now() - timedelta(minutes=1),
            "date_finished": datetime.now(),
        }
    )


def test_each_notification_recipient_gets_own_link(
    app, interface_no_jobs, source_data_dcatus, monkeypatch
):
    job = finished_job(interface_no_jobs, source_data_dcatus)
    source = HarvestSource(job.id)
    source.notification_emails = ["one@gsa.gov", "two@gsa.gov"]
    monkeypatch.setenv("FLASK_APP_SECRET_KEY", app.config["SECRET_KEY"])
    results = {
        "records_added": 0,
        "records_updated": 0,
        "records_deleted": 0,
        "records_ignored": 0,
        "records_errored": 0,
        "records_warned": 0,
        "records_validated": 0,
    }

    with (
        patch(
            "harvester.harvest.SMTP_CONFIG",
            {
                "base_url": "https://harvest.data.gov",
                "recipient": "support@gsa.gov",
            },
        ),
        patch("harvester.harvest.send_email_to_recipients") as send_email,
    ):
        source.send_notification_emails(results)
        interface_no_jobs.update_harvest_job(job.id, {"status": "error"})
        interface_no_jobs.update_harvest_source(
            source_data_dcatus["id"], {"notification_frequency": "on_error"}
        )
        source.send_notification_emails(results, job_status="error")

    assert send_email.call_count == 6
    for index, call in enumerate(send_email.call_args_list):
        recipient = call.args[0][0]
        body = call.args[2]
        html_body = call.kwargs["html_body"]
        assert call.args[0] == [recipient]
        for version in (body, BeautifulSoup(html_body, "html.parser").get_text()):
            assert (
                "A newer harvest job expires links in earlier notification emails."
                in version
            )
            if index < 3:
                assert "Notification frequency:" not in version
            else:
                assert "Notification frequency: On error." in version
                assert "Ask the Data.gov team to set it to Always" in version
        line = BeautifulSoup(html_body, "html.parser").select_one(
            ".notification-frequency-line"
        )
        if index < 3:
            assert line is None
        else:
            assert line is not None
            assert [tag.get_text() for tag in line.find_all("strong")] == [
                "On error",
                "Always",
            ]
            assert "\n\nNotification frequency: On error." in body
            assert line.get_text() == (
                "Notification frequency: On error. Ask the Data.gov team to set "
                "it to Always to receive a new link after every finished harvest job."
            )
        url = next(
            line
            for line in body.splitlines()
            if line.startswith("https://harvest.data.gov/")
        )
        token = parse_qs(urlsplit(url).query)["token"][0]
        payload = read_reharvest_token(app.config["SECRET_KEY"], token)
        assert payload == {
            "email": recipient,
            "source_id": source_data_dcatus["id"],
            "job_id": job.id,
        }


def test_failed_job_cleanup_email_also_has_a_link(
    app, interface_no_jobs, source_data_dcatus, monkeypatch
):
    monkeypatch.setenv("FLASK_APP_SECRET_KEY", app.config["SECRET_KEY"])
    with (
        patch(
            "harvester.lib.load_manager.SMTP_CONFIG",
            {
                "base_url": "https://harvest.data.gov",
                "recipient": "support@gsa.gov",
            },
        ),
        patch("harvester.lib.load_manager.send_email_to_recipients") as send_email,
    ):
        LoadManager._email_recipients_of_failed_jobs(
            None, [("job-1", "Example source", source_data_dcatus["id"])]
        )

    body = send_email.call_args.args[2]
    html_body = send_email.call_args.kwargs["html_body"]
    for version in (body, BeautifulSoup(html_body, "html.parser").get_text()):
        assert (
            "A newer harvest job expires links in earlier notification emails."
            in version
        )
        assert "Notification frequency:" not in version
    assert (
        BeautifulSoup(html_body, "html.parser").select_one(
            ".notification-frequency-line"
        )
        is None
    )
    url = next(
        line
        for line in body.splitlines()
        if line.startswith("https://harvest.data.gov/harvest_source/refresh")
    )
    token = parse_qs(urlsplit(url).query)["token"][0]
    assert read_reharvest_token(app.config["SECRET_KEY"], token) == {
        "email": "support@gsa.gov",
        "source_id": source_data_dcatus["id"],
        "job_id": "job-1",
    }

    interface_no_jobs.update_harvest_source(
        source_data_dcatus["id"], {"notification_frequency": "on_error_or_update"}
    )
    with (
        patch(
            "harvester.lib.load_manager.SMTP_CONFIG",
            {
                "base_url": "https://harvest.data.gov",
                "recipient": "support@gsa.gov",
            },
        ),
        patch("harvester.lib.load_manager.send_email_to_recipients") as send_email,
    ):
        LoadManager._email_recipients_of_failed_jobs(
            None, [("job-1", "Example source", source_data_dcatus["id"])]
        )
    body = send_email.call_args.args[2]
    line = BeautifulSoup(
        send_email.call_args.kwargs["html_body"], "html.parser"
    ).select_one(".notification-frequency-line")
    assert line is not None
    assert line.get_text() == (
        "Notification frequency: On error or update. Ask the Data.gov team "
        "to set it to Always to receive a new link after every finished harvest job."
    )
    assert "\n\n" + line.get_text() in body
    assert [tag.get_text() for tag in line.find_all("strong")] == [
        "On error or update",
        "Always",
    ]


def test_refresh_link_requires_confirmation_and_latest_job(
    app, client, interface_no_jobs, source_data_dcatus, caplog, monkeypatch
):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    path = f"/harvest_source/refresh?token={token}"

    response = client.get(path)
    assert response.status_code == 200
    assert "Refresh now" in response.text
    source_link = BeautifulSoup(response.text, "html.parser").find(
        "a", href=f"/harvest_source/{source_data_dcatus['id']}"
    )
    assert source_link is not None
    assert source_link.get_text(strip=True) == source_data_dcatus["name"]
    assert "no-store" in response.headers["Cache-Control"]
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        == report_job.id
    )

    csrf = BeautifulSoup(response.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    with patch(
        "app.deps.load_manager.start_job",
        return_value=("Harvest requested", "success"),
    ):
        response = client.post(
            "/harvest_source/refresh",
            data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
        )
    assert response.status_code == 200
    assert "Harvest requested" in response.text

    new_job = interface_no_jobs.get_latest_harvest_job_for_source(
        source_data_dcatus["id"]
    )
    assert new_job.id != report_job.id
    assert new_job.status == "new"
    assert new_job.job_type == "harvest"
    stale_get = client.get(path)
    assert stale_get.status_code == 410
    assert BeautifulSoup(stale_get.text, "html.parser").find(
        "a", href=f"/harvest_source/{source_data_dcatus['id']}"
    )
    assert "This link is from an earlier harvest notification." in stale_get.text
    assert "A newer harvest job is queued or running." in stale_get.text
    assert "Wait for it to finish" in stale_get.text
    assert "Notification frequency:" not in stale_get.text
    assert (
        BeautifulSoup(stale_get.text, "html.parser").select_one(
            ".notification-frequency-line"
        )
        is None
    )
    assert "Audit reharvest recipient=person@gsa.gov" in caplog.text
    assert f"report_job_id={report_job.id}" in caplog.text
    assert f"new_job_id={new_job.id}" in caplog.text
    assert token not in caplog.text

    stale_post = client.post(
        "/harvest_source/refresh",
        data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
    )
    assert stale_post.status_code == 410
    assert "This link is from an earlier harvest notification." in stale_post.text

    interface_no_jobs.delete_harvest_job(new_job.id)
    response = client.get(path)
    assert response.status_code == 200
    csrf = BeautifulSoup(response.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    with patch(
        "app.deps.load_manager.start_job",
        return_value=("Harvest requested", "success"),
    ):
        retry = client.post(
            "/harvest_source/refresh",
            data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
        )
    assert retry.status_code == 200
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        != report_job.id
    )


def test_newer_job_suggests_always_when_notifications_are_conditional(
    app, client, interface_no_jobs, source_data_dcatus
):
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    new_job = interface_no_jobs.add_harvest_job(
        {
            "harvest_source_id": source_data_dcatus["id"],
            "status": "new",
            "job_type": "harvest",
            "date_created": datetime.now(),
        }
    )

    for frequency in ("on_error", "on_error_or_update"):
        interface_no_jobs.update_harvest_source(
            source_data_dcatus["id"], {"notification_frequency": frequency}
        )
        response = client.get(f"/harvest_source/refresh?token={token}")
        assert response.status_code == 410
        assert "This link is from an earlier harvest notification." in response.text
        soup = BeautifulSoup(response.text, "html.parser")
        line = soup.select_one(".notification-frequency-line")
        assert line is not None
        assert soup.select_one(".usa-alert--warning") is None
        label = "On error" if frequency == "on_error" else "On error or update"
        assert line.get_text() == (
            f"Notification frequency: {label}. Ask the Data.gov team to set "
            "it to Always to receive a new link after every finished harvest job."
        )
        assert [tag.get_text() for tag in line.find_all("strong")] == [
            label,
            "Always",
        ]

    interface_no_jobs.update_harvest_job(
        new_job.id, {"status": "complete", "date_finished": datetime.now()}
    )
    interface_no_jobs.update_harvest_source(
        source_data_dcatus["id"], {"notification_frequency": "always"}
    )
    response = client.get(f"/harvest_source/refresh?token={token}")
    assert response.status_code == 410
    assert "This link is from an earlier harvest notification." in response.text
    assert "A newer harvest job has finished." in response.text
    assert "If you received a newer notification email" in response.text
    assert "Notification frequency:" not in response.text
    assert (
        BeautifulSoup(response.text, "html.parser").select_one(
            ".notification-frequency-line"
        )
        is None
    )


def test_refresh_link_rejects_tampering_and_locked_source(
    app, client, interface_no_jobs, source_data_dcatus, monkeypatch
):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    invalid = client.get(f"/harvest_source/refresh?token={token}x")
    assert invalid.status_code == 400
    assert "This link could not be verified." in invalid.text

    confirmation = client.get(f"/harvest_source/refresh?token={token}")
    csrf = BeautifulSoup(confirmation.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    interface_no_jobs.set_harvest_source_lock(source_data_dcatus["id"], True)
    response = client.get(f"/harvest_source/refresh?token={token}")
    assert response.status_code == 409
    assert "Harvesting is locked for this source." in response.text
    assert "have it unlocked" in response.text
    response = client.post(
        "/harvest_source/refresh",
        data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
    )
    assert response.status_code == 409
    assert "have it unlocked" in response.text
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        == report_job.id
    )

    interface_no_jobs.set_harvest_source_lock(source_data_dcatus["id"], False)
    assert client.get(f"/harvest_source/refresh?token={token}").status_code == 200


def test_refresh_link_explains_missing_job_and_removed_source(
    app, client, interface_no_jobs, source_data_dcatus
):
    interface_no_jobs.add_harvest_job(
        {
            "harvest_source_id": source_data_dcatus["id"],
            "status": "complete",
            "job_type": "harvest",
            "date_created": datetime.now() - timedelta(minutes=2),
            "date_finished": datetime.now() - timedelta(minutes=1),
        }
    )
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    path = f"/harvest_source/refresh?token={token}"

    interface_no_jobs.delete_harvest_job(report_job.id)
    missing_job = client.get(path)
    assert missing_job.status_code == 410
    assert (
        "could not find the harvest job associated with this link" in missing_job.text
    )
    assert "Contact the Data.gov team" in missing_job.text

    _, status = interface_no_jobs.delete_harvest_source(source_data_dcatus["id"])
    assert status == 200
    missing_source = client.get(path)
    assert missing_source.status_code == 410
    assert "This harvest source has been removed." in missing_source.text
    assert "should be restored" in missing_source.text


def test_refresh_link_explains_unfinished_job(
    app, client, interface_no_jobs, source_data_dcatus
):
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    interface_no_jobs.update_harvest_job(
        report_job.id, {"status": "in_progress", "date_finished": None}
    )

    response = client.get(f"/harvest_source/refresh?token={token}")
    assert response.status_code == 410
    assert "not recorded as finished" in response.text
    assert "Wait for it to finish." in response.text


def test_refresh_link_explains_job_queued_during_confirmation(
    app, client, interface_no_jobs, source_data_dcatus, monkeypatch
):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    confirmation = client.get(f"/harvest_source/refresh?token={token}")
    csrf = BeautifulSoup(confirmation.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]

    def queue_newer_job(*args, **kwargs):
        interface_no_jobs.add_harvest_job(
            {
                "harvest_source_id": source_data_dcatus["id"],
                "status": "new",
                "job_type": "harvest",
                "date_created": datetime.now(),
            }
        )
        raise StaleReharvestLinkError(source_data_dcatus["id"])

    with patch("app.deps.db.enqueue_harvest_job_if_idle", side_effect=queue_newer_job):
        response = client.post(
            "/harvest_source/refresh",
            data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
        )
    assert response.status_code == 410
    assert "This link is from an earlier harvest notification." in response.text
    assert "Wait for it to finish" in response.text


def test_refresh_link_explains_source_removed_during_confirmation(
    app, client, interface_no_jobs, source_data_dcatus, monkeypatch
):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    report_job = finished_job(interface_no_jobs, source_data_dcatus)
    token = create_reharvest_token(
        app.config["SECRET_KEY"],
        "person@gsa.gov",
        source_data_dcatus["id"],
        report_job.id,
    )
    confirmation = client.get(f"/harvest_source/refresh?token={token}")
    csrf = BeautifulSoup(confirmation.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]

    def remove_source(*args, **kwargs):
        _, status = interface_no_jobs.delete_harvest_source(source_data_dcatus["id"])
        assert status == 200
        raise ValueError("Harvest source not found")

    with patch("app.deps.db.enqueue_harvest_job_if_idle", side_effect=remove_source):
        response = client.post(
            "/harvest_source/refresh",
            data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
        )
    assert response.status_code == 410
    assert "This harvest source has been removed." in response.text
    assert "should be restored" in response.text


def test_simultaneous_link_requests_queue_only_one_job(
    app, organization_data, source_data_dcatus
):
    db.session.add(Organization(**organization_data))
    db.session.add(SourceModel(**source_data_dcatus))
    report_job = HarvestJob(
        harvest_source_id=source_data_dcatus["id"],
        status="complete",
        job_type="harvest",
        date_created=datetime.now() - timedelta(minutes=1),
        date_finished=datetime.now(),
    )
    db.session.add(report_job)
    db.session.commit()
    report_job_id = report_job.id
    source_id = source_data_dcatus["id"]
    sessions = sessionmaker(bind=db.engine)
    barrier = Barrier(2)

    def request_harvest():
        with sessions() as session:
            interface = HarvesterDBInterface(session=session)
            barrier.wait(timeout=5)
            try:
                job, _ = interface.enqueue_harvest_job_if_idle(
                    source_id, expected_latest_job_id=report_job_id
                )
                return job.id
            except StaleReharvestLinkError:
                return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: request_harvest(), range(2)))

    assert sum(result is not None for result in results) == 1
    jobs = db.session.query(HarvestJob).filter_by(harvest_source_id=source_id).all()
    assert len(jobs) == 2
