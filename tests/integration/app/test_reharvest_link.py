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
        source.send_notification_emails(results, job_status="error")

    assert send_email.call_count == 6
    for call in send_email.call_args_list:
        recipient = call.args[0][0]
        body = call.args[2]
        assert call.args[0] == [recipient]
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


def test_failed_job_cleanup_email_also_has_a_link(app, monkeypatch):
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
            None, [("job-1", "Example source", "source-1")]
        )

    body = send_email.call_args.args[2]
    url = next(
        line
        for line in body.splitlines()
        if line.startswith("https://harvest.data.gov/harvest_source/refresh")
    )
    token = parse_qs(urlsplit(url).query)["token"][0]
    assert read_reharvest_token(app.config["SECRET_KEY"], token) == {
        "email": "support@gsa.gov",
        "source_id": "source-1",
        "job_id": "job-1",
    }


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
    assert "no-store" in response.headers["Cache-Control"]
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        == report_job.id
    )

    csrf = BeautifulSoup(response.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    with patch("app.deps.load_manager.start_job", return_value="Harvest requested"):
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
    assert client.get(path).status_code == 410
    assert "Audit reharvest recipient=person@gsa.gov" in caplog.text
    assert f"report_job_id={report_job.id}" in caplog.text
    assert f"new_job_id={new_job.id}" in caplog.text
    assert token not in caplog.text

    stale_post = client.post(
        "/harvest_source/refresh",
        data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
    )
    assert stale_post.status_code == 410

    interface_no_jobs.delete_harvest_job(new_job.id)
    response = client.get(path)
    assert response.status_code == 200
    csrf = BeautifulSoup(response.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    with patch("app.deps.load_manager.start_job", return_value="Harvest requested"):
        retry = client.post(
            "/harvest_source/refresh",
            data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
        )
    assert retry.status_code == 200
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        != report_job.id
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
    assert client.get(f"/harvest_source/refresh?token={token}x").status_code == 400

    confirmation = client.get(f"/harvest_source/refresh?token={token}")
    csrf = BeautifulSoup(confirmation.text, "html.parser").find(
        "input", {"name": "csrf_token"}
    )["value"]
    interface_no_jobs.set_harvest_source_lock(source_data_dcatus["id"], True)
    response = client.get(f"/harvest_source/refresh?token={token}")
    assert response.status_code == 409
    assert "locked" in response.text
    response = client.post(
        "/harvest_source/refresh",
        data={"token": token, "csrf_token": csrf, "refresh": "Refresh now"},
    )
    assert response.status_code == 409
    assert (
        interface_no_jobs.get_latest_harvest_job_for_source(source_data_dcatus["id"]).id
        == report_job.id
    )

    interface_no_jobs.set_harvest_source_lock(source_data_dcatus["id"], False)
    assert client.get(f"/harvest_source/refresh?token={token}").status_code == 200


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
