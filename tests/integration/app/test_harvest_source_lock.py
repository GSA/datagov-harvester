from datetime import datetime, timedelta

import pytest

from database.interface import HarvestSourceLockedError
from harvester.lib.load_manager import LoadManager


def test_lock_blocks_queuing_and_starting_until_unlock(
    interface_no_jobs, source_data_dcatus
):
    source_id = source_data_dcatus["id"]
    db = interface_no_jobs
    source = db.get_harvest_source(source_id)
    assert source.is_locked is False

    queued_job = db.add_harvest_job(
        {"harvest_source_id": source_id, "status": "new", "job_type": "harvest"}
    )
    next_run = datetime.now() - timedelta(days=1)
    db.update_harvest_source(source_id, {"date_next_run": next_run})
    db.set_harvest_source_lock(source_id, True)

    with pytest.raises(HarvestSourceLockedError):
        db.enqueue_harvest_job_if_idle(source_id)
    assert db.get_due_harvest_sources() == []
    assert db.get_new_harvest_jobs_in_past() == []
    assert "locked" in LoadManager()._prepare_job_in_db(queued_job.id)
    assert db.get_harvest_job(queued_job.id).status == "new"
    assert db.get_harvest_source(source_id).date_next_run == next_run

    db.set_harvest_source_lock(source_id, False)
    assert [job.id for job in db.get_new_harvest_jobs_in_past()] == [queued_job.id]
    assert LoadManager()._prepare_job_in_db(queued_job.id) is None
    assert db.get_harvest_job(queued_job.id).status == "in_progress"


def test_locked_source_rejects_admin_request_and_logs_lock(
    app, client, interface_no_jobs, source_data_dcatus, caplog
):
    app.config["WTF_CSRF_ENABLED"] = False
    source_id = source_data_dcatus["id"]
    with client.session_transaction() as session:
        session["user"] = "admin@example.gov"

    response = client.post(f"/harvest_source/{source_id}", data={"lock": "Lock"})
    assert response.status_code == 302
    assert interface_no_jobs.get_harvest_source(source_id).is_locked is True
    assert "Audit lock harvest_source" in caplog.text

    message = LoadManager().trigger_manual_job(source_id)
    assert "locked" in message
    assert interface_no_jobs.get_active_harvest_job_for_source(source_id) is None

    response = client.post(f"/harvest_source/{source_id}", data={"unlock": "Unlock"})
    assert response.status_code == 302
    assert interface_no_jobs.get_harvest_source(source_id).is_locked is False


def test_locked_source_rejects_job_add_api(
    app, client, interface_no_jobs, source_data_dcatus
):
    source_id = source_data_dcatus["id"]
    interface_no_jobs.set_harvest_source_lock(source_id, True)
    response = client.post(
        "/api/v1/harvest_job/add",
        json={
            "harvest_source_id": source_id,
            "status": "new",
            "job_type": "harvest",
        },
        headers={"X-API-Key": app.config["API_TOKEN"]},
    )
    assert response.status_code == 409
    assert response.json == {"error": "Harvest source is locked."}
    assert interface_no_jobs.get_active_harvest_job_for_source(source_id) is None
