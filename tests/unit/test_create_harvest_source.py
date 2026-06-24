from unittest.mock import MagicMock, patch

from app.routes import _create_harvest_source


def test_create_harvest_source_duplicate_url():
    existing = MagicMock(id="existing-id")
    with patch("app.routes.db") as mock_db, patch("app.routes.load_manager"):
        mock_db.get_harvest_source_by_url.return_value = existing

        source, message, status = _create_harvest_source({"url": "https://example.com"})

    assert source is None
    assert status == 409
    assert "existing-id" in message
    mock_db.try_add_harvest_source.assert_not_called()


def test_create_harvest_source_success():
    new_source = MagicMock(id="new-id", organization_id="org-id", name="Test")
    with (
        patch("app.routes.db") as mock_db,
        patch("app.routes.load_manager") as mock_load_manager,
        patch("app.routes._log_mutation") as mock_log_mutation,
    ):
        mock_db.get_harvest_source_by_url.return_value = None
        mock_db.try_add_harvest_source.return_value = (new_source, None)
        mock_load_manager.schedule_first_job.return_value = "Job scheduled."

        source, message, status = _create_harvest_source(
            {"url": "https://new.example.com"}
        )

    assert source is new_source
    assert status == 200
    assert "new-id" in message
    mock_log_mutation.assert_called_once()


def test_create_harvest_source_try_add_failure():
    with patch("app.routes.db") as mock_db, patch("app.routes.load_manager"):
        mock_db.get_harvest_source_by_url.return_value = None
        mock_db.try_add_harvest_source.return_value = (None, "bad data")

        source, message, status = _create_harvest_source({"url": "https://example.com"})

    assert source is None
    assert status == 400
    assert message == "bad data"


def test_create_harvest_source_schedule_failure():
    new_source = MagicMock(id="new-id")
    with (
        patch("app.routes.db") as mock_db,
        patch("app.routes.load_manager") as mock_load_manager,
        patch("app.routes._log_mutation") as mock_log_mutation,
    ):
        mock_db.get_harvest_source_by_url.return_value = None
        mock_db.try_add_harvest_source.return_value = (new_source, None)
        mock_load_manager.schedule_first_job.return_value = None

        source, message, status = _create_harvest_source({"url": "https://example.com"})

    assert source is new_source
    assert status == 500
    assert "Failed to schedule" in message
    mock_log_mutation.assert_not_called()
