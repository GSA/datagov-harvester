from datetime import datetime, timezone
from unittest.mock import patch

from app.filters import elapsed_time, running_elapsed_time


def test_running_elapsed_time_uses_current_utc_time():
    with patch("app.filters.datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2026, 10, 27, 12, 5)
        assert running_elapsed_time(datetime(2026, 10, 27, 11, 0)) == "1h 5m"
        mock_datetime.now.assert_called_once_with(timezone.utc)


def test_running_elapsed_time_with_no_start_is_na():
    assert running_elapsed_time(None) == "N/A"


def test_elapsed_time_with_missing_finish_is_na():
    assert elapsed_time(datetime(2026, 10, 27, 11), None) == "N/A"


def test_elapsed_time_for_finished_job_uses_finish_time():
    assert (
        elapsed_time(datetime(2026, 10, 27, 11), datetime(2026, 10, 27, 11, 3)) == "3m"
    )
