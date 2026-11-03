from datetime import datetime, timedelta, timezone

from flask import Flask

from app.json_provider import ISO8601JSONProvider


def test_datetime_serialization():
    app = Flask(__name__)
    app.json = ISO8601JSONProvider(app)

    with app.app_context():
        result = app.json.loads(
            app.json.dumps(
                {
                    "date_created": datetime(2026, 5, 19, 3, 2, 5, 678000),
                    "date_finished": None,
                }
            )
        )

    assert result["date_created"] == "2026-05-19T03:02:05.678Z"
    assert result["date_finished"] is None


def test_timezone_conversion():
    app = Flask(__name__)
    app.json = ISO8601JSONProvider(app)

    value = datetime(
        2026,
        5,
        19,
        8,
        2,
        5,
        tzinfo=timezone(timedelta(hours=5)),
    )

    assert app.json.loads(app.json.dumps(value)) == "2026-05-19T03:02:05.000Z"
