from datetime import datetime

from flask.json.provider import DefaultJSONProvider

from app.datetime_utils import utc_isoformat


class ISO8601JSONProvider(DefaultJSONProvider):
    """Serialize datetime objects as ISO 8601 UTC timestamps."""

    @staticmethod
    def default(obj):
        if isinstance(obj, datetime):
            return utc_isoformat(obj)

        return DefaultJSONProvider.default(obj)
