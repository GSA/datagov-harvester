from datetime import timezone


def utc_isoformat(value):
    """Convert datetime values to ISO 8601 UTC with millisecond precision."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
