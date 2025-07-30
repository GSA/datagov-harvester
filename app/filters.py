
import datetime

def usa_icon(str):
    # ruff: noqa: E501
    return f'<svg class="usa-icon" aria-hidden="true" role="img"><use xlink:href="/assets/uswds/img/sprite.svg#{str}"></use></svg>'


def else_na(val):
    return val if val else "N/A"


def utc_isoformat(val):
    """Make an ISO format datetime string with UTC timezone information.

    val should be a TIMEZONE-NAIVE datetime object such as we store in our
    database. If not, return it unchanged.
    """
    try:
        # make it aware that it is in the UTC timezone
        utc_val = val.replace(tzinfo=datetime.timezone(datetime.timedelta(0)))
        return utc_val.isoformat()
    except AttributeError:
        return val
