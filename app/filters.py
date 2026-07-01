import datetime

from app.static_assets import static_url

# Acronyms that should stay upper-cased when humanizing snake_case keys.
_HUMANIZE_ACRONYMS = {"id": "ID", "url": "URL", "ckan": "CKAN", "uuid": "UUID"}


def humanize(key):
    """Turn a snake_case field key into a human-readable label.

    e.g. 'organization_type' -> 'Organization type', 'url' -> 'URL',
    'harvest_source_id' -> 'Harvest source ID'. Used so config tables can
    keep iterating model dicts while showing polished labels.
    """
    if not isinstance(key, str):
        return key
    words = key.replace("-", "_").split("_")
    rendered = []
    for i, word in enumerate(words):
        if word.lower() in _HUMANIZE_ACRONYMS:
            rendered.append(_HUMANIZE_ACRONYMS[word.lower()])
        elif i == 0:
            rendered.append(word.capitalize())
        else:
            rendered.append(word.lower())
    return " ".join(rendered)


def usa_icon(str):
    sprite_path = static_url("assets/uswds/img/sprite.svg")
    return (
        '<svg class="usa-icon" aria-hidden="true" role="img">'
        f'<use xlink:href="{sprite_path}#{str}"></use></svg>'
    )


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
