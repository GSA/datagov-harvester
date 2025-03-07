def usa_icon(str):
    # ruff: noqa: E501
    return f'<svg class="usa-icon" aria-hidden="true" role="img"><use xlink:href="/assets/uswds/img/sprite.svg#{str}"></use></svg>'


def else_na(val):
    return val if val else "N/A"
