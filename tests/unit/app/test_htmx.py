from flask_htmx import HTMX

import app as app_module
from app.main import harvest_jobs, harvest_sources, pages


def test_views_share_the_apps_htmx_extension(app):
    # pytest collection can import the views (via app.routes) before any app
    # exists, so they must not hold a None bound at import (GSA/data.gov#6474).
    assert isinstance(app_module.htmx, HTMX)
    for view_module in (harvest_jobs, harvest_sources, pages):
        assert view_module.htmx is app_module.htmx
    assert app.htmx is app_module.htmx
