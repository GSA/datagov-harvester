import logging
import logging.config
import os
import time
from urllib.parse import urlsplit

from apiflask import APIFlask
from dotenv import load_dotenv
from flask import g, request, session
from flask_bootstrap import Bootstrap5
from flask_htmx import HTMX
from flask_migrate import Migrate
from flask_talisman import Talisman

from app.filters import else_na, usa_icon, utc_isoformat
from app.local_dev_auth import is_running_on_cloud_foundry
from app.startup_validation import validate_required_env_vars
from config.logger_config import LOGGING_CONFIG
from database.models import db
from harvester.lib.load_manager import LoadManager
from scripts.sync_datasets import register_cli

logger = logging.getLogger("harvest_admin")

load_manager = LoadManager()

load_dotenv()
logging.config.dictConfig(LOGGING_CONFIG)

# fixes a bug with Flask-HTMX not being able to find the app context
htmx = None
HSTS_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
HSTS_HEADER = f"max-age={HSTS_MAX_AGE_SECONDS}; includeSubDomains; preload"


def current_unix_timestamp() -> int:
    return int(time.time())


def _external_route_to_server_url(route: str | None) -> str | None:
    """Return a normalized external server URL, or None for empty input."""
    if not route:
        return None

    route = route.strip().rstrip("/")
    if not route:
        return None

    if urlsplit(route).scheme:
        return route

    return f"https://{route}"


def create_app():
    env_values = validate_required_env_vars()
    app = APIFlask(__name__, static_url_path="", static_folder="static", docs_path=None)

    # OpenAPI fields
    app.config["INFO"] = {
        "title": "Datagov Harvester",
        "version": "0.1.0",
    }
    external_server_url = _external_route_to_server_url(os.getenv("EXTERNAL_ROUTE"))
    if external_server_url:
        app.config["SERVERS"] = [{"url": external_server_url}]

    # don't include auth information in the OpenAPI spec
    @app.spec_processor
    def remove_auth(spec):
        spec.get("components", {}).pop("securitySchemes", None)
        return spec

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = env_values["FLASK_APP_SECRET_KEY"]
    app.config["API_TOKEN"] = env_values["HARVEST_API_TOKEN"]
    app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024
    app.config["SESSION_COOKIE_NAME"] = os.getenv(
        "SESSION_COOKIE_NAME", "harvest_session"
    )
    app.config["SESSION_COOKIE_SECURE"] = is_running_on_cloud_foundry()
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["AUTH_COOKIE_NAME"] = os.getenv("AUTH_COOKIE_NAME", "harvest_auth")
    app.config["SESSION_IDLE_TIMEOUT_SECONDS"] = int(
        os.getenv("SESSION_IDLE_TIMEOUT_SECONDS", "900")
    )

    def get_session_cookie_name():
        return app.config["SESSION_COOKIE_NAME"]

    def get_auth_cookie_name():
        return app.config["AUTH_COOKIE_NAME"]

    def get_cookie_path():
        return app.config.get("SESSION_COOKIE_PATH") or "/"

    def get_cookie_domain():
        return app.config.get("SESSION_COOKIE_DOMAIN")

    def get_cookie_secure():
        return app.config.get("SESSION_COOKIE_SECURE", False)

    def get_cookie_samesite():
        return app.config.get("SESSION_COOKIE_SAMESITE", "Lax")

    def clear_session_cookie(response):
        response.delete_cookie(
            get_session_cookie_name(),
            path=get_cookie_path(),
            domain=get_cookie_domain(),
        )
        return response

    def set_auth_cookie(response):
        """mark the response as authenticated for CloudFront cache separation."""
        response.set_cookie(
            get_auth_cookie_name(),
            "1",
            path=get_cookie_path(),
            domain=get_cookie_domain(),
            secure=get_cookie_secure(),
            httponly=True,
            samesite=get_cookie_samesite(),
        )
        return response

    def clear_auth_cookie(response):
        """remove the authenticated marker cookie when login state is gone."""
        response.delete_cookie(
            get_auth_cookie_name(),
            path=get_cookie_path(),
            domain=get_cookie_domain(),
        )
        return response

    @app.before_request
    def expire_stale_session():
        g.clear_session_cookie = False
        g.sync_auth_cookie = False

        if request.path.startswith("/assets/"):
            return

        if not session.get("user"):
            return

        now = current_unix_timestamp()
        last_activity = session.get("last_activity")
        timeout_seconds = app.config["SESSION_IDLE_TIMEOUT_SECONDS"]

        try:
            last_activity = int(last_activity)
        except (TypeError, ValueError):
            last_activity = None

        if last_activity is None:
            session["last_activity"] = now
            return

        if now - last_activity > timeout_seconds:
            logger.info(
                "Session expired for user=%s path=%s",
                session.get("user"),
                request.path,
            )
            session.clear()
            g.clear_session_cookie = True
            g.sync_auth_cookie = True
            return

        session["last_activity"] = now

    def set_private_no_store(response):
        response.headers["Cache-Control"] = "private, no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    def set_public_cache(response, ttl):
        response.headers["Cache-Control"] = f"public, max-age={ttl}, s-maxage={ttl}"
        response.headers.pop("Pragma", None)
        response.headers.pop("Expires", None)
        return response

    @app.after_request
    def apply_cache_headers(response):
        path = request.path or "/"
        method = request.method
        has_session_user = bool(session.get("user"))

        if getattr(g, "clear_session_cookie", False):
            response = clear_session_cookie(response)

        if has_session_user:
            response = set_auth_cookie(response)
        elif getattr(g, "sync_auth_cookie", False) or request.cookies.get(
            get_auth_cookie_name()
        ):
            response = clear_auth_cookie(response)

        sets_cookie = bool(response.headers.getlist("Set-Cookie"))

        if path.startswith(("/login", "/callback", "/logout")) or path == "/login/oidc":
            return set_private_no_store(response)

        if path.startswith("/assets/"):
            return set_public_cache(response, 3600)

        if method not in {"GET", "HEAD"}:
            return set_private_no_store(response)

        if has_session_user or sets_cookie or response.status_code >= 400:
            return set_private_no_store(response)

        return set_public_cache(response, 60)

    Bootstrap5(app)
    global htmx
    htmx = HTMX(app)

    db.init_app(app)

    Migrate(app, db)

    from .local_dev_auth import log_local_dev_login_status
    from .routes import register_routes

    register_routes(app)

    log_local_dev_login_status()

    from .commands import register_commands

    register_commands(app)

    add_template_filters(app)
    register_cli(app)

    with app.app_context():
        # SQL-Alchemy can't be used to create the schema here
        # Instead, `flask db upgrade` must already have been run
        # db.create_all()
        try:
            load_manager.start()
        except Exception as e:
            # we need to get to app start up, so ignore all errors
            # from the load manager but log them
            logger.warning("Load manager startup failed with exception: %s", repr(e))

    # Content-Security-Policy headers
    # single quotes need to appear in some of the strings
    csp = {
        "default-src": "'self'",
        "script-src": " ".join(
            [
                "'self'",
                "'unsafe-hashes'",
                "https://cdn.jsdelivr.net",  # Bootstrap CDN
                "https://www.googletagmanager.com",
                "https://unpkg.com",  # Swagger
            ]
        ),
        "font-src": " ".join(
            [
                "'self'",  # USWDS fonts
                "https://cdnjs.cloudflare.com",  # font awesome
            ]
        ),
        "img-src": " ".join(
            [
                "'self'",
                "data:",
                "https://s3-us-gov-west-1.amazonaws.com",  # GSA Starmark
                "https://raw.githubusercontent.com",  # github logos repo
                "https://*.google-analytics.com",  # GA4 beacons
                "https://*.googletagmanager.com",  # GTM beacons
            ]
        ),
        "connect-src": " ".join(
            [
                "'self'",
                "https://*.google-analytics.com",  # GA4 collect endpoint
                "https://*.analytics.google.com",  # GA4 analytics
                "https://*.googletagmanager.com",  # GTM
                "https://www.google.com",  # GA4 collect fallback
            ]
        ),
        "frame-src": "https://www.googletagmanager.com",
        "style-src-attr": " ".join(
            [
                "'self'",
            ]
        ),
        "style-src-elem": " ".join(
            [
                "'self'",
                "'unsafe-hashes'",  # local styles.css
                "https://cdn.jsdelivr.net",  # Bootstrap CDN
                "https://cdnjs.cloudflare.com",  # font-awesome
                "'sha256-faU7yAF8NxuMTNEwVmBz+VcYeIoBQ2EMHW3WaVxCvnk='",  # htmx.min.js
                "https://unpkg.com",  # Swagger
            ]
        ),
    }
    Talisman(
        app,
        content_security_policy=csp,
        content_security_policy_nonce_in=["script-src", "style-src-elem"],
        strict_transport_security_max_age=HSTS_MAX_AGE_SECONDS,
        strict_transport_security_preload=True,
        # our https connections are terminated outside this app
        force_https=False,
        session_cookie_secure=is_running_on_cloud_foundry(),
    )

    return app


def add_template_filters(app):
    for fn in [usa_icon, else_na, utc_isoformat]:
        app.add_template_filter(fn)
