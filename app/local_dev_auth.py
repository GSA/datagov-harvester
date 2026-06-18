"""Development-only login helpers. Never enabled in production."""

import logging
import os
import secrets

logger = logging.getLogger("harvest_admin.local_dev_auth")

LOCAL_DEV_USERNAME = "admin"
LOCAL_DEV_PASSWORD = "admin"
LOCAL_DEV_SESSION_EMAIL = "admin@local.gov"


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes")


def is_running_on_cloud_foundry() -> bool:
    return "VCAP_APPLICATION" in os.environ


def is_local_dev_login_enabled() -> bool:
    return not is_running_on_cloud_foundry() and _env_flag_enabled(
        "ENABLE_LOCAL_DEV_LOGIN"
    )


def log_local_dev_login_status() -> None:
    if is_local_dev_login_enabled():
        logger.warning(
            "Local dev login is ENABLED (admin/admin). "
            "Disable ENABLE_LOCAL_DEV_LOGIN outside local development."
        )


def validate_local_dev_credentials(username: str, password: str) -> bool:
    if not username or not password:
        return False
    username_ok = secrets.compare_digest(username.strip(), LOCAL_DEV_USERNAME)
    password_ok = secrets.compare_digest(password, LOCAL_DEV_PASSWORD)
    return username_ok and password_ok
