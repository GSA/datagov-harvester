import json
import os

SPACE_EMAIL_PREFIXES = {"development": "Dev", "staging": "Stage"}


def is_running_on_cloud_foundry() -> bool:
    return "VCAP_APPLICATION" in os.environ


def cf_space_name() -> str | None:
    try:
        return json.loads(os.getenv("VCAP_APPLICATION", "{}")).get("space_name")
    except (json.JSONDecodeError, AttributeError):
        return None


def with_environment_prefix(title: str) -> str:
    """Prefix non-prod titles with the environment, e.g. "Dev Harvest Job Failed"."""
    prefix = SPACE_EMAIL_PREFIXES.get(cf_space_name())
    return f"{prefix} {title}" if prefix else title
