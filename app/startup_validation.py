import os
from collections.abc import Mapping, Sequence


REQUIRED_ENV_VARS = ("FLASK_APP_SECRET_KEY",)


class StartupValidationError(RuntimeError):
    """Raised when the application cannot safely start."""


def validate_required_env_vars(
    required_env_vars: Sequence[str] = REQUIRED_ENV_VARS,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    env = os.environ if environ is None else environ
    missing_env_vars = [
        env_var
        for env_var in required_env_vars
        if not env.get(env_var) or not env[env_var].strip()
    ]

    if missing_env_vars:
        missing_list = ", ".join(missing_env_vars)
        raise StartupValidationError(
            f"Missing required environment variable(s): {missing_list}"
        )

    return {env_var: env[env_var] for env_var in required_env_vars}
