"""APIFlask auth object to wrap our login_required logic."""

import logging
import typing as t
from functools import wraps

from apiflask.security import APIKeyHeaderAuth
from flask import current_app, g, redirect, request, session, url_for

from app.api_credentials import authenticate_api_key
from app.auth_logging import get_client_ip

logger = logging.getLogger("harvest_admin.auth")


class LoginRequiredAuth(APIKeyHeaderAuth):
    API_KEY_HEADER_NAME = "X-API-Key"

    def get_security_scheme(self) -> dict[str, t.Any]:
        """Hardcode our particular security scheme."""
        security_scheme = {
            "type": "apiKey",
            "name": self.API_KEY_HEADER_NAME,
            "in": "header",
        }
        return security_scheme

    def login_required(self, **kwargs):
        """login_required returns the decorator that will be called on the view."""

        def login_required_internal(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                provided_token = request.headers.get(self.API_KEY_HEADER_NAME)
                if request.is_json or provided_token is not None:
                    if provided_token is None:
                        logger.warning(
                            "API auth rejected: missing API key header for %s %s",
                            request.method,
                            request.path,
                        )
                        return f"error: {self.API_KEY_HEADER_NAME} header missing", 401
                    identity = authenticate_api_key(
                        provided_token,
                        current_app.config.get("API_CLIENTS"),
                        current_app.config.get("API_TOKEN"),
                    )
                    if identity is None:
                        logger.warning(
                            "API auth rejected: invalid API key header for %s %s",
                            request.method,
                            request.path,
                        )
                        return "error: Unauthorized", 401
                    g.request_actor = identity.actor
                    g.request_auth_type = identity.auth_type
                    if identity.client_id is None:
                        logger.info(
                            "event=user_login method=api_token user=<api_token> "
                            "ip=%s path=%s",
                            get_client_ip(),
                            request.path,
                        )
                    else:
                        logger.info(
                            "event=user_login method=api_client user=%s "
                            "client_id=%s ip=%s path=%s",
                            identity.actor,
                            identity.client_id,
                            get_client_ip(),
                            request.path,
                        )
                    return f(*args, **kwargs)

                # check session-based authentication for web users
                if "user" not in session:
                    if request.method == "GET":
                        session["next"] = request.url
                    logger.info(
                        "Web auth redirecting anonymous request for %s %s to login",
                        request.method,
                        request.path,
                    )
                    return redirect(url_for("main.login"))
                g.request_actor = session["user"]
                g.request_auth_type = "session"
                return f(*args, **kwargs)

            return decorated_function

        return login_required_internal
