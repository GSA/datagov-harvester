"""APIFlask auth object to wrap our login_required logic."""

import logging
import os
import typing as t
from functools import wraps

from apiflask.security import APIKeyHeaderAuth
from flask import g, redirect, request, session, url_for

logger = logging.getLogger("harvest_admin.auth")


class LoginRequiredAuth(APIKeyHeaderAuth):

    def get_security_scheme(self) -> dict[str, t.Any]:
        """Hardcode our particular security scheme."""
        security_scheme = {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
        }
        return security_scheme

    def login_required(self, **kwargs):
        """login_required returns the decorator that will be called on the view."""

        def login_required_internal(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                provided_token = request.headers.get("Authorization")
                if request.is_json or provided_token is not None:
                    if provided_token is None:
                        logger.warning(
                            "API auth rejected: missing Authorization header for %s %s",
                            request.method,
                            request.path,
                        )
                        return "error: Authorization header missing", 401
                    api_token = os.getenv("FLASK_APP_SECRET_KEY")
                    if provided_token != api_token:
                        logger.warning(
                            "API auth rejected: invalid Authorization header for %s %s",
                            request.method,
                            request.path,
                        )
                        return "error: Unauthorized", 401
                    g.request_actor = "<api_token>"
                    g.request_auth_type = "api_token"
                    return f(*args, **kwargs)

                # check session-based authentication for web users
                if "user" not in session:
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
