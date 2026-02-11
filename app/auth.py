"""APIFlask auth object to wrap our login_required logic."""

import os
import typing as t
from functools import wraps

from apiflask.security import APIKeyHeaderAuth
from flask import redirect, request, session, url_for


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
                        return "error: Authorization header missing", 401
                    api_token = os.getenv("FLASK_APP_SECRET_KEY")
                    if provided_token != api_token:
                        return "error: Unauthorized", 401
                    return f(*args, **kwargs)

                # check session-based authentication for web users
                if "user" not in session:
                    session["next"] = request.url
                    return redirect(url_for("main.login"))
                return f(*args, **kwargs)

            return decorated_function

        return login_required_internal
