from functools import wraps
from flask import request


def login_required(f):
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
            return redirect(url_for("harvest.login"))
        return f(*args, **kwargs)

    return decorated_function
