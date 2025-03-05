from functools import wraps
from flask.testing import FlaskClient


# ruff: noqa: E501
# decorator to spoof login in session
def force_login(email=None):
    def inner(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if email:
                for key, val in kwargs.items():
                    if isinstance(val, FlaskClient):
                        with val:
                            with val.session_transaction() as sess:
                                sess["user"] = email
                            return f(*args, **kwargs)
            return f(*args, **kwargs)

        return wrapper

    return inner
