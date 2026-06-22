import os
import secrets
import time
import uuid

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from flask import flash, make_response, redirect, render_template, request, session, url_for

from app import current_unix_timestamp
from app.blueprints import deps
from app.blueprints.deps import logger
from app.forms import LocalDevLoginForm
from app.local_dev_auth import (
    LOCAL_DEV_SESSION_EMAIL,
    is_local_dev_login_enabled,
    validate_local_dev_credentials,
)

from . import main

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
ISSUER = os.getenv("ISSUER")
AUTH_URL = ISSUER + "/openid_connect/authorize"
TOKEN_URL = ISSUER + "/api/openid_connect/token"


def create_client_assertion():
    private_key_data = os.getenv("OPENID_PRIVATE_KEY")
    if not private_key_data:
        raise ValueError("No private key found in the environment variable")

    private_key = load_pem_private_key(
        private_key_data.encode("utf-8"), password=None, backend=default_backend()
    )

    now = int(time.time())
    payload = {
        "iss": CLIENT_ID,
        "sub": CLIENT_ID,
        "aud": TOKEN_URL,
        "jti": uuid.uuid4().hex,
        "exp": now + 900,
        "iat": now - 100,
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


def _redirect_to_login_gov():
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    session["state"] = state
    session["nonce"] = nonce
    logger.info(
        "Login initiated from ip=%s next=%s",
        request.headers.get("X-Forwarded-For", request.remote_addr),
        session.get("next"),
    )

    auth_request_url = (
        f"{AUTH_URL}?response_type=code"
        f"&acr_values=urn:acr.login.gov:auth-only+http://idmanagement.gov/ns/assurance/aal/2?hspd12=true"
        f"&client_id={CLIENT_ID}"
        f"&nonce={nonce}"
        "&prompt=select_account"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=openid+email"
        f"&state={state}"
    )
    return redirect(auth_request_url)


@main.route("/login/oidc", methods=["GET"])
def login_oidc():
    return _redirect_to_login_gov()


@main.route("/login", methods=["GET", "POST"])
def login():
    if not is_local_dev_login_enabled():
        if request.method == "POST":
            return make_response("Not Found", 404)
        return _redirect_to_login_gov()

    form = LocalDevLoginForm(meta={"csrf": False})
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if validate_local_dev_credentials(username, password):
            session["user"] = LOCAL_DEV_SESSION_EMAIL
            session["last_activity"] = current_unix_timestamp()
            logger.info(
                "Local dev login succeeded for user=%s ip=%s",
                LOCAL_DEV_SESSION_EMAIL,
                request.headers.get("X-Forwarded-For", request.remote_addr),
            )
            next_url = session.pop("next", None)
            if next_url:
                return redirect(next_url)
            return redirect(url_for("main.index"))
        flash("Invalid username or password.", "danger")
        logger.warning(
            "Local dev login failed from ip=%s",
            request.headers.get("X-Forwarded-For", request.remote_addr),
        )

    return render_template("local_login.html", form=form)


@main.route("/logout")
def logout():
    user = session.get("user")
    session.clear()
    logger.info(
        "Logout completed for user=%s ip=%s",
        user or "<anonymous>",
        request.headers.get("X-Forwarded-For", request.remote_addr),
    )
    return redirect(url_for("main.index"))


@main.route("/callback")
def callback():
    code = request.args.get("code")
    state = request.args.get("state")
    request_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

    logger.info(
        "Login callback received ip=%s has_code=%s has_state=%s",
        request_ip,
        bool(code),
        bool(state),
    )

    if state != session.pop("state", None):
        logger.warning("Login callback failed: state mismatch ip=%s", request_ip)
        return "State mismatch error", 400

    try:
        client_assertion = create_client_assertion()
    except Exception as e:
        logger.exception(
            "Login callback failed: could not create client assertion ip=%s error=%s",
            request_ip,
            repr(e),
        )
        return "Failed to prepare login request", 500

    token_payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": client_assertion,
    }
    try:
        response = requests.post(TOKEN_URL, data=token_payload)
    except Exception as e:
        logger.exception(
            "Login callback failed: token exchange request error ip=%s error=%s",
            request_ip,
            repr(e),
        )
        return "Failed to fetch access token", 400

    if response.status_code != 200:
        logger.error(
            "Login callback failed: token endpoint returned status=%s ip=%s body=%s",
            response.status_code,
            request_ip,
            response.text,
        )
        return "Failed to fetch access token", 400

    try:
        token_data = response.json()
    except ValueError as e:
        logger.exception(
            "Login callback failed: token endpoint returned invalid JSON ip=%s error=%s",
            request_ip,
            repr(e),
        )
        return "Failed to fetch access token", 400

    id_token = token_data.get("id_token")
    if not id_token:
        logger.error(
            "Login callback failed: id_token missing from token response ip=%s",
            request_ip,
        )
        return "Failed to fetch access token", 400

    try:
        decoded_id_token = jwt.decode(id_token, options={"verify_signature": False})
    except Exception as e:
        logger.exception(
            "Login callback failed: unable to decode id_token ip=%s error=%s",
            request_ip,
            repr(e),
        )
        return "Failed to decode login token", 400

    usr_email = decoded_id_token.get("email")
    if not usr_email:
        logger.error(
            "Login callback failed: email missing from id_token claims ip=%s sub=%s",
            request_ip,
            decoded_id_token.get("sub"),
        )
        return "Failed to identify user", 400

    usr_email = usr_email.lower()
    usr_ssoid = decoded_id_token.get("sub")
    if not usr_ssoid:
        logger.error(
            "Login callback failed: sub missing from id_token claims user=%s ip=%s",
            usr_email,
            request_ip,
        )
        return "Failed to identify user", 400

    usr_info = {"email": usr_email, "ssoid": usr_ssoid}
    usr = deps.db.verify_user(usr_info)

    if usr:
        session["user"] = usr_email
        session["last_activity"] = current_unix_timestamp()
        logger.info(
            "Login succeeded for user=%s ssoid=%s ip=%s",
            usr_email,
            usr_ssoid,
            request_ip,
        )
        next_url = session.pop("next", None)
        if next_url:
            return redirect(url_for(next_url))
        else:
            return redirect(url_for("main.index"))
    else:
        logger.warning(
            "Login rejected for unregistered user=%s ssoid=%s ip=%s",
            usr_email,
            usr_ssoid,
            request_ip,
        )
        flash("Please request registration from the admin before proceeding.")
        return redirect(url_for("main.index"))
