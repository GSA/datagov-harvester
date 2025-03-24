import os

from app.routes import main
import secrets
from flask import session


# Login authentication
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
ISSUER = os.getenv("ISSUER")
AUTH_URL = ISSUER + "/openid_connect/authorize"
TOKEN_URL = ISSUER + "/api/openid_connect/token"


@mod.route("/login")
def login():
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    session["state"] = state
    session["nonce"] = nonce

    auth_request_url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=openid email"
        f"&state={state}"
        f"&nonce={nonce}"
        f"&acr_values=http://idmanagement.gov/ns/assurance/loa/1"
    )
    return redirect(auth_request_url)


@mod.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("harvest.index"))


@mod.route("/callback")
def callback():
    code = request.args.get("code")
    state = request.args.get("state")

    if state != session.pop("state", None):
        return "State mismatch error", 400

    client_assertion = create_client_assertion()
    # ruff: noqa: E501
    token_payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": client_assertion,
    }

    response = requests.post(TOKEN_URL, data=token_payload)

    if response.status_code != 200:
        return "Failed to fetch access token", 400

    token_data = response.json()
    id_token = token_data.get("id_token")

    decoded_id_token = jwt.decode(id_token, options={"verify_signature": False})

    usr_email = decoded_id_token["email"].lower()

    usr_info = {"email": usr_email, "ssoid": decoded_id_token["sub"]}
    usr = db.verify_user(usr_info)

    if usr:
        session["user"] = usr_email
        next_url = session.pop("next", None)
        if next_url:
            return redirect(url_for(next_url))
        else:
            return redirect(url_for("harvest.index"))
    else:
        flash("Please request registration from the admin before proceeding.")
        return redirect(url_for("harvest.index"))
