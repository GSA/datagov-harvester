#!/usr/bin/env python3
"""Capture full-page screenshots of the running harvester admin UI.

Reuses the Playwright auth-cookie approach from tests/playwright/conftest.py so
authenticated pages render. Intended for UI-polish review work.

Usage:
    python scripts/ui_screenshot.py <label> <path> [<label> <path> ...]
    python scripts/ui_screenshot.py --out-dir /tmp/ui_after org_list /organization_list/

Each (label, path) pair is fetched at http://localhost:8080<path> and saved to
<out-dir>/<label>.png. Pass --width to change viewport width (default 1280).
"""

import argparse
import os
import sys
import time
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import Flask
from flask.sessions import SecureCookieSessionInterface
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_URL = os.getenv("UI_SHOT_BASE_URL", "http://localhost:8080/")


def _signed_session_cookie() -> str:
    secret = os.getenv("FLASK_APP_SECRET_KEY")
    if not secret:
        raise RuntimeError("FLASK_APP_SECRET_KEY is required")
    app = Flask(__name__)
    app.secret_key = secret
    serializer = SecureCookieSessionInterface().get_signing_serializer(app)
    if serializer is None:
        raise RuntimeError("Unable to create Flask session serializer")
    user = os.getenv("PLAYWRIGHT_AUTH_USER", "user@test.local")
    return serializer.dumps({"user": user, "last_activity": int(time.time())})


def _authed_storage_state(base_url: str) -> dict:
    host = urlparse(base_url).hostname or "localhost"
    return {
        "cookies": [
            {
                "name": "harvest_session",
                "value": _signed_session_cookie(),
                "domain": host,
                "path": "/",
                "expires": -1,
                "httpOnly": True,
                "secure": False,
                "sameSite": "Lax",
            },
            {
                "name": "harvest_auth",
                "value": "1",
                "domain": host,
                "path": "/",
                "expires": -1,
                "httpOnly": True,
                "secure": False,
                "sameSite": "Lax",
            },
        ],
        "origins": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="/tmp/ui_shots")
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument(
        "--no-auth", action="store_true", help="capture as logged-out user"
    )
    parser.add_argument("pairs", nargs="+", help="alternating <label> <path> values")
    args = parser.parse_args()

    if len(args.pairs) % 2 != 0:
        parser.error("pairs must be an even number of <label> <path> values")

    os.makedirs(args.out_dir, exist_ok=True)
    targets = list(zip(args.pairs[0::2], args.pairs[1::2]))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chrome")
        ctx_kwargs = {
            "base_url": BASE_URL,
            "viewport": {"width": args.width, "height": 900},
        }
        if not args.no_auth:
            ctx_kwargs["storage_state"] = _authed_storage_state(BASE_URL)
        context = browser.new_context(**ctx_kwargs)
        page = context.new_page()
        page.set_default_timeout(8000)
        page.set_default_navigation_timeout(20000)
        for label, path in targets:
            out = os.path.join(args.out_dir, f"{label}.png")
            try:
                page.goto(path, wait_until="networkidle")
            except Exception:
                # networkidle can time out on pages with long-poll/htmx; fall back
                page.goto(path, wait_until="load")
            page.wait_for_timeout(600)
            page.screenshot(path=out, full_page=True)
            print(f"{label}\t{path}\t{out}")
        context.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
