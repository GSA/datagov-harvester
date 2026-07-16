#!/usr/bin/env bash
# Refresh the running dev app after editing SCSS (or to force a Python reload),
# then optionally screenshot.
#
# Template edits hot-reload via FLASK_DEBUG + TEMPLATES_AUTO_RELOAD (see
# docker-compose.yml). SCSS must still be recompiled with gulp — run
# `make watch-static` in another terminal for continuous rebuilds.
#
# Usage:
#   scripts/ui_refresh.sh [--scss] [--out DIR] [label path ...]
#
#   --scss      recompile USWDS SCSS via gulp before reloading (needed for any
#               app/static/_scss change)
#   --out DIR   screenshot output dir (default /tmp/ui_after)
#   remaining args are passed through to ui_screenshot.py as label/path pairs;
#   if none are given, no screenshots are taken (just reload).
set -euo pipefail
cd "$(dirname "$0")/.."

DO_SCSS=0
OUT_DIR=/tmp/ui_after
PAIRS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scss) DO_SCSS=1; shift ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    *) PAIRS+=("$1"); shift ;;
  esac
done

if [[ "$DO_SCSS" == "1" ]]; then
  echo "[ui_refresh] compiling SCSS..."
  ( cd app/static && ./node_modules/.bin/gulp compile >/dev/null 2>&1 )
fi

# Bust the Jinja template cache by triggering the werkzeug reloader.
touch app/__init__.py
echo "[ui_refresh] waiting for reload..."
sleep 4

# Wait until the app answers again.
for _ in $(seq 1 20); do
  if curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/login | grep -q 200; then
    break
  fi
  sleep 1
done

if [[ ${#PAIRS[@]} -gt 0 ]]; then
  poetry run python scripts/ui_screenshot.py --out-dir "$OUT_DIR" "${PAIRS[@]}"
fi
