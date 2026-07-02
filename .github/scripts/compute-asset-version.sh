#!/usr/bin/env bash
set -euo pipefail

static_dirs=()
for dir in app/static/assets app/static/js app/static/css app/static/data; do
  if [ -d "$dir" ]; then
    static_dirs+=("$dir")
  fi
done

if [ "${#static_dirs[@]}" -eq 0 ]; then
  echo "No static asset directories found" >&2
  exit 1
fi

# Hash served static files after `make install-static`.
# Includes built output (assets/) plus JS/CSS/data files served directly from source.
find "${static_dirs[@]}" -type f \
  | sort \
  | xargs sha256sum \
  | sha256sum \
  | awk '{print substr($1, 1, 7)}'
