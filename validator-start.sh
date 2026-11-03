#!/bin/bash
# Start command for the datagov-harvest-validator app (manifest.yml). No
# migrations: the validator has no database. 2 threads per worker because a
# request mostly waits on a remote URL (up to 12s); kept low because each one
# can also hold a catalog of up to 150MB in memory while it validates, which
# peaks at 1.1-1.7GB in that worker (validator_memory_quota in vars.*.yml; see
# docs/validator-security.md). The threads share one GIL, so two large
# validations in a worker take twice as long rather than running in parallel.

DIR="$(dirname "${BASH_SOURCE[0]}")"

echo "Starting validator gunicorn"
exec newrelic-admin run-program gunicorn "validator_api.wsgi:application" --config "$DIR/gunicorn.conf.py" -b "0.0.0.0:$PORT" --chdir "$DIR" --timeout 120 --worker-class gthread --workers 3 --threads 2 --forwarded-allow-ips='*'
