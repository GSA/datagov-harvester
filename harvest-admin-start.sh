#!/bin/bash

DIR="$(dirname "${BASH_SOURCE[0]}")"

exec newrelic-admin run-program gunicorn "wsgi:application" --config "$DIR/gunicorn.conf.py" -b "0.0.0.0:$PORT" --chdir $DIR --timeout 120 --worker-class gevent --workers 8 --forwarded-allow-ips='*'
