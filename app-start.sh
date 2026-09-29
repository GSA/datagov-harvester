#!/bin/bash

DIR="$(dirname "${BASH_SOURCE[0]}")"

# run migrations if we are the first CloudFoundry instance or
# if there is no CF_INSTANCE_INDEX environment variable
# SKIP_DB_MIGRATIONS lets a non-canonical app (e.g. the validator app) share
# this script without also running migrations against the shared database.
if [ "$SKIP_DB_MIGRATIONS" != "true" ] && [ "$CF_INSTANCE_INDEX" = "0" -o -z "$CF_INSTANCE_INDEX" ]; then
    echo Running migrations
    flask db upgrade
else
    # Silent skips here look identical to app-start.sh never having run at
    # all, which is hard to tell apart from a crash-looping container in `cf
    # logs`. Log which branch we took instead.
    echo "Skipping migrations (SKIP_DB_MIGRATIONS=${SKIP_DB_MIGRATIONS:-unset}, CF_INSTANCE_INDEX=${CF_INSTANCE_INDEX:-unset})"
fi

echo "Starting gunicorn"
exec newrelic-admin run-program gunicorn "wsgi:application" --config "$DIR/gunicorn.conf.py" -b "0.0.0.0:$PORT" --chdir $DIR --timeout 120 --worker-class gthread --workers 3 --forwarded-allow-ips='*'
