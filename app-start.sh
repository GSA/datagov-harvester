#!/bin/bash

DIR="$(dirname "${BASH_SOURCE[0]}")"

# run migrations if we are the first CloudFoundry instance or
# if there is no CF_INSTANCE_INDEX environment variable
if [ "$CF_INSTANCE_INDEX" = "0" -o -z "$CF_INSTANCE_INDEX" ]; then
    echo Running migrations
    flask db upgrade

    # `flask db upgrade` can exit 0 with the schema still out of sync with
    # the models -- e.g. a previously-applied migration file gets deleted, or
    # someone edits the DB by hand outside of a migration. Refuse to boot
    # rather than serve traffic against a schema the app doesn't actually
    # match.
    echo Checking for schema drift
    if ! flask db check; then
        echo "Schema drift detected between models, migrations, and the live database. Refusing to start." >&2
        exit 1
    fi
fi

exec newrelic-admin run-program gunicorn "wsgi:application" --config "$DIR/gunicorn.conf.py" -b "0.0.0.0:$PORT" --chdir $DIR --timeout 120 --worker-class gthread --workers 3 --forwarded-allow-ips='*'
