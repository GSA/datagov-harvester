#!/bin/bash

DIR="$(dirname "${BASH_SOURCE[0]}")"

# run migrations if we are the first CloudFoundry instance or
# if there is no CF_INSTANCE_INDEX environment variable
if [ "$CF_INSTANCE_INDEX" = "0" -o -z "$CF_INSTANCE_INDEX" ]; then
    echo Running migrations
    flask db upgrade 
fi

exec gunicorn "wsgi:application" --config "$DIR/gunicorn.conf.py" -b "0.0.0.0:$PORT" --chdir $DIR --timeout 120 --worker-class gthread --workers 3 --forwarded-allow-ips='*'
