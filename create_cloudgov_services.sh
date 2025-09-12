#!/bin/sh

set -e

# If an argument was provided, use it as the service name prefix.
# Otherwise default to "datagov-harvest".
app_name=${1:-datagov-harvest}

# Get the current space and trim leading whitespace
space=$(cf target | grep space | cut -d : -f 2 | xargs)

# create email service
cf service "${app_name}-smtp"  > /dev/null 2>&1 || cf create-service --wait aws-ses domain "${app_name}-smtp" -c '{"admin_email": "datagovhelp@gsa.gov"}'

# create the secrets service if necessary
cf service "${app_name}-secrets"  > /dev/null 2>&1 || cf cups "${app_name}-secrets"

# Production and staging should use bigger DB instances
if [ "$space" = "prod" ] || [ "$space" = "staging" ]; then
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service --wait aws-rds xlarge-gp-psql "${app_name}-db"
else
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service --wait aws-rds medium-gp-psql "${app_name}-db"
fi
