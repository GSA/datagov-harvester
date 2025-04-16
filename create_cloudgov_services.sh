#!/bin/sh

set -e

# If an argument was provided, use it as the service name prefix.
# Otherwise default to "harvesting-logic".
app_name=${1:-datagov-harvest}

# Get the current space and trim leading whitespace
space=$(cf target | grep space | cut -d : -f 2 | xargs)

# Production and staging should use bigger DB instances
if [ "$space" = "prod" ] || [ "$space" = "staging" ]; then
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service aws-rds xlarge-gp-psql-redundant "${app_name}-db" --wait&
else
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service aws-rds smal-psql "${app_name}-db" --wait&
fi

# create email service
cf service "${app_name}-smtp"  > /dev/null 2>&1 || cf create-service datagov-smtp base "${app_name}-smtp" -b "ssb-smtp-gsa-datagov-${space}"

# Wait until all the services are ready
wait

# Check that all the services are in a healthy state. (The OSBAPI spec says that
# the "last operation" should include "succeeded".)
success=0;
for service in "${app_name}-db"
do
    status=$(cf service "$service" | grep 'status:\s*.\+$' )
    echo "$service $status"
    if ! echo "$status" | grep -q 'succeeded' ; then
        success=1;
    fi
done
exit $success
