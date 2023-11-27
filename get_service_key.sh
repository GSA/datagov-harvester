#!/bin/bash

elasticache_credentials=`cf service-key airflow-test-redis airflow-test-service-key | tail -n +3`
echo export elasticache_hostname=`echo "${elasticache_credentials}" | jq -r '.credentials.hostname'`
echo export elasticache_port=`echo "${elasticache_credentials}" | jq -r '.credentials.port'`
echo export elasticache_password=`echo "${elasticache_credentials}" | jq -r '.credentials.password'`
