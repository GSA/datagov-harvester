#!/bin/bash

set -o errexit
set -o pipefail

DB_HOSTNAME=$(cf env datagov-harvest-admin | grep '"host":' | sed -E 's/.*"host": "(.*)".*/\1/')

DB_PORT=$(cf env datagov-harvest-admin | grep '"port":' | sed -E 's/.*"port": "(.*)".*/\1/')

cf ssh -L "15432:$DB_HOSTNAME:$DB_PORT" datagov-harvest-admin