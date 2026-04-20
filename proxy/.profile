#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service () {
    local path name
    name="$1"
    path="$2"
    service_name=datagov-harvest-${name}
    echo "$VCAP_SERVICES" | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | $path"
}

# basic auth
echo "Setting basic auth username and password"
PROXY_USER=$(vcap_get_service secrets .credentials.PROXY_USER)
PROXY_PASSWORD=$(openssl passwd -apr1 "$(vcap_get_service secrets .credentials.PROXY_PASSWORD)")
echo "$PROXY_USER:$PROXY_PASSWORD" > ${HOME}/.htpasswd
