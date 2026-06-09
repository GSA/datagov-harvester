#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service () {
    local path name service_name
    name="$1"
    path="$2"
    service_name=datagov-harvest-${name}
    echo "$VCAP_SERVICES" | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | $path"
}

function require_vcap_value () {
    local service="$1"
    local credential_path="$2"
    local label="$3"
    local _val

    _val=$(vcap_get_service "$service" "$credential_path")
    if [ "$_val" = "null" ] || [ -z "$_val" ]; then
        echo "ERROR: ${label} not found in VCAP_SERVICES. Aborting startup." >&2
        exit 1
    fi
    printf '%s' "$_val"
}

# basic auth
echo "Setting basic auth username and password"
PROXY_USER=$(require_vcap_value secrets .credentials.PROXY_USER PROXY_USER)
PROXY_PASSWORD_RAW=$(require_vcap_value secrets .credentials.PROXY_PASSWORD PROXY_PASSWORD)
PROXY_PASSWORD=$(openssl passwd -apr1 "$PROXY_PASSWORD_RAW")
echo "$PROXY_USER:$PROXY_PASSWORD" > "${HOME}/.htpasswd"
