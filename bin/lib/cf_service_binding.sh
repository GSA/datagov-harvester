#!/bin/bash

cf_service_binding_guid() {
  local app_name service_name
  app_name="$1"
  service_name="$2"

  cf curl \
    "/v3/service_credential_bindings?app_names=${app_name}&service_instance_names=${service_name}" |
    jq -r '.resources[0].guid // empty'
}

cf_service_binding_exists() {
  [[ -n "$(cf_service_binding_guid "$1" "$2")" ]]
}

cf_binding_credential() {
  local app_name service_name key binding_guid
  app_name="$1"
  service_name="$2"
  key="$3"
  binding_guid=$(cf_service_binding_guid "$app_name" "$service_name")

  if [[ -z "$binding_guid" ]]; then
    return 0
  fi

  cf curl "/v3/service_credential_bindings/${binding_guid}/details" |
    jq -r --arg key "$key" '.credentials[$key] // empty'
}
