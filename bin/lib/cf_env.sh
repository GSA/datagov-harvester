#!/bin/bash

# Read a single user-provided environment variable out of `cf env` output.
#
# Note `cf env` reports only user-provided variables, VCAP_*, and env-var
# groups. It does NOT show variables a buildpack `.profile` script exports at
# container start -- use `bin/report_opensearch_cluster.sh` for those.

# Usage: cf_env_value <app_name> <variable_name>
# Prints the value, or nothing when the variable is not set.
cf_env_value () {
  local app_name key
  app_name="$1"
  key="$2"

  # Anchor on the exact key so a longer name sharing this prefix (for example
  # OPENSEARCH_NEXT_SERVICE_NAME vs OPENSEARCH_SERVICE_NAME) cannot match. Take
  # the first hit: user-provided values precede the env-var-group sections.
  cf env "$app_name" |
    awk -v key="$key" '
      !found && $0 ~ "^[[:space:]]*" key ":[[:space:]]*" {
        sub("^[[:space:]]*" key ":[[:space:]]*", "")
        value = $0
        found = 1
      }
      END { if (found) print value }
    '
}

# Read one credential out of an app's binding to a service instance.
#
# Reads the binding itself via the API rather than the container's VCAP_SERVICES,
# so it works without `cf ssh` and without restarting the app -- which matters
# because the caller uses it to decide whether a restart would even be useful.
#
# Usage: cf_binding_credential <app_name> <service_instance> <credential_key>
# Prints the value, or nothing when the binding or key is absent/empty.
cf_binding_credential () {
  local app_name service_name key binding_guid
  app_name="$1"
  service_name="$2"
  key="$3"

  binding_guid=$(
    cf curl "/v3/service_credential_bindings?app_names=${app_name}&service_instance_names=${service_name}" |
      jq -r '.resources[0].guid // empty'
  )
  if [[ -z "$binding_guid" ]]; then
    return 0
  fi

  # `// empty` collapses both a missing key and a JSON null to no output; the
  # caller only cares whether it got a usable value.
  cf curl "/v3/service_credential_bindings/${binding_guid}/details" |
    jq -r --arg key "$key" '.credentials[$key] // empty'
}
