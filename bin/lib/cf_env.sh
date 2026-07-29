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
