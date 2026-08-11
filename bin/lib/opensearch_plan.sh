#!/bin/sh

# OpenSearch plan and engine version per space.
#
# Single source of truth, so a replacement cluster provisioned for a migration
# cannot silently differ from the live one it is meant to replace. Sourced by
# create_cloudgov_services.sh (POSIX sh) and bin/provision_opensearch_cluster.sh
# (bash), so keep this POSIX-compatible: no arrays, no [[ ]], no local.

# The engine version both clusters are created with.
# shellcheck disable=SC2034  # read by the scripts that source this file
OPENSEARCH_ENGINE_VERSION=OpenSearch_2.11

# Print the default plan for a space, or nothing when the space is unrecognized.
#
# Non-HA plans are 3 primary + 2 data nodes; -ha doubles the data nodes to 4.
# An unrecognized space gets no plan, so a sandbox space never provisions a
# multi-node cluster by accident -- callers must treat empty as "refuse".
#
# Usage: opensearch_plan_for_space <space>
opensearch_plan_for_space () {
  case "$1" in
    prod)        echo es-large ;;
    staging)     echo es-medium-ha ;;
    development) echo es-medium ;;
    *)           echo "" ;;
  esac
}

# The space the CF CLI is currently targeting.
#
# Usage: opensearch_current_space
opensearch_current_space () {
  cf target | grep space | cut -d : -f 2 | xargs
}
