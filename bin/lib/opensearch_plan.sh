#!/bin/sh

# Shared OpenSearch service configuration for normal deploys and replacements.

# shellcheck disable=SC2034  # Read by scripts that source this library.
OPENSEARCH_ENGINE_VERSION=OpenSearch_2.11

opensearch_plan_for_space() {
  case "$1" in
    prod) echo es-large ;;
    staging) echo es-medium-ha ;;
    development) echo es-medium ;;
    *) echo "" ;;
  esac
}

opensearch_current_space() {
  cf target | grep space | cut -d : -f 2 | xargs
}
