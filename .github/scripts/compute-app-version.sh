#!/usr/bin/env bash
set -euo pipefail

# Get short commit SHA (7 characters) for application version tracking.
# This is used to identify which code is running in Cloud Foundry.
git rev-parse --short=7 HEAD
