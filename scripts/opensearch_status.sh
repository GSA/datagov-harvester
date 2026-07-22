#!/usr/bin/env bash
#
# Stream a standalone Python status script to a running Cloud Foundry app.

set -o errexit
set -o nounset
set -o pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
app_name="${CF_APP_NAME:-datagov-harvest}"
app_instance="${CF_APP_INSTANCE:-0}"
python_script="$script_dir/opensearch_status.py"

if [ "$#" -gt 2 ]; then
  echo "Usage: $0 [APP_NAME] [PYTHON_SCRIPT]" >&2
  exit 2
fi
if [ "$#" -ge 1 ]; then
  app_name="$1"
fi
if [ "$#" -eq 2 ]; then
  python_script="$2"
fi

if [ ! -r "$python_script" ]; then
  echo "Python status script is not readable: $python_script" >&2
  exit 1
fi
if ! command -v cf >/dev/null 2>&1; then
  echo "The Cloud Foundry CLI (cf) is required." >&2
  exit 1
fi

echo "Cloud Foundry target:"
cf target
echo
echo "Querying OpenSearch through ${app_name} instance ${app_instance}..."
echo "Streaming Python payload: ${python_script}"

remote_command='
set -o errexit
set -o nounset
set -o pipefail

export PATH="$HOME/deps/0/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/deps/0/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cd "$HOME/app"
. "$HOME/app/.profile" >/dev/null

if ! command -v python >/dev/null 2>&1; then
  echo "Python was not found in the Cloud Foundry buildpack PATH." >&2
  exit 1
fi

exec python -
'

cf ssh "$app_name" -i "$app_instance" -T -c "$remote_command" < "$python_script"
