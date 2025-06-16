#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BASE_PATH="/apps/vnstockdata"

# 1) Prepare
if [[ ! -f $BASE_PATH/.venv/bin/activate ]]; then
  echo "ERROR: venv activate not found" >&2
  exit 1
fi

# 2) Enter app root
cd "$BASE_PATH"

# 3) Activate & log start
export PYTHONPATH=$BASE_PATH
source $BASE_PATH/.venv/bin/activate

python3.10 "$1"
