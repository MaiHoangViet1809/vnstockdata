#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BASE_PATH="/apps/vnstockdata"
LOG_FILE="$BASE_PATH/logs/scheduler.log"

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
echo "$(date +'%Y-%m-%d %H:%M:%S') ----------------------- START schedule_5m.py ----------------------------" >> "$LOG_FILE"

pkill -f '/schedule_5m\.py$' 2>/dev/null || true
sleep 3

# 4) Launch
nohup python3 jobs/schedule_5m.py >> "$LOG_FILE" 2>&1 &
