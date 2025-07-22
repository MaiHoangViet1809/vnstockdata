#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BASE_PATH="/apps/vnstockdata"
LOG_DIR="$BASE_PATH/logs"
RUNNER_LOG="$LOG_DIR/scheduler_runner.out"  # just startup / crash notes

# 1) Prepare
if [[ ! -f $BASE_PATH/.venv/bin/activate ]]; then
  echo "ERROR: venv activate not found" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

# 2) Enter app root
cd "$BASE_PATH"

# 3) Activate & stamp start-up
export PYTHONPATH=$BASE_PATH
source "$BASE_PATH/.venv/bin/activate"
echo "$(date '+%Y-%m-%d %H:%M:%S') ---- START schedule_5m.py ----" >> "$RUNNER_LOG"

# 4) Kill any old instance (best-effort)
pkill -f '/schedule_5m\.py$' 2>/dev/null || true
sleep 3

# 5) Launch
#    * stdout/stderr -> RUNNER_LOG so they don't fight with TimedRotatingFileHandler
#    * nohup ensures it survives an SSH logout / cron exec
nohup python3 jobs/schedule_5m.py >> "$RUNNER_LOG" 2>&1 &
