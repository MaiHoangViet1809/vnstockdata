#!/bin/bash

BASE_PATH=/apps/vnstockdata/
source $BASE_PATH/.venv/bin/activate
nohup python3 $BASE_PATH/jobs/schedule_5m.py >> $BASE_PATH/logs/scheduler.log 2>&1 &
