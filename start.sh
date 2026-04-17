#!/bin/sh
set -eu

if [ -n "${RAILWAY_VOLUME_MOUNT_PATH:-}" ]; then
  mkdir -p "$RAILWAY_VOLUME_MOUNT_PATH"
  export STATE_FILE="${STATE_FILE:-$RAILWAY_VOLUME_MOUNT_PATH/monitor_state.json}"
  export ALERT_LOG_FILE="${ALERT_LOG_FILE:-$RAILWAY_VOLUME_MOUNT_PATH/alerts.log}"
fi

exec python smart_money_monitor.py "$@"
