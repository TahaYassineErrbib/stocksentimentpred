#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="logs/streaming"

if [[ ! -d "$LOG_DIR" ]]; then
  echo "No $LOG_DIR directory found."
  exit 0
fi

shopt -s nullglob
PIDS=("$LOG_DIR"/*.pid)
if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No PID files found in $LOG_DIR."
  exit 0
fi

for pidfile in "${PIDS[@]}"; do
  pid="$(cat "$pidfile" || true)"
  if [[ -n "$pid" ]]; then
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping PID $pid from $pidfile"
      kill "$pid" || true
    else
      echo "PID $pid from $pidfile not running"
    fi
  fi
done

echo "Done."
