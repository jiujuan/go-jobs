#!/bin/bash
LOG_DIR="$(cd "$(dirname "$0")/../.." && pwd)/logs"

for svc in admin executor; do
  PID_FILE="$LOG_DIR/$svc.pid"
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      kill "$PID"
      echo "[stop] $svc (PID $PID) stopped"
    fi
    rm -f "$PID_FILE"
  fi
done
