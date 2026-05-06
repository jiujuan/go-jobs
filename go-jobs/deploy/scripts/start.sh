#!/bin/bash
# start.sh — start admin and executor in background

set -e

BIN_DIR="$(cd "$(dirname "$0")/../../bin" && pwd)"
LOG_DIR="$(cd "$(dirname "$0")/../.." && pwd)/logs"
mkdir -p "$LOG_DIR"

echo "[start] Starting go-jobs admin..."
nohup "$BIN_DIR/admin" -config config/admin.prod.yaml \
  > "$LOG_DIR/admin.out" 2>&1 &
echo $! > "$LOG_DIR/admin.pid"
echo "[start] admin PID: $(cat "$LOG_DIR/admin.pid")"

echo "[start] Starting go-jobs executor..."
nohup "$BIN_DIR/executor" -config config/executor.prod.yaml \
  > "$LOG_DIR/executor.out" 2>&1 &
echo $! > "$LOG_DIR/executor.pid"
echo "[start] executor PID: $(cat "$LOG_DIR/executor.pid")"

echo "[start] Done."
