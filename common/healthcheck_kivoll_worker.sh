#!/bin/sh
set -eu

heartbeat_file="/app/data/heartbeat"

log() {
  # Healthcheck output is captured by Docker; keep it short and useful.
  # Use UTC timestamps for consistency.
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 1
}

if [ ! -e "$heartbeat_file" ]; then
  die "Heartbeat file does not exist: $heartbeat_file"
fi

if [ ! -s "$heartbeat_file" ]; then
  die "Heartbeat file exists but is empty: $heartbeat_file"
fi

next_run_raw=$(head -n 1 "$heartbeat_file" | tr -d '\r\n')
if [ -z "$next_run_raw" ]; then
  die "Heartbeat file contains an empty first line: $heartbeat_file"
fi

next_run_ts=$(date -u -d "$next_run_raw" +%s 2>/dev/null || :)
if [ -z "$next_run_ts" ]; then
  die "Could not parse heartbeat timestamp '$next_run_raw'"
fi

now_ts=$(date -u +%s)
allowed_ts=$((next_run_ts + 59))

log "heartbeat_next_run_raw=$next_run_raw heartbeat_next_run_ts=$next_run_ts now_ts=$now_ts allowed_ts=$allowed_ts"

if [ "$now_ts" -le "$allowed_ts" ]; then
  log "OK: heartbeat within allowed window"
  exit 0
fi

die "Heartbeat is stale (now_ts=$now_ts > allowed_ts=$allowed_ts)"
