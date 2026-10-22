#!/usr/bin/env bash
# Build and start the service, run a k6 script against it, then stop it.
# Usage: loadtest/run.sh loadtest/stress.js
set -euo pipefail
cd "$(dirname "$0")/.."

script=${1:?usage: run.sh <k6 script>}
addr=${ADDR:-:8080}
base=${BASE_URL:-http://localhost${addr}}

# Export metrics every 5s (the SDK default is 60s) so a 20s load step has several points.
export OTEL_METRIC_EXPORT_INTERVAL=${OTEL_METRIC_EXPORT_INTERVAL:-5000}

bindir=$(mktemp -d)
bin=$bindir/viewcounter
log=$(mktemp)
go build -o "$bin" ./cmd/viewcounter
ADDR=$addr "$bin" >"$log" 2>&1 &
pid=$!
cleanup() {
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -rf "$bindir"
  if [ -s "$log" ]; then echo "--- service log ---"; cat "$log"; fi
  rm -f "$log"
}
trap cleanup EXIT

for _ in $(seq 50); do
  curl -sf "$base/v1/top" >/dev/null && break
  sleep 0.1
done
curl -sf "$base/v1/top" >/dev/null || { echo "service did not start"; exit 1; }

BASE_URL=$base k6 run "$script"
