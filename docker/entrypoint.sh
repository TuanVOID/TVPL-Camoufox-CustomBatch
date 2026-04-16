#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p \
  /app/config \
  /app/output \
  /app/logs \
  /app/state \
  /app/state/custom_batch_resume \
  /app/state/camoufox_profiles \
  /app/logs/camoufox_custom_batch

if [[ ! -f /app/config/custom_batch.json ]]; then
  cp /app/config/custom_batch.example.json /app/config/custom_batch.json
  echo "[entrypoint] Created /app/config/custom_batch.json from example."
fi

if [[ "${SKIP_CAMOUFOX_FETCH:-0}" != "1" ]]; then
  echo "[entrypoint] Ensuring Camoufox browser is available..."
  python -m camoufox fetch
fi

echo "[entrypoint] Starting: $*"
exec "$@"

