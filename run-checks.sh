#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# != 1 || ! $1 =~ ^[0-9]{1,9}$ ]]; then
  echo "Usage: $0 SEED (1-9 digits)"
  exit 2
fi
cd "$(dirname "$0")"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
log_dir=${QUEUE_LOG_DIR:-runs}
mkdir -p "$log_dir"
log=$(mktemp "$log_dir/run-${stamp}-seed-${1}-XXXXXX.log")
seed=$1
export COMPOSE_PROJECT_NAME="soupcheck${stamp,,}${log##*-}"
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME%.log}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME,,}
export QUEUE_PORT=${QUEUE_PORT:-18080}

finish() {
  status=$?
  trap - EXIT
  if command -v docker >/dev/null; then
    docker compose ps -a || true
    docker compose logs --no-color || true
    docker compose down -v || echo "CLEANUP ERROR"
  fi
  echo "END exit=$status"
  exit "$status"
}

checks() (
  trap finish EXIT
  echo "BEGIN seed=$seed project=$COMPOSE_PROJECT_NAME port=$QUEUE_PORT"
  docker compose version
  docker version
  docker compose up -d --build --scale worker=0 api worker
  docker compose run --rm --no-deps api probe
  docker compose exec -T db psql -X -v ON_ERROR_STOP=1 -U queue -d queue \
    -c 'TRUNCATE job_effects, jobs RESTART IDENTITY'
  docker compose up -d --scale worker=8
  for ((i=0; i<30; i++)); do
    if curl -fsS --max-time 2 "http://127.0.0.1:$QUEUE_PORT/healthz" >/dev/null; then
      break
    fi
    sleep 1
  done
  python3 -u chaos.py --seed "$seed" --url "http://127.0.0.1:$QUEUE_PORT"
)

checks 2>&1 | tee "$log"
