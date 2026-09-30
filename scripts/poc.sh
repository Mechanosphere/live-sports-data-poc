#!/usr/bin/env bash
# Start/stop the whole LiveSportsData POC: DynamoDB Local + Kafka + the
# aurora-postgres canonical-data store (via docker compose), the FastAPI
# app, the provider poller, and the Kafka consumer.
#
# Usage:
#   scripts/poc.sh start   # bring everything up
#   scripts/poc.sh stop    # tear everything down
#   scripts/poc.sh status  # show what's running
#
# API_PORT can be overridden, e.g. `API_PORT=8080 scripts/poc.sh start`.
# Defaults to 8080 since DynamoDB Local already occupies host port 8000.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$RUN_DIR/logs"
API_PORT="${API_PORT:-8080}"

mkdir -p "$LOG_DIR"

PROCS="api poller consumer-football consumer-basketball"

pid_file() { echo "$RUN_DIR/$1.pid"; }

is_running() {
    local pid_file
    pid_file="$(pid_file "$1")"
    [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

start_proc() {
    local name="$1"
    shift
    if is_running "$name"; then
        echo "  $name already running (pid $(cat "$(pid_file "$name")"))"
        return
    fi
    # Run in a subshell so the background job is fully detached from this
    # script's job table -- when the subshell exits, the child process is
    # reparented and keeps running independently (works even where `setsid`
    # isn't available, e.g. macOS).
    (
        nohup "$@" </dev/null >"$LOG_DIR/$name.log" 2>&1 &
        child_pid=$!
        echo "$child_pid" >"$(pid_file "$name")"
    )
    sleep 0.2
    echo "  $name started (pid $(cat "$(pid_file "$name")")), logs: $LOG_DIR/$name.log"
}

stop_proc() {
    local name="$1"
    local pf
    pf="$(pid_file "$name")"
    if is_running "$name"; then
        local pid
        pid="$(cat "$pf")"
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -9 "$pid" 2>/dev/null || true
        echo "  $name stopped"
    else
        echo "  $name not running"
    fi
    rm -f "$pf"
}

cmd_start() {
    echo "Starting Docker services (DynamoDB Local, Kafka, aurora-postgres)..."
    docker compose up -d

    echo "Waiting for DynamoDB Local to accept connections..."
    for _ in $(seq 1 30); do
        curl -s -o /dev/null "http://localhost:8000" && break
        sleep 1
    done

    echo "Ensuring DynamoDB table exists..."
    python3 -m scripts.create_table

    echo "Starting app processes..."
    start_proc api python3 -m uvicorn app.main:app --host 0.0.0.0 --port "$API_PORT"
    start_proc poller python3 -m app.ingestion.run_poller
    start_proc consumer-football python3 -m app.ingestion.run_consumer --sport football
    start_proc consumer-basketball python3 -m app.ingestion.run_consumer --sport basketball

    echo
    echo "POC is up:"
    echo "  API docs:    http://localhost:$API_PORT/docs"
    echo "  Health:      http://localhost:$API_PORT/api/health"
    echo "  Logs:        $LOG_DIR/"
}

cmd_stop() {
    echo "Stopping app processes..."
    for p in $PROCS; do
        stop_proc "$p"
    done

    echo "Stopping Docker services (DynamoDB Local, Kafka, aurora-postgres)..."
    docker compose down

    echo "POC stopped."
}

cmd_status() {
    for p in $PROCS; do
        if is_running "$p"; then
            echo "  $p: running (pid $(cat "$(pid_file "$p")"))"
        else
            echo "  $p: stopped"
        fi
    done
    echo
    docker compose ps
}

case "${1:-}" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac
