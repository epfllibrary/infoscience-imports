#!/bin/bash
# Stop Streamlit UI and scheduler daemon.

PORT=8501

stop_process() {
    local label="$1"
    local pids="$2"
    if [[ -n "$pids" ]]; then
        echo "⏹  Arrêt $label (PID $pids)…"
        kill $pids 2>/dev/null || true
    else
        echo "ℹ  $label non trouvé."
    fi
}

UI_PIDS=$(lsof -i :$PORT -sTCP:LISTEN -t 2>/dev/null || true)
SCHEDULER_PIDS=$(pgrep -f "python.*scheduler\.py" 2>/dev/null || true)

stop_process "UI (port $PORT)" "$UI_PIDS"
stop_process "Scheduler" "$SCHEDULER_PIDS"
