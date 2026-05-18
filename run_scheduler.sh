#!/bin/bash
# Start the scheduler daemon independently of the UI.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
    if [[ -f "$SCRIPT_DIR/.venv/bin/python3" ]]; then
        PYTHON="$SCRIPT_DIR/.venv/bin/python3"
    else
        PYTHON="python3"
    fi
fi

if pgrep -f "python.*scheduler\.py" &>/dev/null; then
    echo "ℹ  Scheduler déjà en cours d'exécution."
    exit 0
fi

"$PYTHON" scheduler.py >> logs/scheduler.log 2>&1 &
echo "⏰ Scheduler démarré (PID $!)."
