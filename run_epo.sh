#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
PROJECT_DIR="/data/infoscience-imports"
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON_BIN="$VENV_DIR/bin/python"      # le python du venv
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"
cd "$PROJECT_DIR"

# === Activer l'environnement virtuel et les variables d'env ===
if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo "[WARN] No virtualenv found at $VENV_DIR, using system Python."
fi

if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
else
    echo "[WARN] No .env file found in $PROJECT_DIR."
fi

# === Export du PYTHONPATH pour que les imports marchent ===
export PYTHONPATH="$PROJECT_DIR"

# === Logging ===
START_TS=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$START_TS] Starting Infoscience pipeline for EPO Source (365-day sliding window)..."

# Exécution principale : fenêtre glissante de 365 jours, log + tee
"$PYTHON_BIN" data_pipeline/main.py --source epo --window-days 365 -v 2>&1 | tee -a "$LOG_DIR/cron.out"

STATUS=${PIPESTATUS[0]}
END_TS=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$END_TS] Finished with status $STATUS"

exit $STATUS
