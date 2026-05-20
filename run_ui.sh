#!/bin/bash
# Lancer l'interface Infoscience Import + le scheduler de runs programmés.
# Usage : ./run_ui.sh [port]   (défaut : 8501)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-8501}"
PYTHON="${PYTHON:-python3}"

if ! "$PYTHON" -c "import streamlit" 2>/dev/null; then
    echo "❌ Streamlit non trouvé. Installez les dépendances :"
    echo "   pip install -r requirements.txt"
    exit 1
fi

if ! "$PYTHON" -c "import apscheduler" 2>/dev/null; then
    echo "❌ APScheduler non trouvé. Installez les dépendances :"
    echo "   pip install -r requirements.txt"
    exit 1
fi

# ── Démarrer le scheduler en arrière-plan ────────────────────────────────────
"$PYTHON" scheduler.py &
SCHEDULER_PID=$!
echo "⏰ Scheduler démarré (PID $SCHEDULER_PID)"

# Arrêter le scheduler proprement à la sortie du script (Ctrl-C ou fin Streamlit)
cleanup() {
    echo "⏹  Arrêt du scheduler (PID $SCHEDULER_PID)…"
    kill "$SCHEDULER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ── Démarrer Streamlit ────────────────────────────────────────────────────────
echo "🚀 Démarrage de l'interface sur http://localhost:$PORT"
"$PYTHON" -m streamlit run app.py \
    --server.port "$PORT" \
    --server.headless true \
    --server.fileWatcherType none
