#!/bin/bash
# Lancer l'interface Infoscience Import + le scheduler de runs programmés.
# Usage : ./run_ui.sh [port]   (défaut : 8501)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-8500}"
if [[ -z "${PYTHON:-}" ]]; then
    if [[ -f "$SCRIPT_DIR/.venv/bin/python3" ]]; then
        PYTHON="$SCRIPT_DIR/.venv/bin/python3"
    else
        PYTHON="python3"
    fi
fi

if ! "$PYTHON" -c "import streamlit" 2>/dev/null; then
    echo "❌ Streamlit non trouvé. Installez les dépendances :"
    echo "   pip install -r requirements.txt"
    exit 1
fi

# ── Démarrer Streamlit ────────────────────────────────────────────────────────
echo "🚀 Démarrage de l'interface sur http://localhost:$PORT"
"$PYTHON" -m streamlit run app.py \
    --server.port "$PORT" \
    --server.headless true \
    --server.fileWatcherType none
