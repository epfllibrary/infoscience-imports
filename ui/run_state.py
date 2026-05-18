"""Gestion de l'état persistant du run en cours.

Écrit un fichier JSON sur disque (data/run_active.json) au démarrage du run
et le supprime à la fin. Ce fichier survit à la navigation entre les pages
Streamlit et permet à n'importe quelle page de détecter et afficher un run actif.
"""

from __future__ import annotations

import json
import os
import signal
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = ROOT / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Kept for backward compatibility — prefer get_state_file() for new code.
STATE_FILE = _DATA_DIR / "run_active.json"


def get_state_file() -> Path:
    """Return the run-lock path for the currently active environment.

    Priority (mirrors env_loader.get_active_env without importing it):
    1. APP_ENV environment variable (set by env_loader.load_env)
    2. Persisted ``data/active_env`` file
    3. Default: "dev"
    """
    _ENVS = ("dev", "test", "prod")
    env = os.environ.get("APP_ENV", "").strip().lower()
    if env not in _ENVS:
        active_env_file = _DATA_DIR / "active_env"
        if active_env_file.exists():
            val = active_env_file.read_text(encoding="utf-8").strip().lower()
            if val in _ENVS:
                env = val
    if env not in _ENVS:
        env = "dev"
    return _DATA_DIR / f"run_active_{env}.json"


def try_acquire_run_lock(
    run_id: str,
    pid: int,
    sources: list[str],
    dry_run: bool,
    log_file: str,
    cmd: list[str],
) -> bool:
    """Tente de créer le fichier d'état de façon atomique.

    Utilise ``open(file, 'x')`` (création exclusive) qui est une opération
    atomique au niveau du système de fichiers sur POSIX : si deux utilisateurs
    soumettent simultanément, un seul réussit — l'autre reçoit ``False``.

    Retourne ``True`` si le verrou a été acquis (run démarré),
    ``False`` si un autre run est déjà actif.
    """
    payload = json.dumps({
        "run_id":     run_id,
        "pid":        pid,
        "started_at": datetime.now().isoformat(),
        "sources":    sources,
        "dry_run":    dry_run,
        "log_file":   str(log_file),
        "cmd":        " ".join(cmd),
    }, indent=2)

    try:
        # 'x' = création exclusive — échoue avec FileExistsError si le fichier existe
        with open(get_state_file(), "x", encoding="utf-8") as f:
            f.write(payload)
        return True
    except FileExistsError:
        # Un autre utilisateur vient de créer le fichier au même instant
        return False


def write_active_run(
    run_id: str,
    pid: int,
    sources: list[str],
    dry_run: bool,
    log_file: str,
    cmd: list[str],
) -> None:
    """Alias de compatibilité — préférer ``try_acquire_run_lock``."""
    try_acquire_run_lock(run_id, pid, sources, dry_run, log_file, cmd)


def read_active_run() -> Optional[dict]:
    """Retourne le run actif s'il existe et que le processus tourne encore."""
    sf = get_state_file()
    if not sf.exists():
        return None
    try:
        state = json.loads(sf.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        sf.unlink(missing_ok=True)
        return None

    pid = state.get("pid")
    if pid and _is_running(pid):
        return state

    # Processus mort mais fichier toujours là → nettoyage
    sf.unlink(missing_ok=True)
    return None


def clear_active_run() -> None:
    get_state_file().unlink(missing_ok=True)


def kill_active_run() -> bool:
    """Envoie SIGTERM au processus actif. Retourne True si le signal a été envoyé."""
    state = read_active_run()
    if not state:
        return False
    pid = state.get("pid")
    if pid and _is_running(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except (ProcessLookupError, PermissionError):
            pass
    get_state_file().unlink(missing_ok=True)
    return False


def _is_running(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        # If the process is our direct child and has already exited it stays
        # as a zombie until collected.  os.kill(zombie, 0) incorrectly
        # returns True for zombies, so try to collect it first.
        waited, _ = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return False  # collected the zombie — process is done
    except ChildProcessError:
        pass  # pid is not our child; fall through to kill-based check
    except OSError:
        pass
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
