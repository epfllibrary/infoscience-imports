"""Lightweight authentication and role-based ACL for the Streamlit UI.

Credentials live in .streamlit/auth.yaml (gitignored, never committed).
Passwords are stored as bcrypt hashes — never in plain text.

Session persistence
-------------------
On login a random 256-bit token is created, stored server-side in
``data/sessions.json`` (expiry: SESSION_HOURS), and written to the URL via
``st.query_params["_s"] = token``.

Streamlit preserves query params in the URL across reruns.  On browser
refresh the URL (including the token param) is kept by the browser, so the
new WebSocket session can read ``st.query_params.get("_s")`` and restore the
session state immediately — no JS, no cookies, no initialisation delay.

Roles
-----
admin     — full access: all pages including run launcher and configuration
reporting — read-only: dashboard, publications, statistics

CLI — manage users without touching the YAML manually
-----
  python -m ui.auth add    <username> <role>        # prompts for password
  python -m ui.auth remove <username>
  python -m ui.auth list
  python -m ui.auth passwd <username>               # change password
"""

from __future__ import annotations

import getpass
import json
import secrets
import sys
from datetime import datetime, timedelta
from pathlib import Path

import bcrypt
import streamlit as st
import yaml

ROOT          = Path(__file__).resolve().parent.parent
AUTH_FILE     = ROOT / ".streamlit" / "auth.yaml"
SESSIONS_FILE = ROOT / "data" / "sessions.json"

_PARAM_NAME   = "_s"          # URL query param that carries the session token
SESSION_HOURS = 8

ROLE_PAGES: dict[str, list[str]] = {
    "admin": [
        "🏠 Tableau de bord",
        "🚀 Lancer un run",
        "⏰ Programmation",
        "📋 Publications",
        "📊 Statistiques",
        "⚙️ Configuration",
    ],
    "reporting": [
        "🏠 Tableau de bord",
        "📋 Publications",
        "📊 Statistiques",
    ],
}


# ── Credential helpers ────────────────────────────────────────────────────────

def _load_config() -> dict:
    if not AUTH_FILE.exists():
        return {"users": {}}
    with open(AUTH_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f) or {"users": {}}


def _save_config(config: dict) -> None:
    AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(AUTH_FILE, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()


def _verify(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ── Server-side session store ─────────────────────────────────────────────────

def _load_sessions() -> dict:
    if not SESSIONS_FILE.exists():
        return {}
    try:
        return json.loads(SESSIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_sessions(sessions: dict) -> None:
    SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SESSIONS_FILE.write_text(json.dumps(sessions, indent=2), encoding="utf-8")


def _create_session(username: str, role: str, name: str) -> str:
    token = secrets.token_hex(32)
    now = datetime.utcnow()
    sessions = {
        k: v for k, v in _load_sessions().items()
        if datetime.fromisoformat(v["expires_at"]) > now
    }
    sessions[token] = {
        "username":   username,
        "role":       role,
        "name":       name,
        "expires_at": (now + timedelta(hours=SESSION_HOURS)).isoformat(),
    }
    _save_sessions(sessions)
    return token


def _verify_session(token: str) -> dict | None:
    if not token:
        return None
    sessions = _load_sessions()
    session = sessions.get(token)
    if not session:
        return None
    if datetime.fromisoformat(session["expires_at"]) <= datetime.utcnow():
        sessions.pop(token, None)
        _save_sessions(sessions)
        return None
    return session


def _revoke_session(token: str) -> None:
    sessions = _load_sessions()
    sessions.pop(token, None)
    _save_sessions(sessions)


# ── Query-param session helpers ───────────────────────────────────────────────

def _write_token(token: str) -> None:
    """Persist the session token in the URL query params."""
    st.query_params[_PARAM_NAME] = token


def _read_token() -> str | None:
    """Read the session token from the URL query params."""
    return st.query_params.get(_PARAM_NAME)


def _clear_token() -> None:
    """Remove the session token from the URL query params."""
    st.query_params.pop(_PARAM_NAME, None)


# ── Session state helpers ─────────────────────────────────────────────────────

def _apply_session(session: dict, token: str) -> None:
    st.session_state["_auth_ok"]    = True
    st.session_state["_auth_user"]  = session["username"]
    st.session_state["_auth_role"]  = session["role"]
    st.session_state["_auth_name"]  = session["name"]
    st.session_state["_auth_token"] = token


# ── Streamlit runtime API ─────────────────────────────────────────────────────

def login_wall() -> tuple[str, str]:
    """Block rendering until the user is authenticated.

    On browser refresh Streamlit preserves the URL (including ``?_s=TOKEN``),
    so the new WebSocket session reads the token from ``st.query_params`` and
    restores the session transparently — no JS, no cookie initialisation delay.
    Returns ``(username, role)`` once authenticated; calls ``st.stop()``
    otherwise.
    """
    # ── 1. Already authenticated in this WebSocket session ────────────────
    if st.session_state.get("_auth_ok"):
        return st.session_state["_auth_user"], st.session_state["_auth_role"]

    if not AUTH_FILE.exists():
        st.error(
            f"Fichier de credentials manquant : `{AUTH_FILE.relative_to(ROOT)}`  \n"
            "Créez-le avec `python -m ui.auth add <username> <role>`."
        )
        st.stop()

    # ── 2. Restore session from URL query param (survives browser refresh) ─
    token = _read_token()
    if token:
        session = _verify_session(token)
        if session:
            _apply_session(session, token)
            # Remove the token from the visible URL bar without a page reload
            # so it is not recorded in browser history or sent in Referer
            # headers during navigation.  The token stays valid in
            # sessions.json for the next browser refresh.
            st.html(
                f"<script>"
                f"(function(){{var u=new URL(window.location);"
                f"u.searchParams.delete('{_PARAM_NAME}');"
                f"window.history.replaceState({{}}, '', u);}})()"
                f"</script>",
                unsafe_allow_javascript=True,
            )
            st.rerun()
        else:
            # Token expired or invalid — remove it and show login
            _clear_token()

    # ── 3. Show login form ────────────────────────────────────────────────
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(
            "<div style='text-align:center;font-size:1.6rem;font-weight:700;"
            "margin-bottom:4px'>📚 Infoscience Imports</div>"
            "<div style='text-align:center;color:#707070;margin-bottom:24px'>"
            "Connexion requise</div>",
            unsafe_allow_html=True,
        )
        with st.form("_login"):
            username = st.text_input("Identifiant")
            password = st.text_input("Mot de passe", type="password")
            ok = st.form_submit_button("Se connecter", use_container_width=True)

        if ok:
            config = _load_config()
            user = config.get("users", {}).get(username)
            if user and _verify(password, user["password"]):
                role  = user.get("role", "reporting")
                name  = user.get("name", username)
                token = _create_session(username, role, name)
                _write_token(token)
                _apply_session(
                    {"username": username, "role": role, "name": name},
                    token,
                )
                st.rerun()
            else:
                st.error("Identifiant ou mot de passe incorrect.")

    st.stop()


def logout() -> None:
    """Revoke the server-side session, clear the URL token, return to login."""
    token = st.session_state.pop("_auth_token", None)
    if token:
        _revoke_session(token)
    _clear_token()
    for k in ("_auth_ok", "_auth_user", "_auth_role", "_auth_name"):
        st.session_state.pop(k, None)
    st.rerun()


def get_allowed_pages(role: str) -> list[str]:
    return ROLE_PAGES.get(role, ROLE_PAGES["reporting"])


def current_user() -> tuple[str, str, str]:
    """Return ``(username, display_name, role)`` for the active session."""
    return (
        st.session_state.get("_auth_user", ""),
        st.session_state.get("_auth_name", ""),
        st.session_state.get("_auth_role", "reporting"),
    )


# ── CLI user management ───────────────────────────────────────────────────────

def _cli_add(username: str, role: str) -> None:
    if role not in ROLE_PAGES:
        print(f"Rôle invalide '{role}'. Choix : {', '.join(ROLE_PAGES)}")
        sys.exit(1)
    password = getpass.getpass(f"Mot de passe pour {username!r} : ")
    confirm  = getpass.getpass("Confirmer : ")
    if password != confirm:
        print("Les mots de passe ne correspondent pas.")
        sys.exit(1)
    config = _load_config()
    config.setdefault("users", {})[username] = {
        "name":     username,
        "password": _hash(password),
        "role":     role,
    }
    _save_config(config)
    print(f"Utilisateur '{username}' ({role}) créé dans {AUTH_FILE}.")


def _cli_remove(username: str) -> None:
    config = _load_config()
    if username not in config.get("users", {}):
        print(f"Utilisateur '{username}' introuvable.")
        sys.exit(1)
    del config["users"][username]
    _save_config(config)
    print(f"Utilisateur '{username}' supprimé.")


def _cli_list() -> None:
    config = _load_config()
    users = config.get("users", {})
    if not users:
        print("Aucun utilisateur configuré.")
        return
    print(f"{'Identifiant':<20} {'Nom':<25} {'Rôle'}")
    print("-" * 55)
    for uname, info in users.items():
        print(f"{uname:<20} {info.get('name', ''):<25} {info.get('role', '')}")


def _cli_passwd(username: str) -> None:
    config = _load_config()
    if username not in config.get("users", {}):
        print(f"Utilisateur '{username}' introuvable.")
        sys.exit(1)
    password = getpass.getpass(f"Nouveau mot de passe pour {username!r} : ")
    confirm  = getpass.getpass("Confirmer : ")
    if password != confirm:
        print("Les mots de passe ne correspondent pas.")
        sys.exit(1)
    config["users"][username]["password"] = _hash(password)
    _save_config(config)
    print(f"Mot de passe de '{username}' mis à jour.")


if __name__ == "__main__":
    import argparse as _ap
    p = _ap.ArgumentParser(
        description="Gestion des utilisateurs Infoscience Imports UI",
        epilog="Exemple : python -m ui.auth add admin admin",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    s_add = sub.add_parser("add",    help="Créer un utilisateur")
    s_add.add_argument("username");  s_add.add_argument("role", choices=list(ROLE_PAGES))

    s_rm  = sub.add_parser("remove", help="Supprimer un utilisateur")
    s_rm.add_argument("username")

    sub.add_parser("list", help="Lister les utilisateurs")

    s_pw  = sub.add_parser("passwd", help="Changer le mot de passe")
    s_pw.add_argument("username")

    args = p.parse_args()
    if   args.cmd == "add":    _cli_add(args.username, args.role)
    elif args.cmd == "remove": _cli_remove(args.username)
    elif args.cmd == "list":   _cli_list()
    elif args.cmd == "passwd": _cli_passwd(args.username)
