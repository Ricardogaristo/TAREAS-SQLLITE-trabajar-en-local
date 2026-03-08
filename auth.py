"""
auth.py — Módulo de autenticación del Gestor de Tareas
=======================================================
Perfiles:
  0 → Usuario   : accede por Google OAuth o registro normal. Ve sus tareas.
  1 → Admin     : acceso con usuario+contraseña (hash). Gestiona tareas y formación.
  2 → SuperAdmin : acceso con usuario+contraseña (hash). Acceso total + gráficos globales.

Dependencias extra:
  pip install werkzeug authlib requests python-dotenv
"""

import os
from functools import wraps

from flask import (Blueprint, render_template, request, redirect,
                   session, url_for, flash, jsonify)
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

from db_mysql import get_tareas_conn

load_dotenv()

auth_bp = Blueprint("auth", __name__, template_folder="templates")

# ── Niveles de perfil ─────────────────────────────────────────────────────────
PERFIL_USUARIO    = 0
PERFIL_ADMIN      = 1
PERFIL_SUPERADMIN = 2

# ── OAuth Google (opcional — requiere GOOGLE_CLIENT_ID y GOOGLE_CLIENT_SECRET en .env)
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_ENABLED       = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)

if GOOGLE_ENABLED:
    from authlib.integrations.flask_client import OAuth
    _oauth_registry = None  # se inicializa en init_oauth()

    def init_oauth(app):
        global _oauth_registry
        _oauth_registry = OAuth(app)
        _oauth_registry.register(
            name="google",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )
        return _oauth_registry
else:
    def init_oauth(app):
        return None


# ── Helpers de BD ──────────────────────────────────────────────────────────────
def _get_conn():
    return get_tareas_conn()


def _get_user_by_id(uid):
    conn = _get_conn()
    row  = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _get_user_by_identity(ident):
    """Busca por username o email."""
    conn = _get_conn()
    row  = conn.execute(
        "SELECT * FROM usuarios WHERE username=? OR email=?", (ident, ident)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _get_user_by_google_id(google_id):
    conn = _get_conn()
    row  = conn.execute(
        "SELECT * FROM usuarios WHERE google_id=?", (google_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _create_user(username, email, password_plain=None, es_admin=PERFIL_USUARIO, google_id=None):
    """Crea usuario. Si password_plain es None guarda hash vacío (solo OAuth)."""
    pw_hash = generate_password_hash(password_plain) if password_plain else ""
    conn    = _get_conn()
    cursor  = conn.cursor()
    cursor.execute(
        "INSERT INTO usuarios (username, email, password, es_admin, google_id) VALUES (?,?,?,?,?)",
        (username, email, pw_hash, es_admin, google_id)
    )
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return new_id


def _set_session(user: dict):
    """Carga los datos del usuario en la sesión Flask."""
    session["user_id"]  = user["id"]
    session["user"]     = user["username"]
    session["es_admin"] = user["es_admin"]
    session["email"]    = user.get("email", "")
    session["avatar"]   = user.get("avatar", "")


# ── Decoradores de acceso ─────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("auth.login"))
        if session.get("es_admin") not in (PERFIL_ADMIN, PERFIL_SUPERADMIN):
            return redirect("/")
        return f(*args, **kwargs)
    return decorated


def superadmin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("auth.login"))
        if session.get("es_admin") != PERFIL_SUPERADMIN:
            return redirect("/")
        return f(*args, **kwargs)
    return decorated


# ── Ruta: login ───────────────────────────────────────────────────────────────
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect("/")

    error = None
    if request.method == "POST":
        ident    = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        user = _get_user_by_identity(ident)

        if user and user["password"] and check_password_hash(user["password"], password):
            _set_session(user)
            return redirect("/")
        else:
            error = "Usuario o contraseña incorrectos."

    return render_template("login.html", error=error, google_enabled=GOOGLE_ENABLED)


# ── Ruta: registro ────────────────────────────────────────────────────────────
@auth_bp.route("/registro", methods=["GET", "POST"])
def registro():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email    = request.form.get("email",    "").strip()
        password = request.form.get("password", "").strip()

        if not username or not email or not password:
            error = "Todos los campos son obligatorios."
        elif len(password) < 6:
            error = "La contraseña debe tener al menos 6 caracteres."
        else:
            try:
                _create_user(username, email, password, es_admin=PERFIL_USUARIO)
                return redirect(url_for("auth.login") + "?registered=1")
            except Exception:
                error = "El usuario o email ya existe."

    return render_template("registro.html", error=error)


# ── Ruta: Google OAuth — inicio ───────────────────────────────────────────────
@auth_bp.route("/login/google")
def login_google():
    if not GOOGLE_ENABLED:
        return redirect(url_for("auth.login"))
    google = _oauth_registry.google
    redirect_uri = url_for("auth.login_google_callback", _external=True)
    return google.authorize_redirect(redirect_uri)


# ── Ruta: Google OAuth — callback ─────────────────────────────────────────────
@auth_bp.route("/login/google/callback")
def login_google_callback():
    if not GOOGLE_ENABLED:
        return redirect(url_for("auth.login"))

    try:
        google   = _oauth_registry.google
        token    = google.authorize_access_token()
        userinfo = token.get("userinfo") or google.userinfo()
    except Exception:
        return redirect(url_for("auth.login") + "?error=google")

    google_id = userinfo["sub"]
    email     = userinfo.get("email", "")
    name      = userinfo.get("name", email.split("@")[0])
    avatar    = userinfo.get("picture", "")

    # Buscar usuario existente por google_id o por email
    user = _get_user_by_google_id(google_id)
    if not user:
        user_by_email = _get_user_by_identity(email)
        if user_by_email:
            # Vincular google_id al usuario existente
            conn = _get_conn()
            conn.execute(
                "UPDATE usuarios SET google_id=?, avatar=? WHERE id=?",
                (google_id, avatar, user_by_email["id"])
            )
            conn.commit()
            conn.close()
            user = _get_user_by_id(user_by_email["id"])
        else:
            # Crear usuario nuevo con perfil Usuario
            new_id = _create_user(name, email, password_plain=None,
                                  es_admin=PERFIL_USUARIO, google_id=google_id)
            # Guardar avatar
            conn = _get_conn()
            conn.execute("UPDATE usuarios SET avatar=? WHERE id=?", (avatar, new_id))
            conn.commit()
            conn.close()
            user = _get_user_by_id(new_id)

    _set_session(user)
    return redirect("/")


# ── Ruta: logout ──────────────────────────────────────────────────────────────
@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


# ── Ruta: cambiar contraseña (Admin / SuperAdmin) ─────────────────────────────
@auth_bp.route("/perfil/cambiar_password", methods=["POST"])
@login_required
def cambiar_password():
    """Solo Admin y SuperAdmin pueden cambiar contraseña."""
    if session.get("es_admin") not in (PERFIL_ADMIN, PERFIL_SUPERADMIN):
        return jsonify({"ok": False, "error": "Sin permisos"}), 403

    data         = request.get_json(force=True) or {}
    actual       = data.get("actual", "")
    nueva        = data.get("nueva", "")
    confirmacion = data.get("confirmacion", "")

    if not actual or not nueva or not confirmacion:
        return jsonify({"ok": False, "error": "Todos los campos son obligatorios."})
    if nueva != confirmacion:
        return jsonify({"ok": False, "error": "La nueva contraseña no coincide."})
    if len(nueva) < 6:
        return jsonify({"ok": False, "error": "Mínimo 6 caracteres."})

    user = _get_user_by_id(session["user_id"])
    if not user or not check_password_hash(user["password"], actual):
        return jsonify({"ok": False, "error": "Contraseña actual incorrecta."})

    conn = _get_conn()
    conn.execute(
        "UPDATE usuarios SET password=? WHERE id=?",
        (generate_password_hash(nueva), session["user_id"])
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── Ruta: admin cambia contraseña de otro usuario ─────────────────────────────
@auth_bp.route("/usuarios/cambiar_password/<int:uid>", methods=["POST"])
@admin_required
def admin_cambiar_password(uid):
    data  = request.get_json(force=True) or {}
    nueva = data.get("nueva", "")

    if len(nueva) < 6:
        return jsonify({"ok": False, "error": "Mínimo 6 caracteres."})

    conn = _get_conn()
    conn.execute(
        "UPDATE usuarios SET password=? WHERE id=?",
        (generate_password_hash(nueva), uid)
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
