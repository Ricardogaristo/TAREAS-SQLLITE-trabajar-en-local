"""
cursos.py — Repositorio de cursos en ZIP
==========================================
Permite a los tutores subir, listar y descargar
los contenidos de cada curso empaquetados en .zip.

Los ficheros se guardan en:
  CURSOS_UPLOAD_FOLDER / <tutor_id> / <nombre_archivo>.zip
"""

import os
import io
import zipfile
from functools import wraps
from datetime import datetime

from flask import (Blueprint, render_template, request, redirect,
                   session, url_for, jsonify, send_file, abort)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from db_mysql import get_form_conn

load_dotenv()

cursos_bp = Blueprint("cursos", __name__, template_folder="templates")

# ── Carpeta de almacenamiento ──────────────────────────────────────────────────
BASE_DIR            = os.path.dirname(os.path.abspath(__file__))
CURSOS_UPLOAD_FOLDER = os.path.join(BASE_DIR, "cursos_repositorio")
MAX_ZIP_MB          = 200          # tamaño máximo por fichero en MB
ALLOWED_EXT         = {".zip"}


# ── Decorador login ───────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ── Helpers ───────────────────────────────────────────────────────────────────
def _tutor_dir(tutor_id: int) -> str:
    """Devuelve (y crea si hace falta) la carpeta del tutor."""
    d = os.path.join(CURSOS_UPLOAD_FOLDER, str(tutor_id))
    os.makedirs(d, exist_ok=True)
    return d


def _listar_zips(tutor_id: int) -> list[dict]:
    """Lista todos los .zip del tutor con metadatos."""
    d = _tutor_dir(tutor_id)
    archivos = []
    for fname in sorted(os.listdir(d)):
        if not fname.lower().endswith(".zip"):
            continue
        fpath = os.path.join(d, fname)
        stat  = os.stat(fpath)
        size_mb = stat.st_size / (1024 * 1024)
        mtime   = datetime.fromtimestamp(stat.st_mtime)

        # Intentar contar archivos dentro del ZIP
        num_files = 0
        try:
            with zipfile.ZipFile(fpath, "r") as z:
                num_files = len([n for n in z.namelist() if not n.endswith("/")])
        except Exception:
            pass

        archivos.append({
            "nombre":    fname,
            "nombre_sin_ext": os.path.splitext(fname)[0],
            "size_mb":   round(size_mb, 2),
            "size_str":  f"{size_mb:.1f} MB" if size_mb >= 1 else f"{stat.st_size/1024:.0f} KB",
            "fecha":     mtime.strftime("%d/%m/%Y %H:%M"),
            "num_files": num_files,
        })
    return archivos


def _cursos_alumno(tutor_id: int) -> list[str]:
    """Devuelve la lista de cursos activos del tutor (para el selector)."""
    conn = get_form_conn()
    rows = conn.execute(
        "SELECT DISTINCT curso FROM alumnos "
        "WHERE tutor_id=%s AND (archivado IS NULL OR archivado=0) "
        "AND curso IS NOT NULL ORDER BY curso",
        (tutor_id,)
    ).fetchall()
    conn.close()
    return [r["curso"] for r in rows if r["curso"]]


# ══════════════════════════════════════════════════════════════════════════════
# RUTAS
# ══════════════════════════════════════════════════════════════════════════════

@cursos_bp.route("/formacion/cursos")
@login_required
def repositorio_cursos():
    tutor_id = session["user_id"]
    zips     = _listar_zips(tutor_id)
    cursos   = _cursos_alumno(tutor_id)
    return render_template(
        "cursos_repositorio.html",
        zips=zips,
        cursos=cursos,
        max_mb=MAX_ZIP_MB,
    )


@cursos_bp.route("/formacion/cursos/subir", methods=["POST"])
@login_required
def subir_zip():
    tutor_id = session["user_id"]

    if "archivo" not in request.files:
        return jsonify({"error": "No se recibió ningún archivo"}), 400

    f = request.files["archivo"]
    if not f or not f.filename:
        return jsonify({"error": "Archivo vacío"}), 400

    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ALLOWED_EXT:
        return jsonify({"error": "Solo se aceptan archivos .zip"}), 400

    # Nombre personalizado opcional
    nombre_custom = request.form.get("nombre_curso", "").strip()
    if nombre_custom:
        safe_name = secure_filename(nombre_custom) + ".zip"
    else:
        safe_name = secure_filename(f.filename)

    # Leer y validar tamaño
    data = f.read()
    size_mb = len(data) / (1024 * 1024)
    if size_mb > MAX_ZIP_MB:
        return jsonify({"error": f"El archivo supera el límite de {MAX_ZIP_MB} MB"}), 413

    # Validar que es un ZIP real
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            num_files = len([n for n in z.namelist() if not n.endswith("/")])
    except zipfile.BadZipFile:
        return jsonify({"error": "El archivo no es un ZIP válido"}), 400

    dest = os.path.join(_tutor_dir(tutor_id), safe_name)
    with open(dest, "wb") as out:
        out.write(data)

    return jsonify({
        "ok":        True,
        "nombre":    safe_name,
        "size_str":  f"{size_mb:.1f} MB" if size_mb >= 1 else f"{len(data)/1024:.0f} KB",
        "num_files": num_files,
        "fecha":     datetime.now().strftime("%d/%m/%Y %H:%M"),
    })


@cursos_bp.route("/formacion/cursos/descargar/<path:nombre>")
@login_required
def descargar_zip(nombre):
    tutor_id = session["user_id"]
    safe     = secure_filename(nombre)
    fpath    = os.path.join(_tutor_dir(tutor_id), safe)

    if not os.path.isfile(fpath):
        abort(404)

    return send_file(fpath, as_attachment=True, download_name=safe)


@cursos_bp.route("/formacion/cursos/eliminar/<path:nombre>", methods=["POST"])
@login_required
def eliminar_zip(nombre):
    tutor_id = session["user_id"]
    safe     = secure_filename(nombre)
    fpath    = os.path.join(_tutor_dir(tutor_id), safe)

    if not os.path.isfile(fpath):
        return jsonify({"error": "Archivo no encontrado"}), 404

    os.remove(fpath)
    return jsonify({"ok": True})


@cursos_bp.route("/formacion/cursos/renombrar", methods=["POST"])
@login_required
def renombrar_zip():
    tutor_id   = session["user_id"]
    nombre_old = secure_filename(request.json.get("nombre_old", ""))
    nombre_new = secure_filename(request.json.get("nombre_new", ""))

    if not nombre_old or not nombre_new:
        return jsonify({"error": "Nombre inválido"}), 400

    if not nombre_new.lower().endswith(".zip"):
        nombre_new += ".zip"

    d       = _tutor_dir(tutor_id)
    old_path = os.path.join(d, nombre_old)
    new_path = os.path.join(d, nombre_new)

    if not os.path.isfile(old_path):
        return jsonify({"error": "Archivo no encontrado"}), 404
    if os.path.exists(new_path):
        return jsonify({"error": "Ya existe un archivo con ese nombre"}), 409

    os.rename(old_path, new_path)
    return jsonify({"ok": True, "nombre_new": nombre_new})
