"""
migrar_sqlite_mysql.py
======================
Migra todos los datos de formacion.db y tareas.db (SQLite) a MySQL.

Uso:
    python migrar_sqlite_mysql.py

El script es IDEMPOTENTE: puede ejecutarse varias veces sin duplicar datos.
"""

import sqlite3
import os
import sys
from dotenv import load_dotenv

load_dotenv()

SQLITE_FORMACION = os.getenv('SQLITE_PATH',       'formacion.db')
SQLITE_TAREAS    = os.getenv('SQLITE_TAREAS_PATH', 'tareas.db')

MYSQL_CONFIG = {
    'host':     os.getenv('MYSQL_HOST',     'localhost'),
    'port':     int(os.getenv('MYSQL_PORT', '3306')),
    'user':     os.getenv('MYSQL_USER',     'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DB',       'formacion'),
    'charset':  'utf8mb4',
}

# Orden importa por las claves foraneas
TABLAS_TAREAS = ['usuarios', 'tareas', 'subtareas']
TABLAS_FORMACION = [
    'alumnos', 'historial_snapshots', 'historial_automatico',
    'alarmas_completadas', 'progreso_historial',
]


def sqlite_rows(sq, tabla):
    c = sq.cursor()
    c.execute(f"SELECT * FROM {tabla}")
    cols = [d[0] for d in c.description]
    return cols, c.fetchall()


def migrar_tablas(sq, mc, my, tablas, db_name, total):
    for tabla in tablas:
        existe = sq.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tabla,)
        ).fetchone()
        if not existe:
            print(f"   ⚠  '{tabla}' no existe en SQLite, omitiendo.")
            continue

        cols, rows = sqlite_rows(sq, tabla)
        if not rows:
            print(f"   ℹ  '{tabla}': vacía, omitiendo.")
            continue

        mc.execute(f"DELETE FROM {tabla}")
        try:
            mc.execute(f"ALTER TABLE {tabla} AUTO_INCREMENT = 1")
        except Exception:
            pass

        mc.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (db_name, tabla)
        )
        cols_mysql = {r[0] for r in mc.fetchall()}
        cols_ok    = [c for c in cols if c in cols_mysql]

        if not cols_ok:
            print(f"   ⚠  '{tabla}': sin columnas coincidentes, omitiendo.")
            continue

        idx          = [cols.index(c) for c in cols_ok]
        placeholders = ", ".join(["%s"] * len(cols_ok))
        col_names    = ", ".join(f"`{c}`" for c in cols_ok)
        sql          = f"INSERT INTO {tabla} ({col_names}) VALUES ({placeholders})"
        converted    = [tuple(row[i] for i in idx) for row in rows]

        mc.executemany(sql, converted)
        my.commit()
        print(f"   ✅ '{tabla}': {len(rows)} filas ({len(cols_ok)}/{len(cols)} columnas).")
        total += len(rows)
    return total


def migrar():
    import mysql.connector

    try:
        my = mysql.connector.connect(**MYSQL_CONFIG)
        mc = my.cursor()
        print(f"✅ Conectado a MySQL: {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}\n")
    except Exception as e:
        print(f"❌ Error conectando a MySQL: {e}")
        sys.exit(1)

    print("🔧 Inicializando tablas MySQL...")
    from formacion import inicializar_formacion
    from app_web import inicializar_todo
    inicializar_formacion()
    inicializar_todo()
    print()

    mc.execute("SET FOREIGN_KEY_CHECKS=0")
    db_name = MYSQL_CONFIG['database']
    total   = 0

    # ── tareas.db: usuarios, tareas, subtareas ─────────────────────────────────
    if not os.path.exists(SQLITE_TAREAS):
        print(f"⚠  No se encontró {SQLITE_TAREAS}, omitiendo usuarios y tareas.")
    else:
        sq = sqlite3.connect(SQLITE_TAREAS)
        sq.row_factory = sqlite3.Row
        print(f"📂 {SQLITE_TAREAS} — usuarios, tareas, subtareas:")
        total = migrar_tablas(sq, mc, my, TABLAS_TAREAS, db_name, total)
        sq.close()
        print()

    # ── formacion.db: alumnos e historial ─────────────────────────────────────
    if not os.path.exists(SQLITE_FORMACION):
        print(f"⚠  No se encontró {SQLITE_FORMACION}, omitiendo formacion.")
    else:
        sq = sqlite3.connect(SQLITE_FORMACION)
        sq.row_factory = sqlite3.Row
        print(f"📂 {SQLITE_FORMACION} — alumnos e historial:")
        total = migrar_tablas(sq, mc, my, TABLAS_FORMACION, db_name, total)
        sq.close()

    mc.execute("SET FOREIGN_KEY_CHECKS=1")
    my.commit()
    mc.close()
    my.close()

    print(f"\n🎉 Migración completada: {total} filas en total.")
    print("   Podés guardar los .db como backup o borrarlos.")


if __name__ == '__main__':
    migrar()