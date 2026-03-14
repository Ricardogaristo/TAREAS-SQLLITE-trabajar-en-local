"""
migrar_sqlite_mysql.py
======================
Migra todos los datos de tareas.db y formacion.db (SQLite) a MySQL.
Crea las tablas automáticamente si no existen.
Es IDEMPOTENTE: puede ejecutarse varias veces sin duplicar datos.

Uso:
    pip install mysql-connector-python python-dotenv
    python migrar_sqlite_mysql.py

Variables de entorno (o fichero .env):
    SQLITE_TAREAS_PATH   ruta a tareas.db    (defecto: tareas.db)
    SQLITE_PATH          ruta a formacion.db (defecto: formacion.db)
    MYSQL_HOST           (defecto: localhost)
    MYSQL_PORT           (defecto: 3306)
    MYSQL_USER           (defecto: root)
    MYSQL_PASSWORD       (defecto: "")
    MYSQL_DB             (defecto: formacion)
"""

import sqlite3
import os
import sys

# ── dotenv opcional ────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── rutas a los SQLite ─────────────────────────────────────────────────────────
SQLITE_FORMACION = os.getenv('SQLITE_PATH',        'formacion.db')
SQLITE_TAREAS    = os.getenv('SQLITE_TAREAS_PATH',  'tareas.db')

# ── configuración MySQL ────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     os.getenv('MYSQL_HOST',     'localhost'),
    'port':     int(os.getenv('MYSQL_PORT', '3306')),
    'user':     os.getenv('MYSQL_USER',     'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DB',       'formacion'),
    'charset':  'utf8mb4',
}

# ── DDL: definición de todas las tablas ───────────────────────────────────────
DDL = """
-- ─── tareas.db ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS usuarios (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    username   VARCHAR(255) NOT NULL UNIQUE,
    email      VARCHAR(255),
    password   VARCHAR(255) NOT NULL,
    es_admin   TINYINT(1)   NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tareas (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    descripcion TEXT         NOT NULL,
    categoria   VARCHAR(255),
    fecha       VARCHAR(50),
    completada  TINYINT(1)   NOT NULL DEFAULT 0,
    codigo      VARCHAR(100),
    usuario_id  INT,
    prioridad   INT          NOT NULL DEFAULT 2,
    favorita    TINYINT(1)   NOT NULL DEFAULT 0,
    notas       TEXT,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS subtareas (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    tarea_id  INT          NOT NULL,
    texto     TEXT         NOT NULL,
    hecha     TINYINT(1)   NOT NULL DEFAULT 0,
    FOREIGN KEY (tarea_id) REFERENCES tareas(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cursos (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    codigo      VARCHAR(100)  NOT NULL UNIQUE,
    nombre      VARCHAR(255)  NOT NULL,
    fecha_ini   VARCHAR(50),
    fecha_fin   VARCHAR(50),
    moodle_url  TEXT,
    cerrado     TINYINT(1)    NOT NULL DEFAULT 0,
    creado_en   DATETIME      DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alumnos_curso (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    curso_id    INT          NOT NULL,
    nombre      VARCHAR(255) NOT NULL,
    progreso    DOUBLE       NOT NULL DEFAULT 0,
    nota_media  DOUBLE,
    superado    TINYINT(1)   NOT NULL DEFAULT 0,
    FOREIGN KEY (curso_id) REFERENCES cursos(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS historial_curso (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    curso_id    INT          NOT NULL,
    alumno      VARCHAR(255) NOT NULL,
    progreso    DOUBLE,
    nota_media  DOUBLE,
    superado    TINYINT(1)   NOT NULL DEFAULT 0,
    cerrado_en  DATETIME     DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (curso_id) REFERENCES cursos(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS whatsapp_log (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    curso_id    INT,
    mensaje     TEXT,
    enviado_en  DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (curso_id) REFERENCES cursos(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── formacion.db ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alumnos (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    curso               VARCHAR(255),
    nombre              VARCHAR(255)   NOT NULL,
    progreso            DOUBLE         NOT NULL DEFAULT 0,
    examenes            INT            NOT NULL DEFAULT 0,
    fecha_inicio        VARCHAR(50),
    fecha_fin           VARCHAR(50),
    supera_75           TINYINT(1)     NOT NULL DEFAULT 0,
    telefono            VARCHAR(50),
    tutor_id            INT,
    created_at          DATETIME       DEFAULT CURRENT_TIMESTAMP,
    archivado           TINYINT(1)     NOT NULL DEFAULT 0,
    archivado_at        DATETIME,
    ultima_importacion  VARCHAR(100),
    delta_progreso      DOUBLE         NOT NULL DEFAULT 0,
    avanzo              TINYINT(1)     NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS historial_snapshots (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    tutor_id       INT,
    fecha          VARCHAR(50),
    label          VARCHAR(255),
    total          INT,
    superan_75     INT,
    pct_exito      DOUBLE,
    avg_progreso   DOUBLE,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS historial_automatico (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    tutor_id        INT,
    fecha           VARCHAR(50),
    evento          VARCHAR(255),
    total_alumnos   INT,
    total_cursos    INT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alarmas_completadas (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    tutor_id    INT          NOT NULL,
    clave       VARCHAR(255) NOT NULL,
    fecha_dia   VARCHAR(50)  NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS progreso_historial (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    alumno_id       INT    NOT NULL,
    tutor_id        INT    NOT NULL,
    fecha_import    VARCHAR(100) NOT NULL,
    progreso        DOUBLE NOT NULL DEFAULT 0,
    examenes        INT    NOT NULL DEFAULT 0,
    delta_progreso  DOUBLE NOT NULL DEFAULT 0,
    avanzo          TINYINT(1) NOT NULL DEFAULT 0,
    FOREIGN KEY (alumno_id) REFERENCES alumnos(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Orden de truncado/inserción (respetando FK)
ORDEN_TAREAS = ['subtareas', 'tareas', 'whatsapp_log', 'historial_curso',
                'alumnos_curso', 'cursos', 'usuarios']
ORDEN_FORMACION = ['progreso_historial', 'alarmas_completadas',
                   'historial_automatico', 'historial_snapshots', 'alumnos']
# Orden de inserción es el inverso del truncado
INSERT_TAREAS    = list(reversed(ORDEN_TAREAS))
INSERT_FORMACION = list(reversed(ORDEN_FORMACION))


# ──────────────────────────────────────────────────────────────────────────────
def crear_tablas(mc, my):
    """Ejecuta el DDL completo (CREATE TABLE IF NOT EXISTS)."""
    print("🔧 Creando/verificando tablas en MySQL...")
    for stmt in DDL.strip().split(';'):
        stmt = stmt.strip()
        if stmt and not stmt.startswith('--'):
            mc.execute(stmt)
    my.commit()
    print("   ✅ Tablas listas.\n")


def sqlite_rows(sq, tabla):
    c = sq.cursor()
    c.execute(f"SELECT * FROM [{tabla}]")
    cols = [d[0] for d in c.description]
    return cols, c.fetchall()


def migrar_tabla(sq, mc, my, tabla, db_name):
    """Migra una tabla de SQLite a MySQL. Devuelve nº de filas migradas."""
    existe = sq.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tabla,)
    ).fetchone()
    if not existe:
        print(f"   ⚠  '{tabla}' no existe en SQLite, omitiendo.")
        return 0

    cols, rows = sqlite_rows(sq, tabla)
    if not rows:
        print(f"   ℹ  '{tabla}': sin datos, omitiendo.")
        return 0

    # Columnas que existen en MySQL
    mc.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (db_name, tabla)
    )
    cols_mysql = {r[0] for r in mc.fetchall()}
    cols_ok    = [c for c in cols if c in cols_mysql]

    if not cols_ok:
        print(f"   ⚠  '{tabla}': sin columnas coincidentes, omitiendo.")
        return 0

    idx          = [cols.index(c) for c in cols_ok]
    placeholders = ", ".join(["%s"] * len(cols_ok))
    col_names    = ", ".join(f"`{c}`" for c in cols_ok)
    sql          = f"INSERT INTO `{tabla}` ({col_names}) VALUES ({placeholders})"
    converted    = [tuple(row[i] for i in idx) for row in rows]

    mc.executemany(sql, converted)
    my.commit()

    omitidas = len(cols) - len(cols_ok)
    nota_cols = f" ({omitidas} col. omitidas)" if omitidas else ""
    print(f"   ✅ '{tabla}': {len(rows)} filas migradas{nota_cols}.")
    return len(rows)


def truncar_en_orden(mc, my, tablas_en_orden_borrado):
    """Vacía las tablas en el orden correcto (hijos antes que padres)."""
    for tabla in tablas_en_orden_borrado:
        try:
            mc.execute(f"DELETE FROM `{tabla}`")
            mc.execute(f"ALTER TABLE `{tabla}` AUTO_INCREMENT = 1")
        except Exception:
            pass
    my.commit()


# ──────────────────────────────────────────────────────────────────────────────
def migrar():
    try:
        import mysql.connector
    except ImportError:
        print("❌ Falta mysql-connector-python.")
        print("   Instálalo con:  pip install mysql-connector-python")
        sys.exit(1)

    # Conexión MySQL
    try:
        my = mysql.connector.connect(**MYSQL_CONFIG)
        mc = my.cursor()
        print(f"✅ Conectado a MySQL → {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}"
              f"/{MYSQL_CONFIG['database']}\n")
    except Exception as e:
        print(f"❌ Error conectando a MySQL: {e}")
        print("\n   Comprueba las variables de entorno o crea un fichero .env con:")
        print("   MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB")
        sys.exit(1)

    db_name = MYSQL_CONFIG['database']

    # Crear tablas
    crear_tablas(mc, my)

    # Deshabilitar FK para poder truncar/insertar en cualquier orden
    mc.execute("SET FOREIGN_KEY_CHECKS = 0")

    total = 0

    # ── tareas.db ────────────────────────────────────────────────────────────
    if not os.path.exists(SQLITE_TAREAS):
        print(f"⚠  No se encontró '{SQLITE_TAREAS}', omitiendo tareas.")
    else:
        print(f"📂 Migrando {SQLITE_TAREAS}  (usuarios · tareas · subtareas …)")
        sq = sqlite3.connect(SQLITE_TAREAS)
        truncar_en_orden(mc, my, ORDEN_TAREAS)
        for tabla in INSERT_TAREAS:
            total += migrar_tabla(sq, mc, my, tabla, db_name)
        sq.close()
        print()

    # ── formacion.db ─────────────────────────────────────────────────────────
    if not os.path.exists(SQLITE_FORMACION):
        print(f"⚠  No se encontró '{SQLITE_FORMACION}', omitiendo formacion.")
    else:
        print(f"📂 Migrando {SQLITE_FORMACION}  (alumnos · historial · alarmas …)")
        sq = sqlite3.connect(SQLITE_FORMACION)
        truncar_en_orden(mc, my, ORDEN_FORMACION)
        for tabla in INSERT_FORMACION:
            total += migrar_tabla(sq, mc, my, tabla, db_name)
        sq.close()
        print()

    mc.execute("SET FOREIGN_KEY_CHECKS = 1")
    my.commit()
    mc.close()
    my.close()

    print(f"🎉 Migración completada: {total} filas en total.")
    print("   Podés conservar los .db como backup o borrarlos.")


if __name__ == '__main__':
    migrar()