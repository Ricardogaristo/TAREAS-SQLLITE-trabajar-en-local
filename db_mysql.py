"""
db_mysql.py
===========
Helper de conexión MySQL que sustituye a sqlite3.
Proporciona una interfaz compatible con el código existente:
  - conn.execute(sql, params)  ← igual que sqlite3
  - conn.cursor()              ← igual que sqlite3
  - Parámetros con ?           ← se convierten a %s automáticamente
  - Filas como dict            ← igual que sqlite3.Row
  - Fechas como string         ← igual que TEXT en SQLite
"""

import os
import datetime
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ── Configuración ──────────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     os.getenv('MYSQL_HOST', 'localhost'),
    'port':     int(os.getenv('MYSQL_PORT', '3306')),
    'user':     os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DB', 'formacion'),
    'charset':  'utf8mb4',
    'autocommit': False,
}


def get_db_name() -> str:
    """Devuelve el nombre de la base de datos configurada."""
    return MYSQL_CONFIG['database']


# ── Serialización de tipos MySQL → str (compatible con código anterior) ────────
def _serialize(value):
    """Convierte datetime/date de MySQL a string ISO, igual que SQLite TEXT."""
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    return value


class SqliteCompatRow:
    """
    Fila compatible con sqlite3.Row:
    - acceso por nombre:  row["columna"]
    - acceso por índice:  row[0]
    - conversión a dict:  dict(row)
    - iteración:          for v in row
    - keys():             row.keys()
    """
    def __init__(self, data: dict):
        self._data = {k: _serialize(v) for k, v in data.items()}
        self._keys = list(self._data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[self._keys[key]]
        return self._data[key]

    def __iter__(self):
        return iter(self._data.values())

    def __len__(self):
        return len(self._data)

    def keys(self):
        return self._keys

    def get(self, key, default=None):
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        return f"SqliteCompatRow({self._data})"


def _serialize_row(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return SqliteCompatRow(row)
    return row


# ── Wrappers ───────────────────────────────────────────────────────────────────
class MysqlCursorWrapper:
    """Cursor wrapper: convierte ? → %s y filas a dict."""

    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, params=None):
        sql = sql.replace('?', '%s')
        self._cursor.execute(sql, params if params is not None else ())
        return self  # permite encadenar .fetchall()

    def fetchone(self):
        return _serialize_row(self._cursor.fetchone())

    def fetchall(self):
        return [_serialize_row(r) for r in self._cursor.fetchall()]

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    def __iter__(self):
        for row in self._cursor:
            yield _serialize_row(row)


class MysqlConnectionWrapper:
    """Conexión wrapper: imita sqlite3.Connection."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        """Crea un cursor, ejecuta y devuelve el wrapper (listo para .fetchall())."""
        sql = sql.replace('?', '%s')
        cursor = self._conn.cursor(dictionary=True)
        cursor.execute(sql, params if params is not None else ())
        return MysqlCursorWrapper(cursor)

    def cursor(self):
        return MysqlCursorWrapper(self._conn.cursor(dictionary=True))

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    # Context manager: permite `with get_connection() as conn:`
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()
        return False


# ── Conexión principal ─────────────────────────────────────────────────────────
def get_form_conn() -> MysqlConnectionWrapper:
    """Devuelve una conexión MySQL. Si la DB no existe, la crea automáticamente."""
    db_name = MYSQL_CONFIG["database"]
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        return MysqlConnectionWrapper(conn)
    except mysql.connector.errors.ProgrammingError as e:
        if e.errno != 1049:
            raise
    # DB no existe: crearla
    cfg = {k: v for k, v in MYSQL_CONFIG.items() if k != "database"}
    tmp = mysql.connector.connect(**cfg)
    cur = tmp.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS `" + db_name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    tmp.commit(); cur.close(); tmp.close()
    print(f"✅ Base de datos '{db_name}' creada automáticamente.")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    return MysqlConnectionWrapper(conn)


# ── Helper de migraciones ──────────────────────────────────────────────────────
def column_exists(cursor: MysqlCursorWrapper, table: str, column: str) -> bool:
    """Comprueba si una columna ya existe en la tabla (para ALTER TABLE seguro)."""
    cursor.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (get_db_name(), table, column)
    )
    row = cursor.fetchone()
    return bool(row and row.get('cnt', 0) > 0)


def get_tareas_conn() -> MysqlConnectionWrapper:
    """Conexión para las tablas de tareas (misma DB que formacion)."""
    return get_form_conn()