import sqlite3

DB_NAME = "tareas.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def crear_tablas_y_admin():
    conn = get_connection()
    cursor = conn.cursor()

    # Crear tabla tareas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tareas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT,
            descripcion TEXT NOT NULL,
            categoria TEXT,
            fecha TEXT,
            completada INTEGER DEFAULT 0,
            usuario TEXT
        )
    """)

    # Crear tabla usuarios
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    """)

    # Crear admin si no existe
    cursor.execute("SELECT * FROM usuarios WHERE username='admin'")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO usuarios (username, password) VALUES (?, ?)",
            ("admin", "1234")
        )

    # Verificar columna usuario
    cursor.execute("PRAGMA table_info(tareas)")
    columnas = [col[1] for col in cursor.fetchall()]

    if "usuario" not in columnas:
        cursor.execute("ALTER TABLE tareas ADD COLUMN usuario TEXT")
        print("Columna usuario creada correctamente")

    conn.commit()
    conn.close()

crear_tablas_y_admin()