import sqlite3

DB_NAME = "tareas.db"

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

try:
    cursor.execute("ALTER TABLE tareas ADD COLUMN codigo TEXT")
    print("✅ Columna 'codigo' agregada correctamente")
except sqlite3.OperationalError:
    print("⚠️ La columna 'codigo' ya existe")

conn.commit()
conn.close()
