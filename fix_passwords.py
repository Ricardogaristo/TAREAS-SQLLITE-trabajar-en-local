"""
Ejecuta este script UNA VEZ desde la carpeta del proyecto:
  python fix_passwords.py
"""
from werkzeug.security import generate_password_hash
from db_mysql import get_tareas_conn

conn   = get_tareas_conn()
cursor = conn.cursor()

cursor.execute("SELECT id, username, password FROM usuarios")
users = cursor.fetchall()

migrados = 0
ya_hash  = 0

for row in users:
    uid      = row["id"]
    username = row["username"]
    pw       = row["password"] or ""

    if pw and not pw.startswith("pbkdf2:") and not pw.startswith("scrypt:"):
        hashed = generate_password_hash(pw)
        cursor.execute("UPDATE usuarios SET password=? WHERE id=?", (hashed, uid))
        print(f"  🔑 {username} (id={uid}) — hasheada")
        migrados += 1
    else:
        print(f"  ✅ {username} (id={uid}) — ya tenía hash")
        ya_hash += 1

# Asegurar que admin es SuperAdmin
cursor.execute("UPDATE usuarios SET es_admin=2 WHERE username=?", ("admin",))
print("\n  👑 admin → es_admin=2 (SuperAdmin)")

conn.commit()
conn.close()

print(f"\n✅ Listo. Hasheadas: {migrados} | Ya tenían hash: {ya_hash}")
print("   Ahora puedes entrar en /login con tus credenciales normales.")
