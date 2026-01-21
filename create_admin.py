import os
from werkzeug.security import generate_password_hash
from app.services.duckdb_service import DuckDBService

def create_admin_user():
    print("👤 Creating Admin User...")

    # 1️⃣ Read from environment first (Docker-safe)
    username = os.getenv("ADMIN_USERNAME", "admin")
    password = os.getenv("ADMIN_PASSWORD")

    # 2️⃣ Fallback to interactive only if not provided
    if not password:
        try:
            password = input("Enter admin password: ")
        except EOFError:
            print("❌ Password not provided and input not available.")
            return

    if not password:
        print("❌ Password cannot be empty.")
        return

    db = DuckDBService()
    if not db.connect():
        print("❌ Failed to connect to database.")
        return

    db.create_table()  # Ensure tables exist

    existing = db.get_user_by_username(username)
    if existing:
        print(f"⚠️ Admin user '{username}' already exists.")
        db.disconnect()
        return

    password_hash = generate_password_hash(password)
    user_id = db.create_user(username, password_hash, role="ADMIN")

    if user_id:
        print(f"✅ Admin user created successfully!")
        print(f"   Username: {username}")
        print(f"   Role: ADMIN")
    else:
        print("❌ Failed to create admin user.")

    db.disconnect()

if __name__ == "__main__":
    create_admin_user()
