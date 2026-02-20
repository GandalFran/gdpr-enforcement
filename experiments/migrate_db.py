#!/usr/bin/env python3
"""Quick migration script to add citizen_id column to PostgreSQL audit_log table."""
import os
import psycopg2

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "adminpassword")
PG_DB = "smartcity_audit"

def migrate():
    try:
        conn = psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='audit_log' AND column_name='citizen_id';
        """)
        
        if cur.fetchone():
            print("Column 'citizen_id' already exists in audit_log table.")
        else:
            print("Adding 'citizen_id' column to audit_log table...")
            cur.execute("ALTER TABLE audit_log ADD COLUMN citizen_id TEXT;")
            conn.commit()
            print("Migration complete!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Migration failed: {e}")
        print("This is expected if PostgreSQL is not running (will use SQLite instead).")

if __name__ == "__main__":
    migrate()
