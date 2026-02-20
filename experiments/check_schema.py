#!/usr/bin/env python3
"""Check the actual schema of the PostgreSQL audit_log table."""
import os
import psycopg2

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "adminpassword")
PG_DB = "smartcity_audit"

try:
    conn = psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)
    cur = conn.cursor()
    
    print("=== Current audit_log table schema ===")
    cur.execute("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'audit_log'
        ORDER BY ordinal_position;
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}" + (f"({row[2]})" if row[2] else ""))
    
    cur.close()
    conn.close()
    print("\nSchema check complete!")
except Exception as e:
    print(f"Failed to check schema: {e}")
