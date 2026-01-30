```python
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import os
import psycopg2
import logging
from datetime import datetime
import sqlite3
from confluent_kafka.admin import AdminClient # Added this import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("audit-api")

app = FastAPI(title="Smart City Governance Audit API")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "adminpassword")
PG_DB = "smartcity_audit"

# This function was previously not wrapped in a def, causing syntax errors.
# It's now properly defined as get_db_connection.
def get_db_connection():
    mode = os.getenv("MODE", "PROD")

    # DEV MODE: Usage of SQLite for lightweight simulation
    if mode == "DEV":
         # logger.info("Running in DEV mode (SQLite)") # Avoid logger inside simple util if possible or ensure initialized
         db_path = os.getenv("SQLITE_DB_PATH", "audit_log.db")
         conn = sqlite3.connect(db_path)
         conn.execute('''CREATE TABLE IF NOT EXISTS audit_log 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         group_id TEXT, 
                         topic TEXT, 
                         partition INTEGER, 
                         offset_val INTEGER, 
                         timestamp DATETIME,
                         citizen_id TEXT)''')
         return conn

    # PROD MODE: Usage of Postgres
    try:
        conn = psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)
        return conn
    except Exception as e:
        # logger.error(f"Postgres connection failed in PROD mode: {e}")
        # In PROD, we fail hard. No fallback to SQLite.
        raise e

class AuditRecord(BaseModel):
    group_id: str
    topic: str
    partition: int
    offset_val: int
    timestamp: datetime
    citizen_id: Optional[str] = None

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/test/reset")
def test_reset():
    """Reset the database for testing purposes."""
    conn = get_db_connection()
    cur = conn.cursor()
    # Handle different DB syntaxes if needed, but simple DELETE works for both
    try:
        cur.execute("DELETE FROM audit_log")
        conn.commit()
    except Exception as e:
        logger.error(f"Reset failed: {e}")
        pass
    conn.close()
    return {"status": "reset_complete"}

@app.post("/test/seed")
def test_seed(records: List[AuditRecord]):
    """Seed the database with known records for verification."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Adapt for SQLite vs Postgres placeholders (?) -> purely using SQLite logic for fallback or standard SQL
    # Postgres uses %s, SQLite uses ?
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    
    data = [(r.group_id, r.topic, r.partition, r.offset_val, r.timestamp, r.citizen_id) for r in records]
    
    try:
        cur.executemany(f"INSERT INTO audit_log (group_id, topic, partition, offset_val, timestamp, citizen_id) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})", data)
        conn.commit()
    except Exception as e:
        logger.error(f"Seed failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    
    return {"status": "seeded", "count": len(records)}

@app.get("/dsar/{citizen_id}", response_model=List[dict])
def dsar_request(citizen_id: str):
    """
    S1: DSAR Reconstruction.
    Now performs a REAL query filtering by citizen_id.
    """
    # 1. Start timer
    start_time = datetime.now()
    
    # 2. Query Audit Log 
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    
    query = f"SELECT group_id, topic, offset_val, timestamp, citizen_id FROM audit_log WHERE citizen_id = {placeholder} ORDER BY timestamp DESC"
    cur.execute(query, (citizen_id,))
    rows = cur.fetchall()
    conn.close()
    
    # 3. Format result
    results = []
    for r in rows:
        results.append({
            "service": r[0],
            "action": "PROCESSED",
            "topic": r[1],
            "timestamp": r[3] if isinstance(r[3], str) else r[3].isoformat(),
            "evidence": f"Offset {r[2]}",
            "citizen_id": r[4]
        })
        
    execution_time = (datetime.now() - start_time).total_seconds()
    
    return [
        {"meta": {"execution_time_seconds": execution_time, "citizen_id": citizen_id, "record_count": len(results)}},
        {"records": results}
    ]

@app.get("/audit/regulatory", response_model=List[dict])
def regulatory_audit(purpose: str = Query(..., description="Target purpose")):
    """S2: Regulatory Inquiry for Purpose Access."""
    start_time = datetime.now()
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    param = f"%{purpose}%"
    
    cur.execute(f"SELECT * FROM audit_log WHERE topic LIKE {placeholder} LIMIT 100", (param,))
    rows = cur.fetchall()
    conn.close()
    
    ex_time = (datetime.now() - start_time).total_seconds()
    return [{"meta": {"execution_time": ex_time}, "count": len(rows), "samples": [str(r) for r in rows[:5]]}]

@app.get("/audit/incident")
def incident_forensics(topic: str = Query(...), start: int = Query(...), end: int = Query(...)):
    """S3: Incident Forensics (Impact Analysis)."""
    start_time = datetime.now()
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    
    # Implement REAL query logic
    # Find unique citizen_ids affected by events in this topic/window
    query = f"SELECT DISTINCT citizen_id FROM audit_log WHERE topic = {placeholder} AND timestamp BETWEEN {placeholder} AND {placeholder}"
    # Convert timestamps if necessary (assuming DB stores ISO strings or timestamps)
    # Our DB seed uses ISO strings, so we might need string comparison or conversion.
    # For robust SQLite/PG compat, we'll try string comparison if ISO format is standard
    try:
        t_start = datetime.fromtimestamp(start/1000).isoformat()
        t_end = datetime.fromtimestamp(end/1000).isoformat()
    except:
        t_start = str(start)
        t_end = str(end)
        
    cur.execute(f"SELECT DISTINCT citizen_id FROM audit_log WHERE topic = {placeholder} AND timestamp >= {placeholder} AND timestamp <= {placeholder}", (topic, t_start, t_end))
    rows = cur.fetchall()
    
    # Also get total count
    cur.execute(f"SELECT COUNT(*) FROM audit_log WHERE topic = {placeholder} AND timestamp >= {placeholder} AND timestamp <= {placeholder}", (topic, t_start, t_end))
    count_res = cur.fetchone()
    total_count = count_res[0] if count_res else 0
    
    conn.close()
    
    affected_subjects = [r[0] for r in rows if r[0]]
    
    ex_time = (datetime.now() - start_time).total_seconds()
    return {
        "meta": {"execution_time": ex_time, "impact_window": f"{t_start} to {t_end}"},
        "impact_count": total_count,
        "affected_subjects": affected_subjects
    }

@app.get("/ropa/live")
def live_ropa():
    """S4: Live RoPA Generation."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT group_id, topic FROM audit_log")
    rows = cur.fetchall()
    conn.close()
    
    ropa = []
    opa = []
    for r in rows:
        grp = r[0]
        tpc = r[1]
        
        # Dynamic Logic based on topic patterns (Paper Specs)
        # Legal Basis: Public Interest for traffic, Consent for others
        legal_basis = "Public Interest" if "traffic" in tpc else "Consent"
        
        # Retention: 24h for raw, 7d for processed, 90d for aggregated
        if "raw" in tpc:
            retention = "24h"
        elif "processed" in tpc:
            retention = "7d"
        elif "aggregated" in tpc:
            retention = "90d"
        else:
            retention = "Unknown"
            
        ropa.append({
            "processor": grp,
            "data_category": tpc,
            "legal_basis": legal_basis,
            "retention": retention,
            "derived_from_audit_log": True
        })
        
    return {"generated_at": datetime.now().isoformat(), "processing_activities": ropa}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

