import argparse
import json
import random
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("accountability")

def run_checks(output_dir):
    logger.info("Starting Accountability & Erasure Checks (Real Calculation)...")
    
    # Imports
    try:
        from src.erasure.coordinator import propagate_tombstone
        from src.ingestion.main import augment_with_pii, minimize_data
    except ImportError:
        import sys
        # Add code to path
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        from src.erasure.coordinator import propagate_tombstone
        from src.ingestion.main import augment_with_pii, minimize_data
    
    
    # 1. Erasure Check
    logger.info("Verifying Erasure Latency & Success...")
    latencies = []
    success_count = 0
    total_checks = 20
    
    # Simulated network latency for local testing (approximate real-world ACK time)
    SIMULATED_NETWORK_DELAY_MEAN = 3.0
    SIMULATED_NETWORK_DELAY_JITTER = 0.5
    
    for _ in range(total_checks):
        try:
            # Measure time for propagation
            lat = propagate_tombstone(f"user_delete_{_}", dry_run=False) 
            
            # Use simulated delay for local environment where async consumers are mocked
            # In DEV, we just want to verify the loop works, not fake a performance number.
            # Removing artificial sleep to reflect actual execution time.
            
            latencies.append(lat)
            success_count += 1
        except Exception as e:
            logger.error(f"Erasure failed: {e}")
            
    erasure_mean = sum(latencies) / len(latencies) if latencies else 0
    latencies.sort()
    erasure_p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    success_rate = (success_count / total_checks) * 100.0
    
    # 2. Audit Completeness Check
    # Check if audit_log.db exists and has records (created by API)
    audit_score = 0.0
    if os.path.exists("audit_log.db"):
        import sqlite3
        conn = sqlite3.connect("audit_log.db")
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM audit_log")
        c = cur.fetchone()[0]
        conn.close()
        
        # Calculate coverage score
        # In a real environment, this would compare against Kafka offsets.
        # For local verification, existence of logs implies capture.
        if c > 0:
            logger.info(f"Audit Log found with {c} entries.")
            # In local/dev environment, we cannot calculate exact recall without the full replication dataset.
            # Setting to -1.0 to indicate "Unknown/Unverified" locally to avoid misleading metrics.
            audit_score = -1.0 
        else:
            logger.warning("Audit Log empty.")
    else:
        logger.warning("audit_log.db not found. Assuming fresh environment.")
        # Cannot verify without DB
        audit_score = -1.0

    # 3. Minimization Ratio (Real Data)
    # Generate random row, measure size before/after
    row = {"timestamp": 123456789, "detector_id": "d1", "flow": 50, "speed": 80, "city": "Madrid", "irrelevant": "payload"}
    enriched = augment_with_pii(row)
    orig_len = len(json.dumps(enriched))
    minimized = minimize_data(enriched, "traffic")
    min_len = len(json.dumps(minimized))
    min_ratio = min_len / orig_len
    
    results = {
        "audit_trail_completeness_percent": audit_score,
        "erasure_latency_seconds": {
            "mean": round(erasure_mean, 2),
            "p95": round(erasure_p95, 2)
        },
        "erasure_success_rate_percent": success_rate,
        "data_minimization_ratio": round(min_ratio, 3), # Real calc
        "blocked_purpose_violations_percent": -1.0, # Not verified in this script
        "retention_policy_compliance_percent": -1.0, # Not verified in this script
        "gdpr_mapping": {
            "Art_30": "Supported",
            "Art_17": "Supported",
            "Art_5_1_c": "Supported",
            "Art_5_1_b": "Supported"
        }
    }
    
    logger.info("Checks complete.")
    
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "accountability_results.json"), "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    
    run_checks(args.output_dir)
