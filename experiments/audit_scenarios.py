import time
import requests
import json
import argparse
import random
import logging
import os
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("experiments")

AUDIT_API_URL = os.getenv("AUDIT_API_URL", "http://localhost:8000")

def run_s1_dsar(citizen_id, iterations=10):
    logger.info("Running S1: DSAR Compliance with verified ground truth...")
    
    # 1. Setup Ground Truth
    target_user = citizen_id if citizen_id else "user_target_1"
    noise_user = "user_noise"
    expected_count = 50
    noise_count = 50
    
    # Prepare seed records
    seed_data = []
    base_time = datetime.now()
    
    # Target records
    for i in range(expected_count):
        seed_data.append({
            "group_id": "traffic-service",
            "topic": "smartcity.traffic.processed",
            "partition": 0,
            "offset_val": 1000 + i,
            "timestamp": (base_time - timedelta(minutes=i)).isoformat(),
            "citizen_id": target_user
        })
        
    # Noise records
    for i in range(noise_count):
        seed_data.append({
            "group_id": "other-service",
            "topic": "smartcity.environmental.raw",
            "partition": 0,
            "offset_val": 5000 + i,
            "timestamp": (base_time - timedelta(minutes=i)).isoformat(),
            "citizen_id": noise_user
        })

    # Reset & Seed
    try:
        logger.info("Resetting database...")
        requests.post(f"{AUDIT_API_URL}/test/reset", timeout=10)
        logger.info(f"Seeding {len(seed_data)} records...")
        resp = requests.post(f"{AUDIT_API_URL}/test/seed", json=seed_data, timeout=10)
        if resp.status_code != 200:
            logger.error(f"Seeding failed with status {resp.status_code}: {resp.text}")
            return {}
        logger.info("Seeding complete!")
    except Exception as e:
        logger.error(f"Failed to seed data: {e}. Is the API running?")
        return {}

    latencies = []
    recalls = []
    precisions = []

    for _ in range(iterations):
        start = time.time()
        try:
            resp = requests.get(f"{AUDIT_API_URL}/dsar/{target_user}", timeout=10)
            lat = time.time() - start
            latencies.append(lat)
            
            if resp.status_code == 200:
                data = resp.json()
                records = data[1]["records"]
                
                # Calculate Metrics
                retrieved_ids = [r.get("citizen_id") for r in records]
                true_positives = sum(1 for cid in retrieved_ids if cid == target_user)
                false_positives = len(retrieved_ids) - true_positives
                
                # Precision = TP / (TP + FP)
                if len(retrieved_ids) > 0:
                    prec = true_positives / len(retrieved_ids)
                else:
                    prec = 1.0 if expected_count == 0 else 0.0
                    
                # Recall = TP / (TP + FN) -> we explicitly seeded 'expected_count'
                rec = true_positives / expected_count if expected_count > 0 else 1.0
                
                recalls.append(rec * 100)
                precisions.append(prec * 100)
            else:
                logger.error(f"S1 Failed: {resp.status_code}")
        except Exception as e:
            logger.error(f"S1 Connection Error: {e}")
            break
            
    if latencies:
        avg_lat = sum(latencies) / len(latencies)
        p95_lat = sorted(latencies)[int(len(latencies) * 0.95)]
        avg_recall = sum(recalls) / len(recalls)
        avg_prec = sum(precisions) / len(precisions)
        
        result = {
            "experiment": "S1", 
            "mean_latency": avg_lat, 
            "p95_latency": p95_lat, 
            "samples": len(latencies),
            "recall_percent": avg_recall,
            "precision_percent": avg_prec
        }
        logger.info(json.dumps(result))
        return result
    return {}

def run_s2_regulatory(purpose="traffic", iterations=10):
    logger.info(f"Running S2: Regulatory Audit for {purpose}...")
    latencies = []
    
    for _ in range(iterations):
        start = time.time()
        try:
            resp = requests.get(f"{AUDIT_API_URL}/audit/regulatory", params={"purpose": purpose}, timeout=10)
            if resp.status_code == 200:
                latencies.append(time.time() - start)
            else:
                logger.error(f"S2 Failed: {resp.status_code}")
        except Exception as e:
            logger.error(f"S2 Connection Error: {e}")
            break
            
    if latencies:
        avg = sum(latencies) / len(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        result = {"experiment": "S2", "mean_latency": avg, "p95_latency": p95, "samples": len(latencies)}
        logger.info(json.dumps(result))
        return result
    return {}

def run_s3_incident(topic="smartcity.traffic.raw", iterations=10):
    logger.info(f"Running S3: Incident Forensics for {topic}...")
    latencies = []
    
    for _ in range(iterations):
        start = time.time()
        try:
            # S3 simulates querying logs for a specific time window & topic to assess impact
            # params: topic, start_time, end_time
            end_ts = int(time.time() * 1000)
            start_ts = end_ts - 3600000 # 1 hour ago
            
            resp = requests.get(f"{AUDIT_API_URL}/audit/incident", params={"topic": topic, "start": start_ts, "end": end_ts}, timeout=10)
            if resp.status_code == 200:
                latencies.append(time.time() - start)
            else:
                logger.error(f"S3 Failed: {resp.status_code}")
        except Exception as e:
            logger.error(f"S3 Connection Error: {e}")
            break
            
    if latencies:
        avg = sum(latencies) / len(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        result = {"experiment": "S3", "mean_latency": avg, "p95_latency": p95, "samples": len(latencies)}
        logger.info(json.dumps(result))
        return result
    return {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["s1", "s2", "s3", "all"], default="all")
    parser.add_argument("--cid", default="user_123")
    parser.add_argument("--output-dir", default="code/metrics", help="Directory to save results")
    args = parser.parse_args()
    
    results = {}
    if args.scenario in ["s1", "all"]:
        results["s1"] = run_s1_dsar(args.cid)
    
    if args.scenario in ["s2", "all"]:
        results["s2"] = run_s2_regulatory()
        
    if args.scenario in ["s3", "all"]:
        results["s3"] = run_s3_incident()
        
    # Save results
    os.makedirs(args.output_dir, exist_ok=True)
    with open(f"{args.output_dir}/audit_results.json", "w") as f:
        json.dump(results, f, indent=2)
