import time
import requests
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("experiments-ropa")

AUDIT_API_URL = os.getenv("AUDIT_API_URL", "http://localhost:8000")

def run_s4_ropa():
    logger.info("Running S4: Live RoPA Generation...")
    start = time.time()
    try:
        resp = requests.get(f"{AUDIT_API_URL}/ropa/live")
        if resp.status_code == 200:
            duration = time.time() - start
            ropa = resp.json()
            result = {
                "experiment": "S4",
                "latency": duration,
                "activities_count": len(ropa.get("processing_activities", [])),
                "ropa_content": ropa
            }
            logger.info(result)
            return result
        else:
             logger.error(f"S4 Failed: {resp.status_code}")
    except Exception as e:
        logger.error(f"S4 Error: {e}")
        
    return {}

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="code/metrics", help="Directory to save results")
    args = parser.parse_args()

    res = run_s4_ropa()
    
    os.makedirs(args.output_dir, exist_ok=True)
    with open(f"{args.output_dir}/ropa_results.json", "w") as f:
        json.dump(res, f, indent=2)
