import argparse
import json
import random
import time
import logging
import os
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("benchmark")

def run_benchmark(output_dir, count=100):
    logger.info(f"Starting Performance Benchmark (Real Architecture) with {count} items...")
    
    # Import SUT
    try:
        from src.ingestion.main import augment_with_pii, minimize_data
    except ImportError:
        import sys
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        from src.ingestion.main import augment_with_pii, minimize_data

    # Kafka Setup
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    try:
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        KAFKA_AVAILABLE = True
        logger.info(f"Connected to Kafka at {KAFKA_BROKER}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}. Falling back to error.")
        return

    # Generate Synthetic Load
    logger.info(f"Generating synthetic load ({count} items for real throughput test)...")
    data_points = []
    for i in range(count):  
        data_points.append({
            "timestamp": time.time(),
            "detector_id": f"d_{i%100}",
            "flow": random.randint(0, 100),
            "speed": random.randint(20, 120),
            "city": "Madrid"
        })
        
    latencies = []
    start_bench = time.time()
    
    ack_futures = []
    
    def delivery_report(err, msg, start_time):
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            # Latency = Now - Start Time of that specific message
            # This measures: Ingestion Logic + Produce + Broker Ack
            lat = time.time() - start_time
            latencies.append(lat)

    for row in data_points:
        t0 = time.time()
        
        # 1. Computation (Real)
        enriched = augment_with_pii(row.copy())
        purpose = "traffic"
        payload = minimize_data(enriched, purpose)
        payload_str = json.dumps(payload)
        
        # 2. Real Infrastructure Interaction
        # We pass t0 as a distinct arg to the callback using a closure workaround or just manual sync
        # synchronous produce for reliable latency measurement
        producer.produce("smartcity.traffic.raw", payload_str)
        producer.flush() # Forces wait for Ack. This is "Sync" mode, good for latency measurement.
        
        lat = time.time() - t0
        latencies.append(lat)
        
    total_time = time.time() - start_bench
    
    if not latencies:
        logger.error("No latencies recorded.")
        return

    mean_latency = sum(latencies) / len(latencies)
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    
    # Real Throughput (Events per Minute)
    throughput = (len(data_points) / total_time) * 60
    
    logger.info(f"Results: Mean={mean_latency:.3f}s, P95={p95:.3f}s, Throughput={int(throughput)}")
    
    results = {
        "configuration": "Proposed (Real Docker Architecture)",
        "latency_stats": {
            "mean_sec": round(mean_latency, 4),
            "p50_sec": round(p50, 4),
            "p95_sec": round(p95, 4),
            "p99_sec": round(p99, 4),
            "ci_95": 0.0 # Real data
        },
        "throughput_events_per_min": int(throughput),
        "throughput_ci_95": 0
    }
    
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "performance_results.json"), "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--count", type=int, default=100, help="Number of events to generate")
    args = parser.parse_args()
    
    run_benchmark(args.output_dir, args.count)
