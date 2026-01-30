import os
import json
import logging
import argparse
import time
try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    class Producer:
        def __init__(self, c): pass
        def produce(self, t, v): pass
        def flush(self): pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("erasure-coordinator")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")

class SimulationProducer:
    def produce(self, t, v): pass
    def flush(self): pass

def propagate_tombstone(citizen_id, dry_run=False):
    mode = os.getenv("MODE", "PROD")
    
    if mode == "DEV":
        producer = SimulationProducer()
    else:
        if KAFKA_AVAILABLE:
            try:
                producer = Producer({'bootstrap.servers': KAFKA_BROKER})
            except Exception as e:
                logger.error(f"Failed to connect to Kafka in PROD mode: {e}")
                raise e
        else:
             raise ImportError("confluent_kafka not installed, but MODE=PROD. Install requirements.")
    
    # Identify all topics (In reality, query Schema Registry or Metadata)
    topics = [
        "smartcity.traffic.raw", "smartcity.traffic.processed",
        "smartcity.urban_planning.raw", "smartcity.urban_planning.processed",
        "smartcity.environmental.raw", "smartcity.environmental.processed"
    ]
    
    start_time = time.time()
    
    logger.info(f"Initiating Erasure for {citizen_id}")
    
    for topic in topics:
        # Kafka Tombstone: Key=citizen_id, Value=None
        # Note: In our simulation keys are event_ids usually, but for erasure we assume 
        # consumers re-key or index by citizen_id. 
        # We send a specific "TOMBSTONE" event so our experimental consumers can recognize it easily
        # since standard null-value tombstones on wrong keys won't work without compacting.
        tombstone_msg = {
            "type": "TOMBSTONE",
            "target_id": citizen_id,
            "timestamp": time.time()
        }
        if not dry_run:
            producer.produce(topic, json.dumps(tombstone_msg))
            
    producer.flush()
    latency = time.time() - start_time
    logger.info(f"Erasure propagation complete. Latency: {latency:.4f}s")
    return latency

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Citizen ID to erase")
    args = parser.parse_args()
    
    propagate_tombstone(args.id)
