import pandas as pd
import json
import time
import os
import random
import uuid
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ingestion")

# Kafka Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    class Producer:
        def __init__(self, config): pass
        def produce(self, topic, value, callback=None): 
            if callback: callback(None, None)
        def flush(self): pass
    class AdminClient:
        def __init__(self, config): pass
        def create_topics(self, new_topics): return {}

class SimulationProducer:
    def produce(self, topic, value, callback=None):
        if callback: callback(None, None)
    def flush(self): pass

# Schemas (Hardcoded for simplicity, usually in Schema Registry)
SCHEMAS = {
    "traffic": ["event_id", "timestamp", "detector_id", "flow", "speed", "city", "purpose"],
    "urban_planning": ["event_id", "timestamp", "city", "flow", "purpose"],
    "environmental": ["event_id", "timestamp", "city", "speed", "purpose"]
}

def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")

def create_topics(admin_client, topics):
    """Create topics if they don't exist."""
    if not KAFKA_AVAILABLE: return
    try:
        new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
        futures = admin_client.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created")
            except Exception:
                pass
    except Exception:
        pass

def augment_with_pii(row):
    """Synthetically add PII and Consent data."""
    row["event_id"] = str(uuid.uuid4())
    row["citizen_id"] = f"user_{random.randint(1, 1000)}" 
    row["consent_status"] = "GRANTED"
    return row

def minimize_data(row, purpose):
    """Article 5(1)(c): Data Minimization. Project only allowed fields."""
    allowed_fields = SCHEMAS.get(purpose, [])
    if not allowed_fields:
        return None
    minimized = {k: row.get(k) for k in allowed_fields if k in row}
    minimized["purpose"] = purpose
    return minimized
def run_ingestion(input_file, speed_factor, producer=None):
    if producer is None:
        if KAFKA_AVAILABLE:
            try:
                producer = Producer({'bootstrap.servers': KAFKA_BROKER})
                admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
                # Create Topics for Purposes
                purposes_init = ["traffic", "urban_planning", "environmental"]
                topics = [f"smartcity.{p}.raw" for p in purposes_init]
                create_topics(admin_client, topics)
            except Exception as e:
                logger.warning(f"Kafka connection failed: {e}. Using SimulationProducer.")
                producer = SimulationProducer()
        else:
            logger.warning("confluent_kafka not installed. Using SimulationProducer.")
            producer = SimulationProducer()
    
    purposes = ["traffic", "urban_planning", "environmental"]

    logger.info(f"Reading {input_file}...")
    try:
        # Iterate over chunks to handle large files if needed, though we use mock data mostly
        for chunk in pd.read_csv(input_file, chunksize=1000):
            for _, row in chunk.iterrows():
                row_dict = row.to_dict()
                row_dict = augment_with_pii(row_dict)
                
                # Simulate events for different purposes
                # In reality, a sensor might produce for multiple purposes or dedicated sensors
                # Here we randomly assign a purpose to the event for simulation variety
                target_purpose = random.choice(purposes)
                
                # Apply Minimization (Privacy Control)
                payload = minimize_data(row_dict, target_purpose)
                
                if payload:
                    topic = f"smartcity.{target_purpose}.raw"
                    producer.produce(topic, json.dumps(payload), callback=delivery_report)
            
            producer.flush()
            time.sleep(1 / speed_factor) # Control ingestion rate
            
        logger.info("Ingestion complete.")
        
    except FileNotFoundError:
        logger.error(f"File {input_file} not found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="code/mock/mock_data.csv")
    parser.add_argument("--speed", type=float, default=10.0, help="Speed factor for replay")
    args = parser.parse_args()
    
    run_ingestion(args.input, args.speed)
