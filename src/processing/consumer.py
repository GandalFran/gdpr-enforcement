import os
import json
import logging
import argparse
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("processing")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def run_consumer(purpose, group_id):
    c_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False # Manual commit for Audit Accuracy
    }
    
    p_conf = {'bootstrap.servers': KAFKA_BROKER}
    
    consumer = Consumer(c_conf)
    producer = Producer(p_conf)
    
    # Subscribe to raw topic
    topic = f"smartcity.{purpose}.raw"
    consumer.subscribe([topic])
    logger.info(f"Subscribed to {topic} as {group_id}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break
            
            # Simulate Processing
            raw_val = msg.value()
            if raw_val is None:
                continue
                
            data = json.loads(raw_val)
            
            # Handle Tombstones (Article 17)
            if data.get("type") == "TOMBSTONE":
                target_id = data.get("target_id")
                logger.info(f"Received ERASURE REQUEST for {target_id}. Purging local state...")
                # Simulate deletion latency
                time.sleep(0.05) 
                
                # Send ACK (Simulating Coordinated Erasure)
                ack_topic = "smartcity.erasure.acks"
                ack_msg = {
                    "type": "ERASURE_ACK",
                    "target_id": target_id,
                    "service": f"{purpose}-service",
                    "timestamp": time.time()
                }
                producer.produce(ack_topic, json.dumps(ack_msg))
                producer.flush()
                consumer.commit(asynchronous=False)
                continue

            # Normal Processing
            # logger.info(f"Processing event {data.get('event_id')} for {purpose}")
            
            # Produce to Processed Topic
            processed_topic = f"smartcity.{purpose}.processed"
            data["processed_at"] = os.getenv("HOSTNAME", "worker")
            producer.produce(processed_topic, json.dumps(data))
            
            # Manual Commit (Article 30 Evidence)
            consumer.commit(asynchronous=False)
            
            producer.poll(0)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--purpose", required=True, help="Processing purpose (traffic, urban_planning, environmental)")
    parser.add_argument("--group", required=True, help="Consumer Group ID")
    args = parser.parse_args()
    
    run_consumer(args.purpose, args.group)
