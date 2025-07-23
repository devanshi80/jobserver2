# file: src/streaming/kafka_consumer.py

import json
from kafka import KafkaConsumer
from processor import add_to_queue

def consume_network_flows():
    print("[Consumer] Starting consumer setup...")

    consumer = KafkaConsumer(
        'network_flows',
        bootstrap_servers=['localhost:9092'],
        group_id='job_server_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("[Consumer] Connected to Kafka and listening to 'network_flows'...")

    for msg in consumer:
        print(f"[Consumer] Received raw message: {msg.value}")
        try:
            data = json.loads(msg.value.decode('utf-8'))
            print(f"[Consumer] Parsed JSON: {data}")
            add_to_queue(data)
        except json.JSONDecodeError:
            print("[Consumer] ⚠️ Invalid JSON format")
