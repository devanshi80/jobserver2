# file: src/main.py

import time
from threading_manager import run_in_thread
from kafka_consumer import consume_network_flows
from processor import process_data

if __name__ == "__main__":
    print("[Main] Starting Job Server...")

    # Start the Kafka consumer in a background thread
    run_in_thread(consume_network_flows)

    # Start multiple processing threads
    for _ in range(4):
        run_in_thread(process_data)

    # Keep main thread alive
    while True:
        time.sleep(1)
