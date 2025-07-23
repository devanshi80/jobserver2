# file: src/streaming/processor.py

import queue
import uuid
import time
from ml_client import send_to_ml_rest

processing_queue = queue.Queue(maxsize=10000)

def add_to_queue(data):
    try:
        processing_queue.put_nowait(data)
    except queue.Full:
        print("[Processor] Queue full, dropping data")

def process_data():
    print("[Processor] ✅ Flow processor thread started")
    while True:
        try:
            item = processing_queue.get(timeout=5)
        except queue.Empty:
            print("[Processor] ⏳ Queue empty, waiting...")
            continue

        try:
            # Ensure required fields are present
            item["request_id"] = item.get("request_id") or str(uuid.uuid4())
            item["timestamp"] = item.get("timestamp") or int(time.time() * 1_000_000)
            item["src_ip"] = item.get("src_ip", "0.0.0.0")
            item["dst_ip"] = item.get("dst_ip", "0.0.0.0")

            print(f"[Processor] 🚦 Flow: {item['src_ip']} → {item['dst_ip']}")

            # Call ML prediction
            prediction = send_to_ml_rest(item)

            # Print ML prediction result
            if prediction.get("prediction") == 1:
                print(f"[Processor] 🚨 MALICIOUS DETECTED | Confidence = {prediction.get('confidence', 0):.3f}")
            else:
                print(f"[Processor] ✅ BENIGN Flow | Confidence = {prediction.get('confidence', 0):.3f}")

        except Exception as e:
            print(f"[Processor] ❌ Prediction error: {e}")

        finally:
            processing_queue.task_done()

