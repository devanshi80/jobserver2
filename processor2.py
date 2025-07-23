# file: src/streaming/processor.py

import queue
from ml_client import send_to_ml_rest

processing_queue = queue.Queue(maxsize=10000)

def add_to_queue(data):
    try:
        processing_queue.put_nowait(data)
    except queue.Full:
        print("[Processor] Queue full, dropping data")

def process_data():
    while True:
        try:
            item = processing_queue.get(timeout=5)
        except queue.Empty:
            continue

        try:
            print(f"[Processor] Processing flow from Kafka")
            print(f"→ Flow Source: {item.get('src_ip')} → {item.get('dst_ip')}")

            # Call ML prediction
            prediction = send_to_ml_rest(item)

            if prediction.get("prediction") == 1:
                print(f"🚨 MALICIOUS DETECTED - Confidence: {prediction.get('confidence', 0):.3f}")
            else:
                print(f"✅ Benign flow - Confidence: {prediction.get('confidence', 0):.3f}")

        except Exception as e:
            print(f"[Processor] Prediction error: {e}")
        finally:
            processing_queue.task_done()
