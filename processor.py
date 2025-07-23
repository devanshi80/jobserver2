# file: src/streaming/processor.py

import queue

processing_queue = queue.Queue(maxsize=10000)

def add_to_queue(data):
    try:
        processing_queue.put_nowait(data)
    except queue.Full:
        print("[Processor] Queue full, dropping data")

def process_data():
    while True:
        try:
            item = processing_queue.get(timeout=5)  # Wait for new item
        except queue.Empty:
            continue

        try:
            # Skipping ML prediction and alerting for now
            print(f"[Processor] Processing flow: {item}")
        except Exception as e:
            print(f"[Processor] Error: {e}")
        finally:
            processing_queue.task_done()
