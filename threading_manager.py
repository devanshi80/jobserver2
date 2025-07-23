# file: src/streaming/threading_manager.py

from concurrent.futures import ThreadPoolExecutor
import os

executor = ThreadPoolExecutor(max_workers=os.cpu_count() * 2)

def run_in_thread(task, *args, **kwargs):
    executor.submit(task, *args, **kwargs)
