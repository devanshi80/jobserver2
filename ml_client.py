# file: src/streaming/ml_client.py

import os
import requests

ML_API_URL = os.getenv("ML_API_URL", "https://n29wxtvm-8000.inc1.devtunnels.ms/predict")
API_KEY = os.getenv("ML_API_KEY", "abc")  # Placeholder, if auth is required

def send_to_ml_rest(data):
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(ML_API_URL, json=data, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"ML server error: {response.status_code} - {response.text}")
    except Exception as e:
        raise Exception(f"ML client request failed: {e}")
