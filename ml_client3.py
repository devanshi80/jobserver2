# file: src/streaming/ml_client.py

import os
import uuid
import time
import json
import requests

ML_API_URL = os.getenv("ML_API_URL", "https://n29wxtvm-8000.inc1.devtunnels.ms/predict")
API_KEY = os.getenv("ML_API_KEY", "abc")  # Optional

def send_to_ml_rest(flow_data):
    # Compose full payload for ML server (all required fields)
    enriched_data = {
        "request_id": flow_data.get("request_id", str(uuid.uuid4())),
        "src_ip": flow_data.get("src_ip", "0.0.0.0"),
        "dst_ip": flow_data.get("dst_ip", "0.0.0.0"),
        "timestamp": flow_data.get("timestamp", int(time.time() * 1_000_000)),
    }

    required_fields = [
        "protocol", "flow_duration", "total_fwd_packets", "total_backward_packets",
        "fwd_packet_length_max", "fwd_packet_length_min", "fwd_packet_length_mean",
        "packet_length_mean", "packet_length_std", "flow_bytes_per_second",
        "flow_packets_per_second", "flow_iat_mean", "flow_iat_std", "flow_iat_max",
        "flow_iat_min", "fwd_iat_total", "fwd_iat_mean", "fwd_iat_std", "fwd_iat_max",
        "fwd_iat_min", "bwd_iat_total", "bwd_iat_mean", "bwd_iat_std", "bwd_iat_max",
        "bwd_iat_min", "fwd_psh_flags", "bwd_psh_flags", "fwd_urg_flags"
    ]
    for key in required_fields:
        enriched_data[key] = flow_data.get(key, 0)

    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    try:
        print(f"[ML Client] 🔄 Sending request to ML → {ML_API_URL}")
        print("[ML Client] Payload:", json.dumps(enriched_data, indent=2))
        response = requests.post(
            ML_API_URL,
            json=enriched_data,
            headers=headers,
            timeout=10
        )

        print(f"[ML Client] Status code: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"[ML Client] ✅ Prediction received - ID: {result.get('request_id')}, "
                  f"Prediction: {result.get('prediction')}, Confidence: {result.get('confidence'):.3f}")
            return result
        else:
            print(f"[ML Client] ❌ ML API error: {response.status_code} - {response.text}")
            return {}

    except Exception as e:
        print(f"[ML Client] 🚨 Error connecting to ML: {e}")
        return {}
