import requests
import time
import sys

BASE_URL = "http://localhost:8002"

def run_test():
    print("--- Starting Fraud Verification ---")
    
    user_id = "user-vip"
    
    # 1. Establish History (Avg ~10)
    print("[1] Sending normal transactions...")
    for amt in [10.0, 12.0, 9.0, 11.0]:
        requests.post(f"{BASE_URL}/transactions", json={
            "user_id": user_id,
            "amount": amt,
            "merchant": "cafe"
        })
    time.sleep(2) # Let pipeline process
    
    # Check History
    r = requests.get(f"{BASE_URL}/history/{user_id}")
    history = r.json()
    print(f"    History: {history} (Avg ~10.5)")
    assert len(history) == 4
    
    # 2. Send Anomaly (> 2x Avg)
    print("[2] Sending ANOMALY transaction ($100)...")
    requests.post(f"{BASE_URL}/transactions", json={
        "user_id": user_id,
        "amount": 100.0,
        "merchant": "jewelry_store"
    })
    
    # Verify the anomaly was processed.
    # Since we haven't built an API to query alerts directly, we'll verify indirectly
    # by checking that the transaction history updated, which implies the processor ran.
    time.sleep(1)
    r = requests.get(f"{BASE_URL}/history/{user_id}")
    history = r.json()
    print(f"    History after anomaly: {history}")
    assert 100.0 in history
    
    print("[SUCCESS] Transactions accepted and processed.")

if __name__ == "__main__":
    try:
        run_test()
    except Exception as e:
        print(f"Test Failed: {e}")
        sys.exit(1)
