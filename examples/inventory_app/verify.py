import asyncio
import time
import requests
import sys

BASE_URL = "http://localhost:8000"

def test_inventory_flow():
    print("Starting E2E Verification...")
    
    # Use a unique ID per run to avoid stale state from previous runs in Valkey
    import uuid
    run_id = uuid.uuid4().hex[:4]
    item_id = f"item-{run_id}"
    
    print(f"\n[1] Creating item {item_id}...")
    r = requests.post(f"{BASE_URL}/items/{item_id}", json={"name": f"Widget {run_id}", "initial_qty": 0})
    print(f"Response: {r.status_code} {r.json()}")
    assert r.status_code == 200
    
    # Poll for creation (async pipeline)
    print("Waiting for item creation...")
    state = {}
    for i in range(10):
        r = requests.get(f"{BASE_URL}/items/{item_id}")
        state = r.json()
        if state.get('exists'):
            break
        time.sleep(0.5)
        
    print(f"State: {state}")
    assert state.get('name') == f"Widget {run_id}"
    assert state.get('qty') == 0

    # 2. Check In 10
    print(f"\n[2] Checking in 10 units...")
    r = requests.post(f"{BASE_URL}/items/{item_id}/checkin", json={"qty": 10, "user_id": "u1"})
    assert r.status_code == 200
    
    time.sleep(1)
    r = requests.get(f"{BASE_URL}/items/{item_id}")
    print(f"State: {r.json()['qty']}")
    assert r.json()['qty'] == 10

    # 3. Check Out 5 (Success)
    print(f"\n[3] Checking out 5 units...")
    r = requests.post(f"{BASE_URL}/items/{item_id}/checkout", json={"qty": 5, "user_id": "u2"})
    assert r.status_code == 200
    
    time.sleep(1)
    r = requests.get(f"{BASE_URL}/items/{item_id}")
    print(f"State: {r.json()['qty']}")
    assert r.json()['qty'] == 5

    # 4. Check Out 7 (Should be Rejected eventually)
    print(f"\n[4] Checking out 7 units (Overdraft)...")
    r = requests.post(f"{BASE_URL}/items/{item_id}/checkout", json={"qty": 7, "user_id": "u3"})
    print(f"Response: {r.status_code} {r.json()} (Optimistic Success)")
    assert r.status_code == 200
    
    # Ideally, immediately after, specific state depends on if the pipeline caught up.
    # But eventually (after compensation), it should be 5.
    # If we read extremely fast, we might see -2?
    # Let's poll for stability.
    
    print("Waiting for compensation...")
    for i in range(10):
        r = requests.get(f"{BASE_URL}/items/{item_id}")
        qty = r.json()['qty']
        print(f"Poll {i}: Qty={qty}")
        if qty == 5:
            print("Stock restored to 5 (Compensation successful)!")
            break
        time.sleep(0.5)
        
    final_qty = requests.get(f"{BASE_URL}/items/{item_id}").json()['qty']
    if final_qty != 5:
        print(f"ERROR: Final qty is {final_qty}, expected 5.")
        sys.exit(1)
        
    print("\n[SUCCESS] All checks passed.")

if __name__ == "__main__":
    # Ensure server is up
    try:
        requests.get(f"{BASE_URL}/docs", timeout=1)
    except Exception:
        print("Server not running on localhost:8000")
        sys.exit(1)
        
    test_inventory_flow()
