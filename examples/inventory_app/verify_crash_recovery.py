import subprocess
import time
import requests
import sys
import os
import signal

BASE_URL = "http://localhost:8001" # Different port to avoid conflicts
SERVER_CMD = [
    sys.executable, "-m", "uvicorn", "examples.inventory_app.api:app",
    "--host", "0.0.0.0", "--port", "8001"
]

def wait_for_server(retries=10):
    for i in range(retries):
        try:
            requests.get(f"{BASE_URL}/docs", timeout=1)
            return True
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    return False

def run_test():
    print("--- Starting Crash Recovery Test ---")
    
    # 1. Start Server (Instance A)
    print("[1] Starting Server Instance A...")
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    proc_a = subprocess.Popen(SERVER_CMD, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    if not wait_for_server():
        print("Failed to start Instance A")
        proc_a.kill()
        sys.exit(1)

    try:
        # 2. Modify State
        import uuid
        item_id = f"crash-test-{uuid.uuid4().hex[:8]}"
        print(f"[2] Creating item '{item_id}' with 100 units...")
        
        # Create
        requests.post(f"{BASE_URL}/items/{item_id}", json={"name": "Important Data", "initial_qty": 0})
        # Check-in 100
        requests.post(f"{BASE_URL}/items/{item_id}/checkin", json={"qty": 100, "user_id": "tester"})
        
        time.sleep(1) # Let pipeline process
        
        # Verify State in Instance A
        r = requests.get(f"{BASE_URL}/items/{item_id}")
        qty = r.json()['qty']
        print(f"    Instance A reports Qty: {qty}")
        assert qty == 100

        # 3. CRASH SERVER
        print("[3] KILLING SERVER (Simulating Crash)...")
        proc_a.send_signal(signal.SIGKILL) # Hard kill
        proc_a.wait()
        print("    Server killed.")

        # 4. Start Server (Instance B)
        print("[4] Starting Server Instance B (Recovery)...")
        proc_b = subprocess.Popen(SERVER_CMD, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        if not wait_for_server():
            print("Failed to start Instance B")
            proc_b.kill()
            sys.exit(1)

        try:
            # 5. Verify State survives
            print("[5] Verifying State in Instance B...")
            # Ideally, the server replays the log on startup. 
            # Since we have small data, it should be instant.
            
            # We might need to poll if startup replay is async background task?
            # In our implementation, executor starts in background.
            # So we poll.
            
            restored_qty = -1
            for i in range(10):
                try:
                    r = requests.get(f"{BASE_URL}/items/{item_id}")
                    if r.status_code == 200:
                        data = r.json()
                        # If 'exists' is false, it hasn't replayed creation yet.
                        if data.get('exists'):
                            restored_qty = data['qty']
                            if restored_qty == 100:
                                break
                except:
                    pass
                time.sleep(0.5)
                print(f"    Polling recovery... got {restored_qty}")

            print(f"    Instance B reports Qty: {restored_qty}")
            
            if restored_qty == 100:
                print("\n[SUCCESS] State fully recovered from persistent log!")
            else:
                print(f"\n[FAILURE] State mismatch. Expected 100, got {restored_qty}")
                sys.exit(1)

        finally:
            proc_b.kill()

    finally:
        if proc_a.poll() is None:
            proc_a.kill()

if __name__ == "__main__":
    run_test()
