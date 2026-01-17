import requests
import time
import sys

BASE_URL = "http://localhost:8001"

def run_test():
    print("--- Starting Courier Verification ---")
    
    # 1. Create a Job
    print("[1] Creating Job J1...")
    r = requests.post(f"{BASE_URL}/jobs", json={
        "customer_id": "cust-1",
        "pickup": "123 Main",
        "dropoff": "456 Elm"
    })
    job_id = r.json()["job_id"]
    print(f"    Job Created: {job_id}")

    # 2. Verify no assignment yet
    r = requests.get(f"{BASE_URL}/assignments")
    assignments = r.json()
    print(f"    Assignments (Should be empty): {assignments}")
    assert job_id not in assignments

    # 3. Bring Driver Online
    print("[2] Driver D1 coming online...")
    requests.post(f"{BASE_URL}/drivers/online", json={
        "driver_id": "driver-1",
        "vehicle": "bike"
    })
    
    # 4. Wait for match
    print("[3] Waiting for assignment...")
    matched = False
    for _ in range(10):
        time.sleep(1)
        r = requests.get(f"{BASE_URL}/assignments")
        assignments = r.json()
        if job_id in assignments and assignments[job_id] == "driver-1":
            matched = True
            break
    
    if matched:
        print(f"    [SUCCESS] Job {job_id} assigned to driver-1!")
    else:
        print(f"    [FAILURE] Assignment not found. State: {assignments}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        run_test()
    except Exception as e:
        print(f"Test Failed: {e}")
        sys.exit(1)
