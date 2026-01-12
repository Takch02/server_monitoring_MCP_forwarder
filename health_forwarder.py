import os, time, json, requests, hashlib

SERVER_NAME = os.getenv("SERVER_NAME", "unknown")
HEALTH_URL = os.getenv("HEALTH_URL")
MCP_HEALTH_INGEST_URL = os.getenv("MCP_HEALTH_INGEST_URL")
MCP_TOKEN = os.getenv("MCP_TOKEN")
HEALTH_INTERVAL = int(os.getenv("HEALTH_INTERVAL", "10"))

HEADERS = {
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN
}

def check_health():
    if not HEALTH_URL:
        return None

    ts = int(time.time() * 1000)
    start = time.time()
    try:
        resp = requests.get(HEALTH_URL, timeout=2, allow_redirects=False)
        latency_ms = int((time.time() - start) * 1000)

        status = "UP" if resp.status_code == 200 else "DOWN"
        msg = ""

        # actuator health면 {"status":"UP", ...} 형태가 많음
        try:
            data = resp.json()
            if isinstance(data, dict) and "status" in data:
                status = data.get("status", status)
        except:
            pass

        return {
            "ts": ts,
            "status": status,
            "latencyMs": latency_ms,
            "httpStatus": resp.status_code,
            "message": msg
        }
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        return {
            "ts": ts,
            "status": "DOWN",
            "latencyMs": latency_ms,
            "httpStatus": 0,
            "message": str(e)[:300]
        }

def send_health(payload):
    if not payload:
        return

    # 필요하면 eventId 추가
    payload["eventId"] = hashlib.sha1(
        f"{SERVER_NAME}|{payload['ts']}|HEALTH".encode()
    ).hexdigest()

    try:
        requests.post(MCP_HEALTH_INGEST_URL, headers=HEADERS, json=payload, timeout=3)
    except Exception:
        pass

def main():
    if not HEALTH_URL:
        print("[Health] HEALTH_URL is not set. disabled.")
        return

    while True:
        payload = check_health()
        if payload:
            send_health(payload)
        time.sleep(HEALTH_INTERVAL)

if __name__ == "__main__":
    main()
