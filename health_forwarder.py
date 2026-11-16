import os, time, json, requests, hashlib

from delivery import post_json
from forwarder_status import STATUS

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
            "message": type(e).__name__
        }

def send_health(payload):
    if not payload:
        return

    payload = dict(payload)
    payload['eventId'] = hashlib.sha1(f"{SERVER_NAME}|{payload['ts']}|HEALTH".encode()).hexdigest()
    result = post_json('health', MCP_HEALTH_INGEST_URL, HEADERS, payload)
    print('[Health] 전송 완료' if result.success else '[Health] 전송 실패: ' + result.reason)
    return result.success


def collect_once():
    STATUS.event('health', 'poll')
    payload = check_health()
    if payload is None:
        STATUS.event('health', 'collection_failed', reason='health_not_configured')
        return False
    # 대상의 DOWN 응답도 수집에 성공한 관측 결과다.
    STATUS.event('health', 'collected', 1)
    return send_health(payload)


def main():
    if not HEALTH_URL:
        print("[Health] HEALTH_URL is not set. disabled.")
        return

    if not MCP_HEALTH_INGEST_URL or not MCP_TOKEN:
        raise ValueError('Health 전송 주소와 토큰이 필요합니다.')
    while True:
        collect_once()
        time.sleep(HEALTH_INTERVAL)

if __name__ == "__main__":
    main()
