# metric.py
import os
import time
import json
import requests
import hashlib
import math
from delivery import post_json
from forwarder_status import STATUS

# --- 환경 설정 ---
SERVER_NAME = os.getenv("SERVER_NAME", "unknown")
MCP_METRIC_INGEST_URL = os.getenv("MCP_METRIC_INGEST_URL")
MCP_TOKEN = os.getenv("MCP_TOKEN")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
ACTUATOR_BASE_URL = os.getenv("ACTUATOR_URL") 
COLLECT_INTERVAL = int(os.getenv("METRIC_INTERVAL", "10")) # 10초마다 수집

HEADERS = {
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN,  # 서버 인식 헤더
    "X-DISCORD-WEBHOOK-URL": DISCORD_WEBHOOK_URL  # 디스코드 웹훅 URL 헤더 (선택사항)
}

def get_actuator_value(metric_name):
    """Actuator에서 특정 메트릭 값 하나만 쏙 빼오는 함수"""
    try:
        url = f"{ACTUATOR_BASE_URL}/{metric_name}"
        params = {'tag': 'area:heap'} if metric_name.startswith('jvm.memory.') else None
        resp = requests.get(url, params=params, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            # Actuator JSON 구조: {"measurements": [{"value": 123.4, ...}]}
            return next(item['value'] for item in data['measurements'] if item['statistic'] == 'VALUE')
    except Exception as e:
        print(f"[Metric] 지표 조회 실패: {metric_name} ({type(e).__name__})")
        return None
    return None

def valid_metric(cpu, memory_used, memory_max):
    values = (cpu, memory_used, memory_max)
    return (all(isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) for value in values)
            and 0 <= cpu <= 1 and memory_max > 0 and 0 <= memory_used <= memory_max)


def send_metric(cpu, memory_used, memory_max):
    """MCP 서버로 메트릭 전송"""
    if not valid_metric(cpu, memory_used, memory_max):
        STATUS.event('metrics', 'collection_failed', reason='invalid_metric')
        return False
    ts = int(time.time() * 1000)
    
    # 메모리 사용률 계산 (%)
    mem_percent = (memory_used / memory_max) * 100

    payload = {
        "serverName": SERVER_NAME,
        "ts": ts,
        "type": "METRIC", # 로그랑 구분하기 위한 타입 태그
        "data": {
            "cpuUsage": cpu,          # 0.0 ~ 1.0
            "memoryUsed": memory_used, # Byte 단위
            "memoryMax": memory_max,   # Byte 단위
            "memoryPercent": mem_percent
        },
        # 이벤트 ID는 중복 제거용이 아니라 추적용
        "eventId": hashlib.sha1(f"{SERVER_NAME}|{ts}|METRIC".encode()).hexdigest()
    }

    result = post_json('metrics', MCP_METRIC_INGEST_URL, HEADERS, payload)
    print('[Metric] 전송 완료' if result.success else '[Metric] 전송 실패: ' + result.reason)
    return result.success


def collect_once():
    STATUS.event('metrics', 'poll')
    values = [get_actuator_value(name) for name in
              ('system.cpu.usage', 'jvm.memory.used', 'jvm.memory.max')]
    if not valid_metric(*values):
        STATUS.event('metrics', 'collection_failed', reason='missing_or_invalid_metric')
        return False
    STATUS.event('metrics', 'collected', 1)
    return send_metric(*values)


def main():
    if not ACTUATOR_BASE_URL:
        print("[Metric] ACTUATOR_URL is not set. Metric collection disabled.")
        return

    print(f"[Metric] Starting collector (Interval: {COLLECT_INTERVAL}s)")
    
    if not MCP_METRIC_INGEST_URL or not MCP_TOKEN:
        raise ValueError('지표 전송 주소와 토큰이 필요합니다.')
    while True:
        collect_once()
        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    main()