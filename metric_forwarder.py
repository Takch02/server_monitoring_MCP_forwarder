# metric.py
import os
import time
import json
import requests
import hashlib

# --- 환경 설정 ---
SERVER_NAME = os.getenv("SERVER_NAME", "unknown")
MCP_METRIC_INGEST_URL = os.getenv("MCP_METRIC_INGEST_URL")
MCP_TOKEN = os.getenv("MCP_TOKEN")
USER_KAKAO_TOKEN = os.getenv("USER_KAKAO_TOKEN", "")
ACTUATOR_BASE_URL = os.getenv("ACTUATOR_URL") 
COLLECT_INTERVAL = int(os.getenv("METRIC_INTERVAL", "10")) # 10초마다 수집

HEADERS = {
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN,  # 서버 인식 헤더
    "X-USER-KAKAO-TOKEN": USER_KAKAO_TOKEN  # 카카오 메시지 토큰 헤더 (선택사항)
}

def get_actuator_value(metric_name):
    """Actuator에서 특정 메트릭 값 하나만 쏙 빼오는 함수"""
    try:
        url = f"{ACTUATOR_BASE_URL}/{metric_name}"
        resp = requests.get(url, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            # Actuator JSON 구조: {"measurements": [{"value": 123.4, ...}]}
            return data['measurements'][0]['value']
    except Exception as e:
        # 타겟 서버가 아직 안 켜졌거나 죽었을 때
        # print(f"[Metric] Fetch error {metric_name}: {e}") 
        return None
    return None

def send_metric(cpu, memory_used, memory_max):
    """MCP 서버로 메트릭 전송"""
    ts = int(time.time() * 1000)
    
    # 메모리 사용률 계산 (%)
    mem_percent = 0
    if memory_max and memory_max > 0:
        mem_percent = (memory_used / memory_max) * 100

    payload = {
        "serverName": SERVER_NAME,
        "ts": ts,
        "type": "METRIC", # 로그랑 구분하기 위한 타입 태그
        "data": {
            "cpuUsage": cpu,          # 0.0 ~ 1.0 (또는 퍼센트)
            "memoryUsed": memory_used, # Byte 단위
            "memoryMax": memory_max,   # Byte 단위
            "memoryPercent": mem_percent
        },
        # 이벤트 ID는 중복 제거용이 아니라 추적용
        "eventId": hashlib.sha1(f"{SERVER_NAME}|{ts}|METRIC".encode()).hexdigest()
    }

    try:
        # 로그 보내던 곳과 같은 주소로 보내거나, 메트릭 전용 엔드포인트 사용
        # 여기서는 같은 URL을 쓴다고 가정 (MCP 서버가 type으로 구분해야 함)
        requests.post(MCP_METRIC_INGEST_URL, headers=HEADERS, json=payload, timeout=3)
        print("Metric 전송 완료")
        # print(f"[Metric] Sent: CPU={cpu}, Mem={mem_percent:.1f}%")
    except Exception as e:
        print(f"[Metric] 전송 중 오류 발생 : {e}")

def main():
    if not ACTUATOR_BASE_URL:
        print("[Metric] ACTUATOR_URL is not set. Metric collection disabled.")
        return

    print(f"[Metric] Starting collector for {ACTUATOR_BASE_URL} (Interval: {COLLECT_INTERVAL}s)")
    
    while True:
        # 1. Actuator 조회
        # system.cpu.usage: 전체 시스템 CPU 사용률
        # jvm.memory.used: 자바 힙 메모리 사용량
        cpu = get_actuator_value("system.cpu.usage")
        mem_used = get_actuator_value("jvm.memory.used")
        mem_max = get_actuator_value("jvm.memory.max")

        # 2. 전송 (값이 있을 때만)
        if cpu is not None and mem_used is not None:
            send_metric(cpu, mem_used, mem_max)
        
        time.sleep(COLLECT_INTERVAL)

if __name__ == "__main__":
    main()