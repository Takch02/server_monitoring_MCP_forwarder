# main.py
import threading
import time
import os

# 기존 파일들을 모듈로 불러옵니다
import log_forwarder as forwarder 
import metric_forwarder as metric
import health_forwarder as health

def run_log_forwarder():
    print("🚀 [Main] Starting Log Forwarder...")
    try:
        forwarder.main()
    except Exception as e:
        print(f"❌ [Main] Log Forwarder crashed: {e}")

def run_metric_collector():
    print("🚀 [Main] Starting Metric Collector...")
    try:
        metric.main()
    except Exception as e:
        print(f"❌ [Main] Metric Collector crashed: {e}")

def run_health_collector():
    print("🚀 [Main] Starting Health Collector...")
    try:
        health.main()
    except Exception as e:
        print(f"❌ [Main] Health Collector crashed: {e}")

if __name__ == "__main__":
    # 스레드 1: 로그 수집 (무한루프)
    t1 = threading.Thread(target=run_log_forwarder, daemon=True)
    t1.start()

    # 스레드 2: 메트릭 수집 (무한루프)
    t2 = threading.Thread(target=run_metric_collector, daemon=True)
    t2.start()

    # 스레드 3: 헬스 체크 (무한루프)
    t3 = threading.Thread(target=run_health_collector, daemon=True)
    t3.start()

    # 메인 프로세스가 죽지 않게 대기
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 [Main] Stopping...")