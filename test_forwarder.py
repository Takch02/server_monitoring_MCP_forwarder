import os
import sys
import time
import json
import threading
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler

# --- 설정 ---
FORWARDER_SCRIPT = "forwarder.py"
TEST_LOG_FILE = "test_real_error.log"
MOCK_PORT = 9999
MOCK_URL = f"http://localhost:{MOCK_PORT}/ingest"

received_batches = []

class MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        data = self.rfile.read(length)
        try:
            batch = json.loads(data.decode('utf-8'))
            received_batches.extend(batch)
            self.send_response(200)
            self.end_headers()
        except:
            self.send_error(400)
    
    def log_message(self, format, *args): pass

def run_test():
    # 1. 파일 초기화
    if os.path.exists(TEST_LOG_FILE): os.remove(TEST_LOG_FILE)
    open(TEST_LOG_FILE, "w").close()

    # 2. 서버 실행
    server = HTTPServer(('localhost', MOCK_PORT), MockHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    # 3. 포워더 실행
    env = os.environ.copy()
    env.update({
        "MCP_INGEST_URL": MOCK_URL, "MCP_TOKEN": "test", "LOG_PATH": TEST_LOG_FILE,
        "START_AT_END": "false", "FLUSH_INTERVAL_MS": "500", "BATCH_MAX_LINES": "1"
    })
    proc = subprocess.Popen([sys.executable, FORWARDER_SCRIPT], env=env)
    
    try:
        time.sleep(2) # 실행 대기
        
        # 4. ★ 실제 로그 주입 (복사한 내용)
        print("[Test] Writing Real Spring Boot Log...")
        with open(TEST_LOG_FILE, "a", encoding="utf-8") as f:
            # Header
            f.write("2026-01-03T21:46:06.098+09:00 ERROR 43010 --- [Highlight_Backend] [nio-8085-exec-1] c.h.h.controller.DemoController          : error-demo triggered\n")
            # Body (들여쓰기 없음)
            f.write("java.lang.RuntimeException: demo exception\n")
            # Stack Trace (들여쓰기 있음)
            f.write("\tat com.highlight.highlight_backend.controller.DemoController.errorDemo(DemoController.java:20) ~[main/:na]\n")
            f.write("\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]\n")
            
            # 다음 로그 (새 이벤트 시작 확인용)
            f.write("2026-01-03T21:46:07.000 INFO Another Log started\n")
            f.flush()

        time.sleep(2) # 전송 대기

        # 5. 검증
        if not received_batches:
            print("❌ FAILED: No logs received.")
            return

        print(f"✅ Received {len(received_batches)} events.")
        
        evt = received_batches[0]
        msg = evt["message"]
        
        print("\n--- Parsed Message Content ---")
        print(msg)
        print("------------------------------")

        # 검증 포인트 1: 스택 트레이스가 합쳐졌는가?
        if "java.lang.RuntimeException" in msg and "NativeMethodAccessorImpl" in msg:
            print("✅ SUCCESS: Stack trace merged correctly.")
        else:
            print("❌ FAILED: Stack trace was split.")

        # 검증 포인트 2: 로그 레벨 인식
        if evt["level"] == "ERROR":
            print("✅ SUCCESS: Level detected as ERROR.")
        else:
            print(f"❌ FAILED: Level detected as {evt['level']}")

    finally:
        proc.terminate()
        if os.path.exists(TEST_LOG_FILE): os.remove(TEST_LOG_FILE)

if __name__ == "__main__":
    run_test()