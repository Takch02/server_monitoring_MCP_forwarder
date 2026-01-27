import os
import time
import json
import re
import hashlib
import queue
import threading
import random
from datetime import datetime
import requests

def env(key, default=None):
    v = os.getenv(key)
    return default if v is None or str(v).strip() == "" else v.strip()

def env_int(key, default):
    try: return int(env(key, str(default)))
    except: return default

def env_bool(key, default=True):
    v = env(key, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y")

MCP_LOG_INGEST_URL = env("MCP_LOG_INGEST_URL")
MCP_TOKEN = env("MCP_TOKEN")
DISCORD_WEBHOOK_URL = env("DISCORD_WEBHOOK_URL", "")
LOG_PATH = env("LOG_PATH")
SERVER_NAME = env("SERVER_NAME", "unknown")

START_AT_END = env_bool("START_AT_END", True)
BATCH_MAX_LINES = env_int("BATCH_MAX_LINES", 100)
FLUSH_INTERVAL_MS = env_int("FLUSH_INTERVAL_MS", 1000)
MAX_LINE_BYTES = env_int("MAX_LINE_BYTES", 4096)
MAX_EVENT_BYTES = env_int("MAX_EVENT_BYTES", 32 * 1024)

HTTP_TIMEOUT_MS = env_int("HTTP_TIMEOUT_MS", 5000)
BACKOFF_INITIAL_MS = env_int("BACKOFF_INITIAL_MS", 500)
BACKOFF_MAX_MS = env_int("BACKOFF_MAX_MS", 10000)
LOG_QUEUE_MAX_BATCHES = env_int("LOG_QUEUE_MAX_BATCHES", 300)  # 메모리 허용치
DROP_AFTER_S = 60.0  # 1분 지나면 드랍


send_q = queue.Queue(maxsize=LOG_QUEUE_MAX_BATCHES)  # batch 전송에 실패 시 Queue에 넣음 (메모리에 임시 저장)
stop_event = threading.Event()


if not MCP_LOG_INGEST_URL or not MCP_TOKEN or not LOG_PATH:
    raise SystemExit("MCP_LOG_INGEST_URL, MCP_TOKEN, LOG_PATH are required.")

HEADERS = { 
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN,  # 서버 인식 헤더
    "X-DISCORD-WEBHOOK-URL": DISCORD_WEBHOOK_URL  # 디스코드 웹훅 URL 헤더 (선택사항)
}

# 민감 정보 마스킹
REDACT_PATTERNS = [
    (re.compile(r"(Authorization:\s*Bearer\s+)[A-Za-z0-9\-\._~\+\/]+=*", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r"(Bearer\s+)[A-Za-z0-9\-\._~\+\/]+=*", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r"(\b(token|access_token|refresh_token|secret|password)\s*=\s*)[^\s&]+", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r'("?(token|access_token|refresh_token|secret|password)"?\s*:\s*)"?[^"\s,}]+', re.IGNORECASE), r'\1"[REDACTED]"'),
]

# ★ 핵심 변경: 로그의 시작점(Timestamp)을 감지하는 정규식
# Spring Log Format: 2026-01-03T21:46:06.098+09:00 ...
# 날짜로 시작하면 "새로운 로그"로 판단합니다.
LOG_START_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

# 레벨 파싱용 (이벤트 생성 시 사용)
LEVEL_RE = re.compile(r"\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+")

def enqueue_drop_oldest(batch):
    while True:
        try:
            send_q.put_nowait(batch)
            return True
        except queue.Full:
            try:
                send_q.get_nowait()   # 가장 오래된 배치 버림
                send_q.task_done()
                print("[forwarder] queue full -> drop oldest batch")
            except queue.Empty:
                continue

def sender_loop():
    while not stop_event.is_set():
        try:
            batch = send_q.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            r = send_with_retry(batch)   # 실패하면 내부에서 계속 재시도
            
            if r is True:
                print(f"[forwarder] {len(batch)} batch sent successfully")

            elif r is False:
                # ✅ 60초 동안 실패 -> 버리지 말고 뒤로 다시 넣기
                enqueue_drop_oldest(batch)
                time.sleep(0.2)

            else:
                # None: 4xx 같은 영구 실패 -> 버림
                pass
        finally:
            send_q.task_done()

def start_sender_thread():
    t = threading.Thread(target=sender_loop, daemon=True)
    t.start()


def redact(s: str) -> str:
    out = s
    for pat, rep in REDACT_PATTERNS:
        out = pat.sub(rep, out)
    return out

# ISO 8601 형식의 날짜 문자열을 밀리초 단위의 epoch 시간으로 변환
def parse_iso_to_epoch_ms(iso_str: str) -> int:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except:
        return int(time.time() * 1000)

def extract_metadata(line: str):
    """
    로그 첫 줄에서 timestamp, level을 추출
    """
    ts = int(time.time() * 1000)
    lvl = "INFO"
    
    # Timestamp 추출 (ISO 포맷 가정)
    # 공백으로 끊어서 첫번째 토큰이 날짜라고 가정
    parts = line.split(' ', 1)
    if len(parts) > 0 and LOG_START_RE.match(parts[0]):
        ts = parse_iso_to_epoch_ms(parts[0])
    
    # Level 추출
    m_lvl = LEVEL_RE.search(line)
    if m_lvl:
        lvl = m_lvl.group(1)
        
    return ts, lvl

def open_file_wait(path: str):
    while True:
        try:
            f = open(path, "r", encoding="utf-8", errors="replace")
            if START_AT_END:
                f.seek(0, os.SEEK_END)
            return f
        except Exception as e:
            print(f"[forwarder] log file not ready ({path}); retrying...")
            time.sleep(1)

def file_signature(path: str):
    try:
        st = os.stat(path)
        return (st.st_ino, st.st_size)
    except:
        return (None, None)

def make_event_id(server_name, ts, msg):
    head = msg[:512]
    base = f"{server_name}|{ts}|{head}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

# 배치 전송 함수
# 재전송 로직을 추가
# 1분간 재시도 후 드랍
def send_with_retry(batch):
    body = json.dumps(batch, ensure_ascii=False).encode("utf-8")
    backoff = BACKOFF_INITIAL_MS / 1000.0
    backoff_max = BACKOFF_MAX_MS / 1000.0
    timeout_s = HTTP_TIMEOUT_MS / 1000.0
    deadline = time.time() + DROP_AFTER_S
    
    while True:
        
        now = time.time()
        if now >= deadline:
            print(f"[forwarder] drop batch after {DROP_AFTER_S:.0f}s retry (size={len(batch)})")
            return False
        
        try:
            resp = requests.post(
                MCP_LOG_INGEST_URL,
                headers=HEADERS,
                data=body,
                timeout=timeout_s
            )
            
            if 200 <= resp.status_code < 300:
                return True

            # 4xx(429 제외)는 재시도 의미 없음 -> 드랍
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                print(f"[forwarder] rejected {resp.status_code}: {resp.text[:200]}")
                return None

            print(f"[forwarder] ingest failed: {resp.status_code}")

        except Exception as e:
            print(f"[forwarder] ingest error: {e}")

        remaining = deadline - time.time()
        if remaining <= 0:
            print(f"[forwarder] drop batch after {DROP_AFTER_S:.0f}s retry (size={len(batch)})")
            return False
        
        # 남은 시간 안에서만 sleep
        sleep_s = min(backoff, max(0.0, deadline - time.time()))
        time.sleep(sleep_s * random.uniform(0.7, 1.3))  # 지터
        backoff = min(backoff * 2, backoff_max)


def main():
    start_sender_thread()  # 전송 로직은 별도 스레드에서 처리
    f = open_file_wait(LOG_PATH)
    inode, _ = file_signature(LOG_PATH)
    current_offset = f.tell()

    pending = None # 현재 조립 중인 이벤트 {ts, level, message, ...}
    batch = []
    last_flush = time.time()

    def finalize_pending():
        nonlocal pending
        if pending is None: return None
        
        msg = pending["message"]
        if len(msg.encode("utf-8")) > MAX_EVENT_BYTES:
            msg = msg[:MAX_EVENT_BYTES] + "\n...(truncated)"
            
        ev = {
            "serverName": SERVER_NAME,
            "ts": pending["ts"],
            "level": pending["level"],
            "message": msg,
            "eventId": make_event_id(SERVER_NAME, pending["ts"], msg)
        }
        pending = None
        return ev

    # flush 는 재전송이 아닌 batch 를 큐에 넣기만 함
    def flush_batch():
        nonlocal last_flush
        if not batch: return
        enqueue_drop_oldest(batch.copy())
        print(f"[forwarder] queued {len(batch)} events")
        batch.clear()
        last_flush = time.time()

    while True:
        line = f.readline()
        if not line:
            # 파일 로테이션 체크 로직 (생략 없이 유지)
            time.sleep(0.1)
            new_inode, new_size = file_signature(LOG_PATH)
            if (new_size is not None and new_size < current_offset) or (inode and new_inode != inode):
                print("[forwarder] log rotated.")
                f.close()
                f = open_file_wait(LOG_PATH) # 로테이션 시에는 처음부터 읽기
                inode, _ = file_signature(LOG_PATH)
                current_offset = f.tell()
                # 로테이션 시 펜딩 중인 것 강제 전송
                ev = finalize_pending()
                if ev: batch.append(ev)
            
            if batch and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
                flush_batch()
            continue

        current_offset += len(line)
        line = line.rstrip("\r\n")
        line = redact(line)

        # ★ 로직 변경: "날짜로 시작하면 새 이벤트, 아니면 이어 붙이기"
        is_new_start = LOG_START_RE.match(line)

        if is_new_start:
            # 기존 펜딩 마감
            ev = finalize_pending()
            if ev: batch.append(ev)
            
            # 새 이벤트 시작
            ts, lvl = extract_metadata(line)
            pending = {
                "ts": ts,
                "level": lvl,
                "message": line
            }
        else:
            # 날짜로 시작하지 않음 -> 이전 로그의 연속(Stack Trace 등)
            if pending:
                pending["message"] += "\n" + line
            else:
                # 펜딩이 없는데 날짜도 없는 줄이 옴 (파일 중간부터 읽었거나 이상한 로그)
                # 그냥 무시하거나 임시로 만듦. 여기선 현재 시간으로 생성
                pending = {
                    "ts": int(time.time() * 1000),
                    "level": "INFO",
                    "message": line
                }

        if len(batch) >= BATCH_MAX_LINES:
            flush_batch()

if __name__ == "__main__":
    main()