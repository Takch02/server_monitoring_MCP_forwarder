import os
import time
import json
import re
import hashlib
import queue
import threading
from datetime import datetime
from delivery import post_json
from forwarder_status import STATUS, run_worker

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
                dropped = send_q.get_nowait()
                STATUS.event('logs', 'dropped', len(dropped), 'queue_full')
                send_q.task_done()
                print("[forwarder] queue full -> drop oldest batch")
            except queue.Empty:
                continue

def sender_loop():
    while not stop_event.is_set():
        STATUS.event('log_sender', 'poll')
        try:
            batch = send_q.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            r = send_with_retry(batch)   # 실패하면 내부에서 계속 재시도
            
            if r is True:
                print(f"[forwarder] {len(batch)} batch sent successfully")

            elif r is False:
                # 로그의 최신성을 위해 timeout이 지나면 로그를 버림
                print("[forwarder] batch dropped after retries")

            else:
                # None: 4xx 같은 영구 실패 -> 버림
                pass
        finally:
            send_q.task_done()

def start_sender_thread():
    t = threading.Thread(target=run_worker, args=('log_sender', sender_loop, stop_event), daemon=True)
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
        STATUS.event('logs', 'poll')
        try:
            f = open(path, "r", encoding="utf-8", errors="replace")
            if START_AT_END:
                f.seek(0, os.SEEK_END)
            return f
        except Exception as e:
            STATUS.event('logs', 'collection_failed', reason='log_file_unavailable')
            print(f"[forwarder] log file not ready ({path}); retrying...")
            time.sleep(1)

def file_signature(path: str):
    try:
        st = os.stat(path)
        return (st.st_ino, st.st_size)
    except:
        return (None, None)

# SHA1 해시로 이벤트 ID 생성(멱등성 보장)
def make_event_id(server_name, ts, msg):
    head = msg[:512]
    base = f"{server_name}|{ts}|{head}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

# 배치 전송 함수
# 재전송 로직을 추가
# 1분간 재시도 후 드랍
def send_with_retry(batch):
    result = post_json('logs', MCP_LOG_INGEST_URL, HEADERS, batch, count=len(batch),
                       timeout=HTTP_TIMEOUT_MS / 1000.0, max_attempts=None,
                       retry_seconds=DROP_AFTER_S, initial_backoff=BACKOFF_INITIAL_MS / 1000.0,
                       max_backoff=BACKOFF_MAX_MS / 1000.0)
    if result.success:
        return True
    return None if result.reason in ('authentication_failed', 'configuration_error') or (
        result.status_code is not None and result.status_code not in (408, 429)
        and result.status_code < 500) else False


def main():
    if not MCP_LOG_INGEST_URL or not MCP_TOKEN or not LOG_PATH:
        raise ValueError('로그 경로, 전송 주소와 토큰이 필요합니다.')
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
        STATUS.event('logs', 'collected', 1)
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
        STATUS.event('logs', 'poll')
        line = f.readline()
        STATUS.event('logs', 'collected', 0)
        if not line:
            # 파일 로테이션 체크 로직 (생략 없이 유지)
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
            
            # 마지막 flush 이후 1초가 지나면 pending의 로그도 batch로 보냄
            if (batch or pending) and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
                if pending:
                    ev = finalize_pending()
                    if ev: batch.append(ev)
                
                # batch에 데이터가 있다면 즉시 전송 큐로 넘김
                if batch:
                    flush_batch()
            time.sleep(0.05)
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