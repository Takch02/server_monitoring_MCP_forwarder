import os
import time
import json
import requests

def env(key, default=None):
    v = os.getenv(key)
    return default if v is None or str(v).strip() == "" else v.strip()

def env_int(key, default):
    try:
        return int(env(key, str(default)))
    except Exception:
        return default

def env_bool(key, default=True):
    v = env(key, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y")

MCP_INGEST_URL = env("MCP_INGEST_URL")
MCP_TOKEN = env("MCP_TOKEN")
LOG_PATH = env("LOG_PATH")

START_AT_END = env_bool("START_AT_END", True)
BATCH_MAX_LINES = env_int("BATCH_MAX_LINES", 100)
FLUSH_INTERVAL_MS = env_int("FLUSH_INTERVAL_MS", 1000)
MAX_LINE_BYTES = env_int("MAX_LINE_BYTES", 4096)

HTTP_TIMEOUT_MS = env_int("HTTP_TIMEOUT_MS", 5000)
BACKOFF_INITIAL_MS = env_int("BACKOFF_INITIAL_MS", 500)
BACKOFF_MAX_MS = env_int("BACKOFF_MAX_MS", 10000)

if not MCP_INGEST_URL or not MCP_TOKEN or not LOG_PATH:
    raise SystemExit("MCP_INGEST_URL, MCP_TOKEN, LOG_PATH are required.")

HEADERS = {
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN,
}

def guess_level(line: str) -> str:
    u = line.upper()
    if " ERROR " in f" {u} " or u.startswith("ERROR") or "EXCEPTION" in u:
        return "ERROR"
    if " WARN " in f" {u} " or u.startswith("WARN") or "WARNING" in u:
        return "WARN"
    if " INFO " in f" {u} " or u.startswith("INFO"):
        return "INFO"
    return "INFO"

def is_continuation(line: str) -> bool:
    # 스택트레이스/멀티라인 이어붙이기(가벼운 규칙)
    if line == "":
        return True
    if line.startswith("at ") or line.startswith("\tat "):
        return True
    if line.startswith("Caused by:"):
        return True
    if line[0] in (" ", "\t"):
        return True
    return False

def open_file_wait(path: str):
    while True:
        try:
            f = open(path, "r", encoding="utf-8", errors="replace")
            if START_AT_END:
                f.seek(0, os.SEEK_END)
            return f
        except Exception as e:
            print(f"[forwarder] log file not ready ({path}): {e}; retrying...")
            time.sleep(0.5)

def file_signature(path: str):
    """
    로테이션/교체 감지용 서명.
    Unix에서는 inode가 가장 확실하지만, 파이썬에선 stat으로 충분.
    """
    try:
        st = os.stat(path)
        return (st.st_ino, st.st_size)
    except Exception:
        return (None, None)

def send_with_retry(batch):
    body = json.dumps(batch, ensure_ascii=False)
    backoff = BACKOFF_INITIAL_MS / 1000.0
    backoff_max = BACKOFF_MAX_MS / 1000.0

    while True:
        try:
            resp = requests.post(
                MCP_INGEST_URL,
                headers=HEADERS,
                data=body.encode("utf-8"),
                timeout=HTTP_TIMEOUT_MS / 1000.0
            )
            if 200 <= resp.status_code < 300:
                return
            print(f"[forwarder] ingest failed: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"[forwarder] ingest error: {e}")

        time.sleep(backoff)
        backoff = min(backoff * 2, backoff_max)

def main():
    f = open_file_wait(LOG_PATH)
    inode, size = file_signature(LOG_PATH)
    current_offset = f.tell()

    pending = None  # 현재 멀티라인 이벤트
    batch = []
    last_flush = time.time()

    def flush():
        nonlocal pending, batch, last_flush
        if pending is not None:
            batch.append(pending)
            pending = None

        if not batch:
            last_flush = time.time()
            return

        send_with_retry(batch)
        print(f"[forwarder] sent {len(batch)} logs")
        batch.clear()
        last_flush = time.time()

    while True:
        line = f.readline()

        if not line:
            # EOF 상태: 잠깐 쉬고 로테이션/트렁케이트 확인
            time.sleep(0.2)

            new_inode, new_size = file_signature(LOG_PATH)
            # truncate: 파일 사이즈가 현재 오프셋보다 작아짐
            if new_size is not None and new_size < current_offset:
                print("[forwarder] log truncated -> reopen from start")
                f.close()
                f = open_file_wait(LOG_PATH)
                inode, size = file_signature(LOG_PATH)
                current_offset = f.tell()

            # rotation/replace: inode 변경
            elif inode is not None and new_inode is not None and new_inode != inode:
                print("[forwarder] log rotated/replaced -> reopen from start")
                f.close()
                # rotate면 보통 새 파일 처음부터 읽어도 됨
                f = open(LOG_PATH, "r", encoding="utf-8", errors="replace")
                inode, size = file_signature(LOG_PATH)
                current_offset = f.tell()

            # 시간 기준 flush
            if batch and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
                flush()
            continue

        current_offset += len(line)
        line = line.rstrip("\r\n")

        # 길이 제한
        if len(line) > MAX_LINE_BYTES:
            line = line[:MAX_LINE_BYTES] + "...(truncated)"

        # 멀티라인 합치기
        if pending is not None and is_continuation(line):
            pending["message"] += "\n" + line
        else:
            # 새 이벤트 시작
            if pending is not None:
                batch.append(pending)

            pending = {
                "ts": int(time.time() * 1000),
                "level": guess_level(line),
                "message": line
            }

        # 개수 기준 flush
        if len(batch) >= BATCH_MAX_LINES:
            flush()

        # 시간 기준 flush
        if batch and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
            flush()

if __name__ == "__main__":
    main()
