import os
import time
import json
import re
import hashlib
from datetime import datetime
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
SERVER_NAME = env("SERVER_NAME", "unknown")

START_AT_END = env_bool("START_AT_END", True)
BATCH_MAX_LINES = env_int("BATCH_MAX_LINES", 100)
FLUSH_INTERVAL_MS = env_int("FLUSH_INTERVAL_MS", 1000)
MAX_LINE_BYTES = env_int("MAX_LINE_BYTES", 4096)
MAX_EVENT_BYTES = env_int("MAX_EVENT_BYTES", 32 * 1024)  # 이벤트 전체 메시지 최대

HTTP_TIMEOUT_MS = env_int("HTTP_TIMEOUT_MS", 5000)
BACKOFF_INITIAL_MS = env_int("BACKOFF_INITIAL_MS", 500)
BACKOFF_MAX_MS = env_int("BACKOFF_MAX_MS", 10000)

if not MCP_INGEST_URL or not MCP_TOKEN or not LOG_PATH:
    raise SystemExit("MCP_INGEST_URL, MCP_TOKEN, LOG_PATH are required.")

HEADERS = {
    "Content-Type": "application/json",
    "X-MCP-TOKEN": MCP_TOKEN,
}

# ---- 레드액션(간단 버전) ----
REDACT_PATTERNS = [
    # Authorization: Bearer xxxx
    (re.compile(r"(Authorization:\s*Bearer\s+)[A-Za-z0-9\-\._~\+\/]+=*", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r"(Bearer\s+)[A-Za-z0-9\-\._~\+\/]+=*", re.IGNORECASE), r"\1[REDACTED]"),
    # token=, secret=, password= 류
    (re.compile(r"(\b(token|access_token|refresh_token|secret|password)\s*=\s*)[^\s&]+", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r'("?(token|access_token|refresh_token|secret|password)"?\s*:\s*)"?[^"\s,}]+', re.IGNORECASE), r'\1"[REDACTED]"'),
]

def redact(s: str) -> str:
    out = s
    for pat, rep in REDACT_PATTERNS:
        out = pat.sub(rep, out)
    return out

# ---- Spring 로그 파싱 ----
# 케이스1) "2026-01-03T20:35:52.751+09:00 ERROR ..."
SPRING_PREFIX_RE = re.compile(
    r"^(?P<iso>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2}))\s+"
    r"(?P<lvl>TRACE|DEBUG|INFO|WARN|ERROR)\b"
)

# 케이스2) "1767440152890 ERROR 2026-01-03T20:35:52.751+09:00 ERROR ..."
DOUBLE_PREFIX_RE = re.compile(
    r"^(?P<rid>\d+)\s+(?P<prefixLvl>TRACE|DEBUG|INFO|WARN|ERROR)\s+"
    r"(?P<iso>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2}))\s+"
    r"(?P<lvl>TRACE|DEBUG|INFO|WARN|ERROR)\b"
)

def parse_iso_to_epoch_ms(iso: str) -> int:
    # datetime.fromisoformat은 "+09:00" 형식 지원
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)

def parse_level_and_ts(line: str):
    """
    line에서 (ts_ms, level, requestId?)를 최대한 정확히 뽑는다.
    """
    m2 = DOUBLE_PREFIX_RE.match(line)
    if m2:
        iso = m2.group("iso")
        lvl = m2.group("lvl")  # 실제 레벨은 iso 뒤의 토큰을 우선
        rid = m2.group("rid")
        return parse_iso_to_epoch_ms(iso), lvl, rid

    m1 = SPRING_PREFIX_RE.match(line)
    if m1:
        iso = m1.group("iso")
        lvl = m1.group("lvl")
        return parse_iso_to_epoch_ms(iso), lvl, None

    # fallback
    u = line.upper()
    if " ERROR " in f" {u} " or u.startswith("ERROR") or "EXCEPTION" in u:
        return int(time.time() * 1000), "ERROR", None
    if " WARN " in f" {u} " or u.startswith("WARN") or "WARNING" in u:
        return int(time.time() * 1000), "WARN", None
    if " INFO " in f" {u} " or u.startswith("INFO"):
        return int(time.time() * 1000), "INFO", None
    return int(time.time() * 1000), "INFO", None

def is_continuation(line: str) -> bool:
    # 빈 줄은 무조건 continuation 처리하지 않음(엉뚱한 이벤트 붙는 부작용 방지)
    if line == "":
        return False
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
    try:
        st = os.stat(path)
        return (st.st_ino, st.st_size)
    except Exception:
        return (None, None)

def make_event_id(server_name: str, ts_ms: int, message: str, rid: str | None):
    # 재시도/중복 대비용. (서버+시간+앞부분) 기반 해시
    head = message[:512]
    base = f"{server_name}|{ts_ms}|{rid or ''}|{head}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

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
    inode, _ = file_signature(LOG_PATH)
    current_offset = f.tell()

    pending = None
    batch = []
    last_flush = time.time()

    def finalize_pending():
        nonlocal pending
        if pending is None:
            return None

        # 이벤트 크기 제한
        msg = pending["message"]
        if len(msg.encode("utf-8")) > MAX_EVENT_BYTES:
            # 너무 길면 tail을 자르되, 앞부분(원인) 우선 보존
            cut = msg.encode("utf-8")[:MAX_EVENT_BYTES]
            pending["message"] = cut.decode("utf-8", errors="replace") + "\n...(event truncated)"
        # eventId 부여
        pending["eventId"] = make_event_id(
            pending["serverName"],
            pending["ts"],
            pending["message"],
            pending.get("requestId")
        )
        out = pending
        pending = None
        return out

    def flush():
        nonlocal batch, last_flush
        ev = finalize_pending()
        if ev is not None:
            batch.append(ev)

        if not batch:
            last_flush = time.time()
            return

        send_with_retry(batch)
        print(f"[forwarder] sent {len(batch)} events")
        batch.clear()
        last_flush = time.time()

    while True:
        line = f.readline()
        if not line:
            time.sleep(0.2)

            new_inode, new_size = file_signature(LOG_PATH)
            if new_size is not None and new_size < current_offset:
                print("[forwarder] log truncated -> reopen")
                f.close()
                f = open_file_wait(LOG_PATH)
                inode, _ = file_signature(LOG_PATH)
                current_offset = f.tell()

            elif inode is not None and new_inode is not None and new_inode != inode:
                print("[forwarder] log rotated/replaced -> reopen")
                f.close()
                f = open(LOG_PATH, "r", encoding="utf-8", errors="replace")
                inode, _ = file_signature(LOG_PATH)
                current_offset = f.tell()

            if batch and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
                flush()
            continue

        current_offset += len(line)
        line = line.rstrip("\r\n")

        if len(line) > MAX_LINE_BYTES:
            line = line[:MAX_LINE_BYTES] + "...(truncated)"

        line = redact(line)

        # 멀티라인 합치기
        if pending is not None and is_continuation(line):
            pending["message"] += "\n" + line
        else:
            # 새 이벤트 시작 전, 기존 pending 확정
            ev = finalize_pending()
            if ev is not None:
                batch.append(ev)

            ts_ms, lvl, rid = parse_level_and_ts(line)
            pending = {
                "serverName": SERVER_NAME,
                "ts": ts_ms,
                "level": lvl,
                "requestId": rid,  # 있으면 활용
                "message": line
            }

        if len(batch) >= BATCH_MAX_LINES:
            flush()

        if batch and (time.time() - last_flush) >= (FLUSH_INTERVAL_MS / 1000.0):
            flush()

if __name__ == "__main__":
    main()
