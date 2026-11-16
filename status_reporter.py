"""현재 상태만 독립적으로 보고한다. 보고 실패가 수집기 상태에 재귀적으로 섞이지 않는다."""
import os
import time
import uuid

from delivery import post_json
from forwarder_status import STATUS
from log_forwarder import send_q

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = time.time()
VERSION = os.getenv('FORWARDER_VERSION', 'unknown')
INGEST_URL = os.getenv('MCP_FORWARDER_STATUS_INGEST_URL')
TOKEN = os.getenv('MCP_TOKEN')
REPORT_INTERVAL = max(1, int(os.getenv('FORWARDER_REPORT_INTERVAL', '15')))


def report_once(sequence):
    payload = STATUS.snapshot()
    payload.update(schema_version=1, run_id=RUN_ID, started_at=RUN_STARTED_AT,
                   sequence=sequence, version=VERSION, log_queue_size=send_q.qsize(),
                   log_queue_capacity=send_q.maxsize)
    # 상태 자체의 전송 실패는 다음 주기의 최신 보고로 대체한다. 별도 큐를 만들지 않는다.
    result = post_json('forwarder_status', INGEST_URL, {'X-MCP-TOKEN': TOKEN}, payload,
                       max_attempts=1, retry_seconds=5, timeout=3, record_status=False)
    print('[status-report] ' + ('sent' if result.success else result.reason), flush=True)
    return result


def main(stop_event):
    sequence = 0
    while not stop_event.is_set():
        sequence += 1
        report_once(sequence)
        stop_event.wait(REPORT_INTERVAL)
