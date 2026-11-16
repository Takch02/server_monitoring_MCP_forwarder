"""수집과 전송의 상태를 구분해 기록한다. 원문·토큰·URL은 저장하지 않는다."""
import copy
import json
import math
import os
import threading
import time


class StatusRegistry:
    def __init__(self, clock=time.time):
        self.clock = clock
        self.lock = threading.Lock()
        self.streams = {}

    def update(self, name, **values):
        with self.lock:
            state = self.streams.setdefault(name, {
                'enabled': True, 'state': 'starting', 'started_at': self.clock(),
                'last_poll_at': None, 'last_collection_success_at': None,
                'last_collection_error': None, 'last_send_success_at': None,
                'last_send_error': None, 'failed_attempts': 0, 'collection_failures': 0,
                'collected_items': 0, 'sent_items': 0, 'dropped_items': 0,
            })
            state.update(values)

    def event(self, name, kind, count=0, reason=None):
        self.update(name)
        with self.lock:
            state = self.streams[name]
            now = self.clock()
            if kind == 'poll':
                state['last_poll_at'] = now
            elif kind == 'collected':
                state['last_collection_success_at'] = now
                state['last_collection_error'] = None
                state['collected_items'] += count
            elif kind == 'collection_failed':
                state['collection_failures'] += 1
                state['last_collection_error'] = reason
                state['last_collection_failure_at'] = now
            elif kind == 'send_failed':
                state['failed_attempts'] += 1
                state['last_send_error'] = reason
                state['last_send_failure_at'] = now
            elif kind == 'sent':
                state['sent_items'] += count
                state['last_send_success_at'] = now
                state['last_send_error'] = None
            elif kind == 'dropped':
                state['dropped_items'] += count
                state['last_drop_reason'] = reason
                state['last_drop_at'] = now

    def snapshot(self):
        with self.lock:
            return {'updated_at': self.clock(), 'streams': copy.deepcopy(self.streams)}


STATUS = StatusRegistry()


def run_worker(name, target, stop_event=None):
    STATUS.update(name, state='running')
    STATUS.event(name, 'poll')
    try:
        target()
    except (Exception, SystemExit) as error:
        STATUS.update(name, state='failed', worker_error=type(error).__name__)
    else:
        stopped = stop_event is not None and stop_event.is_set()
        STATUS.update(name, state='stopped' if stopped else 'failed',
                      worker_error=None if stopped else 'unexpected_return')


def write_status(path, snapshot):
    temporary = path + '.tmp'
    with open(temporary, 'w', encoding='utf-8') as output:
        json.dump(snapshot, output, ensure_ascii=False)
    os.replace(temporary, path)


def poll_stale_limit(interval_seconds=0, collection_budget=0, retry_budget=15, request_timeout=3):
    # 순차 수집·전송·정상 대기에 스케줄링 여유를 더한다.
    minimum = max(1, int(os.getenv('FORWARDER_STALE_SECONDS', '75')))
    return max(minimum, interval_seconds + collection_budget + retry_budget + request_timeout + 10)


def healthy(snapshot, now=None, stale_seconds=75):
    now = time.time() if now is None else now
    age = now - snapshot['updated_at']
    if age < 0 or age > stale_seconds:
        return False
    enabled = [stream for stream in snapshot['streams'].values() if stream['enabled']]
    if not enabled:
        return False
    for stream in enabled:
        poll = stream['last_poll_at']
        poll_limit = max(stale_seconds, stream.get('poll_stale_seconds', stale_seconds))
        if not math.isfinite(poll_limit) or stream['state'] != 'running' or poll is None or not 0 <= now - poll <= poll_limit:
            return False
        if stream['last_collection_error'] or stream['last_send_error']:
            return False
        dropped_at = stream.get('last_drop_at')
        if dropped_at is not None and 0 <= now - dropped_at <= stale_seconds:
            return False
        if stream.get('worker_error'):
            return False
    return True
