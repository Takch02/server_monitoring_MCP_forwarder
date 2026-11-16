"""동일 페이로드를 재사용하고, 재시도 가능한 실패만 제한적으로 재시도한다."""
import json
import random
import time
from dataclasses import dataclass
from typing import Optional

import requests
from forwarder_status import STATUS


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    reason: str
    attempts: int
    status_code: Optional[int] = None


def post_json(stream, url, headers, payload, count=1, timeout=3, max_attempts=3,
              retry_seconds=15, initial_backoff=0.5, max_backoff=10, record_status=True):
    def record(kind, amount=0, reason=None):
        if record_status:
            STATUS.event(stream, kind, amount, reason)

    attempts = 0
    reason = 'configuration_error'
    code = None
    if not url or not headers.get('X-MCP-TOKEN'):
        record('send_failed', reason=reason)
        record('dropped', count, reason)
        return DeliveryResult(False, reason, attempts)
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8')
    deadline = time.monotonic() + retry_seconds
    backoff = initial_backoff
    while time.monotonic() < deadline and (max_attempts is None or attempts < max_attempts):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts += 1
        code = None
        retryable = True
        try:
            response = requests.post(url, headers=headers, data=body,
                                     timeout=min(timeout, remaining),
                                     allow_redirects=False)
            try:
                code = response.status_code
            finally:
                response.close()
            if 200 <= code < 300:
                record('sent', count)
                return DeliveryResult(True, 'sent', attempts, code)
            reason = 'authentication_failed' if code in (401, 403) else 'http_' + str(code)
            retryable = code in (408, 429) or 500 <= code < 600
        except requests.Timeout:
            reason = 'timeout'
        except requests.ConnectionError:
            reason = 'connection_error'
        except requests.RequestException:
            reason = 'request_error'
            retryable = False
        record('send_failed', reason=reason)
        if not retryable or (max_attempts is not None and attempts >= max_attempts):
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(backoff * random.uniform(0.7, 1.3), remaining))
        backoff = min(backoff * 2, max_backoff)
    record('dropped', count, reason)
    return DeliveryResult(False, reason, attempts, code)
