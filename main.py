import json
import os
import threading
import time

import log_forwarder as forwarder
import metric_forwarder as metric
import health_forwarder as health
import status_reporter
from forwarder_status import STATUS, poll_stale_limit, run_worker, write_status


def main():
    stop_event = threading.Event()
    collectors = [
        ('logs', forwarder.main, bool(os.getenv('LOG_PATH'))),
        ('metrics', metric.main, bool(os.getenv('ACTUATOR_URL'))),
        ('health', health.main, bool(os.getenv('HEALTH_URL'))),
    ]
    poll_limits = {
        'logs': poll_stale_limit(retry_budget=0, request_timeout=0),
        'log_sender': poll_stale_limit(retry_budget=forwarder.DROP_AFTER_S,
                                      request_timeout=min(forwarder.HTTP_TIMEOUT_MS / 1000.0, forwarder.DROP_AFTER_S)),
        # timeout=2의 연결·읽기 제한을 반영한다. Metrics는 3회, Health는 1회 조회한다.
        'metrics': poll_stale_limit(metric.COLLECT_INTERVAL, collection_budget=12),
        'health': poll_stale_limit(health.HEALTH_INTERVAL, collection_budget=4),
    }
    logs_enabled = collectors[0][2]
    STATUS.update('log_sender', enabled=logs_enabled, state='starting' if logs_enabled else 'disabled',
                  poll_stale_seconds=poll_limits['log_sender'])
    for name, target, enabled in collectors:
        STATUS.update(name, enabled=enabled, state='starting' if enabled else 'disabled',
                      poll_stale_seconds=poll_limits[name])
    for name, target, enabled in collectors:
        if enabled:
            thread = threading.Thread(target=run_worker, args=(name, target, stop_event), daemon=True)
            thread.start()
    if not any(enabled for _, _, enabled in collectors):
        raise SystemExit('수집 대상이 설정되지 않았습니다.')
    if status_reporter.INGEST_URL:
        reporter = threading.Thread(target=status_reporter.main, args=(stop_event,), daemon=True)
        reporter.start()
    else:
        print('[status-report] disabled: MCP_FORWARDER_STATUS_INGEST_URL is not set', flush=True)
    path = os.getenv('FORWARDER_STATUS_PATH', '/tmp/forwarder-status.json')
    interval = max(1, int(os.getenv('FORWARDER_STATUS_INTERVAL', '10')))
    try:
        while True:
            snapshot = STATUS.snapshot()
            try:
                write_status(path, snapshot)
            except OSError as error:
                print('[forwarder-status] write_failed: ' + type(error).__name__, flush=True)
            print('[forwarder-status] ' + json.dumps(snapshot, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        stop_event.set()
        forwarder.stop_event.set()


if __name__ == '__main__':
    main()
