import json
import os
import threading
import time

import log_forwarder as forwarder
import metric_forwarder as metric
import health_forwarder as health
import status_reporter
from forwarder_status import STATUS, run_worker, write_status


def main():
    stop_event = threading.Event()
    collectors = [
        ('logs', forwarder.main, bool(os.getenv('LOG_PATH'))),
        ('metrics', metric.main, bool(os.getenv('ACTUATOR_URL'))),
        ('health', health.main, bool(os.getenv('HEALTH_URL'))),
    ]
    logs_enabled = collectors[0][2]
    STATUS.update('log_sender', enabled=logs_enabled, state='starting' if logs_enabled else 'disabled')
    for name, target, enabled in collectors:
        STATUS.update(name, enabled=enabled, state='starting' if enabled else 'disabled')
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
            write_status(path, snapshot)
            print('[forwarder-status] ' + json.dumps(snapshot, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        stop_event.set()
        forwarder.stop_event.set()


if __name__ == '__main__':
    main()
