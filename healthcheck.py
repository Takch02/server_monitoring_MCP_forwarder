"""포트를 열지 않고 로컬 상태 파일로 컨테이너의 수집 동작을 확인한다."""
import json
import os
import sys
from forwarder_status import healthy


def main():
    try:
        with open(os.getenv('FORWARDER_STATUS_PATH', '/tmp/forwarder-status.json'), encoding='utf-8') as source:
            snapshot = json.load(source)
        return 0 if healthy(snapshot, stale_seconds=int(os.getenv('FORWARDER_STALE_SECONDS', '75'))) else 1
    except (OSError, ValueError, KeyError, TypeError):
        return 1


if __name__ == '__main__':
    sys.exit(main())
