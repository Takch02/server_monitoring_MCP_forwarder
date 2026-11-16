# Server Doctor Forwarder

Spring 앱 내부에서 로그·CPU·JVM 힙 메모리·Health를 수집해 MCP 서버로 전송합니다.
`LOG_PATH`, `ACTUATOR_URL`, `HEALTH_URL` 중 설정한 수집기만 실행합니다.
활성 수집기에는 `MCP_TOKEN`과 해당 `MCP_*_INGEST_URL`이 필요합니다.

## 전송 결과와 재시도

- HTTP 2xx만 전송 성공입니다. 인증 실패(401/403)와 기타 영구 오류는 해당 요청에서 재시도하지 않습니다.
- HTTP 408·429·5xx, 시간 초과·연결 오류는 지수형 대기와 지터로 재시도합니다.
- 지표·Health는 최대 3회, 재시도 예산 15초입니다. 로그는 기존 60초 재시도 예산을 유지합니다.
- 예산이 소진되면 다음 요청을 시작하지 않습니다. 진행 중 HTTP 요청의 실제 종료 시점은 연결·읽기 제한에도 영향을 받습니다.
- 한 이벤트의 ID·발생 시각·본문을 재시도마다 그대로 사용합니다. 다만 현재 지표·Health 수신 API는 이벤트 ID 기반 중복 제거를 보장하지 않으므로, 응답 유실 후 재전송 시 중복 저장될 수 있습니다.
- 인증 실패는 다음 수집 주기의 새 데이터에서도 다시 발생할 수 있습니다. 토큰·주소 변경 후에는 프로세스를 재시작해야 합니다.
- 메모리 최대값 누락·0·음수, 무한대 등 유효하지 않은 지표는 정상값으로 대체하지 않고 수집 실패로 기록합니다.
- JVM 메모리는 `area:heap` 태그로 사용량과 최대값의 영역을 맞춥니다. 서버 전체 RAM 사용량이 아닙니다.

## 수집기 상태 확인

기본 10초마다 `[forwarder-status]` JSON 로그와 `/tmp/forwarder-status.json` 파일을 기록합니다.
파일은 임시 파일을 쓴 후 교체하므로 읽는 도중 일부 JSON만 보이지 않습니다.

```sh
docker exec <컨테이너명> cat /tmp/forwarder-status.json
docker inspect --format '{{json .State.Health}}' <컨테이너명>
```

로그 읽기(`logs`), 로그 전송(`log_sender`), 지표(`metrics`), Health(`health`)를 구분합니다.

| 필드 | 의미 |
|---|---|
| `enabled`, `state` | 비활성·시작·실행·실패·종료 상태 |
| `last_poll_at` | 수집/전송 루프가 마지막으로 진행한 시각 |
| `last_collection_success_at` | 마지막 수집 성공 시각. 빈 로그 파일을 정상적으로 읽는 것도 성공 |
| `last_collection_error`, `collection_failures` | 마지막 수집 실패 원인과 누적 실패 횟수 |
| `last_send_success_at`, `last_send_error` | 마지막 HTTP 수락 시각과 이후 전송 실패 원인 |
| `collected_items`, `sent_items` | 수집한 데이터 수와 HTTP 2xx로 수락된 데이터 수 |
| `failed_attempts` | 재시도를 포함한 전송 실패 횟수 |
| `dropped_items`, `last_drop_reason` | 로컬에서 보관·재시도를 포기한 데이터 수와 원인 |

시각은 Unix 초 단위이며, 카운터와 상태는 프로세스 재시작 시 초기화됩니다.
`sent_items`는 DB에 새로 저장된 행 수가 아닙니다. 응답이 유실되면 로컬에서 폐기한 데이터가 이미 서버에 저장됐을 수도 있습니다.
상태 파일에는 토큰·전송 주소·로그 원문·HTTP 응답 본문을 넣지 않습니다.

## 컨테이너 헬스체크

`healthcheck.py`는 파일과 각 활성 루프의 최신성, 스레드 실패, 수집·전송 오류와 최근 폐기를 확인합니다.
기본 최신성 기준은 로그 재시도 예산보다 긴 75초입니다. 최근 폐기도 75초 동안 상태에 반영합니다.
수집 스레드가 예외로 끝나거나 예상치 못하게 반환하면 메인 프로세스가 살아 있어도 비정상입니다.
대상 앱의 `DOWN`을 수집하고 전송한 것은 정상적인 수집 동작입니다. 대상 앱 상태와 Forwarder 상태를 구분합니다.

| 환경변수 | 기본값 |
|---|---|
| `FORWARDER_STATUS_PATH` | `/tmp/forwarder-status.json` |
| `FORWARDER_STATUS_INTERVAL` | `10`초 |
| `FORWARDER_STALE_SECONDS` | `75`초 |

Docker는 15초마다 검사하고 연속 3회 실패 시 `unhealthy`로 표시합니다. 시작 유예는 30초입니다.
`unhealthy` 표시만으로 Docker가 자동 재시작하지는 않습니다. 스레드 자동 재시작과 외부 알림은 별도 연동이 필요합니다. 중앙 서버 상태 전송은 아래 설정으로 활성화합니다.
로그 큐는 상한 도달 시 오래된 배치를 폐기합니다. 무손실·재시작 후 복구·이벤트 식별 방식 보완은 이번 변경의 보장 범위가 아닙니다.

## 중앙 상태 보고

`MCP_FORWARDER_STATUS_INGEST_URL`을 설정하면 별도 스레드가 기본 15초마다 현재 상태를 보냅니다.
기존의 로컬 상태 파일과 컨테이너 검사는 유지됩니다. 주소를 설정하지 않으면 중앙 보고만 비활성화됩니다.

```yaml
environment:
  SERVER_NAME: "${SERVER_NAME}"
  MCP_TOKEN: "${INGEST_TOKEN}"
  MCP_FORWARDER_STATUS_INGEST_URL: "${MCP_DOMAIN}/api/servers/${SERVER_NAME}/ingest/forwarder-status"
  FORWARDER_REPORT_INTERVAL: "15"
  FORWARDER_VERSION: "v1.0.0" # 실제 이미지 태그 또는 커밋으로 변경
```

- 서버의 상태 수신 API를 먼저 배포해야 합니다. `get_forwarder_status`와 `diagnose_server`에서 확인할 수 있습니다.
- 기존 `streams`에 실행 UUID·시작 시각·보고 순번·버전·로그 큐 배치 수/용량을 추가합니다. 스키마 버전은 1입니다.
- `FORWARDER_VERSION`을 지정하지 않으면 `unknown`입니다. 토큰·주소·원문은 보고 본문에 넣지 않습니다.
- 보고는 로그 큐를 사용하지 않으며 각 주기에 한 번만 전송합니다. 실패하면 `[status-report]`에 원인 코드를 출력하고 다음 주기에 최신 상태를 보냅니다.
- 상태 보고 자체의 전송 결과는 `STATUS.streams`에 추가하지 않습니다. 보고 실패가 수집 상태를 재귀적으로 오염시키지 않습니다.
- 상태 전송이 실패해도 로그·지표·Health 전송과 로컬 검사는 계속됩니다. 다만 중앙에서는 기본 75초 후 수집 상태를 확인할 수 없다고 표시합니다.
- 한 서버 이름에는 활성 Forwarder 하나를 사용합니다. 실행 순서는 호스트 시작 시각으로 비교하므로 재시작 때 시계가 뒤로 이동하면 시간 동기화를 확인해야 합니다.
- `OK`는 활성 수집기의 현재 동작 상태이며 앱 정상이나 과거 전체 수집을 보장하지 않습니다. 로그가 없는 것과 로그 수집 실패를 구분하는 근거입니다.

## 테스트

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

`python test_forwarder.py`로도 같은 테스트를 실행할 수 있습니다.
HTTP 모의 응답과 localhost의 임시 HTTP 서버·파일·별도 프로세스를 사용합니다. 운영 수신 API와 DB에는 연결하지 않습니다.
테스트는 인증 오류, 재시도·복구·시간 초과, 빈 로그 파일, 스레드 종료, 로그 읽기부터 HTTP 수신·상태 파일·헬스체크까지 검증합니다.
