FROM python:3.9-slim

WORKDIR /app

# 필요 라이브러리 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 런타임 모듈 복사
COPY log_forwarder.py .
COPY metric_forwarder.py .
COPY health_forwarder.py .
COPY main.py .
COPY delivery.py .
COPY forwarder_status.py .
COPY status_reporter.py .
COPY healthcheck.py .

# 로그 즉시 출력
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=3 CMD ["python", "healthcheck.py"]

# 실행 명령 변경
CMD ["python", "-u", "main.py"]
