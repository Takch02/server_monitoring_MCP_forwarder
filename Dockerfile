FROM python:3.12-slim

WORKDIR /app

# 네트워크/재시도용 HTTP 클라이언트
RUN pip install --no-cache-dir requests

# 포워더 스크립트 복사
COPY forwarder.py /app/forwarder.py

# 로그 즉시 출력
ENV PYTHONUNBUFFERED=1

# 기본 실행
ENTRYPOINT ["python", "/app/forwarder.py"]
