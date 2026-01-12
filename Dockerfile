FROM python:3.9-slim

WORKDIR /app

# 필요 라이브러리 설치
RUN pip install requests

# 파일 3개 모두 복사
COPY log_forwarder.py .
COPY metric_forwarder.py .
COPY health_forwarder.py .
COPY main.py .

# 로그 즉시 출력
ENV PYTHONUNBUFFERED=1

# 실행 명령 변경
CMD ["python", "-u", "main.py"]
