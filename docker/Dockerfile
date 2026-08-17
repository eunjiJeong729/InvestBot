# Production (EC2): docker build --platform linux/amd64 -f docker/Dockerfile .
# Local Mac (arm64):  docker compose -f docker/docker-compose.yml up --build
ARG TARGETPLATFORM=linux/amd64
FROM --platform=${TARGETPLATFORM} python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Seoul \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY infra/ /app/infra/
COPY src/ /app/src/
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
