FROM python:3.11-slim

# 仅使用 Python 标准库，无第三方依赖
WORKDIR /srv
COPY app/ /srv/app/

ENV APP_DATA_DIR=/data \
    HOST=0.0.0.0 \
    PORT=8080 \
    JANITOR_ENABLED=true \
    COMPACTION_MIN_SEGMENTS=4 \
    SEGMENT_BYTES=1048576 \
    DEFAULT_TTL_MS=30000

RUN mkdir -p /data && useradd -r -u 10001 appuser && chown -R appuser:appuser /data /srv
USER appuser
EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2).status==200 else 1)"

CMD ["python", "-m", "app"]
