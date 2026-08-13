FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GOC_HEADLESS=1 \
    GOC_STATE_FILE=/app/data/state.json \
    GOC_HEARTBEAT_FILE=/app/data/heartbeat

WORKDIR /app

RUN useradd --create-home --uid 10001 goc
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY goc_auto.py ./
RUN mkdir -p /app/data && chown -R goc:goc /app

USER goc

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD python -c "import os,time,sys; p=os.environ['GOC_HEARTBEAT_FILE']; sys.exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p)<90 else 1)"

CMD ["python", "-u", "goc_auto.py"]
