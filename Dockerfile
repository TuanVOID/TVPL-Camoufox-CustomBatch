FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TESSERACT_CMD=/usr/bin/tesseract

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /app/requirements.txt

COPY 08_camoufox_crawl_by_org.py /app/08_camoufox_crawl_by_org.py
COPY 09_parallel_camoufox_custom_batch.py /app/09_parallel_camoufox_custom_batch.py
COPY run_custom_batch.py /app/run_custom_batch.py
COPY config/custom_batch.example.json /app/config/custom_batch.example.json
COPY docker/entrypoint.sh /app/docker/entrypoint.sh

RUN chmod +x /app/docker/entrypoint.sh \
    && mkdir -p /app/config /app/output /app/logs /app/state

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["python", "run_custom_batch.py", "--config", "/app/config/custom_batch.json", "--headless"]

