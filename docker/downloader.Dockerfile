FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ curl ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements-downloader.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scripts/ndvi_download.py /app/scripts/ndvi_download.py

CMD ["python", "scripts/ndvi_download.py"]
