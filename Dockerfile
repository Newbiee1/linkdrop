FROM python:3.13-alpine

RUN apk add --no-cache ffmpeg
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV DOWNLOAD_DIR=/data/downloads \
    MAX_CONCURRENT_DOWNLOADS=2 \
    MAX_BATCH=8 \
    DOWNLOAD_TTL_SECONDS=86400
EXPOSE 8080
VOLUME ["/data/downloads"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
