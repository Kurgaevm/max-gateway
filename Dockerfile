FROM python:3.12-slim

WORKDIR /app

# ffmpeg: конвертация голосовых (ogg/oga -> mp3) для STT-провайдеров без поддержки ogg
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "maxapi-python>=2.2,<3" \
    fastapi \
    uvicorn \
    httpx \
    "telethon>=1.36,<2" \
    qrcode \
    pillow \
    python-multipart

COPY app /app

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
