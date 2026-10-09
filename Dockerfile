FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "maxapi-python>=2.2,<3" \
    fastapi \
    uvicorn \
    httpx \
    "telethon>=1.36,<2" \
    qrcode \
    pillow \
    "playwright>=1.40"

# Браузер для VK-инстансов (вход и ротация web_token, профиль в /data)
RUN python -m playwright install --with-deps chromium

COPY app /app

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
