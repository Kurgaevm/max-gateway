FROM python:3.12-slim

# Канал VK (личные страницы) тянет playwright+chromium (~500МБ). Для клиентских
# серверов обычно не нужен: собирайте с INSTALL_VK=0 (вопрос установщика или .env).
ARG INSTALL_VK=1

WORKDIR /app

RUN pip install --no-cache-dir \
    "maxapi-python>=2.2,<3" \
    fastapi \
    uvicorn \
    httpx \
    "telethon>=1.36,<2" \
    qrcode \
    pillow

# Браузер для VK-инстансов (вход и ротация web_token, профиль в /data)
RUN if [ "$INSTALL_VK" = "1" ]; then \
        pip install --no-cache-dir "playwright>=1.40" \
        && python -m playwright install --with-deps chromium; \
    fi

COPY app /app

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
