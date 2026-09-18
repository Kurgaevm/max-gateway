#!/usr/bin/env bash
# Установщик MAX Шлюза. Запуск:
#   curl -fsSL https://raw.githubusercontent.com/Kurgaevm/max-gateway/main/install.sh | bash
set -euo pipefail

REPO="https://github.com/Kurgaevm/max-gateway.git"
DIR="${MAXGW_DIR:-$HOME/max-gateway}"

command -v docker >/dev/null 2>&1 || { echo "Ошибка: Docker не установлен. Инструкция: https://docs.docker.com/engine/install/"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Ошибка: нужен Docker Compose v2 (плагин compose)."; exit 1; }

if [ -d "$DIR/.git" ]; then
  echo "Обновляю установку в $DIR"
  git -C "$DIR" pull --ff-only
else
  git clone "$REPO" "$DIR"
fi
cd "$DIR"

if [ ! -f .env ]; then
  KEY=$(openssl rand -hex 24)
  cat > .env <<EOF
MAXGW_API_KEY=$KEY
MAXGW_WEBHOOK_URL=
MAXGW_PHONE=
MAXGW_REG_FIRST=Salon
MAXGW_REG_LAST=Bot
EOF
  echo "Создан .env со свежим API-ключом."
fi

docker compose up -d --build

KEY=$(grep '^MAXGW_API_KEY=' .env | cut -d= -f2)
echo
echo "=============================================="
echo " Готово."
echo " Веб-интерфейс: http://127.0.0.1:8090/"
echo " API-ключ: $KEY"
echo " Дальше: README.md, раздел «Первый вход»"
echo "=============================================="
