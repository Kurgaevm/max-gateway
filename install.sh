#!/usr/bin/env bash
# Установщик MAX Шлюза. Запуск:
#   curl -fsSL https://raw.githubusercontent.com/Kurgaevm/max-gateway/main/install.sh | bash
# Сразу с доменом (неинтерактивно):
#   MAXGW_DOMAIN=max.example.ru curl -fsSL https://raw.githubusercontent.com/Kurgaevm/max-gateway/main/install.sh | bash
set -euo pipefail

REPO="https://github.com/Kurgaevm/max-gateway.git"
DIR="${MAXGW_DIR:-$HOME/max-gateway}"
STACK_DIR="${SELFHOST_DIR:-$HOME/selfhost-ai}"
ADDON_FILE="$STACK_DIR/caddy-addon/tls-snippet.conf"
ADDON_MARK="max-gateway — добавлено установщиком"

say() { printf '%s\n' "$*"; }
die() { say "Ошибка: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "Docker не установлен. Инструкция: https://docs.docker.com/engine/install/"
docker compose version >/dev/null 2>&1 || die "нужен Docker Compose v2 (плагин compose)."

if [ -d "$DIR/.git" ]; then
  say "Обновляю установку в $DIR"
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
  say "Создан .env со свежим API-ключом."
fi

# --- Домен (опционально): HTTPS-доступ снаружи через Caddy из стека selfhost-ai ---
DOMAIN="${MAXGW_DOMAIN:-}"
if [ -z "$DOMAIN" ] && [ -r /dev/tty ]; then
  printf 'Домен для веб-интерфейса (например max.salon.ru; Enter — пропустить): '
  IFS= read -r DOMAIN < /dev/tty || DOMAIN=""
fi

WANT_DOMAIN=0
if [ -n "$DOMAIN" ]; then
  [[ "$DOMAIN" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+$ ]] \
    || die "'$DOMAIN' не похож на домен"

  [ -d "$STACK_DIR" ] || die "каталог стека $STACK_DIR не найден. Укажи SELFHOST_DIR=<путь к selfhost-ai> и перезапусти, либо ставь без домена."
  docker ps --format '{{.Names}}' | grep -qx caddy || die "контейнер caddy не запущен. Сначала установи стек selfhost-ai, потом запускай с доменом."
  docker network inspect localai_default >/dev/null 2>&1 || die "docker-сеть localai_default не найдена. Сначала установи стек selfhost-ai."

  # Подключаем шлюз к сети стека, чтобы Caddy звал его по имени maxgateway:8090.
  # Файл в .gitignore — обновления репозитория его не трогают.
  if [ ! -f docker-compose.override.yml ]; then
    cat > docker-compose.override.yml <<'EOF'
services:
  maxgateway:
    networks:
      - n8n

networks:
  n8n:
    name: localai_default
    external: true
EOF
  fi
  WANT_DOMAIN=1
fi

docker compose up -d --build

# --- Логин/пароль веб-доступа (basic auth): env из .env, иначе из data/http_auth.json ---
HTTP_USER=$(grep '^MAXGW_HTTP_USER=' .env 2>/dev/null | cut -d= -f2 || true); HTTP_USER=${HTTP_USER:-admin}
HTTP_PASS=$(grep '^MAXGW_HTTP_PASS=' .env 2>/dev/null | cut -d= -f2 || true)
if [ -z "$HTTP_PASS" ]; then
  AUTH_FILE="$DIR/data/http_auth.json"
  for _ in $(seq 1 30); do
    [ -s "$AUTH_FILE" ] && break
    sleep 1
  done
  HTTP_PASS=$(sed -n 's/.*"password": *"\([^"]*\)".*/\1/p' "$AUTH_FILE" 2>/dev/null || true)
  [ -n "$HTTP_PASS" ] || HTTP_PASS="(не создан — смотри: docker logs maxgateway)"
fi

if [ "$WANT_DOMAIN" = 1 ]; then
  docker network connect localai_default maxgateway 2>/dev/null || true
  if grep -q "$ADDON_MARK" "$ADDON_FILE" 2>/dev/null; then
    say "Блок max-gateway уже есть в $ADDON_FILE. Домен меняется там руками + 'docker exec caddy caddy reload --config /etc/caddy/Caddyfile'."
  else
    cat >> "$ADDON_FILE" <<EOF

# --- $ADDON_MARK ---
$DOMAIN {
    import service_tls
    reverse_proxy maxgateway:8090
}
EOF
    docker exec caddy caddy reload --config /etc/caddy/Caddyfile \
      || die "caddy reload не прошёл. Проверь $ADDON_FILE и выполни: docker exec caddy caddy reload --config /etc/caddy/Caddyfile"
    say "Caddy настроен: $DOMAIN -> maxgateway:8090"
  fi
  PUBLIC_IP=$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || echo "неизвестен (нет исходящего curl)")
  say "Не забудь DNS: A-запись $DOMAIN -> $PUBLIC_IP (без неё Let's Encrypt не выпустит сертификат)."
  URL="https://$DOMAIN/"
else
  URL="http://127.0.0.1:8090/"
fi

KEY=$(grep '^MAXGW_API_KEY=' .env | cut -d= -f2)
say ""
say "=============================================="
say " Готово."
say " Веб-интерфейс: $URL"
say " Логин:  $HTTP_USER"
say " Пароль: $HTTP_PASS"
say " API-ключ: $KEY"
say " Логин/пароль позже: docker exec maxgateway cat /data/http_auth.json"
say " Дальше: README.md, раздел «Первый вход»"
say "=============================================="
