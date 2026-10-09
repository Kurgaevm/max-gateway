"""MAX Шлюз: self-hosted шлюз для мессенджера MAX (мультиаккаунтный).

FastAPI + PyMax (неофициальный внутренний API MAX).
- Несколько аккаунтов (инстансов) на одну установку
- Вход по телефону + SMS-код (код передаётся через API/веб-интерфейс)
- Входящие сообщения -> вебхук на URL инстанса (формат совместим с Green-API)
- Голосовые сообщения: скачивание + расшифровка через OpenAI-совместимый STT
- Исходящие: POST /i/{name}/sendText
- Инстансы Telegram (Telethon): тот же API, вход по коду из Telegram или QR
"""

import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from pymax import Client, ExtraConfig, RegistrationConfig

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("maxgw")

# Telegram-адаптер (telethon). Если библиотеки нет — шлюз работает только с MAX.
TG = None
try:
    try:
        import tg as TG  # запуск из каталога app (docker: uvicorn main:app)
    except ImportError:
        from app import tg as TG  # запуск из корня репозитория
except Exception as _tg_err:  # noqa: BLE001
    log.warning("Telegram adapter unavailable: %s", _tg_err)

# VK-адаптер (личные страницы, web_token + playwright-профиль).
VK = None
try:
    try:
        import vk as VK
    except ImportError:
        from app import vk as VK
except Exception as _vk_err:  # noqa: BLE001
    log.warning("VK adapter unavailable: %s", _vk_err)

API_KEY = os.environ.get("GATEWAY_API_KEY", "")  # админ-ключ: управление инстансами
GLOBAL_WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
WORK_DIR = os.environ.get("WORK_DIR", "/data")
SESSIONS_DIR = os.path.join(WORK_DIR, "sessions")
INST_FILE = os.path.join(WORK_DIR, "instances.json")
PHONE = os.environ.get("PHONE", "").strip()
CODE_TIMEOUT = int(os.environ.get("CODE_TIMEOUT", "900"))
MAX_VOICE_BYTES = 20 * 1024 * 1024

# Расшифровка голосовых: любой OpenAI-совместимый endpoint /v1/audio/transcriptions
STT_BASE_URL = os.environ.get("STT_BASE_URL", "").rstrip("/")
STT_API_KEY = os.environ.get("STT_API_KEY", "")
STT_MODEL = os.environ.get("STT_MODEL", "whisper-1")


# ---------------------------------------------------------------- instances


class Inst:
    def __init__(self, name: str, phone: Optional[str] = None,
                 api_key: Optional[str] = None, webhook_url: str = "",
                 created: Optional[float] = None, type: str = "max") -> None:
        self.name = name
        self.type = type  # max | telegram | vk
        self.phone = phone
        self.api_key = api_key or secrets.token_hex(24)
        self.webhook_url = webhook_url or ""
        self.created = created or time.time()
        # runtime
        self.stage = "idle"
        self.me: Any = None
        self.hint: Any = None
        self.error: Any = None
        self.connected = False
        self.last_incoming: Any = None
        self.last_webhook: Any = None
        self.recent: deque = deque(maxlen=25)
        # chat routing: user_id -> raw DM chat_id (Green-API dialect on the outside)
        self.chat_map: dict[int, int] = {}
        self.known_chats: set[int] = set()
        self.sms_queue: asyncio.Queue = asyncio.Queue()
        self.pw_queue: asyncio.Queue = asyncio.Queue()
        self.client: Optional[Client] = None
        self.task: Optional[asyncio.Task] = None
        # telegram runtime
        self.tg_code_hash: Optional[str] = None
        self.qr_task: Optional[asyncio.Task] = None
        self.qr_url: Optional[str] = None
        # vk runtime
        self.vk_sess: Any = None
        self.vk_lp_task: Optional[asyncio.Task] = None
        self.vk_login_ctx: Any = None
        self.vk_send_lock: Any = None
        self.vk_last_send: float = 0.0

    def _live_connected(self) -> bool:
        if self.type == "vk":
            return self.stage == "authorized"
        if self.client is None:
            return False
        f = getattr(self.client, "is_connected", None)
        if f is None:
            return False
        if callable(f):
            try:
                return bool(f())
            except Exception:  # noqa: BLE001
                return False
        return bool(f)

    def summary(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "phone": self.phone,
            "stage": self.stage,
            "connected": self._live_connected(),
            "me": self.me,
            "webhook_url": self.webhook_url or GLOBAL_WEBHOOK_URL or None,
        }

    def status(self) -> dict:
        return {
            "instance": self.name,
            "type": self.type,
            "stage": self.stage,
            "phone": self.phone,
            "me": self.me,
            "hint": self.hint,
            "error": self.error,
            "connected": self._live_connected(),
            "qr_url": self.qr_url,
            "last_incoming": self.last_incoming,
            "last_webhook": self.last_webhook,
        }


INSTANCES: dict[str, Inst] = {}


def save_instances() -> None:
    data = {
        "instances": [
            {
                "name": i.name,
                "type": i.type,
                "phone": i.phone,
                "api_key": i.api_key,
                "webhook_url": i.webhook_url,
                "created": i.created,
                "chat_map": {str(k): v for k, v in i.chat_map.items()},
                "known_chats": sorted(i.known_chats),
            }
            for i in INSTANCES.values()
        ]
    }
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    with open(INST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_instances() -> None:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    if os.path.exists(INST_FILE):
        with open(INST_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for e in data.get("instances", []):
            cm = {int(k): int(v) for k, v in (e.pop("chat_map", None) or {}).items()}
            kc = set(int(x) for x in (e.pop("known_chats", None) or []))
            inst = Inst(**e)
            inst.chat_map = cm
            inst.known_chats = kc
            INSTANCES[inst.name] = inst
        return
    # миграция с одиночной версии: maxgw.db -> sessions/default.db
    legacy_db = os.path.join(WORK_DIR, "maxgw.db")
    phone = PHONE
    if not phone:
        try:
            with open(os.path.join(WORK_DIR, "phone.txt"), encoding="utf-8") as f:
                phone = f.read().strip()
        except OSError:
            phone = ""
    default = Inst("default", phone=phone or None,
                   api_key=os.environ.get("MAXGW_API_KEY") or None)
    if os.path.exists(legacy_db):
        shutil.move(legacy_db, os.path.join(SESSIONS_DIR, "default.db"))
    INSTANCES[default.name] = default
    save_instances()
    log.info("migrated single-account install to instance 'default'")


# ---------------------------------------------------------------- providers


class InstSmsProvider:
    def __init__(self, inst: Inst) -> None:
        self.inst = inst

    async def get_code(self, phone: str) -> str:
        self.inst.stage = "awaiting_code"
        self.inst.phone = phone
        self.inst.error = None
        save_instances()
        log.info("[%s] SMS code requested for %s", self.inst.name, phone)
        try:
            code = await asyncio.wait_for(self.inst.sms_queue.get(), timeout=CODE_TIMEOUT)
        except asyncio.TimeoutError:
            self.inst.stage = "error"
            self.inst.error = "timeout waiting for SMS code"
            raise
        self.inst.stage = "connecting"
        return code


class InstPasswordProvider:
    def __init__(self, inst: Inst) -> None:
        self.inst = inst

    async def get_password(self, hint: Optional[str] = None) -> str:
        self.inst.stage = "awaiting_password"
        self.inst.hint = hint
        try:
            pw = await asyncio.wait_for(self.inst.pw_queue.get(), timeout=CODE_TIMEOUT)
        except asyncio.TimeoutError:
            self.inst.stage = "error"
            self.inst.error = "timeout waiting for 2FA password"
            raise
        self.inst.stage = "connecting"
        return pw


def build_client(inst: Inst) -> Client:
    extra = ExtraConfig(
        log_level=os.environ.get("PYMAX_LOG_LEVEL", "INFO"),
        reconnect=True,
        reconnect_delay=3,
        registration_config=RegistrationConfig(
            first_name=os.environ.get("REG_FIRST_NAME", "Salon"),
            last_name=os.environ.get("REG_LAST_NAME", "Bot"),
        ),
    )
    return Client(
        phone=inst.phone or "",
        work_dir=WORK_DIR,
        session_name=f"sessions/{inst.name}.db",
        extra_config=extra,
        sms_code_provider=InstSmsProvider(inst),
        password_provider=InstPasswordProvider(inst),
    )


# ---------------------------------------------------------------- helpers


def normalize_phone(p: str) -> str:
    d = "".join(ch for ch in p if ch.isdigit())
    if d.startswith("8"):
        d = "7" + d[1:]
    if len(d) == 10:
        d = "7" + d
    return "+" + d


def attach_type(a: Any) -> str:
    """Значение типа вложения ('AUDIO', 'PHOTO', ...) без префикса enum."""
    t = getattr(a, "type", None)
    if t is None:
        return ""
    return str(getattr(t, "value", t))


def sender_info(c: Client, user_id: Any) -> tuple[Optional[str], Optional[str]]:
    """(display name, phone) from the cached user, if the API returned them."""
    if user_id is None:
        return None, None
    try:
        u = c.get_cached_user(int(user_id))
    except Exception:  # noqa: BLE001
        return None, None
    if u is None:
        return None, None
    parts = []
    for n in list(getattr(u, "names", None) or [])[:1]:
        for attr in ("first_name", "last_name"):
            v = getattr(n, attr, None)
            if isinstance(v, str) and v:
                parts.append(v)
    name = " ".join(parts) or None
    ph = getattr(u, "phone", None)
    return name, (f"+{ph}" if ph else None)


async def transcribe(data: bytes, mime: str = "audio/ogg") -> Optional[str]:
    if not (STT_BASE_URL and STT_API_KEY):
        return None
    try:
        async with httpx.AsyncClient(timeout=120) as hc:
            r = await hc.post(
                STT_BASE_URL,
                headers={"Authorization": f"Bearer {STT_API_KEY}"},
                files={"file": ("voice.ogg", data, mime)},
                data={"model": STT_MODEL, "language": "ru"},
            )
            if r.status_code == 200:
                return (r.json().get("text") or "").strip() or None
            log.warning("STT failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:  # noqa: BLE001
        log.warning("STT error: %s", e)
    return None


async def handle_voice(inst: Inst, c: Client, voice: Any,
                       chat_id: Any, mid: Any) -> dict:
    """Скачивает голосовое и расшифровывает, если настроен STT."""
    url = getattr(voice, "url", None)
    if not url and chat_id is not None and getattr(voice, "audio_id", None) is not None:
        try:
            fr = await c.get_file_by_id(chat_id=chat_id, message_id=mid,
                                        file_id=voice.audio_id)
            url = getattr(fr, "url", None) if fr is not None else None
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] get_file_by_id failed: %s", inst.name, e)
    transcript: Optional[str] = None
    if url:
        try:
            async with httpx.AsyncClient(timeout=90, follow_redirects=True) as hc:
                r = await hc.get(url)
                if r.status_code == 200 and len(r.content) <= MAX_VOICE_BYTES:
                    transcript = await transcribe(r.content)
                else:
                    log.warning("[%s] voice download: status=%s size=%s",
                                inst.name, r.status_code, len(r.content))
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] voice download failed: %s", inst.name, e)
    md: dict[str, Any] = {
        "voiceMessageData": {
            "duration": getattr(voice, "duration", None),
            "transcript": transcript,
        },
    }
    if transcript:
        # Green-API-compatible consumers (incl. the salon n8n brain) treat
        # transcribed voice as plain text without any extra handling.
        md["typeMessage"] = "textMessage"
        md["textMessageData"] = {"textMessage": transcript}
    else:
        md["typeMessage"] = "voiceMessage"
    return md


async def handle_incoming(inst: Inst, message: Any, c: Client) -> None:
    me_id = inst.me
    sid = getattr(message, "sender", None)  # int | None: sender user id
    if me_id is not None and sid is not None and int(sid) == int(me_id):
        return  # собственное исходящее
    text = getattr(message, "text", None)
    chat_id = getattr(message, "chat_id", None)
    mid = getattr(message, "id", None)
    attaches = list(getattr(message, "attaches", None) or [])

    # Green-API dialect: in DMs the chatId equals the peer user_id.
    # Raw MAX DM chat_id = me ^ peer (see Client.get_chat_id, XOR).
    api_chat_id: Any = chat_id
    try:
        raw = int(chat_id)
        changed = raw not in inst.known_chats
        inst.known_chats.add(raw)
        if me_id is not None and sid is not None and raw == int(me_id) ^ int(sid):
            api_chat_id = int(sid)  # личный чат: chatId = user_id собеседника
            if inst.chat_map.get(int(sid)) != raw:
                inst.chat_map[int(sid)] = raw
                changed = True
        if changed:
            save_instances()
    except (TypeError, ValueError):
        pass

    voice = next((a for a in attaches if attach_type(a) == "AUDIO"), None)
    if voice is not None:
        message_data = await handle_voice(inst, c, voice, chat_id, mid)
    elif text:
        message_data = {
            "typeMessage": "textMessage",
            "textMessageData": {"textMessage": text},
        }
    else:
        message_data = {
            "typeMessage": "attachmentMessage",
            "attaches": [{"type": attach_type(a)} for a in attaches],
        }

    sname, sphone = sender_info(c, sid)
    payload = {
        "typeWebhook": "incomingMessageReceived",
        "instanceData": {
            "idInstance": 1,
            "instanceName": inst.name,
            "wid": inst.phone,
            "typeInstance": inst.type,
        },
        "timestamp": int(time.time()),
        "idMessage": str(mid) if mid is not None else uuid.uuid4().hex,
        "senderData": {
            "chatId": str(api_chat_id) if api_chat_id is not None else None,
            "rawChatId": str(chat_id) if chat_id is not None else None,
            "senderId": str(sid) if sid is not None else None,
            "senderName": sname,
            "senderPhoneNumber": sphone,
        },
        "messageData": message_data,
    }
    inst.last_incoming = payload
    inst.recent.append({"ts": time.time(), "payload": payload})
    preview = (text or (voice is not None and "<голосовое>") or "<вложение>")
    log.info("[%s] incoming from %s: %s", inst.name, payload["senderData"]["senderId"],
             str(preview)[:120])

    hook = inst.webhook_url or GLOBAL_WEBHOOK_URL
    if hook:
        try:
            async with httpx.AsyncClient(timeout=15) as hc:
                r = await hc.post(hook, json=payload)
                inst.last_webhook = {"status": r.status_code}
                log.info("[%s] webhook delivered: %s", inst.name, r.status_code)
        except Exception as e:  # noqa: BLE001
            inst.last_webhook = {"error": str(e)}
            log.warning("[%s] webhook failed: %s", inst.name, e)


def attach_handlers(inst: Inst, c: Client) -> None:
    @c.on_start()
    async def _on_start(c: Client) -> None:
        me_id = None
        try:
            me_id = c.me.contact.id
        except Exception:  # noqa: BLE001
            pass
        inst.stage = "authorized"
        inst.connected = True
        inst.me = me_id
        inst.error = None
        save_instances()
        log.info("[%s] AUTHORIZED as user_id=%s", inst.name, me_id)

    @c.on_disconnect()
    async def _on_disc(exc: Exception, reconnect: bool, delay: float) -> None:
        inst.connected = False
        log.warning("[%s] disconnected: %s reconnect=%s", inst.name, exc, reconnect)

    @c.on_message()
    async def _on_message(message: Any, c: Client) -> None:
        try:
            await handle_incoming(inst, message, c)
        except Exception:  # noqa: BLE001
            log.exception("[%s] incoming handler failed", inst.name)


async def start_client(inst: Inst) -> None:
    if inst.type == "telegram":
        require_tg()
        await TG.start_client(inst)
        return
    if inst.task is not None and not inst.task.done():
        if inst.client is not None:
            try:
                await inst.client.stop()
            except Exception:  # noqa: BLE001
                pass
        inst.task.cancel()
        try:
            await inst.task
        except BaseException:  # noqa: BLE001
            pass
    inst.client = build_client(inst)
    attach_handlers(inst, inst.client)
    inst.stage = "connecting"
    inst.error = None
    inst.connected = False
    client_ref = inst.client

    async def runner() -> None:
        try:
            await client_ref.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:  # noqa: BLE001
            inst.stage = "error"
            inst.error = f"{type(e).__name__}: {e}"
            inst.connected = False
            log.exception("[%s] client crashed", inst.name)

    inst.task = asyncio.create_task(runner())


async def stop_client(inst: Inst) -> None:
    if inst.type == "telegram":
        if TG is not None:
            await TG.stop_client(inst)
        return
    if inst.client is not None:
        try:
            await inst.client.stop()
        except Exception:  # noqa: BLE001
            pass
    if inst.task is not None:
        inst.task.cancel()
        try:
            await inst.task
        except BaseException:  # noqa: BLE001
            pass
    inst.client = None
    inst.task = None
    inst.connected = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_instances()
    for inst in list(INSTANCES.values()):
        if inst.type == "vk":
            if VK is not None:
                try:
                    await VK.boot(inst)
                except Exception as e:  # noqa: BLE001
                    inst.stage = "error"
                    inst.error = str(e)
                    log.warning("vk boot %s failed: %s", inst.name, e)
            continue
        has_session = inst.phone or (
            inst.type == "telegram"
            and os.path.exists(os.path.join(SESSIONS_DIR, inst.name + ".session"))
        )
        if has_session:
            try:
                await start_client(inst)
            except Exception as e:  # noqa: BLE001
                inst.stage = "error"
                inst.error = str(e)
                log.warning("start %s failed: %s", inst.name, e)
    yield
    for inst in list(INSTANCES.values()):
        if inst.type == "vk":
            try:
                await VK.stop_longpoll(inst)
            except Exception as e:  # noqa: BLE001
                log.warning("vk stop %s failed: %s", inst.name, e)
            continue
        try:
            await stop_client(inst)
        except Exception as e:  # noqa: BLE001
            log.warning("stop %s failed: %s", inst.name, e)


app = FastAPI(title="max-gateway", lifespan=lifespan)


# ------------------------------------------------- http basic auth (web gate)

HTTP_USER = os.environ.get("MAXGW_HTTP_USER", "admin")


def _load_or_create_http_pass() -> str:
    """MAXGW_HTTP_PASS env wins; else persisted WORK_DIR/http_auth.json;
    else generated once, persisted and printed to stdout (docker logs)."""
    import pathlib
    p = pathlib.Path(os.environ.get("WORK_DIR", "/data")) / "http_auth.json"
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))["password"]
    except Exception:  # noqa: BLE001
        pass
    _alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789"
    pwd = "".join(secrets.choice(_alphabet) for _ in range(14))
    try:
        p.write_text(json.dumps({"user": HTTP_USER, "password": pwd},
                                ensure_ascii=False), encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        log.warning("http_auth persist failed: %s", e)
    print("\n" + "=" * 58)
    print("  ВЕБ-ДОСТУП К ШЛЮЗУ (логин и пароль basic auth)")
    print(f"    логин:  {HTTP_USER}")
    print(f"    пароль: {pwd}")
    print(f"    сохранено: {p}")
    print("=" * 58 + "\n", flush=True)
    return pwd


HTTP_PASS = os.environ.get("MAXGW_HTTP_PASS") or _load_or_create_http_pass()

if HTTP_PASS:
    import base64
    import hashlib
    import hmac

    def _cookie_ok(request) -> bool:
        want = hmac.new(HTTP_PASS.encode(), b"maxgw-session",
                        hashlib.sha256).hexdigest()[:32]
        return hmac.compare_digest(request.cookies.get("maxgw_auth", ""), want)

    def _grant_admin(request):
        headers = [(k, v) for k, v in request.scope.get("headers", [])
                   if k != b"x-api-key"]
        request.scope["headers"] = headers + [(b"x-api-key", API_KEY.encode())]

    @app.middleware("http")
    async def http_basic_auth(request, call_next):
        xk = request.headers.get("x-api-key", "")
        if xk and (is_admin(xk) or any(i.api_key == xk
                                       for i in INSTANCES.values())):
            return await call_next(request)  # valid api key: basic not needed
        if _cookie_ok(request):
            _grant_admin(request)
            return await call_next(request)  # logged-in session
        expected = "Basic " + base64.b64encode(
            f"{HTTP_USER}:{HTTP_PASS}".encode()).decode()
        if not hmac.compare_digest(request.headers.get("authorization", ""),
                                   expected):
            log.info("401 %s basic=%s cookie=%s ua=%s", request.url.path,
                     "y" if request.headers.get("authorization") else "n",
                     "y" if request.cookies.get("maxgw_auth") else "n",
                     (request.headers.get("user-agent", "") or "")[:40])
            return Response(
                status_code=401, content="Unauthorized",
                headers={"WWW-Authenticate": 'Basic realm="max-gateway"'})
        # valid basic auth == admin session from now on
        _grant_admin(request)
        resp = await call_next(request)
        resp.set_cookie("maxgw_auth",
                        hmac.new(HTTP_PASS.encode(), b"maxgw-session",
                                 hashlib.sha256).hexdigest()[:32],
                        httponly=True, samesite="lax", max_age=30 * 86400,
                        secure=request.url.scheme == "https")
        return resp


# ---------------------------------------------------------------- auth


def is_admin(key: str) -> bool:
    return bool(API_KEY) and key == API_KEY


def require_admin(x_api_key: str) -> None:
    if not is_admin(x_api_key):
        log.warning("auth fail (admin): key=%r len=%s",
                    (x_api_key[:6] + "…") if x_api_key else None, len(x_api_key))
        raise HTTPException(status_code=401, detail="bad admin key")


def get_inst(name: str) -> Inst:
    inst = INSTANCES.get(name)
    if inst is None:
        raise HTTPException(status_code=404, detail=f"instance '{name}' not found")
    return inst


def require_inst(x_api_key: str, inst: Inst) -> None:
    if not (is_admin(x_api_key) or (inst.api_key and x_api_key == inst.api_key)):
        log.warning("auth fail (%s): key=%r len=%s", inst.name,
                    (x_api_key[:6] + "…") if x_api_key else None, len(x_api_key))
        raise HTTPException(status_code=401, detail="bad api key")


def require_tg() -> None:
    if TG is None:
        raise HTTPException(
            status_code=503,
            detail="telegram adapter unavailable (установите telethon)",
        )


# ---------------------------------------------------------------- models


class LoginBody(BaseModel):
    phone: str
    password: Optional[str] = None  # vk: пароль страницы


class CodeBody(BaseModel):
    code: str


class PasswordBody(BaseModel):
    password: str


class SendTextBody(BaseModel):
    message: str
    phone: Optional[str] = None
    chat_id: Optional[int] = None


class CreateInstBody(BaseModel):
    name: str
    webhook_url: str = ""
    phone: str = ""
    type: str = "max"


class WebhookBody(BaseModel):
    url: str = ""


# ---------------------------------------------------------------- actions


async def act_login(inst: Inst, phone_raw: str, password: str = "") -> dict:
    if inst.type == "telegram":
        require_tg()
        return await TG.act_login(inst, phone_raw)
    if inst.type == "vk":
        if VK is None:
            raise HTTPException(status_code=503, detail="VK adapter unavailable")
        return await VK.act_login(inst, phone_raw, password or "")
    phone = normalize_phone(phone_raw)
    old_session = os.path.join(SESSIONS_DIR, f"{inst.name}.db")
    if inst.phone and inst.phone != phone and os.path.exists(old_session):
        os.remove(old_session)  # смена аккаунта: сносим старую сессию
    inst.phone = phone
    save_instances()
    await start_client(inst)
    return {"ok": True, "instance": inst.name, "phone": phone, "stage": inst.stage}


async def act_code(inst: Inst, code: str) -> dict:
    if inst.type == "telegram":
        require_tg()
        return await TG.act_code(inst, code)
    if inst.type == "vk":
        return await VK.act_code(inst, code)
    if inst.stage != "awaiting_code":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    await inst.sms_queue.put(code.strip())
    return {"ok": True}


async def act_password(inst: Inst, password: str) -> dict:
    if inst.type == "telegram":
        require_tg()
        return await TG.act_password(inst, password)
    if inst.type == "vk":
        raise HTTPException(status_code=400, detail="vk: пароль вводится сразу в /login")
    if inst.stage != "awaiting_password":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    await inst.pw_queue.put(password)
    return {"ok": True}


async def act_reset(inst: Inst) -> dict:
    if inst.type == "telegram":
        require_tg()
        return await TG.act_reset(inst)
    if inst.type == "vk":
        return await VK.act_reset(inst)
    await stop_client(inst)
    session_path = os.path.join(SESSIONS_DIR, f"{inst.name}.db")
    removed = False
    if os.path.exists(session_path):
        os.remove(session_path)
        removed = True
    inst.stage = "idle"
    inst.me = None
    inst.error = None
    inst.last_incoming = None
    if inst.phone:
        await start_client(inst)
    return {"ok": True, "session_deleted": removed, "stage": inst.stage}


def resolve_chat_id(inst: Inst, value: int) -> int:
    """Accepts Green-API style ids: DM user_id or raw chat (group) id.

    Priority: learned user_id->raw map, then ids seen as real chats,
    then the XOR formula (DM: raw = me ^ user_id).
    """
    v = int(value)
    if v in inst.chat_map:
        return inst.chat_map[v]
    if v in inst.known_chats:
        return v
    if inst.me is not None:
        return int(inst.me) ^ v
    return v


async def act_send_text(inst: Inst, body: SendTextBody) -> dict:
    if inst.type == "telegram":
        require_tg()
        return await TG.act_send_text(inst, body)
    if inst.type == "vk":
        return await VK.act_send_text(inst, body)
    if inst.client is None or inst.stage != "authorized":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    chat_id = body.chat_id
    if chat_id is None:
        if not body.phone:
            raise HTTPException(status_code=400, detail="need phone or chat_id")
        contact = await inst.client.search_by_phone(normalize_phone(body.phone))
        if contact is None:
            raise HTTPException(status_code=404, detail="user not found by phone")
        me_id = inst.me
        if me_id is None:
            raise HTTPException(status_code=500, detail="me unknown")
        res = inst.client.get_chat_id(first_user_id=me_id, second_user_id=contact.id)
        if asyncio.iscoroutine(res):
            res = await res
        chat_id = res  # уже raw chat_id, разрешать не нужно
    else:
        chat_id = resolve_chat_id(inst, chat_id)
    sent = await inst.client.send_message(chat_id=chat_id, text=body.message)
    mid = getattr(sent, "id", None)
    return {"ok": True, "instance": inst.name, "chatId": str(chat_id),
            "messageId": str(mid) if mid is not None else None}


# ---------------------------------------------------------------- routes: UI + health


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/health")
async def health():
    return {"ok": True, "instances": len(INSTANCES),
            "telegram": TG is not None, "vk": VK is not None}


# ---------------------------------------------------------------- routes: admin


@app.get("/admin/overview")
async def admin_overview(x_api_key: str = Header(default="")):
    require_admin(x_api_key)
    return {"instances": [i.summary() for i in INSTANCES.values()]}


@app.post("/admin/instances")
async def create_instance(body: CreateInstBody, x_api_key: str = Header(default="")):
    require_admin(x_api_key)
    name = body.name.strip().lower()
    if not re.fullmatch(r"[a-z0-9_-]{1,32}", name):
        raise HTTPException(status_code=400, detail="name: a-z, 0-9, _ - до 32 символов")
    itype = (body.type or "max").strip().lower()
    if itype not in ("max", "telegram", "vk"):
        raise HTTPException(status_code=400, detail="type: max | telegram | vk")
    if name in INSTANCES:
        raise HTTPException(status_code=409, detail="instance already exists")
    inst = Inst(name, phone=normalize_phone(body.phone) if body.phone else None,
                webhook_url=body.webhook_url.rstrip("/"), type=itype)
    INSTANCES[name] = inst
    save_instances()
    return {"ok": True, "name": name, "type": itype, "api_key": inst.api_key}


@app.get("/admin/instances")
async def list_instances(x_api_key: str = Header(default="")):
    require_admin(x_api_key)
    return {"instances": [
        {**i.summary(), "api_key": i.api_key} for i in INSTANCES.values()
    ]}


@app.post("/admin/instances/{name}/webhook")
async def set_webhook(name: str, body: WebhookBody, x_api_key: str = Header(default="")):
    require_admin(x_api_key)
    inst = get_inst(name)
    inst.webhook_url = body.url.rstrip("/")
    save_instances()
    return {"ok": True, "webhook_url": inst.webhook_url}


@app.delete("/admin/instances/{name}")
async def delete_instance(name: str, x_api_key: str = Header(default="")):
    require_admin(x_api_key)
    inst = get_inst(name)
    if inst.type == "vk":
        await VK.act_reset(inst)  # сессия + браузерный профиль + longpoll
    else:
        await stop_client(inst)
    base = os.path.join(SESSIONS_DIR, inst.name)
    for suffix in (".db", ".session", ".session-journal"):
        p = base + suffix
        if os.path.exists(p):
            os.remove(p)
    del INSTANCES[name]
    save_instances()
    return {"ok": True, "deleted": name}


# ---------------------------------------------------------------- routes: per-instance


@app.get("/i/{name}/status")
async def i_status(name: str, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return {**inst.status(), "webhook_url_set": bool(inst.webhook_url or GLOBAL_WEBHOOK_URL)}


@app.post("/i/{name}/login")
async def i_login(name: str, body: LoginBody, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return await act_login(inst, body.phone, password=body.password or "")


@app.post("/i/{name}/code")
async def i_code(name: str, body: CodeBody, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return await act_code(inst, body.code)


@app.post("/i/{name}/password")
async def i_password(name: str, body: PasswordBody, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return await act_password(inst, body.password)


@app.post("/i/{name}/sendText")
async def i_send_text(name: str, body: SendTextBody, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return await act_send_text(inst, body)


@app.post("/i/{name}/reset")
async def i_reset(name: str, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return await act_reset(inst)


@app.get("/i/{name}/recent")
async def i_recent(name: str, x_api_key: str = Header(default="")):
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    return {"items": list(inst.recent)}


@app.post("/i/{name}/qr")
async def i_qr(name: str, x_api_key: str = Header(default="")):
    """Telegram/VK: начать вход по QR-коду (как в WhatsApp Web)."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type == "telegram":
        require_tg()
        return await TG.qr_start(inst)
    if inst.type == "vk":
        return await VK.qr_start(inst)
    raise HTTPException(status_code=400, detail="qr: только telegram/vk-инстансы")


@app.get("/i/{name}/qr.png")
async def i_qr_png(name: str, x_api_key: str = Header(default="")):
    """Telegram/VK: текущий QR-код картинкой PNG."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type == "telegram":
        url = inst.qr_url
        if not url:
            raise HTTPException(status_code=404, detail="нет активного qr")
        return Response(content=TG.qr_png_bytes(url), media_type="image/png")
    if inst.type == "vk":
        return Response(content=await VK.qr_png_bytes(inst), media_type="image/png")
    raise HTTPException(status_code=400, detail="qr: только telegram/vk-инстансы")


@app.get("/i/{name}/getFile")
async def i_get_file(name: str, message_id: int, chat_id: Optional[int] = None,
                     x_api_key: str = Header(default="")):
    """Telegram: скачать вложение из сообщения."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type != "telegram":
        raise HTTPException(status_code=400, detail="getFile: только для telegram-инстансов")
    require_tg()
    if chat_id is None:
        raise HTTPException(status_code=400, detail="need chat_id")
    return await TG.get_file(inst, chat_id, message_id)


@app.get("/i/{name}/entity")
async def i_entity(name: str, query: str = "", x_api_key: str = Header(default="")):
    """Telegram/VK: @username/ссылка/id -> инфо о чате (числовой chatId)."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type == "telegram":
        require_tg()
        return await TG.resolve_entity(inst, query)
    if inst.type == "vk":
        return await VK.resolve_entity(inst, query)
    raise HTTPException(status_code=501, detail="entity: только telegram/vk-инстансы")


@app.get("/i/{name}/dialogs")
async def i_dialogs(name: str, limit: int = 100, x_api_key: str = Header(default="")):
    """Telegram/VK: список диалогов аккаунта."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type == "telegram":
        require_tg()
        return await TG.list_dialogs(inst, limit)
    if inst.type == "vk":
        return await VK.list_dialogs(inst, limit)
    raise HTTPException(status_code=501, detail="dialogs: только telegram/vk-инстансы")


@app.get("/i/{name}/topics")
async def i_topics(name: str, query: str = "", limit: int = 200,
                   x_api_key: str = Header(default="")):
    """Telegram: темы форума у группы."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type != "telegram":
        raise HTTPException(status_code=501, detail="topics: только telegram-инстансы")
    require_tg()
    return await TG.list_topics(inst, query, limit)


@app.get("/i/{name}/history")
async def i_history(name: str, query: str = "", limit: int = 20, topic_id: Optional[int] = None,
                    x_api_key: str = Header(default="")):
    """Telegram/VK: последние сообщения чата (query: @username, ссылка, chat_id/peer_id)."""
    inst = get_inst(name)
    require_inst(x_api_key, inst)
    if inst.type == "telegram":
        require_tg()
        return await TG.read_history(inst, query, limit, topic_id)
    if inst.type == "vk":
        return await VK.read_history(inst, query, limit)
    raise HTTPException(status_code=501, detail="history: только telegram/vk-инстансы")


# ---------------------------------------------------------------- legacy routes (default instance)

@app.get("/status")
async def legacy_status(x_api_key: str = Header(default="")):
    return await i_status("default", x_api_key)


@app.post("/login")
async def legacy_login(body: LoginBody, x_api_key: str = Header(default="")):
    return await i_login("default", body, x_api_key)


@app.post("/code")
async def legacy_code(body: CodeBody, x_api_key: str = Header(default="")):
    return await i_code("default", body, x_api_key)


@app.post("/password")
async def legacy_password(body: PasswordBody, x_api_key: str = Header(default="")):
    return await i_password("default", body, x_api_key)


@app.post("/sendText")
async def legacy_send_text(body: SendTextBody, x_api_key: str = Header(default="")):
    return await i_send_text("default", body, x_api_key)


@app.post("/reset")
async def legacy_reset(x_api_key: str = Header(default="")):
    return await i_reset("default", x_api_key)


@app.get("/recent")
async def legacy_recent(x_api_key: str = Header(default="")):
    return await i_recent("default", x_api_key)
