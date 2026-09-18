"""MAX Шлюз: self-hosted шлюз для мессенджера MAX (мультиаккаунтный).

FastAPI + PyMax (неофициальный внутренний API MAX).
- Несколько аккаунтов (инстансов) на одну установку
- Вход по телефону + SMS-код (код передаётся через API/веб-интерфейс)
- Входящие сообщения -> вебхук на URL инстанса (формат совместим с Green-API)
- Голосовые сообщения: скачивание + расшифровка через OpenAI-совместимый STT
- Исходящие: POST /i/{name}/sendText
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
from fastapi.responses import FileResponse
from pydantic import BaseModel

from pymax import Client, ExtraConfig, RegistrationConfig

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("maxgw")

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
                 created: Optional[float] = None) -> None:
        self.name = name
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

    def summary(self) -> dict:
        return {
            "name": self.name,
            "phone": self.phone,
            "stage": self.stage,
            "connected": self.connected,
            "me": self.me,
            "webhook_url": self.webhook_url or GLOBAL_WEBHOOK_URL or None,
        }

    def status(self) -> dict:
        return {
            "instance": self.name,
            "stage": self.stage,
            "phone": self.phone,
            "me": self.me,
            "hint": self.hint,
            "error": self.error,
            "connected": bool(self.client is not None and getattr(self.client, "is_connected", False)),
            "last_incoming": self.last_incoming,
            "last_webhook": self.last_webhook,
        }


INSTANCES: dict[str, Inst] = {}


def save_instances() -> None:
    data = {
        "instances": [
            {
                "name": i.name,
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


def sender_display_name(c: Client, user_id: Any) -> Optional[str]:
    if user_id is None:
        return None
    try:
        u = c.get_cached_user(int(user_id))
    except Exception:  # noqa: BLE001
        return None
    if u is None:
        return None
    parts = []
    for n in list(getattr(u, "names", None) or [])[:1]:
        for attr in ("first_name", "last_name"):
            v = getattr(n, attr, None)
            if isinstance(v, str) and v:
                parts.append(v)
    return " ".join(parts) or None


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

    payload = {
        "typeWebhook": "incomingMessageReceived",
        "instanceData": {
            "idInstance": 1,
            "instanceName": inst.name,
            "wid": inst.phone,
            "typeInstance": "max",
        },
        "timestamp": int(time.time()),
        "idMessage": str(mid) if mid is not None else uuid.uuid4().hex,
        "senderData": {
            "chatId": str(api_chat_id) if api_chat_id is not None else None,
            "rawChatId": str(chat_id) if chat_id is not None else None,
            "senderId": str(sid) if sid is not None else None,
            "senderName": sender_display_name(c, sid),
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
        if inst.phone:
            await start_client(inst)
    yield
    for inst in list(INSTANCES.values()):
        await stop_client(inst)


app = FastAPI(title="max-gateway", lifespan=lifespan)


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


# ---------------------------------------------------------------- models


class LoginBody(BaseModel):
    phone: str


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


class WebhookBody(BaseModel):
    url: str = ""


# ---------------------------------------------------------------- actions


async def act_login(inst: Inst, phone_raw: str) -> dict:
    phone = normalize_phone(phone_raw)
    old_session = os.path.join(SESSIONS_DIR, f"{inst.name}.db")
    if inst.phone and inst.phone != phone and os.path.exists(old_session):
        os.remove(old_session)  # смена аккаунта: сносим старую сессию
    inst.phone = phone
    save_instances()
    await start_client(inst)
    return {"ok": True, "instance": inst.name, "phone": phone, "stage": inst.stage}


async def act_code(inst: Inst, code: str) -> dict:
    if inst.stage != "awaiting_code":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    await inst.sms_queue.put(code.strip())
    return {"ok": True}


async def act_password(inst: Inst, password: str) -> dict:
    if inst.stage != "awaiting_password":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    await inst.pw_queue.put(password)
    return {"ok": True}


async def act_reset(inst: Inst) -> dict:
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
    return {"ok": True, "instances": len(INSTANCES)}


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
    if name in INSTANCES:
        raise HTTPException(status_code=409, detail="instance already exists")
    inst = Inst(name, phone=normalize_phone(body.phone) if body.phone else None,
                webhook_url=body.webhook_url.rstrip("/"))
    INSTANCES[name] = inst
    save_instances()
    return {"ok": True, "name": name, "api_key": inst.api_key}


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
    await stop_client(inst)
    session_path = os.path.join(SESSIONS_DIR, f"{inst.name}.db")
    if os.path.exists(session_path):
        os.remove(session_path)
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
    return await act_login(inst, body.phone)


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
