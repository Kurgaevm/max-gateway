"""MAX gateway: self-hosted Green-API-like bridge for MAX messenger.

FastAPI + PyMax (unofficial internal MAX API).
- Login by phone + SMS code (code delivered via POST /code)
- Incoming messages -> optional webhook (Green-API compatible payload)
- Outgoing: POST /sendText (by phone or chat_id)
"""

import asyncio
import logging
import os
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

API_KEY = os.environ.get("GATEWAY_API_KEY", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
WORK_DIR = os.environ.get("WORK_DIR", "/data")
SESSION_NAME = os.environ.get("SESSION_NAME", "maxgw.db")
PHONE = os.environ.get("PHONE", "").strip()
CODE_TIMEOUT = int(os.environ.get("CODE_TIMEOUT", "900"))

state: dict[str, Any] = {
    "stage": "idle",  # idle|connecting|awaiting_code|awaiting_password|authorized|error
    "phone": None,
    "me": None,
    "hint": None,
    "error": None,
    "last_incoming": None,
    "last_webhook": None,
    "started_at": time.time(),
}
recent: deque = deque(maxlen=25)

sms_queue: asyncio.Queue = asyncio.Queue()
pw_queue: asyncio.Queue = asyncio.Queue()

client: Optional[Client] = None
client_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------- providers


class QueueSmsProvider:
    async def get_code(self, phone: str) -> str:
        state.update(stage="awaiting_code", phone=phone, error=None)
        log.info("SMS code requested by MAX for %s", phone)
        try:
            code = await asyncio.wait_for(sms_queue.get(), timeout=CODE_TIMEOUT)
        except asyncio.TimeoutError:
            state.update(stage="error", error="timeout waiting for SMS code")
            raise
        state.update(stage="connecting")
        return code


class QueuePasswordProvider:
    async def get_password(self, hint: Optional[str] = None) -> str:
        state.update(stage="awaiting_password", hint=hint)
        try:
            pw = await asyncio.wait_for(pw_queue.get(), timeout=CODE_TIMEOUT)
        except asyncio.TimeoutError:
            state.update(stage="error", error="timeout waiting for 2FA password")
            raise
        state.update(stage="connecting")
        return pw


def build_client(phone: str) -> Client:
    extra = ExtraConfig(
        log_level=os.environ.get("PYMAX_LOG_LEVEL", "INFO"),
        reconnect=True,
        reconnect_delay=3,
        registration_config=RegistrationConfig(
            first_name=os.environ.get("REG_FIRST_NAME", "Mao"),
            last_name=os.environ.get("REG_LAST_NAME", "Gateway"),
        ),
    )
    return Client(
        phone=phone,
        work_dir=WORK_DIR,
        session_name=SESSION_NAME,
        extra_config=extra,
        sms_code_provider=QueueSmsProvider(),
        password_provider=QueuePasswordProvider(),
    )


# ---------------------------------------------------------------- helpers


def normalize_phone(p: str) -> str:
    d = "".join(ch for ch in p if ch.isdigit())
    if d.startswith("8"):
        d = "7" + d[1:]
    if len(d) == 10:
        d = "7" + d
    return "+" + d


def sender_display_name(c: Client, user_id: Any) -> Optional[str]:
    """Sender display name from the client's contact cache (no network call)."""
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


async def handle_incoming(message: Any, c: Client) -> None:
    me_id = state.get("me")
    sid = getattr(message, "sender", None)  # int | None: sender user id
    if me_id is not None and sid is not None and int(sid) == int(me_id):
        return  # own outgoing echo
    text = getattr(message, "text", None)
    chat_id = getattr(message, "chat_id", None)
    mid = getattr(message, "id", None)
    attaches = list(getattr(message, "attaches", None) or [])

    if text:
        message_data: dict[str, Any] = {
            "typeMessage": "textMessage",
            "textMessageData": {"textMessage": text},
        }
    else:
        message_data = {
            "typeMessage": "attachmentMessage",
            "attaches": [
                {
                    "type": getattr(a, "type", None) or getattr(a, "_type", None),
                }
                for a in attaches
            ],
        }

    payload = {
        "typeWebhook": "incomingMessageReceived",
        "instanceData": {
            "idInstance": 1,
            "wid": state.get("phone"),
            "typeInstance": "max",
        },
        "timestamp": int(time.time()),
        "idMessage": str(mid) if mid is not None else uuid.uuid4().hex,
        "senderData": {
            "chatId": str(chat_id) if chat_id is not None else None,
            "senderId": str(sid) if sid is not None else None,
            "senderName": sender_display_name(c, sid),
        },
        "messageData": message_data,
    }
    state["last_incoming"] = payload
    recent.append({"ts": time.time(), "payload": payload})
    log.info("incoming from %s: %s", payload["senderData"]["senderId"], (text or "<attach>")[:120])

    if WEBHOOK_URL:
        try:
            async with httpx.AsyncClient(timeout=15) as hc:
                r = await hc.post(WEBHOOK_URL, json=payload)
                state["last_webhook"] = {"status": r.status_code}
                log.info("webhook delivered: %s", r.status_code)
        except Exception as e:  # noqa: BLE001
            state["last_webhook"] = {"error": str(e)}
            log.warning("webhook failed: %s", e)


def attach_handlers(c: Client) -> None:
    @c.on_start()
    async def _on_start(c: Client) -> None:
        me_id = None
        try:
            me_id = c.me.contact.id
        except Exception:  # noqa: BLE001
            pass
        state.update(stage="authorized", connected=True, me=me_id, error=None)
        log.info("AUTHORIZED as user_id=%s", me_id)

    @c.on_disconnect()
    async def _on_disc(exc: Exception, reconnect: bool, delay: float) -> None:
        state["connected"] = False
        log.warning("disconnected: %s reconnect=%s delay=%s", exc, reconnect, delay)

    @c.on_message()
    async def _on_message(message: Any, c: Client) -> None:
        try:
            await handle_incoming(message, c)
        except Exception:  # noqa: BLE001
            log.exception("incoming handler failed")


async def start_client(phone: str) -> None:
    global client, client_task
    if client_task is not None and not client_task.done():
        if client is not None:
            try:
                await client.stop()
            except Exception:  # noqa: BLE001
                pass
        client_task.cancel()
        try:
            await client_task
        except BaseException:  # noqa: BLE001
            pass
    client = build_client(phone)
    attach_handlers(client)
    state.update(stage="connecting", phone=phone, error=None, connected=False)

    async def runner() -> None:
        try:
            await client.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:  # noqa: BLE001
            state.update(stage="error", error=f"{type(e).__name__}: {e}", connected=False)
            log.exception("client crashed")

    client_task = asyncio.create_task(runner())


@asynccontextmanager
async def lifespan(app: FastAPI):
    phone = PHONE
    if not phone:
        try:
            with open(os.path.join(WORK_DIR, "phone.txt"), encoding="utf-8") as f:
                phone = f.read().strip()
        except Exception:  # noqa: BLE001
            phone = ""
    if phone:
        await start_client(phone)
    yield
    if client_task is not None:
        client_task.cancel()
        try:
            await client_task
        except BaseException:  # noqa: BLE001
            pass


app = FastAPI(title="max-gateway", lifespan=lifespan)


def auth(x_api_key: str) -> None:
    if not API_KEY or x_api_key != API_KEY:
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


# ---------------------------------------------------------------- endpoints


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/status")
async def status(x_api_key: str = Header(default="")):
    auth(x_api_key)
    connected = bool(client is not None and getattr(client, "is_connected", False))
    return {**state, "connected": connected, "webhook_url_set": bool(WEBHOOK_URL)}


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.post("/login")
async def login(body: LoginBody, x_api_key: str = Header(default="")):
    auth(x_api_key)
    phone = normalize_phone(body.phone)
    current = state.get("phone")
    session_path = os.path.join(WORK_DIR, SESSION_NAME)
    if current and current != phone and os.path.exists(session_path):
        os.remove(session_path)  # switching account: wipe previous session
    await start_client(phone)
    try:
        with open(os.path.join(WORK_DIR, "phone.txt"), "w", encoding="utf-8") as f:
            f.write(phone)
    except OSError:
        pass
    return {"ok": True, "phone": phone, "stage": state["stage"]}


@app.post("/code")
async def submit_code(body: CodeBody, x_api_key: str = Header(default="")):
    auth(x_api_key)
    if state["stage"] != "awaiting_code":
        raise HTTPException(status_code=409, detail=f"stage={state['stage']}")
    await sms_queue.put(body.code.strip())
    return {"ok": True}


@app.post("/password")
async def submit_password(body: PasswordBody, x_api_key: str = Header(default="")):
    auth(x_api_key)
    if state["stage"] != "awaiting_password":
        raise HTTPException(status_code=409, detail=f"stage={state['stage']}")
    await pw_queue.put(body.password)
    return {"ok": True}


@app.post("/reset")
async def reset(x_api_key: str = Header(default="")):
    auth(x_api_key)
    global client_task
    if client is not None:
        try:
            await client.stop()
        except Exception:  # noqa: BLE001
            pass
    if client_task is not None:
        client_task.cancel()
        try:
            await client_task
        except BaseException:  # noqa: BLE001
            pass
    session_path = os.path.join(WORK_DIR, SESSION_NAME)
    removed = False
    if os.path.exists(session_path):
        os.remove(session_path)
        removed = True
    state.update(stage="idle", me=None, error=None, connected=False, last_incoming=None)
    if state.get("phone"):
        await start_client(state["phone"])
    return {"ok": True, "session_deleted": removed, "stage": state["stage"]}


@app.post("/sendText")
async def send_text(body: SendTextBody, x_api_key: str = Header(default="")):
    auth(x_api_key)
    if client is None or state.get("stage") != "authorized":
        raise HTTPException(status_code=409, detail=f"stage={state['stage']}")
    chat_id = body.chat_id
    if chat_id is None:
        if not body.phone:
            raise HTTPException(status_code=400, detail="need phone or chat_id")
        contact = await client.search_by_phone(normalize_phone(body.phone))
        if contact is None:
            raise HTTPException(status_code=404, detail="user not found by phone")
        me_id = None
        try:
            me_id = client.me.contact.id
        except Exception:  # noqa: BLE001
            pass
        if me_id is None:
            raise HTTPException(status_code=500, detail="me unknown")
        res = client.get_chat_id(first_user_id=me_id, second_user_id=contact.id)
        if asyncio.iscoroutine(res):
            res = await res
        chat_id = res
    sent = await client.send_message(chat_id=chat_id, text=body.message)
    mid = getattr(sent, "id", None)
    return {"ok": True, "chatId": str(chat_id), "messageId": str(mid) if mid is not None else None}


@app.get("/recent")
async def get_recent(x_api_key: str = Header(default="")):
    auth(x_api_key)
    return {"items": list(recent)}
