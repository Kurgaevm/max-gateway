"""Telegram-адаптер шлюза: инстансы Telegram на тех же началах, что MAX.

Работает через Telethon (MTProto, user-аккаунт). Нужны api_id/api_hash
своего приложения (https://my.telegram.org/apps): TG_API_ID, TG_API_HASH в .env

- Вход по телефону + код из Telegram (+ облачный пароль 2FA)
- Вход по QR: POST /i/{name}/qr, картинка GET /i/{name}/qr.png
  (в телефоне: Telegram -> Настройки -> Устройства -> Подключить устройство)
- Вебхук входящих в формате Green-API, typeInstance: telegram
- POST /i/{name}/sendText: phone или chat_id
- GET /i/{name}/getFile?chat_id=..&message_id=.. — скачать вложение
- Голосовые: скачивание + расшифровка через настроенный STT
"""

import asyncio
import io
import logging
import os
import time
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import HTTPException
from fastapi.responses import Response
from telethon import TelegramClient, errors, events, utils

log = logging.getLogger("maxgw.tg")

WORK_DIR = os.environ.get("WORK_DIR", "/data")
SESSIONS_DIR = os.path.join(WORK_DIR, "sessions")
TG_API_ID = int(os.environ.get("TG_API_ID", "0") or 0)
TG_API_HASH = os.environ.get("TG_API_HASH", "").strip()
MAX_VOICE_BYTES = 20 * 1024 * 1024
QR_TTL = 45  # сек, срок жизни qr-токена Telegram


def _require_api() -> None:
    if not (TG_API_ID and TG_API_HASH):
        raise HTTPException(
            status_code=503,
            detail="TG_API_ID / TG_API_HASH not set in .env (https://my.telegram.org/apps)",
        )


def session_path(inst: Any) -> str:
    return os.path.join(SESSIONS_DIR, inst.name + ".session")


# ---------------------------------------------------------------- client


def attach_handlers(inst: Any, client: TelegramClient) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def _on_message(event: Any) -> None:
        try:
            await handle_incoming(inst, event)
        except Exception:  # noqa: BLE001
            log.exception("[%s] incoming handler failed", inst.name)


def build_client(inst: Any) -> TelegramClient:
    _require_api()
    client = TelegramClient(
        session_path(inst),
        TG_API_ID,
        TG_API_HASH,
        connection_retries=None,
        retry_delay=3,
        auto_reconnect=True,
    )
    attach_handlers(inst, client)
    return client


async def ensure_client(inst: Any) -> TelegramClient:
    if inst.client is None:
        inst.client = build_client(inst)
        await inst.client.connect()
    elif not inst.client.is_connected():
        await inst.client.connect()
    return inst.client


async def _begin_run(inst: Any) -> None:
    """Фоновая задача: держит соединение до отключения."""

    async def runner() -> None:
        try:
            await inst.client.run_until_disconnected()
        except asyncio.CancelledError:
            pass
        except Exception as e:  # noqa: BLE001
            inst.stage = "error"
            inst.error = f"{type(e).__name__}: {e}"
        finally:
            inst.connected = False

    inst.task = asyncio.create_task(runner())


async def _mark_authorized(inst: Any, me: Any) -> None:
    from main import save_instances  # поздний импорт, чтобы не было цикла

    await qr_stop(inst)
    inst.stage = "authorized"
    inst.connected = True
    inst.error = None
    inst.hint = None
    inst.me = me.id
    ph = getattr(me, "phone", None)
    if ph:
        inst.phone = "+" + str(ph).lstrip("+")
    save_instances()
    log.info("[%s] AUTHORIZED (telegram) as user_id=%s", inst.name, me.id)
    _begin_run(inst)


async def start_client(inst: Any) -> None:
    """Подключение при старте шлюза: живая сессия поднимается автоматически."""
    client = await ensure_client(inst)
    inst.connected = True
    if await client.is_user_authorized():
        me = await client.get_me()
        await _mark_authorized(inst, me)
    else:
        inst.stage = "idle"


async def stop_client(inst: Any) -> None:
    await qr_stop(inst)
    if inst.client is not None:
        try:
            await inst.client.disconnect()
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


# ---------------------------------------------------------------- login flow


async def act_login(inst: Any, phone_raw: str) -> dict:
    from main import normalize_phone, save_instances

    phone = normalize_phone(phone_raw)
    if inst.phone and inst.phone != phone:
        await stop_client(inst)
        for p in (session_path(inst), session_path(inst) + "-journal"):
            if os.path.exists(p):
                os.remove(p)
    inst.phone = phone
    save_instances()
    await qr_stop(inst)
    client = await ensure_client(inst)
    inst.connected = True
    if await client.is_user_authorized():
        me = await client.get_me()
        await _mark_authorized(inst, me)
        return {"ok": True, "instance": inst.name, "phone": phone, "stage": inst.stage}
    try:
        sent = await client.send_code_request(phone)
    except errors.RPCError as e:
        inst.stage = "error"
        inst.error = f"{type(e).__name__}: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e
    inst.tg_code_hash = sent.phone_code_hash
    inst.stage = "awaiting_code"
    inst.error = None
    save_instances()
    log.info("[%s] tg code requested for %s", inst.name, phone)
    return {"ok": True, "instance": inst.name, "phone": phone, "stage": inst.stage}


async def act_code(inst: Any, code: str) -> dict:
    if inst.stage != "awaiting_code":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    client = await ensure_client(inst)
    inst.connected = True
    try:
        me = await client.sign_in(
            phone=inst.phone, code=code.strip(), phone_code_hash=inst.tg_code_hash
        )
    except errors.SessionPasswordNeededError:
        inst.stage = "awaiting_password"
        inst.hint = "облачный пароль 2FA"
        return {"ok": True, "stage": inst.stage}
    except errors.RPCError as e:
        inst.stage = "error"
        inst.error = f"{type(e).__name__}: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e
    if me is None:
        me = await client.get_me()
    await _mark_authorized(inst, me)
    return {"ok": True, "stage": inst.stage}


async def act_password(inst: Any, password: str) -> dict:
    if inst.stage != "awaiting_password":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    client = await ensure_client(inst)
    try:
        me = await client.sign_in(password=password)
    except errors.RPCError as e:
        inst.error = f"{type(e).__name__}: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e
    await _mark_authorized(inst, me)
    return {"ok": True, "stage": inst.stage}


async def act_reset(inst: Any) -> dict:
    await stop_client(inst)
    sp = session_path(inst)
    removed = False
    for p in (sp, sp + "-journal"):
        if os.path.exists(p):
            os.remove(p)
            removed = True
    inst.stage = "idle"
    inst.me = None
    inst.error = None
    inst.hint = None
    inst.tg_code_hash = None
    inst.last_incoming = None
    return {"ok": True, "session_deleted": removed, "stage": inst.stage}


# ---------------------------------------------------------------- QR login


async def qr_stop(inst: Any) -> None:
    t = getattr(inst, "qr_task", None)
    if t is not None and not t.done():
        t.cancel()
        try:
            await t
        except BaseException:  # noqa: BLE001
            pass
    inst.qr_task = None
    inst.qr_url = None


async def qr_start(inst: Any) -> dict:
    _require_api()
    await qr_stop(inst)
    client = await ensure_client(inst)
    inst.connected = True
    if await client.is_user_authorized():
        me = await client.get_me()
        await _mark_authorized(inst, me)
        return {"ok": True, "stage": inst.stage, "qr_url": None}
    inst.error = None
    inst.stage = "awaiting_qr"

    async def loop() -> None:
        while True:
            try:
                qr = await client.qr_login()
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                inst.stage = "error"
                inst.error = f"qr_login: {e}"
                return
            inst.qr_url = qr.url  # tg://login?token=...
            log.info("[%s] new qr token", inst.name)
            try:
                await qr.wait(timeout=QR_TTL)
            except asyncio.TimeoutError:
                continue  # токен истёк, генерируем новый
            except asyncio.CancelledError:
                return
            me = await client.get_me()
            await _mark_authorized(inst, me)
            return

    inst.qr_task = asyncio.create_task(loop())
    for _ in range(100):
        if inst.qr_url:
            break
        await asyncio.sleep(0.1)
    return {"ok": True, "stage": inst.stage, "qr_url": inst.qr_url}


def qr_png_bytes(url: str) -> bytes:
    import qrcode  # лёгкая зависимость образа

    img = qrcode.make(url, box_size=8, border=2)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------- incoming


def _attach_kind(msg: Any) -> str:
    if msg.voice is not None:
        return "voice"
    if msg.video_note is not None:
        return "video_note"
    if msg.video is not None:
        return "video"
    if msg.photo is not None:
        return "photo"
    if msg.sticker is not None:
        return "sticker"
    if msg.audio is not None:
        return "audio"
    if msg.document is not None:
        return "document"
    return "file"


async def handle_voice(inst: Any, client: TelegramClient, msg: Any) -> dict:
    from main import transcribe

    transcript: Optional[str] = None
    try:
        data = await client.download_media(msg, file=bytes)
        if data and len(data) <= MAX_VOICE_BYTES:
            transcript = await transcribe(bytes(data), "audio/ogg")
    except Exception as e:  # noqa: BLE001
        log.warning("[%s] voice download failed: %s", inst.name, e)
    md: dict = {
        "voiceMessageData": {
            "duration": getattr(getattr(msg, "file", None), "duration", None),
            "transcript": transcript,
        },
    }
    if transcript:
        md["typeMessage"] = "textMessage"
        md["textMessageData"] = {"textMessage": transcript}
    else:
        md["typeMessage"] = "voiceMessage"
    return md


async def handle_incoming(inst: Any, event: Any) -> None:
    from main import GLOBAL_WEBHOOK_URL

    msg = event.message
    me_id = inst.me
    sender = await event.get_sender()
    sid = getattr(sender, "id", None)
    if me_id is not None and sid is not None and int(sid) == int(me_id):
        return  # собственное исходящее

    text = (msg.message or "").strip()
    chat_id = event.chat_id  # маркированный id: в личных чатах = id собеседника
    mid = msg.id

    if msg.voice is not None:
        message_data = await handle_voice(inst, event.client, msg)
    elif msg.media is not None:
        f = getattr(msg, "file", None)
        message_data = {
            "typeMessage": "attachmentMessage",
            "attaches": [
                {
                    "type": _attach_kind(msg),
                    "file_name": getattr(f, "name", None),
                    "size": getattr(f, "size", None),
                    "mime": getattr(f, "mime_type", None),
                }
            ],
        }
        if text:
            message_data["caption"] = text
    elif text:
        message_data = {
            "typeMessage": "textMessage",
            "textMessageData": {"textMessage": text},
        }
    else:
        return

    sname = None
    if sender is not None:
        sname = utils.get_display_name(sender) or None
    sphone = None
    ph = getattr(sender, "phone", None)
    if ph:
        sphone = "+" + str(ph)

    payload = {
        "typeWebhook": "incomingMessageReceived",
        "instanceData": {
            "idInstance": 1,
            "instanceName": inst.name,
            "wid": inst.phone,
            "typeInstance": "telegram",
        },
        "timestamp": int(time.time()),
        "idMessage": str(mid),
        "senderData": {
            "chatId": str(chat_id) if chat_id is not None else None,
            "rawChatId": str(chat_id) if chat_id is not None else None,
            "senderId": str(sid) if sid is not None else None,
            "senderName": sname,
            "senderPhoneNumber": sphone,
        },
        "messageData": message_data,
    }
    inst.last_incoming = payload
    inst.recent.append({"ts": time.time(), "payload": payload})
    preview = text or (msg.voice is not None and "<голосовое>") or "<вложение>"
    log.info("[%s] tg incoming from %s: %s", inst.name, sid, str(preview)[:120])

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


# ---------------------------------------------------------------- actions


async def act_send_text(inst: Any, body: Any) -> dict:
    if inst.client is None or inst.stage != "authorized":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    entity: Any = None
    if body.chat_id is not None:
        entity = int(body.chat_id)
    elif body.phone:
        from main import normalize_phone

        entity = normalize_phone(body.phone)
    else:
        raise HTTPException(status_code=400, detail="need phone or chat_id")
    try:
        m = await inst.client.send_message(entity, body.message)
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail="chat not found: передайте chat_id из входящего вебхука или телефон контакта",
        ) from e
    except errors.RPCError as e:
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}") from e
    return {
        "ok": True,
        "instance": inst.name,
        "chatId": str(m.chat_id),
        "messageId": str(m.id),
    }


async def get_file(inst: Any, chat_id: int, message_id: int) -> Response:
    if inst.client is None:
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    msgs = await inst.client.get_messages(int(chat_id), ids=int(message_id))
    if msgs is None or not getattr(msgs, "media", None):
        raise HTTPException(status_code=404, detail="message has no media")
    data = await inst.client.download_media(msgs, file=bytes)
    if not data:
        raise HTTPException(status_code=404, detail="download failed")
    f = getattr(msgs, "file", None)
    name = getattr(f, "name", None) or ("msg_" + str(message_id))
    mime = getattr(f, "mime_type", None) or "application/octet-stream"
    return Response(
        content=bytes(data),
        media_type=mime,
        headers={
            "Content-Disposition": "attachment; filename*=UTF-8" + "''" + quote(name)
        },
    )
