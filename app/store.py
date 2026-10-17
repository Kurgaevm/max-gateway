"""Хранилище сообщений: SQLite, чаты + сообщения, поиск."""

import json
import os
import sqlite3
import threading
import time
from typing import Optional

DB_PATH = os.environ.get("STORE_DB",
    os.path.join(os.environ.get("WORK_DIR", "/data"), "messages.db"))

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.row_factory = sqlite3.Row
    return _conn


def init() -> None:
    with _lock:
        c = _db()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS chats(
            inst TEXT, chat_key TEXT,
            title TEXT, ctype TEXT, username TEXT, phone TEXT,
            last_ts INTEGER, last_text TEXT, last_dir TEXT,
            msg_count INTEGER DEFAULT 0, unread INTEGER DEFAULT 0,
            PRIMARY KEY(inst, chat_key)
        );
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inst TEXT, chat_key TEXT, msg_id TEXT, ts INTEGER, dir TEXT,
            sender_id TEXT, sender_name TEXT, kind TEXT, text TEXT,
            media TEXT,
            UNIQUE(inst, chat_key, msg_id, dir)
        );
        CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(inst, chat_key, ts);
        CREATE INDEX IF NOT EXISTS idx_msg_text ON messages(text);
        """)
        c.commit()


# ---------------------------------------------------------------- record


def record(inst: str, chat_key: str, msg_id: str, ts: int, direction: str,
           sender_id: str = "", sender_name: str = "", kind: str = "text",
           text: str = "", media: str = "",
           chat_title: Optional[str] = None, ctype: Optional[str] = None,
           username: Optional[str] = None, phone: Optional[str] = None,
           count_unread: bool = False) -> None:
    """Записать сообщение (если новое) и обновить сводку чата."""
    with _lock:
        c = _db()
        try:
            cur = c.execute(
                "INSERT OR IGNORE INTO messages"
                " (inst,chat_key,msg_id,ts,dir,sender_id,sender_name,kind,text,media)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (inst, chat_key, msg_id, ts, direction,
                 sender_id, sender_name, kind, text, media))
            if cur.rowcount:
                c.execute(
                    "INSERT OR IGNORE INTO chats(inst,chat_key,msg_count)"
                    " VALUES(?,?,0)", (inst, chat_key))
                # title/ctype/... обновляем только если были NULL:
                # явные метаданные импорта (touch_chat) не затираем
                sets = ["last_ts=?", "last_text=?", "last_dir=?",
                        "msg_count=msg_count+1"]
                vals: list = [ts, (text or kind)[:200], direction]
                if chat_title:
                    sets.append("title=COALESCE(title,?)"); vals.append(chat_title)
                if ctype:
                    sets.append("ctype=COALESCE(ctype,?)"); vals.append(ctype)
                if username:
                    sets.append("username=COALESCE(username,?)"); vals.append(username)
                if phone:
                    sets.append("phone=COALESCE(phone,?)"); vals.append(phone)
                if count_unread and direction == "in":
                    sets.append("unread=unread+1")
                vals.extend([inst, chat_key])
                c.execute(
                    f"UPDATE chats SET {','.join(sets)}"
                    f" WHERE inst=? AND chat_key=?", vals)
            c.commit()
        except Exception:
            c.rollback()
            raise


def touch_chat(inst: str, chat_key: str, title: Optional[str] = None,
               ctype: Optional[str] = None, username: Optional[str] = None,
               phone: Optional[str] = None) -> None:
    """Upsert чата без сообщения (метаданные диалога при импорте)."""
    with _lock:
        c = _db()
        c.execute("INSERT OR IGNORE INTO chats(inst,chat_key) VALUES(?,?)",
                  (inst, chat_key))
        sets, vals = [], []
        if title:
            sets.append("title=?"); vals.append(title)
        if ctype:
            sets.append("ctype=?"); vals.append(ctype)
        if username:
            sets.append("username=?"); vals.append(username)
        if phone:
            sets.append("phone=?"); vals.append(phone)
        if sets:
            vals.extend([inst, chat_key])
            c.execute(f"UPDATE chats SET {','.join(sets)}"
                      f" WHERE inst=? AND chat_key=?", vals)
        c.commit()


# ---------------------------------------------------------------- record from payload


def record_payload(inst_name: str, inst_type: str, payload: dict,
                   ts: Optional[int] = None) -> None:
    """Записать входящее из Green-API payload (live-вебхук, любые каналы)."""
    sd = payload.get("senderData") or {}
    md = payload.get("messageData") or {}
    chat_key = str(sd.get("chatId") or sd.get("rawChatId") or "")
    msg_id = str(payload.get("idMessage") or "")
    ts = int(ts or payload.get("timestamp") or time.time())
    sender_id = str(sd.get("senderId") or "")
    sender_name = str(sd.get("senderName") or "")

    kind, text, media = "text", "", ""
    tm = md.get("textMessageData")
    if tm:
        text = (tm.get("textMessage") or "")[:4000]
    vm = md.get("voiceMessageData")
    if vm and kind == "text" and not text:
        kind = "voice"
        text = vm.get("transcript") or ""
        media = json.dumps({"duration": vm.get("duration"),
                            "transcript": vm.get("transcript")})
    am = md.get("attachmentMessage")
    if am and kind == "text" and not text:
        attaches = am.get("attaches") or []
        if attaches:
            kind = str(attaches[0].get("type", "file")).lower()
        media = json.dumps(attaches)

    record(inst_name, chat_key, msg_id, ts, "in",
           sender_id=sender_id, sender_name=sender_name,
           kind=kind, text=text, media=media,
           chat_title=sender_name or None, ctype="user",
           count_unread=True)


# ---------------------------------------------------------------- queries


def list_chats(insts: Optional[list] = None, q: str = "",
               limit: int = 200, unread_only: bool = False) -> list[dict]:
    with _lock:
        c = _db()
        wh, vals = [], []
        if insts:
            ph = ",".join("?" for _ in insts)
            wh.append(f"inst IN ({ph})"); vals.extend(insts)
        if q:
            wh.append("(title LIKE ? OR last_text LIKE ? OR username LIKE ?)")
            v = f"%{q}%"
            vals.extend([v, v, v])
        if unread_only:
            wh.append("unread > 0")
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        rows = c.execute(
            f"SELECT inst,chat_key,title,ctype,username,phone,"
            f"last_ts,last_text,last_dir,msg_count,unread"
            f" FROM chats{where} ORDER BY last_ts DESC LIMIT ?",
            vals + [limit]).fetchall()
        return [dict(r) for r in rows]


def get_thread(inst: str, chat_key: str, before_ts: Optional[int] = None,
               limit: int = 60) -> list[dict]:
    with _lock:
        c = _db()
        wh, vals = ["inst=?", "chat_key=?"], [inst, chat_key]
        if before_ts:
            wh.append("ts<?"); vals.append(before_ts)
        where = " WHERE " + " AND ".join(wh)
        rows = c.execute(
            f"SELECT id,msg_id,ts,dir,sender_id,sender_name,kind,text,media"
            f" FROM messages{where} ORDER BY ts DESC LIMIT ?",
            vals + [limit]).fetchall()
        return [dict(r) for r in reversed(rows)]


def search_messages(insts: Optional[list] = None, q: str = "",
                    limit: int = 100) -> list[dict]:
    with _lock:
        c = _db()
        wh, vals = [], []
        if insts:
            ph = ",".join("?" for _ in insts)
            wh.append(f"inst IN ({ph})"); vals.extend(insts)
        if q:
            wh.append("text LIKE ?"); vals.append(f"%{q}%")
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        rows = c.execute(
            f"SELECT inst,chat_key,msg_id,ts,dir,sender_name,kind,text"
            f" FROM messages{where} ORDER BY ts DESC LIMIT ?",
            vals + [limit]).fetchall()
        return [dict(r) for r in rows]


def mark_read(inst: str, chat_key: str) -> None:
    with _lock:
        c = _db()
        c.execute("UPDATE chats SET unread=0 WHERE inst=? AND chat_key=?",
                  (inst, chat_key))
        c.commit()


def stats() -> dict:
    with _lock:
        c = _db()
        chats = c.execute("SELECT inst, COUNT(*) AS cnt FROM chats GROUP BY inst").fetchall()
        msgs = c.execute("SELECT inst, COUNT(*) AS cnt FROM messages GROUP BY inst").fetchall()
        return {
            "chats": {r["inst"]: r["cnt"] for r in chats},
            "messages": {r["inst"]: r["cnt"] for r in msgs},
        }
