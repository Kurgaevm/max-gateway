"""VK-адаптер шлюза: личные страницы ВК как инстансы (по образцу MAX/Telegram).

Неофициальный путь: web_token залогиненного браузера (ключ localStorage
`6287487:web_token:login:auth`), IP-bound. Поэтому шлюз держит собственный
headless Chromium (playwright) с персистентным профилем на инстанс:
- вход: телефон + пароль + код подтверждения (СМС/2FA);
- токен живёт недолго и ротируется: перечитываем из открытой страницы;
- все HTTP-запросы идут с того же IP, что и браузер (сам сервер).

Возможности:
- POST /i/{name}/login {phone, password} -> стадия awaiting_code
- POST /i/{name}/code {code} -> authorized, сессия сохранена
- POST /i/{name}/sendText {chat_id|domain} — отправка текста (peer_id)
- GET  /i/{name}/entity?query=... — id/домен -> карточка (users.get)
- GET  /i/{name}/dialogs, /i/{name}/history
- Входящие: longpoll (messages.getLongPollServer) -> вебхук Green-API, typeInstance: vk

Анти-бан: отправка через очередь, мин. интервал + джиттер; longpoll сам по себе
невидим (это просто чтение, как у открытой вкладки).
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
from typing import Any, Optional

import httpx
from fastapi import HTTPException

log = logging.getLogger("maxgw.vk")

WORK_DIR = os.environ.get("WORK_DIR", "/data")
SESSIONS_DIR = os.path.join(WORK_DIR, "sessions")
PROFILES_DIR = os.path.join(WORK_DIR, "vk_profiles")
API = "https://api.vk.com/method/"
API_V = "5.199"
WEB_TOKEN_KEY = "6287487:web_token:login:auth"  # как в реальном веб-клиенте
TOKEN_TTL = 6 * 3600          # считаем токен протухшим через 6ч и перечитываем
SEND_MIN_INTERVAL = 4.0       # анти-бан: мин. пауза между исходящими, сек
SEND_JITTER = (0.8, 3.2)      # + случайная задержка
LONGPOLL_WAIT = 25            # сек ожидания на цикл longpoll


# ---------------------------------------------------------------- session


def session_path(inst: Any) -> str:
    return os.path.join(SESSIONS_DIR, inst.name + ".vk.json")


def profile_dir(inst: Any) -> str:
    return os.path.join(PROFILES_DIR, inst.name)


def load_session(inst: Any) -> Optional[dict]:
    p = session_path(inst)
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def save_session(inst: Any, sess: dict) -> None:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    with open(session_path(inst), "w", encoding="utf-8") as f:
        json.dump(sess, f, ensure_ascii=False, indent=2)


def _vk_client(inst: Any) -> Optional[dict]:
    c = getattr(inst, "vk_sess", None)
    if isinstance(c, dict):
        return c
    sess = load_session(inst)
    if sess:
        inst.vk_sess = sess
        return sess
    return None


class VKError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"vk error {code}: {message}")
        self.code = code
        self.message = message


# ---------------------------------------------------------------- api core


async def api(inst: Any, method: str, **params: Any) -> dict:
    """Вызов метода ВК с web_token. При ошибке 5/10 (токен) — попытка refresh."""
    sess = _vk_client(inst)
    if not sess or not sess.get("web_token"):
        raise HTTPException(status_code=409, detail=f"stage={inst.stage or 'idle'} (нет web_token)")
    for attempt in (1, 2):
        p = {"access_token": sess["web_token"], "v": API_V, **params}
        if sess.get("device_id"):
            p.setdefault("device_id", sess["device_id"])
        async with httpx.AsyncClient(timeout=30) as hc:
            r = await hc.post(API + method, data=p)
        try:
            j = r.json()
        except ValueError as e:
            raise VKError(-1, f"bad json http {r.status_code}") from e
        err = j.get("error")
        if not err:
            return j.get("response", {})
        code, msg = int(err.get("error_code", 0)), str(err.get("error_msg", ""))[:200]
        if code in (5, 10) and attempt == 1:
            log.info("[%s] token rejected (%s), trying refresh", inst.name, code)
            refreshed = await refresh_token(inst)
            if refreshed:
                sess = _vk_client(inst) or {}
                if not sess.get("web_token"):
                    break
                continue
        if code == 6:  # too many requests per second
            await asyncio.sleep(0.7)
            continue
        raise VKError(code, msg)
    raise VKError(5, "token expired")


# ---------------------------------------------------------------- playwright


def _pw():
    try:
        from playwright.async_api import async_playwright  # noqa: PLC0415
        return async_playwright
    except ImportError as e:
        raise HTTPException(
            status_code=503,
            detail="playwright не установлен в образе (пересоберите контейнер)",
        ) from e


class _LoginCtx:
    """Держит живой браузер между запуском входа и вводом кода."""

    def __init__(self) -> None:
        self.pw = None
        self.browser = None
        self.context = None
        self.page = None


def _login_ctx(inst: Any) -> Optional[_LoginCtx]:
    return getattr(inst, "vk_login_ctx", None)


async def _kill_login_ctx(inst: Any) -> None:
    ctx = _login_ctx(inst)
    inst.vk_login_ctx = None
    if ctx is None:
        return
    try:
        await ctx.browser.close()
    except Exception:  # noqa: BLE001
        pass


async def _open_logged_page(inst: Any, headless: bool = True) -> tuple[Any, Any, Any]:
    """Открыть персистентный профиль на vk.com. Возврат (pw, ctx, page)."""
    async_playwright = _pw()
    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        profile_dir(inst),
        headless=headless,
        args=["--disable-dev-shm-usage", "--no-sandbox", "--lang=ru"],
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    page.set_default_timeout(25000)
    await page.goto("https://vk.com/im", wait_until="domcontentloaded")
    return pw, ctx, page


def _parse_web_token(raw: Optional[str]) -> Optional[str]:
    """Значение ключа бывает сырым vk1.a... и JSON {"access_token": ...}."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.startswith("{"):
        try:
            return str(json.loads(raw).get("access_token") or "").strip() or None
        except ValueError:
            return None
    return raw


async def _extract_token(page: Any) -> Optional[dict]:
    data = await page.evaluate(
        "() => { const o={}; for (let i=0;i<localStorage.length;i++){const k=localStorage.key(i); o[k]=localStorage.getItem(k);} return o; }"
    )
    tok = _parse_web_token(data.get(WEB_TOKEN_KEY))
    if not tok:
        return None
    dev = (data.get("tracer-device-id") or "").strip().strip('"') or None
    return {"web_token": tok, "device_id": dev,
            "snapshot": {k: v for k, v in data.items() if len(v or "") < 512}}


async def _find_field(page: Any, sel: str, timeout: float = 4000) -> Any:
    """Найти поле на странице или в любом фрейме (VK ID живёт в iframe id.vk.ru)."""
    loc = page.locator(sel).first
    try:
        await loc.wait_for(timeout=timeout)
        return loc
    except Exception:  # noqa: BLE001
        pass
    for fr in page.frames:
        if fr is page.main_frame:
            continue
        loc = fr.locator(sel).first
        try:
            await loc.wait_for(timeout=1500)
            return loc
        except Exception:  # noqa: BLE001
            continue
    return None


async def _dismiss_captcha(page: Any, rounds: int = 2) -> bool:
    """Нажать «Я не робот» на VK ID (капча-чекбокс, живёт в iframe).

    VK ID нередко показывает модалку подтверждения перед формой входа и после
    каждого её шага; без клика по чекбоксу вход висит вечно.
    """
    clicked = False
    for _ in range(rounds):
        hit = False
        for fr in page.frames:  # включает main_frame
            try:
                if await fr.get_by_text("не робот").count() == 0:
                    continue
            except Exception:  # noqa: BLE001
                continue
            for sel in ("label:has-text('Я не робот')", "label:has-text('не робот')",
                        "[role='checkbox']", "input[type='checkbox']"):
                try:
                    el = fr.locator(sel).first
                    if await el.count() and await el.is_visible():
                        await el.click(timeout=2500)
                        clicked = True
                        hit = True
                        log.info("vk: captcha 'я не робот' clicked")
                        break
                except Exception:  # noqa: BLE001
                    continue
            if hit:
                break
        if not hit:
            break
        await page.wait_for_timeout(1500)
    return clicked


async def _authorized_state(inst: Any, page: Any, ctx: Any) -> bool:
    """Вход выполнен? Перебираем известные признаки авторизованной страницы."""
    try:
        url = page.url
        # ВК редиректит на vk.ru (не только vk.com); id.vk.* — экраны входа
        if "/login" not in url and "id.vk." not in url and (
                "vk.com/" in url or "vk.ru/" in url):
            tok = await _extract_token(page)
            if tok:
                sess = _vk_client(inst) or {}
                sess.update(tok)
                sess["web_token_ts"] = time.time()
                cookies = await ctx.cookies()
                sess["cookies"] = {c["name"]: c["value"] for c in cookies
                                   if c["domain"].endswith("vk.com")}
                save_session(inst, sess)
                inst.vk_sess = sess
                return True
    except Exception as e:  # noqa: BLE001
        log.warning("[%s] authorized_state check failed: %s", inst.name, e)
    return False


async def _mark_authorized(inst: Any, me: dict) -> None:
    inst.me = me.get("id")
    inst.stage = "authorized"
    inst.error = None
    inst.connected = True
    sess = _vk_client(inst) or {}
    sess["me"] = me
    save_session(inst, sess)
    from main import save_instances  # noqa: PLC0415
    save_instances()
    log.info("[%s] vk authorized as %s (id=%s)", inst.name, me.get("name"), me.get("id"))
    start_longpoll(inst)


async def act_login(inst: Any, phone_raw: str, password: str) -> dict:
    """Запуск входа: открыть браузер, ввести телефон и пароль, ждать код."""
    await act_reset(inst, keep_profile=True)
    if not password:
        raise HTTPException(status_code=400, detail="vk: нужен phone и password")
    phone = phone_raw.lstrip("+").replace(" ", "")
    inst.phone = phone_raw
    inst.stage = "connecting"
    inst.error = None
    lctx = _LoginCtx()
    inst.vk_login_ctx = lctx
    try:
        lctx.pw, lctx.context, lctx.page = await _open_logged_page(inst)
        page = lctx.page
        # уже залогинен прошлой сессией?
        for _ in range(3):
            if await _authorized_state(inst, page, lctx.context):
                me = await _fetch_me(inst)
                await _kill_login_ctx(inst)
                await _mark_authorized(inst, me)
                return {"ok": True, "instance": inst.name, "stage": inst.stage}
            await asyncio.sleep(1.0)
        # капча VK ID («Я не робот») может стоять прямо перед формой входа
        await _dismiss_captcha(page)
        # форма входа VK ID: на welcome-странице сначала «Войти другим способом»
        login = await _find_field(page, "input[name='login'], input[type='tel']", timeout=3000)
        if login is None:
            try:
                other = page.locator("button:has-text('Войти другим способом')").first
                await other.click(timeout=4000)
                await asyncio.sleep(1.5)
            except Exception:  # noqa: BLE001
                pass
            login = await _find_field(page, "input[name='login'], input[type='tel']", timeout=6000)
        if login is None:
            try:
                os.makedirs(os.path.join(WORK_DIR, "vk_debug"), exist_ok=True)
                html = await page.content()
                with open(os.path.join(WORK_DIR, "vk_debug", inst.name + "_login.html"),
                          "w", encoding="utf-8") as f:
                    f.write(html)
                inst.hint = "не нашли поле логина; дамп: /data/vk_debug/" + inst.name + "_login.html"
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError("login field not found (VK ID form not opened)")
        await login.fill(phone)
        btn = await _find_field(page, "button:has-text('Продолжить')", timeout=4000)
        if btn is None:
            btn = await _find_field(page, "button[type='submit']", timeout=2000)
        if btn is not None:
            await btn.click()
        await asyncio.sleep(1.5)
        await _dismiss_captcha(page)
        # следующий экран: пароль или сразу код (если профиль помнит)
        pw_field = await _find_field(page, "input[type='password']", timeout=8000)
        if pw_field is not None:
            await pw_field.fill(password)
            btn = await _find_field(page, "button:has-text('Продолжить')", timeout=4000)
            if btn is None:
                btn = await _find_field(page, "button[type='submit']", timeout=2000)
            if btn is not None:
                await btn.click()
            await asyncio.sleep(1.0)
            await _dismiss_captcha(page)
        inst.stage = "awaiting_code"
        inst.hint = "код подтверждения ВК (СМС/почта/2FA)"
        log.info("[%s] vk login: code requested for %s", inst.name, phone)
        return {"ok": True, "instance": inst.name, "phone": phone, "stage": inst.stage}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        await _kill_login_ctx(inst)
        inst.stage = "error"
        inst.error = f"vk login: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e


async def act_code(inst: Any, code: str) -> dict:
    """Ввод кода подтверждения. Работает и после входа по QR: ВК может
    показать код на экране после сканирования — вводим его этим же эндпоинтом."""
    if inst.stage not in ("awaiting_code", "awaiting_qr"):
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    lctx = _login_ctx(inst)
    if lctx is None:
        raise HTTPException(status_code=409, detail="login ctx expired: перезапустите login")
    page = lctx.page
    try:
        # после клика по капче форма кода появляется не мгновенно — ищем с retry
        code_field = None
        for _ in range(3):
            if await _dismiss_captcha(page):
                await asyncio.sleep(2.0)
            code_field = await _find_field(
                page, "input[name='code'], input[inputmode='numeric']", timeout=6000)
            if code_field is None:
                code_field = await _find_field(page, "input[type='text']", timeout=3000)
            if code_field is not None:
                break
            await asyncio.sleep(2.0)
        if code_field is None:
            # не фатально: сессия жива, /code можно повторить, stage не сбрасываем
            body = await page.evaluate("() => document.body.innerText.slice(0, 150)")
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)  # QR-ловец был отменён — resurrect
            raise HTTPException(
                status_code=400,
                detail="vk: поле кода не появилось (url=" + page.url + " | " +
                       " ".join(body.split())[:120] + "). Повторите /code",
            )
        qt = getattr(inst, "qr_task", None)
        if qt is not None:
            qt.cancel()
            inst.qr_task = None
        try:
            await code_field.fill(code.strip(), timeout=8000)
        except Exception:  # noqa: BLE001
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)
            raise HTTPException(status_code=400,
                                detail="vk: поле кода перерисовалось. Повторите /code")
        btn = await _find_field(page, "button:has-text('Продолжить')", timeout=3000)
        if btn is None:
            btn = await _find_field(page, "button:has-text('Подтвердить'), button[type='submit']",
                                    timeout=2000)
        submitted = False
        if btn is not None:
            try:
                await btn.click(timeout=5000)
                submitted = True
            except Exception:  # noqa: BLE001
                submitted = False
        if not submitted:
            try:
                await code_field.press("Enter", timeout=5000)
                submitted = True
            except Exception:  # noqa: BLE001
                submitted = False
        if not submitted:
            # не фатально: VK ID перерисовал экран — stage жив, /code повторяем
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)
            raise HTTPException(
                status_code=400,
                detail="vk: не удалось нажать «Продолжить» (экран перерисовался). Повторите /code",
            )
        for i in range(60):
            await asyncio.sleep(1.0)
            if i % 5 == 4:
                await _dismiss_captcha(page)
            if await _authorized_state(inst, page, lctx.context):
                me = await _fetch_me(inst)
                await _kill_login_ctx(inst)
                await _mark_authorized(inst, me)
                return {"ok": True, "stage": inst.stage, "me": me}
        # не залогинилось: возможно неверный код
        err_text = await page.evaluate("() => document.body.innerText.slice(0, 400)")
        raise HTTPException(status_code=400, detail="vk: код не принят: " + " ".join(err_text.split())[:200])
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        inst.stage = "error"
        inst.error = f"vk code: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e


async def _field_exists(page: Any, sel: str) -> bool:
    """Мгновенная проверка (без ожидания) наличия селектора в любом фрейме."""
    for fr in page.frames:
        try:
            if await fr.locator(sel).count() > 0:
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def act_password(inst: Any, password: str) -> dict:
    """Ввод пароля на живой странице посреди входа.

    VK ID может запросить пароль аккаунта ПОСЛЕ сканирования QR (подтверждение)
    или после ввода телефона. В UI шлюза в этот момент нет поля пароля —
    этот эндпоинт заполняет input[type=password] в живом браузере и жмёт
    «Продолжить». Дальше ВК либо пускает, либо просит код (СМС/почта).
    """
    if inst.stage not in ("awaiting_code", "awaiting_qr"):
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    lctx = _login_ctx(inst)
    if lctx is None:
        raise HTTPException(status_code=409, detail="login ctx expired: перезапустите login")
    page = lctx.page
    try:
        pw_field = None
        for _ in range(3):
            if await _dismiss_captcha(page):
                await asyncio.sleep(2.0)
            pw_field = await _find_field(page, "input[type='password']", timeout=6000)
            if pw_field is not None:
                break
            await asyncio.sleep(2.0)
        if pw_field is None:
            # не фатально: сессия жива, /password можно повторить, stage не сбрасываем
            body = await page.evaluate("() => document.body.innerText.slice(0, 150)")
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)
            raise HTTPException(
                status_code=400,
                detail="vk: поле пароля не появилось (url=" + page.url + " | " +
                       " ".join(body.split())[:120] + "). Повторите /password",
            )
        qt = getattr(inst, "qr_task", None)
        if qt is not None:
            qt.cancel()
            inst.qr_task = None
        try:
            await pw_field.fill(password, timeout=8000)
        except Exception:  # noqa: BLE001
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)
            raise HTTPException(status_code=400,
                                detail="vk: поле пароля перерисовалось. Повторите /password")
        btn = await _find_field(page, "button:has-text('Продолжить')", timeout=3000)
        if btn is None:
            btn = await _find_field(page, "button:has-text('Войти'), button[type='submit']",
                                    timeout=2000)
        submitted = False
        if btn is not None:
            try:
                await btn.click(timeout=5000)
                submitted = True
            except Exception:  # noqa: BLE001
                submitted = False
        if not submitted:
            try:
                await pw_field.press("Enter", timeout=5000)
                submitted = True
            except Exception:  # noqa: BLE001
                submitted = False
        if not submitted:
            # не фатально: VK ID перерисовал экран — stage жив, /password повторяем
            if inst.stage == "awaiting_qr":
                _start_qr_watch(inst, lctx)
            raise HTTPException(
                status_code=400,
                detail="vk: не удалось нажать «Продолжить» (экран перерисовался). Повторите /password",
            )
        for i in range(40):
            await asyncio.sleep(1.0)
            if i % 5 == 4:
                await _dismiss_captcha(page)
            if await _authorized_state(inst, page, lctx.context):
                me = await _fetch_me(inst)
                await _kill_login_ctx(inst)
                await _mark_authorized(inst, me)
                return {"ok": True, "stage": inst.stage, "me": me}
            # после пароля ВК обычно просит код подтверждения
            if await _field_exists(page, "input[name='code'], input[inputmode='numeric']"):
                inst.stage = "awaiting_code"
                inst.hint = "пароль принят: введите код подтверждения (СМС/почта)"
                return {"ok": True, "stage": inst.stage}
        # ни авторизации, ни кода: возможно неверный пароль
        if inst.stage == "awaiting_qr":
            _start_qr_watch(inst, lctx)
        err_text = await page.evaluate("() => document.body.innerText.slice(0, 400)")
        raise HTTPException(status_code=400,
                            detail="vk: пароль не принят: " + " ".join(err_text.split())[:200])
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        inst.stage = "error"
        inst.error = f"vk password: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e


async def _fetch_me(inst: Any) -> dict:
    try:
        r = await api(inst, "users.get", fields="domain")
        u = (r or [{}])[0]
        return {"id": u.get("id"), "name": (u.get("first_name", "") + " " + u.get("last_name", "")).strip(),
                "domain": u.get("domain")}
    except VKError as e:
        log.warning("[%s] users.get failed: %s", inst.name, e)
        return {"id": None, "name": None}


async def refresh_token(inst: Any) -> bool:
    """Тихо перечитать web_token из персистентного профиля (без кода)."""
    try:
        pw, ctx, page = await _open_logged_page(inst)
    except Exception as e:  # noqa: BLE001
        log.warning("[%s] refresh: browser failed: %s", inst.name, e)
        inst.stage = "error"
        inst.error = f"vk refresh: {e}"
        return False
    try:
        for _ in range(10):
            if await _authorized_state(inst, page, ctx):
                log.info("[%s] web_token refreshed", inst.name)
                return True
            await _dismiss_captcha(page)
            await asyncio.sleep(1.0)
        inst.stage = "expired"
        inst.hint = "сессия ВК истекла: нужен повторный login (телефон+пароль+код)"
        log.warning("[%s] refresh: not authorized", inst.name)
        return False
    finally:
        try:
            await ctx.close()
            await pw.stop()
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------- QR login


async def qr_stop(inst: Any) -> None:
    """Убить фоновую QR-сессию (она же и есть логин-браузер)."""
    await _kill_login_ctx(inst)


def _start_qr_watch(inst: Any, lctx: "_LoginCtx") -> None:
    """Фоновый ловец авторизации на живой логин-странице (скан QR / код на экране)."""

    async def loop() -> None:
        page = lctx.page
        while True:
            try:
                if inst.vk_login_ctx is not lctx:
                    return  # логин-браузер перезапущен (напр., ввод кода через /code)
                if await _authorized_state(inst, page, lctx.context):
                    me = await _fetch_me(inst)
                    await _kill_login_ctx(inst)
                    await _mark_authorized(inst, me)
                    return
                await _dismiss_captcha(page)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(2)

    qt = getattr(inst, "qr_task", None)
    if qt is not None and not qt.done():
        qt.cancel()
    inst.qr_task = asyncio.create_task(loop())


async def qr_start(inst: Any) -> dict:
    """Вход по QR (как в WhatsApp Web): welcome-страница ВК сама показывает QR.

    Владелец сканирует QR приложением ВК (Профиль -> Настройки -> Вход по QR),
    фоновая страница ловит авторизацию, web_token сохраняется. Пароль и код не нужны.
    """
    await act_reset(inst, keep_profile=True)
    inst.stage = "awaiting_qr"
    inst.error = None
    lctx = _LoginCtx()
    inst.vk_login_ctx = lctx
    try:
        lctx.pw, lctx.context, lctx.page = await _open_logged_page(inst)
    except Exception as e:  # noqa: BLE001
        await _kill_login_ctx(inst)
        inst.stage = "error"
        inst.error = f"vk qr: {e}"
        raise HTTPException(status_code=400, detail=inst.error) from e

    _start_qr_watch(inst, lctx)
    inst.hint = ("отсканируйте QR приложением ВК; после сканирования ВК может показать "
                 "код на экране — введите его в поле «Код»")
    log.info("[%s] vk qr login started", inst.name)
    return {"ok": True, "stage": inst.stage,
            "qr_png": f"/i/{inst.name}/qr.png", "ttl": 120}


async def qr_png_bytes(inst: Any) -> bytes:
    """Скриншот зоны QR с живой логин-страницы."""
    lctx = _login_ctx(inst)
    if lctx is None or lctx.page is None:
        raise HTTPException(status_code=404, detail="нет активной qr-сессии")
    page = lctx.page
    for sel in ("iframe[src*='qr_auth']", "iframe[src*='id.vk']"):
        loc = page.locator(sel).first
        try:
            await loc.wait_for(timeout=3000)
            return await loc.screenshot(type="png")
        except Exception:  # noqa: BLE001
            continue
    return await page.screenshot(type="png")


async def act_reset(inst: Any, keep_profile: bool = False) -> dict:
    await stop_longpoll(inst)
    await _kill_login_ctx(inst)
    removed = False
    p = session_path(inst)
    if os.path.exists(p):
        os.remove(p)
        removed = True
    inst.vk_sess = None
    inst.stage = "idle"
    inst.me = None
    inst.error = None
    inst.hint = None
    if not keep_profile:
        import shutil  # noqa: PLC0415
        shutil.rmtree(profile_dir(inst), ignore_errors=True)
    return {"ok": True, "session_deleted": removed, "stage": inst.stage}


# ---------------------------------------------------------------- send / read


async def act_send_text(inst: Any, body: Any) -> dict:
    if inst.stage != "authorized":
        raise HTTPException(status_code=409, detail=f"stage={inst.stage}")
    chat_id = body.chat_id
    if chat_id is None:
        raise HTTPException(status_code=400, detail="vk: нужен chat_id (peer_id) или entity по domain")
    lock = getattr(inst, "vk_send_lock", None)
    if lock is None:
        inst.vk_send_lock = lock = asyncio.Lock()
    async with lock:
        # анти-бан: от последней отправки не чаще SEND_MIN_INTERVAL + джиттер
        elapsed = time.time() - getattr(inst, "vk_last_send", 0.0)
        await asyncio.sleep(max(0.0, SEND_MIN_INTERVAL - elapsed) + random.uniform(*SEND_JITTER))
        inst.vk_last_send = time.time()
        r = await api(inst, "messages.send", peer_id=int(chat_id),
                      message=body.message, random_id=random.getrandbits(31))
    return {"ok": True, "instance": inst.name, "chatId": str(chat_id), "messageId": str(r)}


async def resolve_entity(inst: Any, query: str) -> dict:
    """id / короткий адрес (domain) / ссылка vk.com/xxx -> карточка пользователя."""
    q = (query or "").strip().lstrip("@")
    if "/" in q:
        q = q.rstrip("/").rsplit("/", 1)[-1]
    if q.isdigit():
        r = await api(inst, "users.get", user_ids=int(q), fields="domain,city")
        u = r[0]
    else:
        r = await api(inst, "utils.resolveScreenName", screen_name=q)
        if not r or r.get("type") != "user":
            raise HTTPException(status_code=404, detail=f"vk: не найдено: {query}")
        u = (await api(inst, "users.get", user_ids=r["object_id"], fields="domain,city"))[0]
    return {
        "ok": True, "instance": inst.name,
        "chatId": str(u["id"]), "type": "user",
        "title": (u.get("first_name", "") + " " + u.get("last_name", "")).strip(),
        "username": u.get("domain"),
    }


async def list_dialogs(inst: Any, limit: int = 50) -> dict:
    limit = max(1, min(int(limit or 50), 200))
    r = await api(inst, "messages.getConversations", count=limit)
    items = []
    for c in r.get("items", []):
        conv, peer = c.get("conversation", {}), c.get("last_message", {})
        pid = conv.get("peer", {}).get("id")
        items.append({
            "chatId": str(pid),
            "title": (conv.get("chat_settings") or {}).get("title") or str(pid),
            "last": (peer.get("text") or "")[:80],
            "ts": peer.get("date"),
        })
    return {"ok": True, "instance": inst.name, "items": items}


async def read_history(inst: Any, query: str, limit: int = 20) -> dict:
    peer = (query or "").strip()
    if not peer:
        raise HTTPException(status_code=400, detail="vk: query = peer_id")
    if not peer.isdigit():
        peer = (await resolve_entity(inst, peer))["chatId"]
    limit = max(1, min(int(limit or 20), 100))
    r = await api(inst, "messages.getHistory", peer_id=int(peer), count=limit)
    items = [{
        "id": m.get("id"), "out": bool(m.get("out")),
        "from": m.get("from_id"), "text": m.get("text", ""),
        "ts": m.get("date"),
    } for m in r.get("items", [])]
    return {"ok": True, "instance": inst.name, "chatId": peer, "items": items}


# ---------------------------------------------------------------- longpoll


async def stop_longpoll(inst: Any) -> None:
    t = getattr(inst, "vk_lp_task", None)
    if t is not None and not t.done():
        t.cancel()
        try:
            await t
        except BaseException:  # noqa: BLE001
            pass
    inst.vk_lp_task = None


def start_longpoll(inst: Any) -> None:
    t = getattr(inst, "vk_lp_task", None)
    if t is not None and not t.done():
        return
    inst.vk_lp_task = asyncio.create_task(_longpoll_loop(inst))


async def _emit_incoming(inst: Any, msg: dict) -> None:
    """Собрать payload Green-API и отправить вебхук."""
    from main import save_instances  # noqa: PLC0415
    peer = msg.get("peer_id")
    if peer is None:
        return
    raw = int(peer)
    api_chat = int(peer) if raw < 2000000000 else raw  # личка: chatId = peer
    inst.known_chats.add(raw)
    if raw < 2000000000:
        inst.chat_map[raw] = raw
    save_instances()
    payload = {
        "typeWebhook": "incomingMessageReceived",
        "instanceData": {
            "idInstance": 1, "instanceName": inst.name,
            "wid": inst.phone, "typeInstance": "vk",
        },
        "timestamp": int(time.time()),
        "idMessage": str(msg.get("conversation_message_id") or msg.get("id") or uuid.uuid4().hex),
        "senderData": {
            "chatId": str(api_chat), "rawChatId": str(raw),
            "senderId": str(msg.get("from_id")),
            "senderName": None,
        },
        "messageData": {"typeMessage": "textMessage",
                        "textMessageData": {"textMessage": msg.get("text", "")}},
    }
    inst.last_incoming = payload
    inst.recent.append({"ts": time.time(), "payload": payload})
    log.info("[%s] vk incoming from %s: %s", inst.name, msg.get("from_id"),
             str(msg.get("text", ""))[:120])
    hook = inst.webhook_url or os.environ.get("WEBHOOK_URL", "")
    if hook:
        try:
            async with httpx.AsyncClient(timeout=15) as hc:
                r = await hc.post(hook, json=payload)
                inst.last_webhook = {"status": r.status_code}
        except Exception as e:  # noqa: BLE001
            inst.last_webhook = {"error": str(e)}
            log.warning("[%s] vk webhook failed: %s", inst.name, e)


async def _longpoll_loop(inst: Any) -> None:
    """messages.getLongPollServer -> im.vk.com longpoll. Новые входящие -> вебхук."""
    backoff = 2.0
    while True:
        try:
            sess = _vk_client(inst)
            if not sess or not sess.get("web_token"):
                await asyncio.sleep(10)
                continue
            r = await api(inst, "messages.getLongPollServer",
                          need_pts=0, lp_version=3)
            server, key, ts = r.get("server"), r.get("key"), r.get("ts")
            if not server:
                raise VKError(-1, "no longpoll server")
            if not str(server).startswith("http"):
                server = "https://" + str(server)
            backoff = 2.0
            while True:
                async with httpx.AsyncClient(timeout=LONGPOLL_WAIT + 15) as hc:
                    rr = await hc.get(str(server), params={
                        "act": "a_check", "key": key, "ts": ts,
                        "wait": LONGPOLL_WAIT, "mode": 2 | 8 | 32 | 64, "version": 3,
                    })
                    j = rr.json()
                ts = j.get("ts", ts)
                if j.get("failed"):
                    break  # пересоздать server/key/ts
                for upd in j.get("updates", []):
                    # [4, msg_id, flags, peer_id, ts, subject, text, ...]
                    if not (isinstance(upd, list) and len(upd) > 4 and upd[0] == 4):
                        continue
                    _code, _mid, flags, peer_id = upd[0], upd[1], upd[2], upd[3]
                    if flags & 2:  # outbox
                        continue
                    # полный текст мог обрезаться в longpoll -> добираем сообщение
                    try:
                        full = await api(inst, "messages.getByIds",
                                         message_ids=str(_mid),
                                         fields="first_name,last_name")
                        msg = (full.get("items") or [{}])[0]
                    except VKError:
                        msg = {"id": _mid, "peer_id": peer_id, "from_id": peer_id,
                               "text": upd[6] if len(upd) > 6 else ""}
                    await _emit_incoming(inst, msg)
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] longpoll error: %s (retry in %.0fs)", inst.name, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ---------------------------------------------------------------- boot


async def boot(inst: Any) -> None:
    """Запуск при старте шлюза: живой токен -> сразу longpoll, иначе refresh."""
    sess = _vk_client(inst)
    if not sess or not sess.get("web_token"):
        inst.stage = "idle"
        return
    inst.stage = "connecting"
    if sess.get("me"):
        inst.me = sess["me"].get("id")
    try:
        await api(inst, "users.get")
    except VKError as e:
        log.info("[%s] boot: token invalid (%s), refreshing", inst.name, e)
        ok = await refresh_token(inst)
        if not ok:
            return
    me = sess.get("me") or await _fetch_me(inst)
    sess["me"] = me
    save_session(inst, sess)
    await _mark_authorized(inst, me)
