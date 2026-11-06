"""
Бот-кабинет для авторассылки (аналог CabinetJarvisbot, без рекламы).

Управление: aiogram (меню в боте).
Отправка: Telethon (от имени вашего отдельного аккаунта).

Настройки читаются так: переменные окружения -> config.json -> data.json.
Обязателен только BOT_TOKEN. OWNER_ID, API_ID и API_HASH можно задать
прямо в боте (кнопка «Настройки»). Если OWNER_ID не задан, владельцем станет
первый, кто напишет боту /start.
(API_ID и API_HASH берутся на https://my.telegram.org -> API development tools)
"""
import asyncio
import json
import logging
import os
import random
import re
from datetime import datetime
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, PhoneCodeInvalidError, SessionPasswordNeededError

_cfg_path = Path("config.json")
_CFG = json.loads(_cfg_path.read_text(encoding="utf-8")) if _cfg_path.exists() else {}


def cfg(name: str, default: str = "") -> str:
    return os.getenv(name) or str(_CFG.get(name, "")) or default


BOT_TOKEN = cfg("BOT_TOKEN")
OWNER_ID = int(cfg("OWNER_ID", "0") or 0)
API_ID = int(cfg("API_ID", "0") or 0)
API_HASH = cfg("API_HASH")

DB_FILE = Path("data.json")
SESS_DIR = Path("sessions")
SESS_DIR.mkdir(exist_ok=True)
PAGE = 10


# ---------------- Хранилище ----------------
class Store:
    def __init__(self):
        self.data = {
            "owner": 0,
            "api": {"id": 0, "hash": ""},
            "admins": [],
            "text": "",
            "interval": 60,  # минут между кругами рассылки
            "delay": 5,  # секунд между группами
            "running": False,
            "autoreply": {"enabled": False, "text": ""},
            "profiles": {},  # p1: {phone, groups:{id:title}, selected:[id]}
            "stats": {"sent": 0, "failed": 0, "cycles": 0, "last_run": "-", "last_error": "-"},
        }
        if DB_FILE.exists():
            self.data.update(json.loads(DB_FILE.read_text(encoding="utf-8")))

    def save(self):
        DB_FILE.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def is_admin(self, uid: int) -> bool:
        return uid == OWNER_ID or uid in self.data["admins"]


store = Store()
D = store.data
OWNER_ID = OWNER_ID or int(D.get("owner", 0))
API_ID = API_ID or int(D["api"]["id"])
API_HASH = API_HASH or D["api"]["hash"]
clients: dict[str, TelegramClient] = {}
BOT = None  # экземпляр aiogram Bot для режима «сам бот»
pending: dict[int, dict] = {}
replied: set[str] = set()
cycle_lock = asyncio.Lock()


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return store.is_admin(event.from_user.id)


class Form(StatesGroup):
    text = State()
    interval = State()
    delay = State()
    reply_text = State()
    phone = State()
    code = State()
    password = State()
    api_id = State()
    api_hash = State()
    web_code = State()


router = Router()
router.message.filter(IsAdmin(), F.chat.type == "private")
router.callback_query.filter(IsAdmin())


def btn(text, data):
    return InlineKeyboardButton(text=text, callback_data=data)


def kb(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


BACK = [btn("⬅️ Меню", "menu")]


# ---------------- Telethon ----------------
async def attach_autoreply(name: str, client: TelegramClient):
    @client.on(events.NewMessage(incoming=True))
    async def handler(ev):
        ar = D["autoreply"]
        if not ar["enabled"] or not ar["text"] or not ev.is_private:
            return
        sender = await ev.get_sender()
        if sender is None or getattr(sender, "bot", False):
            return
        key = f"{name}:{ev.sender_id}"
        if key in replied:
            return
        replied.add(key)
        try:
            await ev.reply(ar["text"])
        except Exception as e:
            logging.warning("autoreply error: %s", e)


async def start_client(name: str) -> bool:
    c = TelegramClient(str(SESS_DIR / name), API_ID, API_HASH)
    c.parse_mode = "html"
    await c.connect()
    if not await c.is_user_authorized():
        await c.disconnect()
        return False
    clients[name] = c
    await attach_autoreply(name, c)
    return True


async def refresh_groups(name: str) -> int:
    prof = D["profiles"][name]
    if name == "bot":  # группы бота собираются автоматически при добавлении
        return len(prof["groups"])
    groups = {}
    async for dlg in clients[name].iter_dialogs():
        ent = dlg.entity
        rights = getattr(ent, "admin_rights", None)
        can_post_channel = (
            dlg.is_channel
            and not dlg.is_group
            and (getattr(ent, "creator", False) or (rights is not None and getattr(rights, "post_messages", False)))
        )
        if dlg.is_group or can_post_channel:
            groups[str(dlg.id)] = dlg.title
    prof["groups"] = groups
    prof["selected"] = [g for g in prof["selected"] if g in groups]
    store.save()
    return len(groups)


# ---------------- Рассылка ----------------
async def run_cycle(force: bool = False):
    async with cycle_lock:
        if not D["text"]:
            return
        for name, prof in list(D["profiles"].items()):
            c = BOT if name == "bot" else clients.get(name)
            if not c:
                continue
            for gid in list(prof["selected"]):
                if not force and not D["running"]:
                    return
                try:
                    await c.send_message(int(gid), D["text"])
                    D["stats"]["sent"] += 1
                except TelegramRetryAfter as e:
                    D["stats"]["failed"] += 1
                    D["stats"]["last_error"] = f"FloodWait {e.retry_after}s"
                    await asyncio.sleep(e.retry_after + 1)
                except FloodWaitError as e:
                    D["stats"]["failed"] += 1
                    D["stats"]["last_error"] = f"FloodWait {e.seconds}s"
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    D["stats"]["failed"] += 1
                    D["stats"]["last_error"] = str(e)[:150]
                store.save()
                await asyncio.sleep(D["delay"] * random.uniform(1.0, 1.5))
        D["stats"]["cycles"] += 1
        D["stats"]["last_run"] = datetime.now().strftime("%d.%m %H:%M")
        store.save()


async def broadcaster():
    while True:
        if not D["running"] or not D["text"]:
            await asyncio.sleep(3)
            continue
        await run_cycle()
        for _ in range(max(1, D["interval"] * 60 // 3)):
            if not D["running"]:
                break
            await asyncio.sleep(3)


# ---------------- Меню ----------------
def status_text() -> str:
    total = sum(len(p["selected"]) for p in D["profiles"].values())
    return (
        "<b>Кабинет авторассылки</b>\n\n"
        f"Статус: {'🟢 работает' if D['running'] else '🔴 выключена'}\n"
        f"Профилей: {len(D['profiles'])} · Групп выбрано: {total}\n"
        f"Интервал: {D['interval']} мин · Пауза между группами: {D['delay']} сек\n"
        f"Текст: {'задан ✅' if D['text'] else 'не задан ❌'}"
    )


def menu_kb(uid: int):
    rows = [
        [btn("📣 Авторассылка", "m_auto"), btn("💬 Текст сообщения", "m_text")],
        [btn("⏱ Интервал", "m_interval"), btn("👥 Настройка групп", "m_groups")],
        [btn("↩️ Autoreply", "m_reply"), btn("⚙️ Полезные функции", "m_tools")],
        [btn("👤 Профили", "m_profiles"), btn("📊 Статистика", "m_stats")],
        [btn("❓ Помощь", "m_help")],
    ]
    if uid == OWNER_ID:
        rows.append([btn("🔐 Админы", "m_admins"), btn("🔑 Настройки", "m_settings")])
    return kb(rows)


async def show(c: CallbackQuery, text: str, markup):
    try:
        await c.message.edit_text(text, reply_markup=markup)
    except Exception:
        await c.message.answer(text, reply_markup=markup)
    await c.answer()


@router.message(CommandStart())
@router.message(Command("menu"))
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(status_text(), reply_markup=menu_kb(m.from_user.id))


@router.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await show(c, status_text(), menu_kb(c.from_user.id))


# ---- Авторассылка ----
@router.callback_query(F.data == "m_auto")
async def cb_auto(c: CallbackQuery):
    if not D["running"]:
        total = sum(len(p["selected"]) for p in D["profiles"].values())
        if not D["text"]:
            return await c.answer("Сначала задайте текст", show_alert=True)
        if not total:
            return await c.answer("Выберите группы в «Настройка групп»", show_alert=True)
    D["running"] = not D["running"]
    store.save()
    await show(c, status_text(), menu_kb(c.from_user.id))


# ---- Текст ----
@router.callback_query(F.data == "m_text")
async def cb_text(c: CallbackQuery, state: FSMContext):
    await state.set_state(Form.text)
    cur = f"\n\nТекущий:\n{D['text']}" if D["text"] else ""
    await show(c, "Пришлите новый текст рассылки одним сообщением (форматирование сохраняется)." + cur, kb([BACK]))


@router.message(Form.text)
async def got_text(m: Message, state: FSMContext):
    if not m.text:
        return await m.answer("Нужно текстовое сообщение.")
    D["text"] = m.html_text
    store.save()
    await state.clear()
    await m.answer("✅ Текст сохранён.", reply_markup=menu_kb(m.from_user.id))


# ---- Интервал ----
@router.callback_query(F.data == "m_interval")
async def cb_interval(c: CallbackQuery, state: FSMContext):
    await state.set_state(Form.interval)
    await show(c, f"Сейчас: {D['interval']} мин.\nВведите интервал между кругами рассылки в минутах (от 5):", kb([BACK]))


@router.message(Form.interval)
async def got_interval(m: Message, state: FSMContext):
    if not (m.text and m.text.isdigit() and int(m.text) >= 5):
        return await m.answer("Введите число не меньше 5.")
    D["interval"] = int(m.text)
    store.save()
    await state.clear()
    await m.answer("✅ Интервал сохранён.", reply_markup=menu_kb(m.from_user.id))


# ---- Полезные функции ----
@router.callback_query(F.data == "m_tools")
async def cb_tools(c: CallbackQuery):
    await show(c, "Полезные функции:", kb([
        [btn("⏳ Пауза между группами", "t_delay")],
        [btn("🚀 Отправить один круг сейчас", "t_once")],
        [btn("👁 Показать текст", "t_show")],
        BACK,
    ]))


@router.callback_query(F.data == "t_delay")
async def cb_delay(c: CallbackQuery, state: FSMContext):
    await state.set_state(Form.delay)
    await show(c, f"Сейчас: {D['delay']} сек.\nВведите паузу между группами в секундах (от 2):", kb([BACK]))


@router.message(Form.delay)
async def got_delay(m: Message, state: FSMContext):
    if not (m.text and m.text.isdigit() and int(m.text) >= 2):
        return await m.answer("Введите число не меньше 2.")
    D["delay"] = int(m.text)
    store.save()
    await state.clear()
    await m.answer("✅ Пауза сохранена.", reply_markup=menu_kb(m.from_user.id))


@router.callback_query(F.data == "t_once")
async def cb_once(c: CallbackQuery):
    if not D["text"]:
        return await c.answer("Сначала задайте текст", show_alert=True)
    if cycle_lock.locked():
        return await c.answer("Круг уже идёт", show_alert=True)
    await c.answer("Запущено")
    asyncio.create_task(run_cycle(force=True))
    await c.message.answer("🚀 Один круг запущен. Итог смотрите в «Статистика».")


@router.callback_query(F.data == "t_show")
async def cb_show(c: CallbackQuery):
    if not D["text"]:
        return await c.answer("Текст не задан", show_alert=True)
    await c.message.answer(D["text"])
    await c.answer()


# ---- Autoreply ----
@router.callback_query(F.data == "m_reply")
async def cb_reply(c: CallbackQuery):
    ar = D["autoreply"]
    await show(c, f"Автоответ в личке аккаунта: {'🟢 включён' if ar['enabled'] else '🔴 выключен'}\n\nТекст: {ar['text'] or '—'}\n\nОтвечает один раз каждому написавшему.", kb([
        [btn("Вкл/выкл", "r_toggle"), btn("Изменить текст", "r_edit")],
        BACK,
    ]))


@router.callback_query(F.data == "r_toggle")
async def cb_r_toggle(c: CallbackQuery):
    ar = D["autoreply"]
    if not ar["enabled"] and not ar["text"]:
        return await c.answer("Сначала задайте текст автоответа", show_alert=True)
    ar["enabled"] = not ar["enabled"]
    store.save()
    await cb_reply(c)


@router.callback_query(F.data == "r_edit")
async def cb_r_edit(c: CallbackQuery, state: FSMContext):
    await state.set_state(Form.reply_text)
    await show(c, "Пришлите текст автоответа:", kb([BACK]))


@router.message(Form.reply_text)
async def got_reply(m: Message, state: FSMContext):
    if not m.text:
        return await m.answer("Нужно текстовое сообщение.")
    D["autoreply"]["text"] = m.html_text
    store.save()
    await state.clear()
    await m.answer("✅ Автоответ сохранён.", reply_markup=menu_kb(m.from_user.id))


# ---- Автополучение API_ID / API_HASH через my.telegram.org ----
MYTG = "https://my.telegram.org"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def _parse_api(html: str):
    i = re.search(r"App api_id:</label>.*?<strong>(\d+)</strong>", html, re.S)
    h = re.search(r"App api_hash:</label>.*?>\s*([0-9a-f]{32})\s*<", html, re.S)
    return (int(i.group(1)), h.group(1)) if i and h else None


async def mytg_send_code(sess: aiohttp.ClientSession, phone: str) -> str:
    async with sess.post(f"{MYTG}/auth/send_password", data={"phone": phone}) as r:
        txt = await r.text()
    try:
        return json.loads(txt)["random_hash"]
    except Exception:
        raise RuntimeError(f"my.telegram.org ответил: {txt[:100]}")


async def mytg_get_api(sess: aiohttp.ClientSession, phone: str, rhash: str, code: str):
    async with sess.post(f"{MYTG}/auth/login", data={"phone": phone, "random_hash": rhash, "password": code}) as r:
        if (await r.text()).strip() != "true":
            raise RuntimeError("неверный код или отказ my.telegram.org")
    async with sess.get(f"{MYTG}/apps") as r:
        html = await r.text()
    found = _parse_api(html)
    if found:
        return found
    m = re.search(r'name="hash"\s+value="([0-9a-f]+)"', html)
    if not m:
        raise RuntimeError("не найдена форма создания приложения")
    data = {
        "hash": m.group(1),
        "app_title": "MailingCabinet",
        "app_shortname": f"mailcab{random.randint(1000, 9999)}",
        "app_url": "",
        "app_platform": "other",
        "app_desc": "",
    }
    async with sess.post(f"{MYTG}/apps/create", data=data) as r:
        await r.text()
    async with sess.get(f"{MYTG}/apps") as r:
        html = await r.text()
    found = _parse_api(html)
    if not found:
        raise RuntimeError("приложение не создано")
    return found


# ---- Профили ----
@router.callback_query(F.data == "m_profiles")
async def cb_profiles(c: CallbackQuery):
    rows = [[btn(f"🗑 {p['phone']}", f"pd:{n}")] for n, p in D["profiles"].items()]
    rows.append([btn("➕ Добавить профиль", "padd")])
    rows.append(BACK)
    await show(c, "Профили (аккаунты для рассылки). Нажмите, чтобы удалить:", kb(rows))


@router.callback_query(F.data.startswith("pd:"))
async def cb_pdel(c: CallbackQuery):
    name = c.data.split(":")[1]
    if name == "bot":
        return await c.answer("Встроенный профиль нельзя удалить", show_alert=True)
    cl = clients.pop(name, None)
    if cl:
        await cl.disconnect()
    D["profiles"].pop(name, None)
    for f in SESS_DIR.glob(f"{name}.session*"):
        f.unlink(missing_ok=True)
    store.save()
    await cb_profiles(c)


@router.callback_query(F.data == "padd")
async def cb_padd(c: CallbackQuery, state: FSMContext):
    await state.set_state(Form.phone)
    extra = "" if (API_ID and API_HASH) else "\n\nAPI_ID и API_HASH бот получит сам (понадобится ещё один код от Telegram)."
    await show(c, "Введите номер аккаунта для рассылки в формате +79991234567.\nЛучше использовать отдельный аккаунт." + extra, kb([BACK]))


@router.message(Form.phone)
async def got_phone(m: Message, state: FSMContext):
    phone = (m.text or "").strip()
    if API_ID and API_HASH:
        return await begin_login(m, state, phone)
    sess = aiohttp.ClientSession(headers=UA)
    try:
        rhash = await mytg_send_code(sess, phone)
    except Exception as e:
        await sess.close()
        await state.clear()
        return await m.answer(
            f"Не удалось получить API автоматически: {e}\n\nУкажите API_ID и API_HASH вручную в «🔑 Настройки».",
            reply_markup=menu_kb(m.from_user.id),
        )
    pending[m.from_user.id] = {"web": sess, "phone": phone, "rhash": rhash}
    await state.set_state(Form.web_code)
    await m.answer("Telegram прислал код в этот аккаунт (сообщение от Telegram). Введите его, чтобы бот сам получил API_ID и API_HASH:")


@router.message(Form.web_code)
async def got_web_code(m: Message, state: FSMContext):
    global API_ID, API_HASH
    p = pending.pop(m.from_user.id, None)
    if not p or "web" not in p:
        return await state.clear()
    try:
        api_id, api_hash = await mytg_get_api(p["web"], p["phone"], p["rhash"], (m.text or "").strip().replace(" ", ""))
    except Exception as e:
        await p["web"].close()
        await state.clear()
        return await m.answer(
            f"Не удалось получить API автоматически: {e}\n\nУкажите API_ID и API_HASH вручную в «🔑 Настройки».",
            reply_markup=menu_kb(m.from_user.id),
        )
    await p["web"].close()
    API_ID, API_HASH = api_id, api_hash
    D["api"] = {"id": api_id, "hash": api_hash}
    store.save()
    await m.answer("✅ API_ID и API_HASH получены и сохранены.")
    await begin_login(m, state, p["phone"])


async def begin_login(m: Message, state: FSMContext, phone: str):
    n = 1
    while f"p{n}" in D["profiles"] or f"p{n}" in clients:
        n += 1
    name = f"p{n}"
    client = TelegramClient(str(SESS_DIR / name), API_ID, API_HASH)
    client.parse_mode = "html"
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except Exception as e:
        await client.disconnect()
        return await m.answer(f"Ошибка: {e}")
    pending[m.from_user.id] = {"client": client, "phone": phone, "name": name, "hash": sent.phone_code_hash}
    await state.set_state(Form.code)
    await m.answer("Код отправлен в Telegram. Введите его с пробелами или дефисами (например 1 2 3 4 5), иначе Telegram может его аннулировать.")


async def finish_login(m: Message, state: FSMContext):
    p = pending.pop(m.from_user.id)
    clients[p["name"]] = p["client"]
    D["profiles"][p["name"]] = {"phone": p["phone"], "groups": {}, "selected": []}
    await attach_autoreply(p["name"], p["client"])
    n = await refresh_groups(p["name"])
    await state.clear()
    await m.answer(f"✅ Профиль {p['phone']} добавлен. Найдено групп: {n}.", reply_markup=menu_kb(m.from_user.id))


@router.message(Form.code)
async def got_code(m: Message, state: FSMContext):
    p = pending.get(m.from_user.id)
    if not p:
        return await state.clear()
    code = (m.text or "").replace(" ", "").replace("-", "")
    try:
        await p["client"].sign_in(p["phone"], code, phone_code_hash=p["hash"])
    except SessionPasswordNeededError:
        await state.set_state(Form.password)
        return await m.answer("Включена двухэтапная защита. Введите пароль (сообщение будет удалено):")
    except PhoneCodeInvalidError:
        return await m.answer("Неверный код, попробуйте ещё раз.")
    except Exception as e:
        return await m.answer(f"Ошибка: {e}")
    await finish_login(m, state)


@router.message(Form.password)
async def got_password(m: Message, state: FSMContext):
    p = pending.get(m.from_user.id)
    if not p:
        return await state.clear()
    try:
        await p["client"].sign_in(password=m.text)
    except Exception as e:
        return await m.answer(f"Ошибка: {e}")
    try:
        await m.delete()
    except Exception:
        pass
    await finish_login(m, state)


# ---- Настройка групп ----
@router.callback_query(F.data == "m_groups")
async def cb_groups(c: CallbackQuery):
    if not D["profiles"]:
        return await c.answer("Сначала добавьте профиль", show_alert=True)
    rows = [[btn(f"{p['phone']} ({len(p['selected'])}/{len(p['groups'])})", f"g:{n}:0")] for n, p in D["profiles"].items()]
    rows.append(BACK)
    await show(c, "Выберите профиль:", kb(rows))


def groups_kb(name: str, page: int):
    prof = D["profiles"][name]
    items = list(prof["groups"].items())
    chunk = items[page * PAGE:(page + 1) * PAGE]
    rows = [[btn(("✅ " if gid in prof["selected"] else "⬜ ") + title[:40], f"gt:{name}:{gid}:{page}")] for gid, title in chunk]
    nav = []
    if page > 0:
        nav.append(btn("◀️", f"g:{name}:{page - 1}"))
    if (page + 1) * PAGE < len(items):
        nav.append(btn("▶️", f"g:{name}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([btn("Выбрать все", f"ga:{name}"), btn("Сбросить", f"gn:{name}"), btn("🔄", f"gr:{name}")])
    rows.append([btn("⬅️ Профили", "m_groups")])
    return kb(rows)


@router.callback_query(F.data.startswith("g:"))
async def cb_g(c: CallbackQuery):
    _, name, page = c.data.split(":")
    await show(c, "Куда отправлять (🔄 — обновить список групп аккаунта):", groups_kb(name, int(page)))


@router.callback_query(F.data.startswith("gt:"))
async def cb_gt(c: CallbackQuery):
    _, name, gid, page = c.data.split(":")
    sel = D["profiles"][name]["selected"]
    sel.remove(gid) if gid in sel else sel.append(gid)
    store.save()
    await c.message.edit_reply_markup(reply_markup=groups_kb(name, int(page)))
    await c.answer()


@router.callback_query(F.data.regexp(r"^g[anr]:"))
async def cb_gbulk(c: CallbackQuery):
    kind, name = c.data.split(":")
    prof = D["profiles"][name]
    if kind == "ga":
        prof["selected"] = list(prof["groups"])
    elif kind == "gn":
        prof["selected"] = []
    else:
        await c.answer("Обновляю…")
        await refresh_groups(name)
    store.save()
    await c.message.edit_reply_markup(reply_markup=groups_kb(name, 0))
    await c.answer()


# ---- Статистика / помощь ----
@router.callback_query(F.data == "m_stats")
async def cb_stats(c: CallbackQuery):
    s = D["stats"]
    await show(c, (
        "<b>Статистика</b>\n\n"
        f"Отправлено: {s['sent']}\nОшибок: {s['failed']}\nКругов: {s['cycles']}\n"
        f"Последний круг: {s['last_run']}\nПоследняя ошибка: {s['last_error']}"
    ), kb([BACK]))


@router.callback_query(F.data == "m_help")
async def cb_help(c: CallbackQuery):
    await show(c, (
        "<b>Как пользоваться</b>\n\n"
        "1. Режим «сам бот» (без API): добавьте бота в группы, они появятся в «Настройка групп» → профиль «🤖 Сам бот». Либо «Профили» → добавьте аккаунт для рассылки от его имени (нужны API_ID и API_HASH).\n"
        "2. «Настройка групп» → отметьте группы.\n"
        "3. «Текст сообщения» → задайте текст.\n"
        "4. «Интервал» → время между кругами.\n"
        "5. «Авторассылка» → включите.\n\n"
        "Совет: используйте отдельный аккаунт, интервал от 30 минут и паузу между группами от 5 секунд, "
        "иначе Telegram может ограничить аккаунт. Пишите только в те группы, где реклама разрешена."
    ), kb([BACK]))


# ---- Админы (только владелец) ----
@router.callback_query(F.data == "m_admins")
async def cb_admins(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        return await c.answer("Только владелец", show_alert=True)
    ids = "\n".join(map(str, D["admins"])) or "— нет —"
    await show(c, f"Администраторы:\n{ids}\n\nДобавить: /addadmin ID\nУдалить: /deladmin ID", kb([BACK]))


@router.message(Command("addadmin"))
async def add_admin(m: Message):
    if m.from_user.id != OWNER_ID:
        return
    try:
        uid = int(m.text.split()[1])
    except (IndexError, ValueError):
        return await m.answer("Использование: /addadmin 123456789")
    if uid not in D["admins"]:
        D["admins"].append(uid)
        store.save()
    await m.answer(f"✅ {uid} теперь администратор.")


@router.message(Command("deladmin"))
async def del_admin(m: Message):
    if m.from_user.id != OWNER_ID:
        return
    try:
        uid = int(m.text.split()[1])
    except (IndexError, ValueError):
        return await m.answer("Использование: /deladmin 123456789")
    if uid in D["admins"]:
        D["admins"].remove(uid)
        store.save()
    await m.answer(f"🗑 {uid} удалён.")


# ---- Настройки (только владелец) ----
@router.callback_query(F.data == "m_settings")
async def cb_settings(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        return await c.answer("Только владелец", show_alert=True)
    await show(c, (
        "<b>Настройки</b>\n\n"
        f"API_ID: {'✅ задан' if API_ID else '❌ не задан'}\n"
        f"API_HASH: {'✅ задан' if API_HASH else '❌ не задан'}\n\n"
        "Значения берутся на my.telegram.org → API development tools. "
        "Обычно заполнять не нужно: при добавлении профиля бот получает их сам. Нужны только для рассылки от имени аккаунтов. Для режима «Сам бот» не нужны."
    ), kb([[btn("Указать API_ID", "s_id"), btn("Указать API_HASH", "s_hash")], BACK]))


@router.callback_query(F.data == "s_id")
async def cb_s_id(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != OWNER_ID:
        return await c.answer("Только владелец", show_alert=True)
    await state.set_state(Form.api_id)
    await show(c, "Пришлите API_ID (число):", kb([BACK]))


@router.message(Form.api_id)
async def got_api_id(m: Message, state: FSMContext):
    global API_ID
    if not (m.text and m.text.strip().isdigit()):
        return await m.answer("API_ID — это число.")
    API_ID = int(m.text.strip())
    D["api"]["id"] = API_ID
    store.save()
    await state.clear()
    await ensure_clients()
    await m.answer("✅ API_ID сохранён.", reply_markup=menu_kb(m.from_user.id))


@router.callback_query(F.data == "s_hash")
async def cb_s_hash(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != OWNER_ID:
        return await c.answer("Только владелец", show_alert=True)
    await state.set_state(Form.api_hash)
    await show(c, "Пришлите API_HASH (сообщение сразу удалится):", kb([BACK]))


@router.message(Form.api_hash)
async def got_api_hash(m: Message, state: FSMContext):
    global API_HASH
    if not m.text:
        return await m.answer("Пришлите текстом.")
    API_HASH = m.text.strip()
    D["api"]["hash"] = API_HASH
    store.save()
    await state.clear()
    try:
        await m.delete()
    except Exception:
        pass
    await ensure_clients()
    await m.answer("✅ API_HASH сохранён.", reply_markup=menu_kb(m.from_user.id))


# ---- Учёт групп, куда добавили самого бота ----
groups_router = Router()


@groups_router.my_chat_member()
async def on_membership(event: ChatMemberUpdated):
    if event.chat.type == "private":
        return
    prof = D["profiles"].setdefault("bot", {"phone": "🤖 Сам бот (без API)", "groups": {}, "selected": []})
    cid = str(event.chat.id)
    if event.new_chat_member.status in ("member", "administrator"):
        prof["groups"][cid] = event.chat.title or cid
    else:
        prof["groups"].pop(cid, None)
        if cid in prof["selected"]:
            prof["selected"].remove(cid)
    store.save()


def _register_chat(chat):
    prof = D["profiles"].setdefault("bot", {"phone": "🤖 Сам бот (без API)", "groups": {}, "selected": []})
    cid = str(chat.id)
    if cid not in prof["groups"]:
        prof["groups"][cid] = chat.title or cid
        store.save()


@groups_router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(m: Message):
    _register_chat(m.chat)


@groups_router.channel_post()
async def on_channel_post(m: Message):
    _register_chat(m.chat)


# ---- Первый запуск: назначение владельца ----
claim_router = Router()


@claim_router.message(CommandStart(), F.chat.type == "private")
async def claim_owner(m: Message):
    global OWNER_ID
    if OWNER_ID:
        return
    OWNER_ID = m.from_user.id
    D["owner"] = OWNER_ID
    store.save()
    await m.answer("👑 Вы назначены владельцем бота.", reply_markup=menu_kb(OWNER_ID))


# ---------------- Запуск ----------------
async def ensure_clients():
    if not (API_ID and API_HASH):
        return
    for name in list(D["profiles"]):
        if name in clients or name == "bot":
            continue
        if not await start_client(name):
            logging.warning("Профиль %s не авторизован, удалён", name)
            D["profiles"].pop(name)
            store.save()


async def main():
    global BOT
    logging.basicConfig(level=logging.INFO)
    if not BOT_TOKEN:
        raise SystemExit("Задайте BOT_TOKEN (переменная окружения или config.json)")
    D["profiles"].setdefault("bot", {"phone": "🤖 Сам бот (без API)", "groups": {}, "selected": []})
    store.save()
    await ensure_clients()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    BOT = bot
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(groups_router)
    dp.include_router(router)
    dp.include_router(claim_router)
    asyncio.create_task(broadcaster())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
