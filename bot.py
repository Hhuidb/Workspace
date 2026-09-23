"""
╔══════════════════════════════════════════════════════════╗
║              Hug Bot — Бананы 🍌 • Грудь 🍒              ║
║   Отношения 💞 • Браки 💍 • Топы • Реакции 🫂            ║
║   asyncpg (PostgreSQL/Aiven) • aiogram 3 • Render-ready  ║
╚══════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import asyncpg
from aiohttp import ClientSession, web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

try:  # локально удобно читать .env (на сервере переменные задаются в окружении)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("HugBot")

# ============================================================================
# НАСТРОЙКИ — все значения пишутся прямо здесь.
# Если такая же переменная окружения задана на хостинге (Render → Environment),
# она имеет приоритет над тем, что написано ниже.
# ============================================================================

BOT_TOKEN: str = os.environ.get(
    "BOT_TOKEN",
    "8660315235:AAHXLHuHvmnetOifibY8V-oM5gyc4U5gZwI",
)

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgres://avnadmin:AVNS_lcs942Qut-yKVwP2ofo@pg-2afdd42b-tyunestefan4-d91a.d.aivencloud.com:14497/defaultdb?sslmode=require",
)

OWNER_ID: int = int(os.environ.get("OWNER_ID", "8856892065"))  # кто активирует группу
PORT: int = int(os.environ.get("PORT", "14497"))
TIMEZONE: ZoneInfo = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))

# Самопинг, чтобы бесплатный Render не засыпал (15 минут без трафика).
# Адрес берётся из RENDER_EXTERNAL_URL, который Render подставляет сам.
PING_INTERVAL: int = int(os.environ.get("PING_INTERVAL", "180"))
APP_URL: str = (os.environ.get("APP_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

GROW_MIN: float = 0.1          # минимальный прирост за раз, см
GROW_MAX: float = 5.0          # максимальный прирост за раз, см
GROW_COOLDOWN: int = 3600      # кулдаун роста: 1 час
ANTISPAM_SECONDS: int = 10     # не отвечать пользователю чаще раза в 10 секунд
TOP_LIMIT: int = 10            # позиций в топе
DB_PING_SECONDS: int = 300     # проверка соединения с базой

ACTION_VERBS: dict[str, tuple[str, str]] = {
    "поцеловать": ("💋", "поцеловал(а)"),
    "обнять": ("🫂", "обнял(а)"),
    "шлепнуть": ("👋", "шлепнул(а)"),
}

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
r = Router()


# ============================================================================
# СОСТОЯНИЕ
# ============================================================================

@dataclass
class State:
    """Глобальное состояние: пул соединений и маленький кэш активации чатов."""

    pool: Optional[asyncpg.Pool] = None
    activated_cache: dict[int, bool] = field(default_factory=dict)
    bg_tasks: list = field(default_factory=list)


st = State()


def _dsn(url: str) -> str:
    """Приводит строку подключения к виду, который понимает asyncpg.

    asyncpg принимает postgres:// и postgresql://, а также параметр sslmode в URL
    (PostgreSQL-стиль), поэтому Service URI из Aiven можно вставлять как есть.
    Здесь убираются только пробелы и «+asyncpg» из строки, если её скопировали
    из примеров SQLAlchemy.
    """
    url = (url or "").strip().strip('"').strip("'")
    return url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "postgres+asyncpg://", "postgres://"
    )


def _dsn_safe(url: str) -> str:
    """Та же строка, но без пароля — для логов."""
    url = _dsn(url)
    if "@" not in url:
        return url
    head, tail = url.rsplit("@", 1)
    if "//" in head:
        scheme, creds = head.split("//", 1)
        user = creds.split(":", 1)[0]
        return f"{scheme}//{user}:***@{tail}"
    return url


# ============================================================================
# БАЗА ДАННЫХ
# ============================================================================

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS game_bot_guilds (
    id           BIGINT PRIMARY KEY,
    activated    BOOLEAN DEFAULT FALSE,
    activated_at BIGINT  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS game_bot_users (
    id            BIGINT PRIMARY KEY,
    name          TEXT    DEFAULT '?',
    banana        DOUBLE PRECISION DEFAULT 0,
    melons        DOUBLE PRECISION DEFAULT 0,
    banana_today  DOUBLE PRECISION DEFAULT 0,
    melons_today  DOUBLE PRECISION DEFAULT 0,
    banana_day    TEXT    DEFAULT '',
    melons_day    TEXT    DEFAULT '',
    banana_used   BIGINT  DEFAULT 0,
    melons_used   BIGINT  DEFAULT 0,
    last_reply    BIGINT  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS game_bot_pairs (
    id      BIGSERIAL PRIMARY KEY,
    kind    TEXT   NOT NULL,
    u1      BIGINT NOT NULL,
    u2      BIGINT NOT NULL,
    started BIGINT NOT NULL,
    ended   BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS game_bot_pairs_users_idx ON game_bot_pairs (u1, u2);
CREATE INDEX IF NOT EXISTS game_bot_pairs_kind_idx  ON game_bot_pairs (kind, ended);
CREATE INDEX IF NOT EXISTS game_bot_users_banana_idx ON game_bot_users (banana DESC);
CREATE INDEX IF NOT EXISTS game_bot_users_melons_idx ON game_bot_users (melons DESC);
"""


async def db_init() -> None:
    """Создаёт пул соединений и таблицы, если их ещё нет."""
    st.pool = await asyncpg.create_pool(
        _dsn(DATABASE_URL),
        min_size=2,
        max_size=10,
        command_timeout=10,
        # Пере-открываем простаивающие соединения раз в 2 минуты, чтобы они не
        # «протухали» из-за idle-таймаутов на стороне Aiven/сетевого файрвола
        # (именно это и было причиной того, что бот «умирал» в группах примерно
        # через час — фоновый db-пинг переставал работать, см. task_db_ping).
        max_inactive_connection_lifetime=120,
    )
    async with st.pool.acquire() as c:
        await c.execute(_INIT_SQL)
    log.info("DB ready: %s", _dsn_safe(DATABASE_URL))


# --- время ---------------------------------------------------------------

def now_epoch() -> int:
    return int(time.time())


def today_str() -> str:
    """Сегодняшняя дата в таймзоне бота: 'YYYY-MM-DD'."""
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d")


def day_start_epoch() -> int:
    """Начало сегодняшнего дня в таймзоне бота, epoch."""
    now = datetime.now(TIMEZONE)
    return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


# --- чаты ----------------------------------------------------------------

async def db_is_activated(chat_id: int) -> bool:
    """Активирован ли бот в чате (с кэшем на процесс)."""
    cached = st.activated_cache.get(chat_id)
    if cached is not None:
        return cached
    async with st.pool.acquire() as c:
        row = await c.fetchrow("SELECT activated FROM game_bot_guilds WHERE id=$1", chat_id)
    result = bool(row["activated"]) if row else False
    st.activated_cache[chat_id] = result
    return result


async def db_set_activated(chat_id: int, activated: bool = True) -> None:
    async with st.pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO game_bot_guilds (id, activated, activated_at) VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET activated = EXCLUDED.activated,
                                           activated_at = EXCLUDED.activated_at
            """,
            chat_id,
            activated,
            now_epoch(),
        )
    st.activated_cache[chat_id] = activated


# --- пользователи --------------------------------------------------------

async def db_touch_user(user_id: int, name: str) -> None:
    """Создаёт пользователя или обновляет имя (имя '?' не затирает прежнее)."""
    async with st.pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO game_bot_users (id, name) VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET name = CASE
                WHEN EXCLUDED.name <> '?' THEN EXCLUDED.name
                ELSE game_bot_users.name END
            """,
            user_id,
            (name or "?")[:64],
        )


async def db_get_user(user_id: int) -> Optional[asyncpg.Record]:
    async with st.pool.acquire() as c:
        return await c.fetchrow("SELECT * FROM game_bot_users WHERE id=$1", user_id)


async def db_update_user(user_id: int, fields: dict[str, object]) -> None:
    """Обновляет поля пользователя. Имена полей — только из белого списка."""
    allowed = {
        "banana", "melons", "banana_today", "melons_today",
        "banana_day", "melons_day", "banana_used", "melons_used", "last_reply",
    }
    pairs = [(k, v) for k, v in fields.items() if k in allowed]
    if not pairs:
        return
    sets = ", ".join(f"{k}=${i + 2}" for i, (k, _) in enumerate(pairs))
    values = [v for _, v in pairs]
    async with st.pool.acquire() as c:
        await c.execute(f"UPDATE game_bot_users SET {sets} WHERE id=$1", user_id, *values)


async def db_gate_and_touch(user_id: int, name: str) -> tuple[bool, Optional[asyncpg.Record]]:
    """Один запрос вместо трёх: создаёт/обновляет пользователя, проверяет антиспам
    (10 сек) и сразу возвращает его строку целиком.

    Раньше это были 3 последовательных обращения к БД (db_touch_user, SELECT,
    UPDATE) на каждое сообщение — из-за сетевой задержки до Aiven это и было
    главной причиной медленных ответов. Теперь всё делается одним атомарным
    UPSERT-ом с RETURNING.
    """
    now = now_epoch()
    async with st.pool.acquire() as c:
        row = await c.fetchrow(
            """
            INSERT INTO game_bot_users (id, name, last_reply)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                name = CASE WHEN EXCLUDED.name <> '?' THEN EXCLUDED.name
                            ELSE game_bot_users.name END,
                last_reply = CASE WHEN $3 - game_bot_users.last_reply >= $4
                                  THEN $3 ELSE game_bot_users.last_reply END
            RETURNING *, (last_reply = $3) AS allowed
            """,
            user_id,
            (name or "?")[:64],
            now,
            ANTISPAM_SECONDS,
        )
    return bool(row["allowed"]), row


async def db_rank(user_id: int, which: str) -> int:
    """Место пользователя в топе за всё время (1-based, 0 — если размер нулевой)."""
    col = "banana" if which == "banana" else "melons"
    user = await db_get_user(user_id)
    if not user or not user[col]:
        return 0
    async with st.pool.acquire() as c:
        row = await c.fetchrow(
            f"SELECT COUNT(*) AS cnt FROM game_bot_users WHERE {col} > $1", user[col]
        )
    return int(row["cnt"] or 0) + 1


# --- пары ----------------------------------------------------------------

async def db_active_pair(user_id: int, kind: str) -> Optional[asyncpg.Record]:
    async with st.pool.acquire() as c:
        return await c.fetchrow(
            """
            SELECT * FROM game_bot_pairs
            WHERE kind=$1 AND ended=0 AND (u1=$2 OR u2=$2)
            ORDER BY id DESC LIMIT 1
            """,
            kind,
            user_id,
        )


async def db_break_pairs(user_id: int, now: int) -> int:
    """Разрывает все активные пары пользователя. Возвращает сколько разорвано."""
    async with st.pool.acquire() as c:
        rows = await c.fetch(
            "UPDATE game_bot_pairs SET ended=$2 WHERE ended=0 AND (u1=$1 OR u2=$1) RETURNING id",
            user_id,
            now,
        )
    return len(rows)


async def db_add_pair(kind: str, u1: int, u2: int, now: int) -> None:
    async with st.pool.acquire() as c:
        await c.execute(
            "INSERT INTO game_bot_pairs (kind, u1, u2, started) VALUES ($1, $2, $3, $4)",
            kind, u1, u2, now,
        )


def pair_seconds(row: asyncpg.Record) -> int:
    end = int(row["ended"] or 0) or now_epoch()
    return max(end - int(row["started"]), 0)


# --- топы ----------------------------------------------------------------

async def top_growth(which: str, period: str) -> tuple[list[str], str]:
    """Топ размеров: which — banana|melons, period — today|all."""
    is_banana = which == "banana"
    emoji = "🍌" if is_banana else "🍒"
    col = "banana" if is_banana else "melons"
    today_col = "banana_today" if is_banana else "melons_today"
    day_col = "banana_day" if is_banana else "melons_day"
    title = f"{emoji} Топ бананов" if is_banana else f"{emoji} Топ грудей"
    title += " · за сегодня" if period == "today" else " · за всё время"

    if period == "today":
        sql = (
            f"SELECT id, name, {today_col} AS val FROM game_bot_users "
            f"WHERE {day_col}=$1 AND {today_col}>0 ORDER BY {today_col} DESC, {col} DESC LIMIT $2"
        )
        args: tuple = (today_str(), TOP_LIMIT)
        prefix = "+"
    else:
        sql = (
            f"SELECT id, name, {col} AS val FROM game_bot_users "
            f"WHERE {col}>0 ORDER BY {col} DESC LIMIT $1"
        )
        args = (TOP_LIMIT,)
        prefix = ""

    async with st.pool.acquire() as c:
        rows = await c.fetch(sql, *args)

    lines: list[str] = []
    for i, row in enumerate(rows):
        med = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i + 1}."
        lines.append(f"{med} {fname(row['id'], row['name'])} — {prefix}{nice_cm(row['val'])} см")
    if not lines:
        lines.append("Пока пусто…")
    return lines, title


async def top_pairs(kind: str, period: str) -> tuple[list[str], str]:
    """Топ пар: сортировка по длительности, разорванные — с 💔."""
    title = "💞 Топ отношений" if kind == "love" else "💍 Топ браков"
    title += " · за сегодня" if period == "today" else " · за всё время"

    sql = "SELECT * FROM game_bot_pairs WHERE kind=$1"
    args: list = [kind]
    if period == "today":
        sql += " AND started >= $2"
        args.append(day_start_epoch())

    async with st.pool.acquire() as c:
        rows = await c.fetch(sql, *args)
        scored = sorted(rows, key=pair_seconds, reverse=True)[:TOP_LIMIT]

        ids = {int(p["u1"]) for p in scored} | {int(p["u2"]) for p in scored}
        names: dict[int, str] = {}
        if ids:
            id_list = list(ids)
            placeholders = ", ".join(f"${i + 1}" for i in range(len(id_list)))
            name_rows = await c.fetch(
                f"SELECT id, name FROM game_bot_users WHERE id IN ({placeholders})", *id_list
            )
            names = {int(nr["id"]): nr["name"] for nr in name_rows}

    lines: list[str] = []
    for i, p in enumerate(scored):
        med = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i + 1}."
        broken = " 💔" if int(p["ended"] or 0) else ""
        u1, u2 = int(p["u1"]), int(p["u2"])
        lines.append(
            f"{med} {fname(u1, names.get(u1))} + {fname(u2, names.get(u2))} — "
            f"{fmt_duration(pair_seconds(p))}{broken}"
        )
    if not lines:
        lines.append("Пока пусто…")
    return lines, title


# ============================================================================
# ФОРМАТИРОВАНИЕ
# ============================================================================

def fname(user_id: int, name: Optional[str] = None) -> str:
    """Кликабельное имя, как в Telegram."""
    return f"<a href='tg://user?id={int(user_id)}'>{name or user_id}</a>"


def nice_cm(value: float) -> str:
    """2.6 см — всегда один знак после запятой."""
    return f"{float(value or 0):.1f}"


def fmt_wait(seconds: int) -> str:
    m, s = divmod(max(int(seconds), 0), 60)
    return f"{m}м. {s}с." if m else f"{s}с."


def fmt_duration(seconds: int) -> str:
    d, seconds = divmod(max(int(seconds), 0), 86400)
    h, seconds = divmod(seconds, 3600)
    m = seconds // 60
    if d:
        return f"{d} дн. {h} ч."
    if h:
        return f"{h} ч. {m} м."
    return f"{m} м."


# ============================================================================
# ЛОГИКА ИГРЫ
# ============================================================================

def roll_growth(user: asyncpg.Record, which: str) -> tuple[dict[str, object], str]:
    """Один запрос роста. Возвращает (обновления полей, текст ответа)."""
    now = now_epoch()
    day = today_str()
    is_banana = which == "banana"

    size = float(user["banana"] if is_banana else user["melons"] or 0)
    used = int(user["banana_used"] if is_banana else user["melons_used"] or 0)
    today = float(user["banana_today"] if is_banana else user["melons_today"] or 0)
    last_day = user["banana_day"] if is_banana else user["melons_day"] or ""
    emoji = "🍌" if is_banana else "🍒"
    phrase = "твой член вырос" if is_banana else "твоя грудь выросла"

    if used and now - used < GROW_COOLDOWN:
        return {}, (
            f"Повтори через {fmt_wait(GROW_COOLDOWN - (now - used))}\n"
            f"Текущий размер — <b>{nice_cm(size)} см</b> {emoji}"
        )

    growth = round(random.uniform(GROW_MIN, GROW_MAX), 1)
    new_size = round(size + growth, 1)
    new_today = round(today + growth, 1) if day == last_day else growth

    if is_banana:
        updates = {
            "banana": new_size, "banana_today": new_today,
            "banana_day": day, "banana_used": now,
        }
    else:
        updates = {
            "melons": new_size, "melons_today": new_today,
            "melons_day": day, "melons_used": now,
        }
    text = f"{phrase} на <b>{nice_cm(growth)} см</b>!\nТекущий размер — <b>{nice_cm(new_size)} см</b> {emoji}"
    return updates, text


START_HINT = (
    "/dick — вырастить член 🍌\n"
    "/sissi — вырастить грудь 🍒\n"
    "/statis — моя статистика\n"
    "/top — топы\n\n"
    "Поцеловать / обнять / шлепнуть — ответьте на сообщение человека этим словом.\n"
    "/love и /marry в ответ на сообщение — вступить в отношения или брак."
)


async def require_user(msg: Message):
    """Возвращает msg.from_user или None + шлёт понятную подсказку.

    В группах сообщение может прийти без пользователя (msg.from_user is None) —
    например, если оно отправлено от имени привязанного канала, либо когда
    администратор пишет анонимно, отправив «от имени группы» без привычного
    account. Раньше в этом случае весь код падал на `msg.from_user.id`
    (AttributeError), исключение гасилось библиотекой молча — бот просто не
    отвечал. Теперь бот явно объясняет, в чём дело.
    """
    if msg.from_user is None or msg.from_user.is_bot and msg.from_user.username == "GroupAnonymousBot":
        await msg.answer(
            "Эта команда не работает от анонимного имени группы 🙏\n"
            "Отключите режим «Оставаться анонимным» в настройках админов и повторите."
        )
        return None
    return msg.from_user


async def pass_gate(msg: Message) -> bool:
    """Активирован ли чат + антиспам 10 секунд."""
    if not await db_is_activated(msg.chat.id):
        return False
    user = await require_user(msg)
    if user is None:
        return False
    allowed, _ = await db_gate_and_touch(user.id, user.first_name)
    return allowed


# ============================================================================
# ХЕНДЛЕРЫ: активация
# ============================================================================

@r.message(CommandStart())
async def cmd_start(msg: Message) -> None:
    if msg.chat.type == "private":
        await msg.answer("👋 Привет!\n\n" + START_HINT)
        return
    if msg.from_user is None or msg.from_user.id != OWNER_ID:
        await msg.answer("Владелец бота активирует группу командой /startdicksisis")
        return
    if await db_is_activated(msg.chat.id):
        await msg.answer("✅ Бот уже активирован в этой группе\n\n" + START_HINT)
    else:
        await msg.answer("Чтобы активировать бота, отправьте /startdicksisis\n\n" + START_HINT)


@r.message(Command("startdicksisis"))
async def cmd_activate(msg: Message) -> None:
    if msg.chat.type == "private":
        await msg.answer("👋 Бот только для групп!")
        return
    if msg.from_user is None or msg.from_user.id != OWNER_ID:
        await msg.answer("Эта команда доступна только владельцу бота.")
        return
    await db_set_activated(msg.chat.id, True)
    await msg.answer("✅ Бот активирован в этой группе!\n\n" + START_HINT)


# ============================================================================
# ХЕНДЛЕРЫ: рост
# ============================================================================

async def _grow_cmd(msg: Message, which: str) -> None:
    if not await db_is_activated(msg.chat.id):
        return
    user_tg = await require_user(msg)
    if user_tg is None:
        return
    allowed, user = await db_gate_and_touch(user_tg.id, user_tg.first_name)
    if not allowed or user is None:
        return
    updates, text = roll_growth(user, which)
    if updates:
        await db_update_user(user_tg.id, updates)
    name = (user["name"] or "?").strip().split()
    await msg.answer(f"{name[0] if name else '?'}, " + text)


@r.message(F.text, Command("dick"))
async def cmd_dick(msg: Message) -> None:
    await _grow_cmd(msg, "banana")


@r.message(F.text, Command("sissi"))
async def cmd_sissi(msg: Message) -> None:
    await _grow_cmd(msg, "melons")


# ============================================================================
# ХЕНДЛЕРЫ: отношения и брак
# ============================================================================

async def _pair_cmd(msg: Message, kind: str) -> None:
    if not await pass_gate(msg):
        return
    me_id, me_name = msg.from_user.id, msg.from_user.first_name
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        what = "вступить в отношения" if kind == "love" else "заключить брак"
        await msg.answer(f"Ответьте на сообщение человека, с которым хотите {what} 🙏")
        return
    target = msg.reply_to_message.from_user
    if target.id == me_id:
        await msg.answer("С самим собой нельзя 🙂")
        return

    await db_touch_user(target.id, target.first_name)
    now = now_epoch()
    broken = await db_break_pairs(me_id, now)
    broken += await db_break_pairs(target.id, now)
    await db_add_pair(kind, me_id, target.id, now)

    me = fname(me_id, me_name)
    them = fname(target.id, target.first_name)
    if kind == "love":
        text = f"{me} вступил(а) в отношения с {them} 💞"
    else:
        text = f"{me} заключил(а) брак с {them} 💍"
    if broken:
        text += "\n💔 Прежние пары распались."
    await msg.answer(text)


@r.message(F.text, Command("love"))
async def cmd_love(msg: Message) -> None:
    await _pair_cmd(msg, "love")


@r.message(F.text, Command("marry"))
async def cmd_marry(msg: Message) -> None:
    await _pair_cmd(msg, "marry")


# ============================================================================
# ХЕНДЛЕРЫ: статистика
# ============================================================================

@r.message(F.text, Command("statis"))
async def cmd_statis(msg: Message) -> None:
    if not await db_is_activated(msg.chat.id):
        return
    user_tg = await require_user(msg)
    if user_tg is None:
        return
    allowed, user = await db_gate_and_touch(user_tg.id, user_tg.first_name)
    if not allowed or user is None:
        return

    pos_b = await db_rank(user_tg.id, "banana")
    pos_m = await db_rank(user_tg.id, "melons")
    lines = [
        f"{fname(user_tg.id, user['name'])} — статистика:",
        f"Член: <b>{nice_cm(user['banana'])} см</b> 🍌",
        f"Грудь: <b>{nice_cm(user['melons'])} см</b> 🍒",
        f"Прирост за сегодня: член +{nice_cm(user['banana_today'])} см, "
        f"грудь +{nice_cm(user['melons_today'])} см",
        f"Место в топе: член — {'#' + str(pos_b) if pos_b else '—'}, "
        f"грудь — {'#' + str(pos_m) if pos_m else '—'}",
    ]

    for kind, label, icon in (("love", "Отношения", "💞"), ("marry", "Брак", "💍")):
        pair = await db_active_pair(user_tg.id, kind)
        if pair:
            other = int(pair["u2"]) if int(pair["u1"]) == user_tg.id else int(pair["u1"])
            other_user = await db_get_user(other)
            lines.append(
                f"{label}: {fname(other, other_user['name'] if other_user else None)} — "
                f"{fmt_duration(pair_seconds(pair))} {icon}"
            )

    await msg.answer("\n".join(lines))


# ============================================================================
# ХЕНДЛЕРЫ: топы (инлайн-клавиатура)
# ============================================================================

TOP_MENU = "top:menu"


def top_keyboard(depth: str) -> InlineKeyboardMarkup:
    if depth == "menu":
        rows = [
            [InlineKeyboardButton(text="🍌 Топ бананов", callback_data="top:banana")],
            [InlineKeyboardButton(text="🍒 Топ грудей", callback_data="top:melons")],
            [InlineKeyboardButton(text="💞 Топ отношений", callback_data="top:love")],
            [InlineKeyboardButton(text="💍 Топ браков", callback_data="top:marry")],
        ]
    else:
        rows = [
            [InlineKeyboardButton(text="За сегодня", callback_data=f"top:{depth}:today")],
            [InlineKeyboardButton(text="За всё время", callback_data=f"top:{depth}:all")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=TOP_MENU)],
        ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


@r.message(F.text, Command("top"))
async def cmd_top(msg: Message) -> None:
    if not await pass_gate(msg):
        return
    await msg.answer("Что показать?", reply_markup=top_keyboard("menu"))


@r.callback_query(F.data == TOP_MENU)
async def cb_top_menu(call: CallbackQuery) -> None:
    await safe_edit(call, "Что показать?", top_keyboard("menu"))
    await call.answer()


@r.callback_query(F.data.startswith("top:"))
async def cb_top(call: CallbackQuery) -> None:
    parts = (call.data or "").split(":")
    if len(parts) == 2:  # выбор периода
        await safe_edit(call, "Выбери период:", top_keyboard(parts[1]))
        await call.answer()
        return

    which, period = parts[1], parts[2]
    if which in ("banana", "melons"):
        lines, title = await top_growth(which, period)
    else:
        lines, title = await top_pairs(which, period)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"top:{which}")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data=TOP_MENU)],
        ]
    )
    await safe_edit(call, f"<b>{title}</b>\n\n" + "\n".join(lines), kb, html=True)
    await call.answer()


async def safe_edit(call: CallbackQuery, text: str, kb: InlineKeyboardMarkup, html: bool = False) -> None:
    """Правит сообщение, игнорируя «message is not modified»."""
    try:
        await call.message.edit_text(text, parse_mode=ParseMode.HTML if html else None, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            raise


# ============================================================================
# ХЕНДЛЕРЫ: слова-действия (последними, чтобы не перехватывать команды)
# ============================================================================

@r.message(F.text)
async def any_text(msg: Message) -> None:
    """«обнять», «поцеловать», «шлепнуть» — ответом на сообщение человека."""
    if not await db_is_activated(msg.chat.id):
        return
    words = (msg.text or "").strip().lower().lstrip("/").split()
    if len(words) != 1 or words[0] not in ACTION_VERBS:
        return
    action = words[0]

    user = await require_user(msg)
    if user is None:
        return
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        await msg.answer("Ответьте на сообщение человека, чтобы сделать это 🙏")
        return
    target = msg.reply_to_message.from_user
    if target.id == user.id:
        await msg.answer("С самим собой нельзя 🙂")
        return

    await db_touch_user(target.id, target.first_name)
    emoji, verb = ACTION_VERBS[action]
    text = f"{fname(user.id, user.first_name)} {verb} {fname(target.id, target.first_name)} {emoji}"
    await msg.answer(text, reply_to_message_id=msg.message_id)


# ============================================================================
# ВЕБ-СЕРВЕР, САМОПИНГ, ФОНОВЫЕ ЗАДАЧИ
# ============================================================================

@dp.error()
async def on_error(event: ErrorEvent) -> bool:
    """Ловит все необработанные исключения из хендлеров.

    Раньше при любой временной ошибке (например, обрыв соединения с БД)
    хендлер падал молча: aiogram гасил исключение, пользователь не получал
    вообще никакого ответа, и со стороны это выглядело так, будто бот
    «перестал работать». Теперь ошибка гарантированно попадает в лог с полным
    traceback, а следующее сообщение обработается заново (пул соединений
    сам восстановит рабочее соединение).
    """
    log.exception("Необработанная ошибка при апдейте %s", event.update.update_id, exc_info=event.exception)
    return True


async def health(_: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_web() -> None:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Web server on port %d", PORT)


async def task_self_ping(session: ClientSession) -> None:
    """Пингуем свой публичный URL, чтобы бесплатный Render не засыпал."""
    if not APP_URL:
        log.info("Самопинг выключен: нет APP_URL / RENDER_EXTERNAL_URL (локальный запуск?)")
        return
    log.info("Самопинг включён: %s/health каждые %d сек", APP_URL, PING_INTERVAL)
    while True:
        await asyncio.sleep(PING_INTERVAL)
        try:
            async with session.get(f"{APP_URL}/health") as resp:
                log.debug("Самопинг %s -> %s", APP_URL, resp.status)
        except Exception as e:  # noqa: BLE001 — сеть может моргнуть, это не повод падать
            log.warning("Самопинг не удался: %s", e)


async def task_db_ping() -> None:
    """Раз в 5 минут проверяем, что соединение с Aiven живое."""
    while True:
        await asyncio.sleep(DB_PING_SECONDS)
        try:
            async with st.pool.acquire() as c:
                await c.fetchval("SELECT 1")
            log.debug("DB ping OK")
        except Exception as e:  # noqa: BLE001
            log.warning("DB ping failed: %s", e)


async def _supervised(name: str, coro_func) -> None:
    """Фоновая задача не должна умирать навсегда: при падении перезапускаем."""
    while True:
        try:
            await coro_func()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.error("Фоновая задача %s упала, перезапуск через 30с: %s", name, e)
            await asyncio.sleep(30)
        else:
            log.error("Фоновая задача %s завершилась, перезапуск через 30с", name)
            await asyncio.sleep(30)


async def main() -> None:
    if "ВСТАВЬТЕ" in DATABASE_URL or "ПАРОЛЬ" in DATABASE_URL:
        log.error(
            "В DATABASE_URL остался пример из шаблона. Впишите Service URI из Aiven "
            "(Aiven Console → ваш сервис → Overview → Connection information) "
            "прямо в bot.py в переменную DATABASE_URL."
        )
        return
    if "ВСТАВЬТЕ" in BOT_TOKEN:
        log.error("В BOT_TOKEN остался пример из шаблона. Впишите токен от @BotFather прямо в bot.py.")
        return

    await db_init()
    await start_web()

    me = await bot.get_me()
    log.info("Bot @%s started (id=%s)", me.username, me.id)

    session = ClientSession()
    dp.include_router(r)

    # ВАЖНО: asyncio.create_task() хранит только слабую ссылку на задачу —
    # если не сохранить сильную ссылку где-то ещё, сборщик мусора может
    # уничтожить задачу в любой момент, даже не завершив её. Раньше ссылки
    # никуда не сохранялись, поэтому самопинг и проверка соединения с БД
    # (task_self_ping / task_db_ping) могли незаметно «умирать» в фоне.
    # Как следствие, через какое-то время простаивающее соединение с БД
    # переставало обновляться и переставало отвечать — а поскольку все
    # групповые команды идут через БД (pass_gate), бот выглядел «сломанным»
    # именно в группах, при этом /start в личке (не использует БД) продолжал
    # отвечать как ни в чём не бывало.
    st.bg_tasks = [
        asyncio.create_task(_supervised("task_self_ping", lambda: task_self_ping(session))),
        asyncio.create_task(_supervised("task_db_ping", task_db_ping)),
    ]

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        for t in st.bg_tasks:
            t.cancel()
        await session.close()
        if st.pool:
            await st.pool.close()


if __name__ == "__main__":
    asyncio.run(main())
