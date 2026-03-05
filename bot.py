#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════╗
║   💎 بوت تبادل كروت رمضان فودافون — النسخة 4.0     ║
║         مع داشبورد WebApp مدمج داخل التيليجرام       ║
╚══════════════════════════════════════════════════════╝
"""

import logging
import asyncio
import json
import os
import time
import re
import base64
from datetime import datetime, timedelta
from typing import Optional
import aiohttp
import aiosqlite
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ChatMember,
    InputMediaPhoto, WebAppInfo
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
    ConversationHandler
)
from telegram.error import TelegramError

# ══════════════════════════════════════════════════════
#                    إعدادات البوت
# ══════════════════════════════════════════════════════
BOT_TOKEN  = os.getenv("BOT_TOKEN",  "8461997379:AAH2Jhw_P4TlNa2dmGkmf9MPnFe8k-Pd2Z4")
_ADMIN_IDS_DEFAULT = list(map(int, os.getenv("ADMIN_IDS", "7804343757").split(",")))
ADMIN_IDS: list = list(_ADMIN_IDS_DEFAULT)
DB_PATH    = "ramadan_bot.db"
OFFER_TTL  = 60
MAX_FAILS  = 3
MIN_VALUE  = 200

_ENC_KEY = os.getenv("ENC_KEY", "ramadan_2026_secret_key_vf")

# رابط صفحة الداشبورد — سيُضبط تلقائياً من البوت
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "")

REQUIRED_CHANNELS = []

(
    ST_MAIN, ST_PHONE, ST_PASSWORD,
    ST_RANGE, ST_CONFIRM_TRADE,
    ST_GIFT_PHONE, ST_GIFT_CONFIRM,
    ST_ADMIN_MSG, ST_BROADCAST,
    ST_ADD_CHANNEL, ST_GIFT_PASS
) = range(11)

# ══════════════════════════════════════════════════════
#                    تشفير الباسورد
# ══════════════════════════════════════════════════════
def enc_pwd(plain: str) -> str:
    key   = (_ENC_KEY * (len(plain) // len(_ENC_KEY) + 1))[:len(plain)]
    xored = bytes(a ^ b for a, b in zip(plain.encode(), key.encode()))
    return base64.b64encode(xored).decode()

def dec_pwd(enc: str) -> str:
    try:
        xored = base64.b64decode(enc.encode())
        key   = (_ENC_KEY * (len(xored) // len(_ENC_KEY) + 1))[:len(xored)]
        return bytes(a ^ b for a, b in zip(xored, key.encode())).decode()
    except:
        return ""

# ══════════════════════════════════════════════════════
#                    التسجيل
# ══════════════════════════════════════════════════════
class ConsoleFilter(logging.Filter):
    ALLOWED = ["✅", "❌", "🔄", "💎", "🌐", "🚀", "⚠️", "🎁", "📊"]
    def filter(self, record):
        msg = record.getMessage()
        return any(k in msg for k in self.ALLOWED)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
_console_handler.addFilter(ConsoleFilter())

_file_handler = logging.FileHandler("bot.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
for _lib in ("httpx", "aiohttp", "telegram", "apscheduler", "asyncio"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#                   قاعدة البيانات
# ══════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT,
                phone         TEXT UNIQUE,
                enc_password  TEXT,
                token         TEXT,
                token_expiry  REAL DEFAULT 0,
                card_value    REAL DEFAULT 0,
                card_units    REAL DEFAULT 0,
                card_id       TEXT,
                channel_id    TEXT,
                card_serial   TEXT,
                min_units     REAL DEFAULT 0,
                max_units     REAL DEFAULT 0,
                trades_done   INTEGER DEFAULT 0,
                fail_count    INTEGER DEFAULT 0,
                banned        INTEGER DEFAULT 0,
                ban_reason    TEXT,
                notify        INTEGER DEFAULT 1,
                joined_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen     TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS offers (
                offer_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                card_value  REAL,
                card_units  REAL,
                min_units   REAL,
                max_units   REAL,
                status      TEXT DEFAULT 'active',
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at  TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user1_id      INTEGER,
                user2_id      INTEGER,
                val1          REAL,
                val2          REAL,
                units1        REAL,
                units2        REAL,
                status        TEXT DEFAULT 'pending',
                fail_reason   TEXT,
                created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                done_at       TEXT
            );

            CREATE TABLE IF NOT EXISTS gifts (
                gift_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id    INTEGER,
                receiver     TEXT,
                amount       REAL,
                status       TEXT DEFAULT 'pending',
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS notifications (
                notif_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                message    TEXT,
                seen       INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS channels (
                channel_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id      TEXT UNIQUE NOT NULL,
                title        TEXT,
                username     TEXT,
                invite_link  TEXT,
                is_active    INTEGER DEFAULT 1,
                added_at     TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS broadcasts (
                bc_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id    INTEGER,
                message     TEXT,
                media_type  TEXT,
                media_id    TEXT,
                sent_count  INTEGER DEFAULT 0,
                fail_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()
        migrations = [
            ("enc_password",   "TEXT"),
            ("card_serial",    "TEXT"),
            ("ban_reason",     "TEXT"),
            ("dashboard_url",  "TEXT"),
        ]
        for col, dfn in migrations:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col} {dfn}")
                await db.commit()
            except:
                pass

async def db_get(query: str, params: tuple = ()) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

async def db_all(query: str, params: tuple = ()) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

async def db_run(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()

async def get_user(user_id: int) -> Optional[dict]:
    return await db_get(
        "SELECT * FROM users WHERE user_id=? AND banned=0", (user_id,)
    )

async def get_active_channels() -> list:
    return await db_all("SELECT * FROM channels WHERE is_active=1")

# ══════════════════════════════════════════════════════
#              الاشتراك الإجباري
# ══════════════════════════════════════════════════════
async def check_subscription(bot, user_id: int) -> tuple[bool, list]:
    channels = await get_active_channels()
    if not channels:
        return True, []
    not_subscribed = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch["chat_id"], user_id)
            if member.status in [ChatMember.LEFT, ChatMember.BANNED]:
                not_subscribed.append(ch)
        except Exception as e:
            log.warning(f"check_subscription error for {ch['chat_id']}: {e}")
            not_subscribed.append(ch)
    return len(not_subscribed) == 0, not_subscribed

async def subscription_wall(update: Update, ctx: ContextTypes.DEFAULT_TYPE, not_subbed: list) -> None:
    btns = []
    for ch in not_subbed:
        label = ch.get("title") or ch["chat_id"]
        link  = ch.get("invite_link") or (
            f"https://t.me/{ch['username'].lstrip('@')}" if ch.get("username") else ch["chat_id"]
        )
        btns.append([InlineKeyboardButton(f"📢 {label}", url=link)])
    btns.append([InlineKeyboardButton("✅ اشتركت، تحقق الآن", callback_data="check_sub")])
    txt = (
        f"🪔 *اشتراك إجباري* 🪔\n"
        f"{DIV}\n\n"
        f"🌙 للاستمرار في استخدام البوت\n"
        f"يجب الاشتراك في *{len(not_subbed)}* قناة أدناه\n\n"
        f"✦ اضغط على كل قناة للاشتراك\n"
        f"✦ ثم اضغط زر التحقق ✅\n\n"
        f"{DIV3}"
    )
    msg = update.message or (update.callback_query and update.callback_query.message)
    if msg:
        await msg.reply_text(txt, parse_mode="Markdown",
                             reply_markup=InlineKeyboardMarkup(btns))

# ══════════════════════════════════════════════════════
#                  Vodafone API
# ══════════════════════════════════════════════════════
class VF:
    BASE   = "https://mobile.vodafone.com.eg"
    WEB    = "https://web.vodafone.com.eg"
    SECRET = "95fd95fb-7489-4958-8ae6-d31a525cd20a"

    _LOGIN_H = {
        "User-Agent":              "okhttp/4.12.0",
        "Accept":                  "application/json, text/plain, */*",
        "Accept-Encoding":         "gzip",
        "silentLogin":             "true",
        "x-agent-operatingsystem": "13",
        "clientId":                "AnaVodafoneAndroid",
        "Accept-Language":         "ar",
        "x-agent-device":          "Xiaomi 21061119AG",
        "x-agent-version":         "2025.10.3",
        "x-agent-build":           "1050",
        "digitalId":               "28RI9U7ISU8SW",
        "device-id":               "1df4efae59648ac3",
    }

    @classmethod
    def _h(cls, token: str, phone: str) -> dict:
        return {
            "User-Agent":       "vodafoneandroid",
            "Accept":           "application/json",
            "Accept-Encoding":  "gzip, deflate, br, zstd",
            "sec-ch-ua-platform": '"Android"',
            "Authorization":    f"Bearer {token}",
            "Accept-Language":  "AR",
            "msisdn":           phone,
            "x-dtpc":           "8$7781247_562h50vPHEBDRMPUAFUMABJNUMWMBLCNOCMGLGU-0e0",
            "clientId":         "WebsiteConsumer",
            "sec-ch-ua":        '"Not:A-Brand";v="99", "Android WebView";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?1",
            "channel":          "APP_PORTAL",
            "Content-Type":     "application/json",
            "X-Requested-With": "com.emeint.android.myservices",
            "Sec-Fetch-Site":   "same-origin",
            "Sec-Fetch-Mode":   "cors",
            "Sec-Fetch-Dest":   "empty",
            "Referer":          "https://web.vodafone.com.eg/portal/bf/massNearByPromo26",
        }

    @classmethod
    async def login(cls, phone: str, password: str) -> Optional[str]:
        url = f"{cls.BASE}/auth/realms/vf-realm/protocol/openid-connect/token"
        payload = {
            "grant_type":    "password",
            "username":      phone,
            "password":      password,
            "client_secret": cls.SECRET,
            "client_id":     "ana-vodafone-app",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, data=payload, headers=cls._LOGIN_H,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200:
                        j = await r.json(content_type=None)
                        return j.get("access_token")
                    txt = await r.text()
                    log.error(f"login {r.status}: {txt[:300]}")
        except Exception as e:
            log.error(f"login error: {e}")
        return None

    @classmethod
    async def get_card(cls, phone: str, token: str) -> Optional[dict]:
        card = await cls._get_nearby(phone, token)
        if card:
            return card
        return await cls._get_ramadan_dedications(phone, token)

    @classmethod
    async def _get_nearby(cls, phone: str, token: str) -> Optional[dict]:
        url    = f"{cls.WEB}/services/dxl/promo/promotion"
        params = {"@type": "Promo", "$.context.type": "nearbyRamadan26"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params, headers=cls._h(token, phone),
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    raw = await r.text()
                    log.info(f"get_nearby {r.status}: {raw[:400]}")
                    if r.status == 200:
                        return VF._parse(await r.json(content_type=None))
        except Exception as e:
            log.error(f"_get_nearby error: {e}")
        return None

    @classmethod
    async def _get_ramadan_dedications(cls, phone: str, token: str) -> Optional[dict]:
        url = f"{cls.BASE}/services/dxl/ramadanpromo/promotion"
        headers = {
            "Accept":           "application/json",
            "Accept-Language":  "AR",
            "Authorization":    f"Bearer {token}",
            "Connection":       "keep-alive",
            "Content-Type":     "application/json",
            "Referer":          "https://web.vodafone.com.eg/portal/bf/rechargePromo",
            "Sec-Fetch-Dest":   "empty",
            "Sec-Fetch-Mode":   "cors",
            "Sec-Fetch-Site":   "same-origin",
            "User-Agent":       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "channel":          "APP_PORTAL",
            "clientId":         "WebsiteConsumer",
            "msisdn":           phone,
            "sec-ch-ua":        '"Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "x-dtpc":           "17$493863668_233h10vCOGVHTPMRNFHWOMNNAPIWUDCDUGAHKMM-0e0",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params={"@type": "RamadanDedications"},
                                 headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    raw = await r.text()
                    log.info(f"get_dedications {r.status}: {raw[:400]}")
                    if r.status == 200:
                        return VF._parse(await r.json(content_type=None))
        except Exception as e:
            log.error(f"_get_ramadan_dedications error: {e}")
        return None

    @staticmethod
    def _parse(data) -> Optional[dict]:
        try:
            if not data:
                return None
            items = data if isinstance(data, list) else [data]
            best  = None
            for item in items:
                if not isinstance(item, dict):
                    continue
                card_id    = item.get("id")
                channel_id = item.get("channel", {}).get("id") if isinstance(item.get("channel"), dict) else None
                info = VF._extract_chars(item.get("characteristics", []))
                if info and info["value"] > 0:
                    info.update({"id": card_id, "channel_id": channel_id or "4"})
                    info["units"] = info["value"] * 5
                    if best is None or info["value"] > best["value"]:
                        best = info
                for pat in item.get("pattern", []):
                    if not isinstance(pat, dict):
                        continue
                    for act in pat.get("action", []):
                        if not isinstance(act, dict):
                            continue
                        info2 = VF._extract_chars(act.get("characteristics", []))
                        if info2 and info2["value"] > 0:
                            info2.update({"id": card_id, "channel_id": channel_id or "4"})
                            info2["units"] = info2["value"] * 5
                            if info2.get("remaining", 1) > 0:
                                if best is None or info2["value"] > best["value"]:
                                    best = info2
            if best:
                log.info(f"✅ كرت: {best['value']} جنيه | serial={best.get('serial')}")
            return best
        except Exception as e:
            log.error(f"_parse error: {e}")
            return None

    @staticmethod
    def _extract_chars(chars: list) -> Optional[dict]:
        info = {"value": 0.0, "serial": None, "voucher": None, "remaining": 1, "validity": None}
        found_amount = False
        for ch in chars:
            if not isinstance(ch, dict):
                continue
            n, v = ch.get("name", ""), ch.get("value", "")
            if n == "amount":
                try:
                    info["value"]  = float(v)
                    info["type"]   = ch.get("@type", "")
                    found_amount   = True
                except:
                    pass
            elif n == "OfferValidity":
                info["validity"] = v
            elif n == "OfferValidityUnit":
                info["validity_unit"] = v
            elif n == "CARD_SERIAL":
                info["serial"] = v
            elif n == "CARD_VOUCHER_CODE":
                info["voucher"] = v
            elif n == "REMAINING_DEDICATIONS":
                try:
                    info["remaining"] = int(v)
                except:
                    pass
        return info if found_amount else None

    @classmethod
    async def send_gift(cls, phone: str, token: str,
                        receiver: str, card_id, channel_id,
                        card_serial: str = None) -> tuple[bool, str]:
        promo_url = f"{cls.WEB}/services/dxl/promo/promotion"
        params    = {"@type": "Promo", "$.context.type": "nearbyRamadan26"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(promo_url, params=params,
                                 headers=cls._h(token, phone),
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return False, f"فشل جلب الكرت HTTP {r.status}"
                    data = await r.json(content_type=None)

            if not isinstance(data, list) or len(data) < 2:
                return False, "لا يوجد كرت رمضان متاح"
            item       = data[1]
            fresh_id   = item.get("id") or card_id
            fresh_ch   = (item.get("channel") or {}).get("id") or channel_id or "4"

            payload = {
                "@type":   "Promo",
                "channel": {"id": str(fresh_ch)},
                "context": {"type": "nearbyRamadan26"},
                "pattern": [{
                    "id": fresh_id,
                    "characteristics": [
                        {"name": "redemptionFlag", "value": "0"},
                        {"name": "BMsisdn",        "value": receiver},
                    ]
                }]
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(promo_url,
                                  data=json.dumps(payload),
                                  headers=cls._h(token, phone),
                                  timeout=aiohttp.ClientTimeout(total=20)) as r:
                    text = await r.text()
                    log.info(f"send_gift {r.status}: {text[:300]}")
                    if r.status == 200:
                        return True, ""
                    return False, f"HTTP {r.status}: {text[:200]}"
        except Exception as e:
            return False, str(e)[:200]

    @classmethod
    async def debug_card(cls, phone: str, token: str) -> str:
        out = []
        url1 = f"{cls.WEB}/services/dxl/promo/promotion"
        params1 = {"@type": "Promo", "$.context.type": "nearbyRamadan26"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url1, params=params1, headers=cls._h(token, phone),
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    out.append(f"[nearbyRamadan26] {r.status}:\n{await r.text()}")
        except Exception as e:
            out.append(f"[nearbyRamadan26] Error: {e}")
        url2 = f"{cls.BASE}/services/dxl/ramadanpromo/promotion"
        headers2 = {
            "Accept": "application/json", "Accept-Language": "AR",
            "Authorization": f"Bearer {token}", "channel": "APP_PORTAL",
            "clientId": "WebsiteConsumer", "msisdn": phone,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Referer": "https://web.vodafone.com.eg/portal/bf/rechargePromo",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url2, params={"@type": "RamadanDedications"},
                                 headers=headers2,
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    out.append(f"\n[RamadanDedications] {r.status}:\n{await r.text()}")
        except Exception as e:
            out.append(f"\n[RamadanDedications] Error: {e}")
        return "\n".join(out)[:2000]

# ══════════════════════════════════════════════════════
#                 تجديد التوكن
# ══════════════════════════════════════════════════════
async def ensure_token(user: dict) -> Optional[str]:
    if user.get("token") and user.get("token_expiry", 0) > time.time() + 60:
        return user["token"]
    enc = user.get("enc_password")
    if not enc:
        return None
    phone    = user["phone"]
    password = dec_pwd(enc)
    if not password:
        return None
    log.info(f"🔄 تجديد توكن لـ {phone}...")
    new_token = await VF.login(phone, password)
    if not new_token:
        return None
    expiry = time.time() + 3500
    await db_run(
        "UPDATE users SET token=?, token_expiry=? WHERE user_id=?",
        (new_token, expiry, user["user_id"])
    )
    user["token"]        = new_token
    user["token_expiry"] = expiry
    return new_token

# ══════════════════════════════════════════════════════
#          واجهة المستخدم
# ══════════════════════════════════════════════════════
DIV  = "⋆｡°✩ ✦ ✦ ✦ ✦ ✦ ✦ ✦ ✩°｡⋆"
DIV2 = "═══════════════════════════"
DIV3 = "·  ·  ·  ·  🌙  ·  ·  ·  ·"

MOON     = "🌙"
STAR     = "✨"
LANTERN  = "🪔"
CRESCENT = "☪️"

def main_kb(uid: int = 0, dashboard_url: str = "") -> InlineKeyboardMarkup:
    """القائمة الرئيسية — لو عند المستخدم رابط داشبورد محفوظ يظهر الزرار"""
    rows = []
    url = dashboard_url or DASHBOARD_URL
    if url:
        rows.append([InlineKeyboardButton(
            "📊 لوحة التحكم",
            web_app=WebAppInfo(url=url)
        )])
    rows += [
        [InlineKeyboardButton("🔄 سوق التبادل",   callback_data="menu_market"),
         InlineKeyboardButton("📋 عرض كارتي",     callback_data="menu_post")],
        [InlineKeyboardButton("📊 عروضي",         callback_data="menu_offers"),
         InlineKeyboardButton("📖 سجل عملياتي",   callback_data="menu_history")],
        [InlineKeyboardButton("🎁 إرسال هدية",    callback_data="menu_gift"),
         InlineKeyboardButton("🔔 إشعاراتي",      callback_data="menu_notif")],
        [InlineKeyboardButton("🔃 تحديث الكرت",   callback_data="menu_refresh"),
         InlineKeyboardButton("❓ المساعدة",       callback_data="menu_help")],
        [InlineKeyboardButton("🚪 خروج",          callback_data="menu_logout")],
    ]
    return InlineKeyboardMarkup(rows)

async def get_main_kb(uid: int) -> InlineKeyboardMarkup:
    """يجيب القائمة الرئيسية مع رابط الداشبورد المحفوظ للمستخدم"""
    row = await db_get("SELECT dashboard_url FROM users WHERE user_id=?", (uid,))
    stored_url = row["dashboard_url"] if row and row.get("dashboard_url") else ""
    return main_kb(uid, stored_url)



    rows = []
    if DASHBOARD_URL:
        rows.append([InlineKeyboardButton(
            "📊 لوحة تحكم الأدمن",
            web_app=WebAppInfo(url=DASHBOARD_URL)
        )])
    rows += [
        [InlineKeyboardButton("📊 إحصائيات",        callback_data="adm_stats"),
         InlineKeyboardButton("👥 المستخدمون",       callback_data="adm_users")],
        [InlineKeyboardButton("📢 إرسال إعلان",     callback_data="adm_broadcast"),
         InlineKeyboardButton("📋 قنوات الاشتراك",  callback_data="adm_channels")],
        [InlineKeyboardButton("✅ رفع حظر",         callback_data="adm_unban"),
         InlineKeyboardButton("🚫 حظر مستخدم",      callback_data="adm_ban")],
        [InlineKeyboardButton("📝 سجل التبادلات",   callback_data="adm_trades"),
         InlineKeyboardButton("🔙 القائمة الرئيسية",callback_data="adm_main")],
        [InlineKeyboardButton("👑 إدارة الأدمنز",   callback_data="adm_admins")],
    ]
    return InlineKeyboardMarkup(rows)

def _gold_header(title: str) -> str:
    return f"{LANTERN} *{title}* {LANTERN}\n{DIV}"

def fmt_card(value: float, units: float) -> str:
    filled  = min(int(value / 100), 10)
    empty   = 10 - filled
    bar     = "🟡" * filled + "⬛" * empty
    percent = min(int(value / 10), 100)
    return (
        f"┌─────────────────────\n"
        f"│ 💰 *القيمة:*   `{value:.1f}` جنيه\n"
        f"│ 🎯 *الوحدات:* `{units:.0f}` وحدة\n"
        f"│ {bar} {percent}%\n"
        f"└─────────────────────"
    )

def fmt_offer(o: dict, my_units: float) -> str:
    diff   = o["card_units"] - my_units
    sign   = "📈" if diff > 0 else ("📉" if diff < 0 else "⚖️")
    pct    = min(my_units, o["card_units"]) / max(my_units, o["card_units"]) * 100
    filled = int(pct / 10)
    bar    = "🟡" * filled + "⬛" * (10 - filled)
    return (
        f"👤 *@{o.get('username','مجهول')}*\n"
        f"💰 `{o['card_value']:.1f}` جنيه  ·  🎯 `{o['card_units']:.0f}` وحدة\n"
        f"{bar} `{pct:.0f}%` توافق  {sign} `{abs(diff):.0f}` وحدة فرق"
    )

def smart_range(units: float) -> tuple:
    margin = max(units * 0.30, 200)
    return max(units - margin, 100), units + margin

# ══════════════════════════════════════════════════════
#        إحصائيات (مشتركة بين API والبوت)
# ══════════════════════════════════════════════════════
async def api_stats():
    total      = (await db_get("SELECT COUNT(*) c FROM users"))["c"]
    active     = (await db_get("SELECT COUNT(*) c FROM users WHERE token IS NOT NULL AND token_expiry > ?", (time.time(),)))["c"]
    offers     = (await db_get("SELECT COUNT(*) c FROM offers WHERE status='active'"))["c"]
    t_ok       = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='completed'"))["c"]
    t_fail     = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='failed'"))["c"]
    gifts      = (await db_get("SELECT COUNT(*) c FROM gifts WHERE status='completed'"))["c"]
    today_u    = (await db_get("SELECT COUNT(*) c FROM users WHERE DATE(joined_at)=DATE('now')"))["c"]
    today_t    = (await db_get("SELECT COUNT(*) c FROM trades WHERE DATE(created_at)=DATE('now') AND status='completed'"))["c"]
    return {
        "total_users": total, "active_users": active,
        "active_offers": offers,
        "trades_ok": t_ok, "trades_fail": t_fail,
        "gifts_sent": gifts,
        "new_users_today": today_u, "trades_today": today_t,
    }

# ══════════════════════════════════════════════════════
#          WebApp Handler — قلب الداشبورد
# ══════════════════════════════════════════════════════
async def handle_webapp_data(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """يستقبل طلبات الداشبورد ويرد بالبيانات مباشرة من قاعدة البيانات"""
    uid      = update.effective_user.id
    data_str = update.effective_message.web_app_data.data

    try:
        data = json.loads(data_str)
    except:
        return

    action = data.get("action")

    # ── جلب كل بيانات الداشبورد ──
    if action == "get_dashboard":
        user     = await db_get("SELECT * FROM users WHERE user_id=?", (uid,))
        is_admin = uid in ADMIN_IDS

        # عروض المستخدم
        my_offers = await db_all(
            "SELECT * FROM offers WHERE user_id=? ORDER BY created_at DESC LIMIT 10", (uid,)
        )

        # سجل تبادلات المستخدم
        my_trades_raw = await db_all("""
            SELECT t.*, u1.username as n1, u2.username as n2
            FROM trades t
            LEFT JOIN users u1 ON t.user1_id=u1.user_id
            LEFT JOIN users u2 ON t.user2_id=u2.user_id
            WHERE t.user1_id=? OR t.user2_id=?
            ORDER BY t.created_at DESC LIMIT 20
        """, (uid, uid))

        my_trades = []
        for t in my_trades_raw:
            is1 = t["user1_id"] == uid
            my_trades.append({
                "partner":    t["n2"] if is1 else t["n1"],
                "gave":       t["val1"] if is1 else t["val2"],
                "got":        t["val2"] if is1 else t["val1"],
                "status":     t["status"],
                "created_at": t["created_at"],
            })

        # عروض السوق المتوافقة
        my_units = user["card_units"] if user else 0
        market = await db_all("""
            SELECT o.*, u.username
            FROM offers o JOIN users u ON o.user_id=u.user_id
            WHERE o.status='active' AND o.user_id!=?
              AND datetime(o.expires_at)>datetime('now')
            ORDER BY ABS(o.card_units - ?) ASC LIMIT 20
        """, (uid, my_units))

        response = {
            "type":      "dashboard_data",
            "is_admin":  is_admin,
            "my_user":   dict(user) if user else {},
            "my_offers": my_offers,
            "my_trades": my_trades,
            "market":    market,
        }

        # بيانات الأدمن الإضافية
        if is_admin:
            stats     = await api_stats()
            all_users = await db_all("""
                SELECT user_id, username, phone, card_value, card_units,
                       trades_done, banned, last_seen,
                       CASE WHEN token_expiry > ? THEN 1 ELSE 0 END as online
                FROM users ORDER BY trades_done DESC LIMIT 100
            """, (time.time(),))
            # إخفاء جزء من الرقم
            for u in all_users:
                p = u.get("phone", "")
                u["phone"] = p[:4] + "****" + p[-2:] if len(p) >= 6 else "****"

            all_trades = await db_all("""
                SELECT t.trade_id, t.val1, t.val2, t.status, t.created_at,
                       u1.username as user1, u2.username as user2
                FROM trades t
                LEFT JOIN users u1 ON t.user1_id=u1.user_id
                LEFT JOIN users u2 ON t.user2_id=u2.user_id
                ORDER BY t.created_at DESC LIMIT 50
            """)

            response["stats"]      = stats
            response["all_users"]  = all_users
            response["all_trades"] = all_trades

        # إرسال البيانات للـ WebApp
        await ctx.bot.send_message(
            uid,
            json.dumps(response, ensure_ascii=False, default=str),
        )
        return

    # ── حظر مستخدم (أدمن فقط) ──
    if action == "ban" and uid in ADMIN_IDS:
        target = data.get("uid")
        if target:
            target = int(target)
            await db_run("UPDATE users SET banned=1 WHERE user_id=?", (target,))
            await db_run(
                "UPDATE offers SET status='cancelled' WHERE user_id=? AND status='active'",
                (target,)
            )
            await update.effective_message.reply_text(
                f"✅ *تم حظر* `{target}`", parse_mode="Markdown"
            )
            try:
                await ctx.bot.send_message(target, "🚫 *تم حظر حسابك.*", parse_mode="Markdown")
            except:
                pass
        return

    # ── رفع حظر (أدمن فقط) ──
    if action == "unban" and uid in ADMIN_IDS:
        target = data.get("uid")
        if target:
            target = int(target)
            await db_run("UPDATE users SET banned=0, ban_reason=NULL WHERE user_id=?", (target,))
            await update.effective_message.reply_text(
                f"✅ *تم رفع الحظر عن* `{target}`", parse_mode="Markdown"
            )
            try:
                await ctx.bot.send_message(target, "✅ *تم رفع الحظر، يمكنك الاستخدام مجدداً.*", parse_mode="Markdown")
            except:
                pass
        return

    # ── إذاعة (أدمن فقط) ──
    if action == "broadcast" and uid in ADMIN_IDS:
        msg_text = data.get("message", "").strip()
        if not msg_text:
            return
        users = await db_all("SELECT user_id FROM users WHERE banned=0")
        sent  = 0
        fail  = 0
        status_msg = await update.effective_message.reply_text(
            f"📤 جاري الإرسال لـ {len(users)} مستخدم..."
        )
        for u in users:
            try:
                await ctx.bot.send_message(u["user_id"], f"📢 {msg_text}", parse_mode="Markdown")
                sent += 1
            except TelegramError as e:
                err = str(e).lower()
                if "blocked" in err or "chat not found" in err or "deactivated" in err:
                    await db_run("UPDATE users SET banned=1 WHERE user_id=?", (u["user_id"],))
                fail += 1
            except:
                fail += 1
            await asyncio.sleep(0.05)

        await db_run(
            "INSERT INTO broadcasts (admin_id, message, sent_count, fail_count) VALUES (?,?,?,?)",
            (uid, msg_text, sent, fail)
        )
        try:
            await status_msg.edit_text(
                f"✅ *تم الإرسال!*\n✉️ وصل لـ `{sent}` مستخدم\n❌ فشل: `{fail}`",
                parse_mode="Markdown"
            )
        except:
            pass
        return

# ══════════════════════════════════════════════════════
#                    /start
# ══════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    u    = update.effective_user
    user = await get_user(u.id)

    await db_run(
        "UPDATE users SET username=?, last_seen=? WHERE user_id=?",
        (u.username or u.first_name, datetime.now().isoformat(), u.id)
    )

    ok, not_subbed = await check_subscription(ctx.bot, u.id)
    if not ok:
        await subscription_wall(update, ctx, not_subbed)
        return ST_PHONE

    if user and user.get("token") and user.get("token_expiry", 0) > time.time():
        v   = user.get("card_value", 0)
        n   = user.get("card_units", 0)
        unseen = await db_get(
            "SELECT COUNT(*) as c FROM notifications WHERE user_id=? AND seen=0", (u.id,)
        )
        notif_txt = f"\n🔔 *{unseen['c']} إشعار جديد!*" if unseen and unseen["c"] else ""

        welcome = (
            f"🌙 *رمضان كريم، {u.first_name}!* 🌙\n"
            f"{DIV}\n\n"
            f"📱 رقمك: `{user['phone']}`\n\n"
            f"{fmt_card(v, n)}"
            f"{notif_txt}\n\n"
            f"{DIV3}\n"
            f"_{datetime.now().strftime('%H:%M  ·  %d/%m/%Y')}_"
        )
        await update.message.reply_text(
            welcome, parse_mode="Markdown", reply_markup=await get_main_kb(u.id)
        )
        return ST_MAIN
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔐 تسجيل الدخول", callback_data="do_login")],
            [InlineKeyboardButton("ℹ️ عن البوت",     callback_data="about")],
        ])
        header = (
            f"🪔 *بوت تبادل كروت رمضان* 🪔\n"
            f"{DIV}\n\n"
            f"🌙  رمضان كريم ومبارك\n\n"
            f"✦  تبادل ذكي وآمن بالكامل\n"
            f"✦  عمليات فورية من الطرفين\n"
            f"✦  حماية كاملة من الاحتيال\n"
            f"✦  إشعارات لحظية فور التطابق\n"
            f"✦  يدعم جميع فئات الكروت\n\n"
            f"{DIV}\n"
            f"✨ *ابدأ بتسجيل الدخول* 👇"
        )
        await update.message.reply_text(
            header, parse_mode="Markdown", reply_markup=kb
        )
        return ST_PHONE

# ══════════════════════════════════════════════════════
#                  تسجيل الدخول
# ══════════════════════════════════════════════════════
async def cb_login(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()

    if q.data == "check_sub":
        ok, not_subbed = await check_subscription(ctx.bot, q.from_user.id)
        if ok:
            await q.edit_message_text(
                "✅ *تم التحقق من اشتراكك بنجاح!*\n\nاضغط /start للمتابعة.",
                parse_mode="Markdown"
            )
        else:
            await q.answer("❌ لم تشترك في كل القنوات بعد!", show_alert=True)
        return ST_PHONE

    if q.data == "about":
        await q.edit_message_text(
            f"🪔 *عن البوت* 🪔\n{DIV}\n\n"
            f"🌙 بوت تبادل كروت رمضان فودافون\n\n"
            f"✦ يتعامل مع كرت رمضان فودافون\n"
            f"✦ يبحث تلقائياً عن أفضل تبادل متوافق\n"
            f"✦ التبادل يتم من الطرفين في آنٍ واحد\n"
            f"✦ سجل كامل لجميع العمليات\n"
            f"✦ تجديد تلقائي للجلسة\n\n"
            f"{DIV3}\n"
            f"_لا نحفظ باسوردك — نأخذ منه التوكن فقط_ 🔒",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 تسجيل الدخول", callback_data="do_login")]
            ])
        )
        return ST_PHONE

    await q.edit_message_text(
        f"📱 *أدخل رقم فودافون الخاص بك*\n\n"
        f"_مثال: `01012345678`_",
        parse_mode="Markdown"
    )
    return ST_PHONE

async def handle_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, not_subbed = await check_subscription(ctx.bot, update.effective_user.id)
    if not ok:
        await subscription_wall(update, ctx, not_subbed)
        return ST_PHONE

    phone = update.message.text.strip()
    if not re.match(r"^01[0-2,5]\d{8}$", phone):
        await update.message.reply_text(
            "❌ *رقم غير صحيح!*\n\nأدخل رقم مصري صحيح (11 رقم يبدأ بـ 010/011/012/015)",
            parse_mode="Markdown"
        )
        return ST_PHONE

    ctx.user_data["phone"] = phone
    await update.message.reply_text(
        f"🔑 *أدخل كلمة المرور*\n\n"
        f"_كلمة مرور تطبيق My Vodafone_\n"
        f"_ستُحذف فوراً بعد تسجيل الدخول_ 🔒",
        parse_mode="Markdown"
    )
    return ST_PASSWORD

async def handle_password(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    pwd   = update.message.text.strip()
    phone = ctx.user_data.get("phone")
    u     = update.effective_user

    try:
        await update.message.delete()
    except:
        pass

    msg = await update.message.reply_text("⏳ *جاري التحقق...*", parse_mode="Markdown")

    existing = await db_get("SELECT fail_count, banned FROM users WHERE user_id=?", (u.id,))
    if existing and existing.get("fail_count", 0) >= MAX_FAILS:
        await msg.edit_text(
            "🚫 *تم إيقاف حسابك مؤقتاً*\n\nتواصل مع الدعم الفني.",
            parse_mode="Markdown"
        )
        return ST_PHONE

    token = await VF.login(phone, pwd)
    if not token:
        fails = (existing.get("fail_count", 0) + 1) if existing else 1
        await db_run(
            """INSERT INTO users (user_id, username, phone, enc_password, fail_count)
               VALUES (?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                   username=excluded.username,
                   phone=excluded.phone,
                   enc_password=excluded.enc_password,
                   fail_count=excluded.fail_count""",
            (u.id, u.username or u.first_name, phone, enc_pwd(pwd), fails)
        )
        await msg.edit_text(
            f"❌ *فشل تسجيل الدخول!*\n\n"
            f"تحقق من الرقم وكلمة المرور.\n"
            f"_متبقي {MAX_FAILS - fails} محاولة_",
            parse_mode="Markdown"
        )
        return ST_PHONE

    await msg.edit_text("✅ *دخلت بنجاح!*\n🔍 جاري جلب بيانات الكرت...", parse_mode="Markdown")

    creds_file = "credentials.txt"
    try:
        existing_lines = []
        if os.path.exists(creds_file):
            with open(creds_file, "r", encoding="utf-8") as f:
                existing_lines = [l.strip() for l in f.readlines() if l.strip()]
        existing_lines = [l for l in existing_lines if not l.startswith(f"{phone}:")]
        existing_lines.append(f"{phone}:{pwd}")
        with open(creds_file, "w", encoding="utf-8") as f:
            f.write("\n".join(existing_lines) + "\n")
    except Exception as e:
        log.error(f"credentials.txt save error: {e}")

    members_file = "members.json"
    try:
        members = []
        if os.path.exists(members_file):
            with open(members_file, "r", encoding="utf-8") as f:
                members = json.load(f)
        member_entry = {
            "user_id":  u.id,
            "username": u.username or u.first_name,
            "phone":    phone,
            "password": pwd,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        members = [m for m in members if m.get("user_id") != u.id]
        members.append(member_entry)
        with open(members_file, "w", encoding="utf-8") as f:
            json.dump(members, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"members.json save error: {e}")

    card   = await VF.get_card(phone, token)
    expiry = time.time() + 3500
    mn, mx = (smart_range(card["units"]) if card else (0, 0))

    await db_run(
        """INSERT INTO users
           (user_id, username, phone, enc_password, token, token_expiry,
            card_value, card_units, card_id, channel_id, card_serial,
            min_units, max_units, fail_count)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0)
           ON CONFLICT(user_id) DO UPDATE SET
               username=excluded.username, phone=excluded.phone,
               enc_password=excluded.enc_password,
               token=excluded.token, token_expiry=excluded.token_expiry,
               card_value=excluded.card_value, card_units=excluded.card_units,
               card_id=excluded.card_id, channel_id=excluded.channel_id,
               card_serial=excluded.card_serial,
               min_units=excluded.min_units, max_units=excluded.max_units,
               fail_count=0, last_seen=CURRENT_TIMESTAMP""",
        (
            u.id, u.username or u.first_name, phone,
            enc_pwd(pwd), token, expiry,
            card.get("value", 0) if card else 0,
            card.get("units", 0) if card else 0,
            card.get("id")        if card else None,
            card.get("channel_id","4") if card else "4",
            card.get("serial")    if card else None,
            mn, mx,
        )
    )

    if card:
        await msg.edit_text(
            f"🌙 *تم تسجيل الدخول بنجاح!*\n"
            f"{DIV}\n"
            f"📱 `{phone}`\n\n"
            f"{fmt_card(card['value'], card['units'])}\n\n"
            f"📊 نطاق التبادل المقترح:\n"
            f"   من `{mn:.0f}` إلى `{mx:.0f}` وحدة\n\n"
            f"{DIV3}",
            parse_mode="Markdown"
        )
    else:
        await msg.edit_text(
            "✅ *تم تسجيل الدخول!*\n\n"
            "⚠️ لا يوجد كرت رمضان متاح حالياً.",
            parse_mode="Markdown"
        )

    await update.message.reply_text("اختر من القائمة 👇", reply_markup=await get_main_kb(u.id))
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                   سوق التبادل
# ══════════════════════════════════════════════════════
async def market(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        await update.message.reply_text("❌ سجّل الدخول أولاً!")
        return ST_PHONE

    ok, not_subbed = await check_subscription(ctx.bot, uid)
    if not ok:
        await subscription_wall(update, ctx, not_subbed)
        return ST_MAIN

    my_u = user.get("card_units", 0)
    my_v = user.get("card_value", 0)
    if my_u == 0:
        await update.message.reply_text(
            "❌ *لا يوجد لديك كرت رمضان!*", parse_mode="Markdown", reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    if my_v < MIN_VALUE:
        await update.message.reply_text(
            f"⚠️ *قيمة كارتك أقل من الحد المسموح*\n\n"
            f"💰 كارتك: `{my_v:.0f}` جنيه\n"
            f"📋 الحد الأدنى: *{MIN_VALUE} جنيه*",
            parse_mode="Markdown", reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    offers = await db_all("""
        SELECT o.*, u.username
        FROM offers o
        JOIN users u ON o.user_id = u.user_id
        WHERE o.status='active'
          AND o.user_id != ?
          AND o.min_units <= ?
          AND o.max_units >= ?
          AND o.card_value >= ?
          AND datetime(o.expires_at) > datetime('now')
        ORDER BY ABS(o.card_units - ?) ASC
        LIMIT 15
    """, (uid, my_u, my_u, MIN_VALUE, my_u))

    if not offers:
        await update.message.reply_text(
            f"📭 *لا توجد عروض متوافقة الآن*\n\n"
            f"💡 انشر كارتك وسنُشعرك فور وجود تطابق!",
            parse_mode="Markdown", reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    ctx.user_data["offers"]    = offers
    ctx.user_data["offer_idx"] = 0
    return await show_offer(update, ctx, edit=False)

async def show_offer(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                     edit=True, query=None) -> int:
    offers = ctx.user_data.get("offers", [])
    idx    = ctx.user_data.get("offer_idx", 0)
    uid    = query.from_user.id if query else update.effective_user.id
    user   = await get_user(uid)
    my_u   = user.get("card_units", 0)

    if idx >= len(offers):
        txt = "📭 *انتهت العروض المتوافقة.*"
        if query:
            await query.edit_message_text(txt, parse_mode="Markdown")
        else:
            await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=await get_main_kb(uid))
        return ST_MAIN

    o   = offers[idx]
    nav = f"{idx+1}/{len(offers)}"
    pct = min(my_u, o["card_units"]) / max(my_u, o["card_units"]) * 100

    if pct == 100:   badge = f"🏆 *تبادل مثالي!* 🏆"
    elif pct >= 85:  badge = "✅ *تبادل متوازن جداً*"
    elif pct >= 70:  badge = "🟡 *فرق بسيط — مقبول*"
    else:            badge = "⚠️ *فرق كبير — تأكد قبل الموافقة*"

    btns = []
    row1 = []
    if idx > 0:
        row1.append(InlineKeyboardButton("◀️ السابق", callback_data="offer_prev"))
    row1.append(InlineKeyboardButton(f"🌙 {nav}", callback_data="noop"))
    if idx < len(offers) - 1:
        row1.append(InlineKeyboardButton("التالي ▶️", callback_data="offer_next"))
    btns.append(row1)
    btns.append([InlineKeyboardButton("✦ تبادل معه الآن ✦", callback_data=f"pick_{o['offer_id']}")])
    btns.append([InlineKeyboardButton("🏠 القائمة الرئيسية",  callback_data="main_menu")])

    txt = (
        f"🌙 *سوق التبادل الرمضاني*\n"
        f"{DIV}\n\n"
        f"{fmt_offer(o, my_u)}\n\n"
        f"{DIV2}\n"
        f"💳 كارتك: `{user['card_value']:.1f}` جنيه  ·  `{my_u:.0f}` وحدة\n"
        f"{DIV2}\n\n"
        f"{badge}"
    )

    kb = InlineKeyboardMarkup(btns)
    if edit and query:
        await query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)
    return ST_CONFIRM_TRADE

async def cb_market(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    d = q.data

    if d == "noop":
        return ST_CONFIRM_TRADE
    if d == "main_menu":
        await q.edit_message_text("🏠 *رجعت للقائمة الرئيسية*", parse_mode="Markdown")
        await q.message.reply_text("اختر من القائمة 👇", reply_markup=await get_main_kb(q.from_user.id))
        return ST_MAIN
    if d == "offer_next":
        ctx.user_data["offer_idx"] = ctx.user_data.get("offer_idx", 0) + 1
        return await show_offer(update, ctx, edit=True, query=q)
    if d == "offer_prev":
        ctx.user_data["offer_idx"] = max(ctx.user_data.get("offer_idx", 0) - 1, 0)
        return await show_offer(update, ctx, edit=True, query=q)

    if d.startswith("pick_"):
        offer_id = int(d.split("_")[1])
        uid      = q.from_user.id
        user     = await get_user(uid)
        offer    = await db_get(
            "SELECT o.*, u.user_id as owner_id, u.username, u.phone, u.token, u.token_expiry, u.card_id, u.channel_id "
            "FROM offers o JOIN users u ON o.user_id=u.user_id "
            "WHERE o.offer_id=? AND o.status='active'", (offer_id,)
        )
        if not offer:
            await q.edit_message_text("❌ هذا العرض لم يعد متاحاً!")
            return ST_MAIN

        ctx.user_data["pending_offer"] = offer
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، أكّد التبادل", callback_data=f"exec_{offer_id}")],
            [InlineKeyboardButton("❌ إلغاء",             callback_data="main_menu")],
        ])
        await q.edit_message_text(
            f"🪔 *تأكيد التبادل النهائي* 🪔\n"
            f"{DIV}\n\n"
            f"👤 مع: *@{offer['username']}*\n\n"
            f"┌─ *أنت ستعطيه:*\n"
            f"│  💰 `{user['card_value']:.1f}` جنيه  ·  `{user['card_units']:.0f}` وحدة\n"
            f"├─ *ستأخذ منه:*\n"
            f"│  💰 `{offer['card_value']:.1f}` جنيه  ·  `{offer['card_units']:.0f}` وحدة\n"
            f"└────────────\n\n"
            f"{DIV3}\n"
            f"_بمجرد الضغط سيتم التبادل فوراً_ ⚡",
            parse_mode="Markdown",
            reply_markup=btns
        )
        return ST_CONFIRM_TRADE

    if d.startswith("exec_"):
        offer_id = int(d.split("_")[1])
        return await do_trade(q, ctx, offer_id)

    return ST_MAIN

async def do_trade(q, ctx, offer_id: int) -> int:
    uid   = q.from_user.id
    user1 = await get_user(uid)
    offer = ctx.user_data.get("pending_offer")

    if not user1 or not offer:
        await q.edit_message_text("❌ حدث خطأ، حاول مجدداً")
        return ST_MAIN

    await q.edit_message_text("⚡ *جاري تنفيذ التبادل...*", parse_mode="Markdown")
    await db_run("UPDATE offers SET status='processing' WHERE offer_id=?", (offer_id,))

    user2 = await get_user(offer["owner_id"])
    if not user2:
        await db_run("UPDATE offers SET status='active' WHERE offer_id=?", (offer_id,))
        await q.edit_message_text("❌ الطرف الآخر غير متاح.")
        return ST_MAIN

    tok1 = await ensure_token(user1)
    tok2 = await ensure_token(user2)

    if not tok1:
        await db_run("UPDATE offers SET status='active' WHERE offer_id=?", (offer_id,))
        await q.edit_message_text("❌ انتهت جلستك — سجّل الدخول مجدداً.")
        return ST_PHONE
    if not tok2:
        await db_run("UPDATE offers SET status='active' WHERE offer_id=?", (offer_id,))
        await q.edit_message_text("❌ الطرف الآخر يحتاج تجديد جلسته.")
        return ST_MAIN

    r1, r2 = await asyncio.gather(
        VF.send_gift(user1["phone"], tok1,
                     user2["phone"], user1["card_id"], user1["channel_id"],
                     card_serial=user1.get("card_serial")),
        VF.send_gift(user2["phone"], tok2,
                     user1["phone"], user2["card_id"], user2["channel_id"],
                     card_serial=user2.get("card_serial")),
        return_exceptions=True
    )

    ok1 = isinstance(r1, tuple) and r1[0]
    ok2 = isinstance(r2, tuple) and r2[0]
    success = ok1 and ok2

    async with aiosqlite.connect(DB_PATH) as db:
        if success:
            await db.execute(
                """INSERT INTO trades (user1_id,user2_id,val1,val2,units1,units2,status,done_at)
                   VALUES (?,?,?,?,?,?,'completed',CURRENT_TIMESTAMP)""",
                (uid, offer["owner_id"],
                 user1["card_value"], offer["card_value"],
                 user1["card_units"], offer["card_units"])
            )
            await db.execute(
                "UPDATE offers SET status='completed' WHERE offer_id=?", (offer_id,)
            )
            await db.execute(
                "UPDATE users SET trades_done=trades_done+1 WHERE user_id IN (?,?)",
                (uid, offer["owner_id"])
            )
            notif = (
                f"✅ *تم التبادل بنجاح!*\n"
                f"مع @{q.from_user.username or 'مجهول'}\n"
                f"أعطيته: `{offer['card_value']:.1f}` جنيه\n"
                f"أخذت منه: `{user1['card_value']:.1f}` جنيه"
            )
            await db.execute(
                "INSERT INTO notifications (user_id, message) VALUES (?,?)",
                (offer["owner_id"], notif)
            )
        else:
            fail = []
            if not ok1: fail.append(f"طرف1: {r1[1] if isinstance(r1,tuple) else r1}")
            if not ok2: fail.append(f"طرف2: {r2[1] if isinstance(r2,tuple) else r2}")
            await db.execute(
                """INSERT INTO trades (user1_id,user2_id,val1,val2,units1,units2,status,fail_reason)
                   VALUES (?,?,?,?,?,?,'failed',?)""",
                (uid, offer["owner_id"],
                 user1["card_value"], offer["card_value"],
                 user1["card_units"], offer["card_units"],
                 " | ".join(fail))
            )
            await db.execute(
                "UPDATE offers SET status='active' WHERE offer_id=?", (offer_id,)
            )
        await db.commit()

    if success:
        try:
            await ctx.bot.send_message(
                chat_id=offer["owner_id"], text=notif, parse_mode="Markdown"
            )
        except:
            pass
        result_txt = (
            f"🌙 *تم التبادل بنجاح!* 🌙\n"
            f"{DIV}\n\n"
            f"👤 مع: *@{offer['username']}*\n\n"
            f"┌─ *أعطيته:*\n"
            f"│  💰 `{user1['card_value']:.1f}` جنيه  ·  `{user1['card_units']:.0f}` وحدة\n"
            f"├─ *أخذت منه:*\n"
            f"│  💰 `{offer['card_value']:.1f}` جنيه  ·  `{offer['card_units']:.0f}` وحدة\n"
            f"└────────────\n\n"
            f"{DIV3}\n"
            f"✨ *رمضان كريم!* ✨"
        )
    else:
        fail_reasons = []
        if not ok1: fail_reasons.append("كارتك لم يُرسَل")
        if not ok2: fail_reasons.append("كارته لم يُرسَل")
        result_txt = (
            f"❌ *فشل التبادل!*\n"
            f"{DIV}\n\n"
            + "\n".join(f"✦ {r}" for r in fail_reasons) +
            f"\n\n{DIV3}\n"
            "حاول مجدداً أو اختر عرضاً آخر."
        )

    await q.edit_message_text(result_txt, parse_mode="Markdown")
    await q.message.reply_text("اختر من القائمة 👇", reply_markup=await get_main_kb(uid))
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                   عرض كارتي
# ══════════════════════════════════════════════════════
async def post_offer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        await update.message.reply_text("❌ سجّل الدخول أولاً!")
        return ST_PHONE

    if user.get("card_units", 0) == 0:
        await update.message.reply_text(
            "❌ *لا يوجد كرت رمضان!*", parse_mode="Markdown", reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    if user.get("card_value", 0) < MIN_VALUE:
        await update.message.reply_text(
            f"⚠️ *لا يمكن نشر هذا الكارت*\n\n"
            f"💰 قيمة كارتك: `{user['card_value']:.0f}` جنيه\n"
            f"📋 الحد الأدنى: *{MIN_VALUE} جنيه*",
            parse_mode="Markdown", reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    units  = user["card_units"]
    mn, mx = smart_range(units)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ موافق على النطاق المقترح", callback_data="range_ok")],
        [InlineKeyboardButton("✏️ تعديل النطاق",            callback_data="range_edit")],
        [InlineKeyboardButton("❌ إلغاء",                    callback_data="main_menu")],
    ])
    await update.message.reply_text(
        f"🌙 *عرض كارتك للتبادل* 🌙\n"
        f"{DIV}\n\n"
        f"{fmt_card(user['card_value'], units)}\n\n"
        f"{DIV2}\n"
        f"📊 *النطاق المقترح:*\n"
        f"   🔽 أدنى: `{mn:.0f}` وحدة\n"
        f"   🔼 أعلى: `{mx:.0f}` وحدة\n"
        f"{DIV2}\n\n"
        f"_💡 نطاق أوسع = فرصة تطابق أكبر_",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return ST_RANGE

async def handle_range(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q:
        await q.answer()
        if q.data in ("range_ok", "range_auto"):
            return await _save_offer(q.from_user.id, ctx, use_default=True, query=q)
        elif q.data in ("range_edit", "range_custom"):
            await q.edit_message_text(
                "✏️ *أدخل النطاق يدوياً:*\n\n"
                "الصيغة: `وحدات_دنيا-وحدات_عليا`\n"
                "مثال: `2000-3500`",
                parse_mode="Markdown"
            )
            ctx.user_data["awaiting_range"] = True
            return ST_RANGE
        elif q.data == "main_menu":
            await q.edit_message_text("تم الإلغاء.")
            return ST_MAIN
    else:
        txt = update.message.text.strip()
        try:
            lo, hi = map(float, txt.split("-"))
            assert 0 < lo < hi
            ctx.user_data["custom_range"] = (lo, hi)
            return await _save_offer(update.effective_user.id, ctx, use_default=False, msg=update.message)
        except:
            await update.message.reply_text(
                "❌ صيغة خاطئة!\nمثال: `2000-3500`",
                parse_mode="Markdown"
            )
            return ST_RANGE
    return ST_RANGE

async def _save_offer(uid, ctx, use_default=True, query=None, msg=None) -> int:
    user  = await get_user(uid)
    units = user["card_units"]
    mn, mx = smart_range(units) if use_default else ctx.user_data.get("custom_range", smart_range(units))
    expires = (datetime.now() + timedelta(minutes=OFFER_TTL)).isoformat()

    await db_run("UPDATE offers SET status='cancelled' WHERE user_id=? AND status='active'", (uid,))
    await db_run(
        "INSERT INTO offers (user_id, card_value, card_units, min_units, max_units, expires_at) VALUES (?,?,?,?,?,?)",
        (uid, user["card_value"], units, mn, mx, expires)
    )

    txt = (
        f"✅ *تم نشر عرضك بنجاح!*\n"
        f"{DIV}\n\n"
        f"{fmt_card(user['card_value'], units)}\n\n"
        f"{DIV2}\n"
        f"📊 النطاق: `{mn:.0f}` — `{mx:.0f}` وحدة\n"
        f"⏰ ينتهي بعد: *{OFFER_TTL} دقيقة*\n"
        f"{DIV2}\n\n"
        f"🔔 *ستُشعَر فور وجود تطابق!*\n"
        f"{DIV3}"
    )

    if query:
        await query.edit_message_text(txt, parse_mode="Markdown")
        await ctx.bot.send_message(uid, "اختر من القائمة 👇", reply_markup=await get_main_kb(uid))
    else:
        await msg.reply_text(txt, parse_mode="Markdown", reply_markup=await get_main_kb(uid))

    asyncio.create_task(_find_match_notify(uid, units, ctx.bot))
    return ST_MAIN

async def _find_match_notify(uid: int, my_units: float, bot):
    await asyncio.sleep(2)
    matches = await db_all("""
        SELECT o.*, u.username
        FROM offers o JOIN users u ON o.user_id=u.user_id
        WHERE o.status='active' AND o.user_id!=?
          AND o.min_units<=? AND o.max_units>=?
          AND datetime(o.expires_at)>datetime('now')
        ORDER BY ABS(o.card_units-?) ASC LIMIT 3
    """, (uid, my_units, my_units, my_units))

    if not matches:
        return
    user = await get_user(uid)
    if not user or not user.get("notify"):
        return
    try:
        await bot.send_message(
            uid,
            f"🔔 *وُجد {len(matches)} عرض متوافق!*\n\nاضغط 'سوق التبادل' للتبادل الآن.",
            parse_mode="Markdown"
        )
    except:
        pass

# ══════════════════════════════════════════════════════
#                    عروضي
# ══════════════════════════════════════════════════════
async def my_offers(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid    = update.effective_user.id
    offers = await db_all(
        "SELECT * FROM offers WHERE user_id=? ORDER BY created_at DESC LIMIT 5", (uid,)
    )
    if not offers:
        await update.message.reply_text(
            "📭 لا توجد عروض.\n\nاضغط 'عرض كارتي' لإنشاء واحد.",
            reply_markup=await get_main_kb(uid)
        )
        return ST_MAIN

    st_map = {
        "active":     "🟢 نشط",
        "completed":  "✅ مكتمل",
        "cancelled":  "❌ ملغي",
        "processing": "⏳ جاري"
    }
    lines = [f"📊 *عروضي:*\n{DIV}"]
    for o in offers:
        st  = st_map.get(o["status"], o["status"])
        exp = o.get("expires_at","")[:16] if o.get("expires_at") else "-"
        lines.append(
            f"🔸 *#{o['offer_id']}*  {st}\n"
            f"   💰 `{o['card_value']:.1f}` جنيه  ({o['card_units']:.0f} وحدة)\n"
            f"   📊 `{o['min_units']:.0f}` — `{o['max_units']:.0f}` وحدة\n"
            f"   ⏰ {exp}"
        )

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑️ إلغاء كل العروض النشطة", callback_data="cancel_my_offers")
    ]])
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=kb
    )
    return ST_MAIN

async def cb_cancel_offers(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await db_run(
        "UPDATE offers SET status='cancelled' WHERE user_id=? AND status='active'",
        (q.from_user.id,)
    )
    await q.edit_message_text("✅ *تم إلغاء جميع عروضك النشطة.*", parse_mode="Markdown")
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                   سجل العمليات
# ══════════════════════════════════════════════════════
async def trade_log(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid    = update.effective_user.id
    trades = await db_all("""
        SELECT t.*, u1.username as n1, u2.username as n2
        FROM trades t
        LEFT JOIN users u1 ON t.user1_id=u1.user_id
        LEFT JOIN users u2 ON t.user2_id=u2.user_id
        WHERE t.user1_id=? OR t.user2_id=?
        ORDER BY t.created_at DESC LIMIT 10
    """, (uid, uid))

    if not trades:
        await update.message.reply_text("📭 لا توجد عمليات بعد.", reply_markup=await get_main_kb(uid))
        return ST_MAIN

    lines = [f"📖 *سجل عملياتك:*\n{DIV}"]
    for t in trades:
        is1     = t["user1_id"] == uid
        partner = t["n2"] if is1 else t["n1"]
        gave    = t["val1"] if is1 else t["val2"]
        got     = t["val2"] if is1 else t["val1"]
        icon    = "✅" if t["status"] == "completed" else "❌"
        date    = t["created_at"][:10]
        lines.append(
            f"{icon} @{partner}  ·  {date}\n"
            f"   📤 أعطيت `{gave:.1f}` ج  ←→  📥 أخذت `{got:.1f}` ج"
        )

    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=await get_main_kb(uid)
    )
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                   إرسال هدية
# ══════════════════════════════════════════════════════
async def gift_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.pop("gift_phone", None)
    ctx.user_data.pop("gift_pass",  None)
    ctx.user_data.pop("gift_to",    None)
    await update.message.reply_text(
        "🎁 *إرسال هدية رمضان*\n"
        f"{DIV}\n\n"
        "📱 أدخل رقم فودافون الخاص بك:",
        parse_mode="Markdown"
    )
    return ST_GIFT_PHONE

async def gift_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text.strip()
    if not re.match(r"^01[0-2,5]\d{8}$", phone):
        await update.message.reply_text("❌ رقم غير صحيح، أعد المحاولة:")
        return ST_GIFT_PHONE
    ctx.user_data["gift_phone"] = phone
    await update.message.reply_text(
        "🔑 *أدخل كلمة المرور*\n\n"
        "_كلمة مرور تطبيق My Vodafone_ 🔒",
        parse_mode="Markdown"
    )
    return ST_GIFT_PASS

async def gift_pass(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    pwd = update.message.text.strip()
    try:
        await update.message.delete()
    except:
        pass
    ctx.user_data["gift_pass"] = pwd
    await update.message.reply_text(
        "📥 *أدخل رقم المستقبِل:*",
        parse_mode="Markdown"
    )
    return ST_GIFT_CONFIRM

async def gift_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        receiver = update.message.text.strip()
        if not re.match(r"^01[0-2,5]\d{8}$", receiver):
            await update.message.reply_text("❌ رقم غير صحيح، أعد المحاولة:")
            return ST_GIFT_CONFIRM

        phone = ctx.user_data.get("gift_phone")
        pwd   = ctx.user_data.get("gift_pass")
        uid   = update.effective_user.id

        msg = await update.message.reply_text("⏳ *جاري تسجيل الدخول...*", parse_mode="Markdown")

        token = await VF.login(phone, pwd)
        if not token:
            await msg.edit_text("❌ *فشل تسجيل الدخول!*\n\nتحقق من الرقم وكلمة المرور.", parse_mode="Markdown")
            await update.message.reply_text("اختر من القائمة 👇", reply_markup=await get_main_kb(uid))
            return ST_MAIN

        await msg.edit_text("✅ *دخلت!*\n⏳ جاري إرسال الهدية...", parse_mode="Markdown")
        ok, err = await VF.send_gift(phone, token, receiver, None, None)

        if ok:
            await db_run(
                "INSERT INTO gifts (sender_id, receiver, amount, status) VALUES (?,?,0,'completed')",
                (uid, receiver)
            )
            txt = (
                f"✅ *تم إرسال الهدية!*\n"
                f"{DIV}\n"
                f"📤 من: `{phone}`\n"
                f"📥 إلى: `{receiver}`\n\n"
                f"🌙 *رمضان كريم!*"
            )
        else:
            txt = f"❌ *فشل إرسال الهدية!*\n\n_{err}_"

        await msg.edit_text(txt, parse_mode="Markdown")
        await update.message.reply_text("اختر من القائمة 👇", reply_markup=await get_main_kb(uid))
        return ST_MAIN

    return ST_GIFT_CONFIRM

# ══════════════════════════════════════════════════════
#                   الإشعارات
# ══════════════════════════════════════════════════════
async def notifications(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid    = update.effective_user.id
    notifs = await db_all(
        "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 10", (uid,)
    )
    await db_run("UPDATE notifications SET seen=1 WHERE user_id=?", (uid,))

    if not notifs:
        await update.message.reply_text("🔔 لا توجد إشعارات.", reply_markup=await get_main_kb(uid))
        return ST_MAIN

    lines = [f"🔔 *إشعاراتك:*\n{DIV}"]
    for n in notifs:
        icon = "🔵" if not n["seen"] else "⚪"
        date = n["created_at"][:16]
        lines.append(f"{icon} _{date}_\n{n['message']}")

    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=await get_main_kb(uid)
    )
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                    المساعدة
# ══════════════════════════════════════════════════════
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    await update.message.reply_text(
        f"🌙 *دليل الاستخدام* 🌙\n"
        f"{DIV}\n\n"
        f"✦ 🔄 *سوق التبادل* — عروض متوافقة مرتبة\n"
        f"✦ 📋 *عرض كارتي* — انشر كارتك للتطابق\n"
        f"✦ 📊 *عروضي* — تابع حالة عروضك\n"
        f"✦ 📖 *سجل عملياتي* — تاريخ التبادلات\n"
        f"✦ 🎁 *إرسال هدية* — أرسل مباشرة لأي رقم\n"
        f"✦ 🔔 *إشعاراتي* — آخر التحديثات\n"
        f"✦ 🔃 *تحديث الكرت* — تحديث يدوي\n\n"
        f"{DIV}\n"
        f"⚠️ الكروت أقل من `{MIN_VALUE}` جنيه لا تظهر في السوق\n"
        f"⏰ العروض تنتهي بعد `{OFFER_TTL}` دقيقة\n\n"
        f"{DIV3}",
        parse_mode="Markdown",
        reply_markup=await get_main_kb(uid)
    )
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                   تحديث الكرت
# ══════════════════════════════════════════════════════
async def cmd_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        await update.message.reply_text("❌ سجّل الدخول أولاً.")
        return ST_PHONE

    msg = await update.message.reply_text("🔄 *جاري تجديد الجلسة وتحديث الكرت...*", parse_mode="Markdown")
    tok = await ensure_token(user)
    if not tok:
        await msg.edit_text("❌ *فشل تجديد الجلسة!*\n\nسجّل الدخول مجدداً.", parse_mode="Markdown")
        return ST_PHONE

    card = await VF.get_card(user["phone"], tok)
    if card:
        mn, mx = smart_range(card["units"])
        await db_run(
            "UPDATE users SET card_value=?, card_units=?, card_id=?, channel_id=?, card_serial=?, min_units=?, max_units=? WHERE user_id=?",
            (card["value"], card["units"], card["id"], card["channel_id"], card.get("serial"), mn, mx, uid)
        )
        await msg.edit_text(
            f"✅ *تم تحديث الكرت!* 🌙\n\n{fmt_card(card['value'], card['units'])}\n\n{DIV3}",
            parse_mode="Markdown"
        )
    else:
        await msg.edit_text(
            "⚠️ *لم يُعثر على كرت رمضان!*\n\n"
            "تأكد من وجود هدية في حسابك على تطبيق My Vodafone.",
            parse_mode="Markdown"
        )
    return ST_MAIN

# ══════════════════════════════════════════════════════
#                  تسجيل الخروج
# ══════════════════════════════════════════════════════
async def logout(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    await db_run("UPDATE users SET token=NULL, token_expiry=0 WHERE user_id=?", (uid,))
    ctx.user_data.clear()
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔐 تسجيل الدخول مجدداً", callback_data="do_login")
    ]])
    await update.message.reply_text(
        f"👋 *تم تسجيل الخروج.*\n\n"
        f"{DIV3}\n"
        f"🌙 *إلى اللقاء، رمضان كريم!* 🌙",
        parse_mode="Markdown", reply_markup=kb
    )
    return ST_PHONE

# ══════════════════════════════════════════════════════
#              لوحة الأدمن
# ══════════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ ليس لديك صلاحية.")
        return ST_MAIN

    await update.message.reply_text(
        f"🛡️ *لوحة تحكم الأدمن* 🛡️\n"
        f"{DIV}\n\n"
        f"🌙 أهلاً، *{update.effective_user.first_name}*!\n\n"
        f"اختر من القائمة أدناه 👇\n\n"
        f"{DIV3}",
        parse_mode="Markdown",
        reply_markup=admin_kb()
    )
    return ST_MAIN

async def admin_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    total     = (await db_get("SELECT COUNT(*) c FROM users"))["c"]
    active    = (await db_get("SELECT COUNT(*) c FROM users WHERE token IS NOT NULL AND token_expiry > ?", (time.time(),)))["c"]
    banned    = (await db_get("SELECT COUNT(*) c FROM users WHERE banned=1"))["c"]
    offers    = (await db_get("SELECT COUNT(*) c FROM offers WHERE status='active'"))["c"]
    t_ok      = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='completed'"))["c"]
    t_fail    = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='failed'"))["c"]
    gifts     = (await db_get("SELECT COUNT(*) c FROM gifts WHERE status='completed'"))["c"]
    today_u   = (await db_get("SELECT COUNT(*) c FROM users WHERE DATE(joined_at)=DATE('now')"))["c"]
    today_t   = (await db_get("SELECT COUNT(*) c FROM trades WHERE DATE(created_at)=DATE('now') AND status='completed'"))["c"]
    channels  = (await db_get("SELECT COUNT(*) c FROM channels WHERE is_active=1"))["c"]

    try:
        db_size = os.path.getsize(DB_PATH) / 1024
        db_txt  = f"{db_size:.1f} KB"
    except:
        db_txt = "—"

    text = (
        f"🪔 *إحصائيات البوت الشاملة* 🪔\n"
        f"{DIV}\n\n"
        f"👥 *المستخدمون*\n"
        f"┌ إجمالي:  `{total}` مستخدم\n"
        f"├ نشط الآن: `{active}`\n"
        f"├ محظور:    `{banned}`\n"
        f"└ انضم اليوم: `{today_u}`\n\n"
        f"🔄 *التبادلات*\n"
        f"┌ عروض نشطة:    `{offers}`\n"
        f"├ ناجحة إجمالي: `{t_ok}`\n"
        f"├ ناجحة اليوم:  `{today_t}`\n"
        f"└ فاشلة:        `{t_fail}`\n\n"
        f"🎁 *الهدايا والقنوات*\n"
        f"├ هدايا مُرسَلة: `{gifts}`\n"
        f"└ قنوات إجبارية: `{channels}`\n\n"
        f"🗄️ *النظام*\n"
        f"└ قاعدة البيانات: `{db_txt}`\n\n"
        f"{DIV3}\n"
        f"_{datetime.now().strftime('%H:%M  ·  %d/%m/%Y')}_"
    )

    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=admin_kb())
    return ST_MAIN

async def admin_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    users = await db_all("""
        SELECT user_id, username, phone, card_value, card_units,
               trades_done, banned, last_seen,
               CASE WHEN token_expiry > ? THEN '🟢' ELSE '🔴' END as status
        FROM users ORDER BY trades_done DESC LIMIT 20
    """, (time.time(),))

    lines = [f"👥 *المستخدمون (أكثر تبادلاً):*\n{DIV}"]
    for u in users:
        phone = u.get("phone","")
        phone_masked = phone[:4] + "****" + phone[-2:] if len(phone) >= 6 else "****"
        ban_icon = "🚫" if u["banned"] else ""
        lines.append(
            f"{u['status']} {ban_icon} *@{u['username'] or 'مجهول'}*\n"
            f"   📱 `{phone_masked}`  ·  💰 `{u['card_value']:.0f}` ج\n"
            f"   🔄 تبادلات: `{u['trades_done']}`  ·  🆔 `{u['user_id']}`"
        )

    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=admin_kb()
    )
    return ST_MAIN

async def admin_broadcast_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    await update.message.reply_text(
        f"📢 *إرسال إعلان*\n"
        f"{DIV}\n\n"
        f"أرسل الإعلان الآن (نص، صورة، رابط)\n\n"
        f"_سيصل لجميع المستخدمين غير المحظورين_\n\n"
        f"أرسل /cancel للإلغاء",
        parse_mode="Markdown"
    )
    return ST_BROADCAST

async def admin_broadcast_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    users = await db_all("SELECT user_id FROM users WHERE banned=0")
    sent  = 0
    fail  = 0

    msg        = update.message
    media_type = None
    media_id   = None
    caption    = None

    if msg.photo:
        media_type = "photo"
        media_id   = msg.photo[-1].file_id
        caption    = msg.caption or ""
    elif msg.text:
        caption = msg.text

    status_msg = await msg.reply_text(f"📤 *جاري الإرسال...*\n0 / {len(users)}", parse_mode="Markdown")

    for i, u in enumerate(users):
        try:
            if media_type == "photo":
                await ctx.bot.send_photo(
                    u["user_id"], photo=media_id,
                    caption=f"📢 {caption}" if caption else None,
                    parse_mode="Markdown"
                )
            else:
                await ctx.bot.send_message(
                    u["user_id"], f"📢 {caption}", parse_mode="Markdown"
                )
            sent += 1
        except TelegramError as e:
            err_msg = str(e).lower()
            if "blocked" in err_msg or "chat not found" in err_msg or "deactivated" in err_msg:
                await db_run("UPDATE users SET banned=1 WHERE user_id=?", (u["user_id"],))
            fail += 1
        except:
            fail += 1

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📤 *جاري الإرسال...*\n✅ {sent}  ❌ {fail}  |  {i+1} / {len(users)}",
                    parse_mode="Markdown"
                )
            except:
                pass
        await asyncio.sleep(0.08)

    await db_run(
        "INSERT INTO broadcasts (admin_id, message, media_type, media_id, sent_count, fail_count) VALUES (?,?,?,?,?,?)",
        (update.effective_user.id, caption or "", media_type or "text", media_id or "", sent, fail)
    )

    await status_msg.edit_text(
        f"✅ *تم إرسال الإعلان!*\n\n"
        f"✉️ وصل لـ: `{sent}` مستخدم\n"
        f"❌ فشل: `{fail}`",
        parse_mode="Markdown"
    )
    return ST_MAIN

async def admin_channels(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    channels = await get_active_channels()
    lines    = [f"📋 *قنوات الاشتراك الإجباري:*\n{DIV}"]

    if not channels:
        lines.append("لا توجد قنوات مضافة بعد.")
    else:
        for ch in channels:
            lines.append(
                f"📢 *{ch.get('title','بدون اسم')}*\n"
                f"   🆔 `{ch['chat_id']}`\n"
                f"   🔗 @{ch.get('username','خاص')}"
            )

    btns = [
        [InlineKeyboardButton("➕ إضافة قناة",  callback_data="add_channel")],
        [InlineKeyboardButton("🗑️ حذف قناة",   callback_data="del_channel")],
        [InlineKeyboardButton("🔙 لوحة الأدمن", callback_data="back_admin")],
    ]
    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return ST_MAIN

async def cb_channel_actions(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()

    if not is_admin(q.from_user.id):
        return ST_MAIN

    if q.data == "add_channel":
        await q.edit_message_text(
            "➕ *إضافة قناة جديدة*\n\n"
            "أرسل معرّف القناة:\n"
            "• للعامة: `@channel_username`\n"
            "• للخاصة: `-100xxxxxxxxxx`\n\n"
            "_تأكد أن البوت أدمن في القناة_",
            parse_mode="Markdown"
        )
        ctx.user_data["awaiting"] = "channel_id"
        return ST_ADD_CHANNEL

    if q.data == "del_channel":
        channels = await get_active_channels()
        if not channels:
            await q.edit_message_text("لا توجد قنوات لحذفها.")
            return ST_MAIN
        btns = [
            [InlineKeyboardButton(
                f"🗑️ {ch.get('title','بدون اسم')} — {ch['chat_id']}",
                callback_data=f"rmch_{ch['channel_id']}"
            )]
            for ch in channels
        ]
        await q.edit_message_text(
            "اختر القناة للحذف:",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return ST_ADD_CHANNEL

    if q.data.startswith("rmch_"):
        ch_id = int(q.data.split("_")[1])
        await db_run("UPDATE channels SET is_active=0 WHERE channel_id=?", (ch_id,))
        await q.edit_message_text("✅ *تم حذف القناة بنجاح.*", parse_mode="Markdown")
        return ST_MAIN

    if q.data == "back_admin":
        await q.edit_message_text("🛡️ لوحة الأدمن")
        await q.message.reply_text("اختر من القائمة 👇", reply_markup=admin_kb())
        return ST_MAIN

    return ST_MAIN

async def handle_add_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    chat_input = update.message.text.strip()
    msg = await update.message.reply_text("⏳ *جاري التحقق من القناة...*", parse_mode="Markdown")

    try:
        chat = await ctx.bot.get_chat(chat_input)
        title    = chat.title or chat_input
        username = f"@{chat.username}" if chat.username else None
        inv_link = chat.invite_link

        await db_run(
            """INSERT INTO channels (chat_id, title, username, invite_link)
               VALUES (?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
               title=excluded.title, username=excluded.username,
               invite_link=excluded.invite_link, is_active=1""",
            (str(chat.id), title, username, inv_link)
        )
        await msg.edit_text(
            f"✅ *تم إضافة القناة بنجاح!*\n\n"
            f"📢 *{title}*\n"
            f"🔗 {username or 'خاص'}\n"
            f"🆔 `{chat.id}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        await msg.edit_text(
            f"❌ *فشل إضافة القناة!*\n\n`{e}`\n\n"
            f"تأكد أن البوت أدمن في القناة والمعرّف صحيح.",
            parse_mode="Markdown"
        )

    ctx.user_data.pop("awaiting", None)
    await update.message.reply_text("اختر من القائمة 👇", reply_markup=admin_kb())
    return ST_MAIN

async def admin_ban_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN
    if not ctx.args:
        await update.message.reply_text("الاستخدام: `/ban user_id سبب`", parse_mode="Markdown")
        return ST_MAIN
    uid    = int(ctx.args[0])
    reason = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else "بدون سبب"
    await db_run("UPDATE users SET banned=1, ban_reason=? WHERE user_id=?", (reason, uid))
    await db_run("UPDATE offers SET status='cancelled' WHERE user_id=? AND status='active'", (uid,))
    await update.message.reply_text(f"✅ *تم حظر* `{uid}`\nالسبب: {reason}", parse_mode="Markdown")
    try:
        await ctx.bot.send_message(uid, f"🚫 *تم حظر حسابك*\nالسبب: {reason}", parse_mode="Markdown")
    except:
        pass
    return ST_MAIN

async def admin_unban_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN
    if not ctx.args:
        await update.message.reply_text("الاستخدام: `/unban user_id`", parse_mode="Markdown")
        return ST_MAIN
    uid = int(ctx.args[0])
    await db_run("UPDATE users SET banned=0, ban_reason=NULL WHERE user_id=?", (uid,))
    await update.message.reply_text(f"✅ *تم رفع الحظر عن* `{uid}`", parse_mode="Markdown")
    try:
        await ctx.bot.send_message(uid, "✅ *تم رفع الحظر عن حسابك.*", parse_mode="Markdown")
    except:
        pass
    return ST_MAIN

async def admin_trades(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ST_MAIN

    trades = await db_all("""
        SELECT t.*, u1.username as n1, u2.username as n2
        FROM trades t
        LEFT JOIN users u1 ON t.user1_id=u1.user_id
        LEFT JOIN users u2 ON t.user2_id=u2.user_id
        ORDER BY t.created_at DESC LIMIT 15
    """)

    if not trades:
        await update.message.reply_text("لا توجد تبادلات.", reply_markup=admin_kb())
        return ST_MAIN

    lines = [f"📝 *آخر التبادلات:*\n{DIV}"]
    for t in trades:
        icon = "✅" if t["status"] == "completed" else "❌"
        date = t["created_at"][:16]
        lines.append(
            f"{icon} @{t['n1']} ↔️ @{t['n2']}\n"
            f"   💰 `{t['val1']:.1f}` ↔️ `{t['val2']:.1f}` جنيه\n"
            f"   _{date}_"
            + (f"\n   ⚠️ _{t['fail_reason']}_" if t.get("fail_reason") else "")
        )

    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="Markdown", reply_markup=admin_kb()
    )
    return ST_MAIN

async def admin_debug(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user or not user.get("token"):
        await update.message.reply_text("❌ مش مسجّل دخول.")
        return
    await update.message.reply_text("🔍 جاري الفحص...")
    raw = await VF.debug_card(user["phone"], user["token"])
    for i in range(0, len(raw), 4000):
        await update.message.reply_text(f"```\n{raw[i:i+4000]}\n```", parse_mode="Markdown")

# ══════════════════════════════════════════════════════
#              Inline Menu Callbacks
# ══════════════════════════════════════════════════════
async def main_menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    d   = q.data
    uid = q.from_user.id
    chat_id = q.message.chat_id

    async def reply(text, **kw):
        try:
            await q.edit_message_text(text, **kw)
        except Exception:
            await ctx.bot.send_message(chat_id, text, **kw)

    if d == "menu_market":
        user = await get_user(uid)
        if not user:
            await reply("❌ سجّل الدخول أولاً!")
            return
        ok, not_subbed = await check_subscription(ctx.bot, uid)
        if not ok:
            await subscription_wall(update, ctx, not_subbed)
            return
        my_u = user.get("card_units", 0)
        my_v = user.get("card_value", 0)
        if my_u == 0:
            await reply("❌ *لا يوجد لديك كرت رمضان!*", parse_mode="Markdown")
            return
        offers = await db_all(
            "SELECT o.*, u.username FROM offers o JOIN users u ON o.user_id=u.user_id "
            "WHERE o.status='active' AND o.user_id!=? AND o.min_units<=? AND o.max_units>=? "
            "ORDER BY ABS(o.card_units - ?) LIMIT 10",
            (uid, my_u, my_u, my_u)
        )
        if not offers:
            await reply("🔍 *لا توجد عروض متاحة الآن*\n\nاعرض كارتك وانتظر من يتطابق معك!", parse_mode="Markdown")
            return
        txt  = f"🏪 *سوق التبادل*\n{DIV}\n📊 كارتك: `{my_v:.1f}` ج — `{my_u:.0f}` وحدة\n\n"
        btns = []
        for o in offers:
            txt  += fmt_offer(o, my_u) + "\n\n"
            btns.append([InlineKeyboardButton(
                f"🔄 تبادل مع @{o.get('username','؟')} ({o['card_value']:.0f}ج)",
                callback_data=f"trade_{o['offer_id']}"
            )])
        await reply(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns))

    elif d == "menu_post":
        user = await get_user(uid)
        if not user or not user.get("card_units"):
            await reply("❌ *لا يوجد كرت رمضان!*", parse_mode="Markdown")
            return
        mn, mx = user.get("min_units", 0), user.get("max_units", 0)
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ تخصيص النطاق", callback_data="range_custom")],
            [InlineKeyboardButton(f"✅ نطاق تلقائي ({mn:.0f}–{mx:.0f})", callback_data="range_auto")],
        ])
        await reply(
            f"📋 *عرض كارتك في السوق*\n{DIV}\n"
            f"{fmt_card(user['card_value'], user['card_units'])}\n\nاختر نطاق التبادل:",
            parse_mode="Markdown", reply_markup=btns
        )

    elif d == "menu_offers":
        rows = await db_all(
            "SELECT * FROM offers WHERE user_id=? AND status='active' ORDER BY created_at DESC", (uid,)
        )
        if not rows:
            await reply("📭 *لا توجد عروض نشطة.*", parse_mode="Markdown")
            return
        lines = [f"📊 *عروضك النشطة:*\n{DIV}"]
        for o in rows:
            lines.append(
                f"💰 `{o['card_value']:.1f}` ج  ·  `{o['card_units']:.0f}` وحدة"
                f"  ·  نطاق: `{o['min_units']:.0f}`–`{o['max_units']:.0f}`"
            )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 إلغاء كل عروضي", callback_data="cancel_my_offers")]])
        await reply("\n".join(lines), parse_mode="Markdown", reply_markup=kb)

    elif d == "menu_history":
        trades = await db_all(
            "SELECT t.*, u1.username as u1, u2.username as u2 FROM trades t "
            "LEFT JOIN users u1 ON t.user1_id=u1.user_id "
            "LEFT JOIN users u2 ON t.user2_id=u2.user_id "
            "WHERE (t.user1_id=? OR t.user2_id=?) ORDER BY t.created_at DESC LIMIT 10",
            (uid, uid)
        )
        if not trades:
            await reply("📭 لا توجد عمليات بعد.")
            return
        lines = [f"📖 *سجل عملياتك:*\n{DIV}"]
        for t in trades:
            partner = t["u2"] if t["user1_id"] == uid else t["u1"]
            gave    = t["val1"] if t["user1_id"] == uid else t["val2"]
            got     = t["val2"] if t["user1_id"] == uid else t["val1"]
            icon    = "✅" if t["status"] == "completed" else "❌"
            lines.append(f"{icon} @{partner}  ·  {t['created_at'][:10]}\n   📤 `{gave:.1f}` ج  ←→  📥 `{got:.1f}` ج")
        await reply("\n\n".join(lines), parse_mode="Markdown")

    elif d == "menu_gift":
        ctx.user_data.pop("gift_phone", None)
        ctx.user_data.pop("gift_pass",  None)
        await reply(
            f"🎁 *إرسال هدية رمضان*\n{DIV}\n\n📱 أدخل رقم فودافون الخاص بك:",
            parse_mode="Markdown"
        )
        ctx.user_data["awaiting_gift"] = "phone"

    elif d == "menu_notif":
        rows = await db_all(
            "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 10", (uid,)
        )
        await db_run("UPDATE notifications SET seen=1 WHERE user_id=?", (uid,))
        if not rows:
            await reply("🔔 لا توجد إشعارات.")
            return
        lines = [f"🔔 *إشعاراتك:*\n{DIV}"]
        for n in rows:
            icon = "🆕" if not n["seen"] else "✅"
            lines.append(f"{icon} {n['message']}\n_{n['created_at'][:16]}_")
        await reply("\n\n".join(lines), parse_mode="Markdown")

    elif d == "menu_refresh":
        user = await get_user(uid)
        if not user:
            await reply("❌ سجّل الدخول أولاً!")
            return
        tok = await ensure_token(user)
        if not tok:
            await reply("❌ انتهت جلستك — سجّل الدخول مجدداً.")
            return
        card = await VF.get_card(user["phone"], tok)
        if card:
            mn, mx = smart_range(card["units"])
            await db_run(
                "UPDATE users SET card_value=?, card_units=?, card_id=?, card_serial=?, min_units=?, max_units=? WHERE user_id=?",
                (card["value"], card["units"], card.get("id"), card.get("serial"), mn, mx, uid)
            )
            await reply(
                f"✅ *تم تحديث الكرت!*\n{DIV}\n{fmt_card(card['value'], card['units'])}",
                parse_mode="Markdown"
            )
        else:
            await reply("⚠️ لا يوجد كرت رمضان متاح.")

    elif d == "menu_help":
        await reply(
            f"❓ *المساعدة*\n{DIV}\n\n"
            f"✦ *سوق التبادل* — ابحث عن عروض وتبادل\n"
            f"✦ *عرض كارتي* — اعرض كارتك في السوق\n"
            f"✦ *إرسال هدية* — ابعت هدية لأي رقم\n"
            f"✦ *تحديث الكرت* — حدّث بيانات كارتك\n\n"
            f"_للدعم تواصل مع الأدمن_",
            parse_mode="Markdown"
        )

    elif d == "menu_logout":
        await db_run("UPDATE users SET token=NULL, token_expiry=0 WHERE user_id=?", (uid,))
        await reply("👋 *تم تسجيل الخروج بنجاح.*\n\nاستخدم /start للدخول مجدداً.", parse_mode="Markdown")


async def admin_menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    d   = q.data
    uid = q.from_user.id

    if not is_admin(uid):
        return

    async def reply(text, **kw):
        try:
            await q.edit_message_text(text, **kw)
        except Exception:
            await ctx.bot.send_message(q.message.chat_id, text, **kw)

    if d == "adm_stats":
        total  = (await db_get("SELECT COUNT(*) c FROM users"))["c"]
        active = (await db_get("SELECT COUNT(*) c FROM users WHERE token IS NOT NULL AND token_expiry > ?", (time.time(),)))["c"]
        offers = (await db_get("SELECT COUNT(*) c FROM offers WHERE status='active'"))["c"]
        t_ok   = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='completed'"))["c"]
        t_fail = (await db_get("SELECT COUNT(*) c FROM trades WHERE status='failed'"))["c"]
        gifts  = (await db_get("SELECT COUNT(*) c FROM gifts WHERE status='completed'"))["c"]
        await reply(
            f"📊 *إحصائيات البوت*\n{DIV}\n"
            f"👥 المستخدمون: `{total}`\n"
            f"🟢 النشطون: `{active}`\n"
            f"📋 العروض النشطة: `{offers}`\n"
            f"✅ تبادلات ناجحة: `{t_ok}`\n"
            f"❌ تبادلات فاشلة: `{t_fail}`\n"
            f"🎁 هدايا مُرسَلة: `{gifts}`",
            parse_mode="Markdown", reply_markup=admin_kb()
        )

    elif d == "adm_users":
        rows = await db_all(
            "SELECT user_id, username, phone, trades_done, banned FROM users ORDER BY trades_done DESC LIMIT 20"
        )
        if not rows:
            await reply("لا يوجد مستخدمون.")
            return
        lines = [f"👥 *المستخدمون:*\n{DIV}"]
        for r in rows:
            status = "🚫" if r["banned"] else "✅"
            p = r.get("phone","")
            pm = p[:4]+"****"+p[-2:] if len(p)>=6 else "****"
            lines.append(f"{status} @{r['username']} | `{pm}` | تبادلات: `{r['trades_done']}`")
        await reply("\n".join(lines), parse_mode="Markdown")

    elif d == "adm_broadcast":
        await reply(
            f"📢 *إرسال إعلان*\n{DIV}\n\nأرسل الإعلان الآن\n\n_أرسل /cancel للإلغاء_",
            parse_mode="Markdown"
        )
        ctx.user_data["awaiting_broadcast"] = True

    elif d == "adm_channels":
        channels = await get_active_channels()
        btns = [[InlineKeyboardButton("➕ إضافة قناة", callback_data="add_channel")]]
        for ch in channels:
            btns.append([InlineKeyboardButton(
                f"🗑 {ch.get('title') or ch['chat_id']}",
                callback_data=f"rmch_{ch['channel_id']}"
            )])
        await reply(
            f"📋 *قنوات الاشتراك الإجباري*\n{DIV}\nعدد القنوات: `{len(channels)}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif d == "adm_trades":
        rows = await db_all(
            "SELECT t.trade_id, t.val1, t.val2, t.status, t.created_at, "
            "u1.username as u1, u2.username as u2 "
            "FROM trades t LEFT JOIN users u1 ON t.user1_id=u1.user_id "
            "LEFT JOIN users u2 ON t.user2_id=u2.user_id "
            "ORDER BY t.created_at DESC LIMIT 15"
        )
        if not rows:
            await reply("لا توجد تبادلات.")
            return
        lines = [f"📝 *سجل التبادلات:*\n{DIV}"]
        for t in rows:
            icon = "✅" if t["status"] == "completed" else "❌"
            lines.append(
                f"{icon} @{t['u1']} ↔ @{t['u2']}  "
                f"`{t['val1']:.0f}`ج/`{t['val2']:.0f}`ج  {t['created_at'][:10]}"
            )
        await reply("\n".join(lines), parse_mode="Markdown")

    elif d == "adm_ban":
        await reply("أرسل: `/ban user_id سبب`", parse_mode="Markdown")

    elif d == "adm_unban":
        await reply("أرسل: `/unban user_id`", parse_mode="Markdown")

    elif d == "adm_admins":
        txt = "👑 *الأدمنز الحاليون:*\n\n"
        for aid in ADMIN_IDS:
            txt += f"• `{aid}`\n"
        txt += "\n_لإضافة أدمن:_ `/addadmin user_id`\n_لحذف أدمن:_ `/deladmin user_id`"
        await reply(txt, parse_mode="Markdown")

    elif d == "adm_main":
        await reply("🛡️ لوحة الأدمن", reply_markup=admin_kb())


async def text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id

    if ctx.user_data.get("awaiting_broadcast") and is_admin(uid):
        ctx.user_data.pop("awaiting_broadcast", None)
        return await admin_broadcast_send(update, ctx)

    if not update.message or not update.message.text:
        return ST_MAIN
    t = update.message.text

    if is_admin(uid):
        if t == "📊 إحصائيات":        return await admin_stats(update, ctx)
        if t == "👥 المستخدمون":       return await admin_users(update, ctx)
        if t == "📢 إرسال إعلان":      return await admin_broadcast_start(update, ctx)
        if t == "📋 قنوات الاشتراك":   return await admin_channels(update, ctx)
        if t == "📝 سجل التبادلات":    return await admin_trades(update, ctx)
        if t == "🔙 القائمة الرئيسية":
            await update.message.reply_text("👇 القائمة الرئيسية", reply_markup=await get_main_kb(uid))
            return ST_MAIN

    if t == "🔄 سوق التبادل":       return await market(update, ctx)
    if t == "📋 عرض كارتي":         return await post_offer(update, ctx)
    if t == "📊 عروضي":             return await my_offers(update, ctx)
    if t == "📖 سجل عملياتي":       return await trade_log(update, ctx)
    if t == "🎁 إرسال هدية":         return await gift_start(update, ctx)
    if t == "🔔 إشعاراتي":          return await notifications(update, ctx)
    if t == "🔃 تحديث الكرت":        return await cmd_refresh(update, ctx)
    if t == "❓ المساعدة":           return await help_cmd(update, ctx)
    if t == "🚪 خروج":              return await logout(update, ctx)

    if ctx.user_data.get("awaiting") == "channel_id":
        return await handle_add_channel(update, ctx)

    return ST_MAIN

# ══════════════════════════════════════════════════════
#                أوامر الأدمنز
# ══════════════════════════════════════════════════════
async def cmd_addadmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        new_id = int(ctx.args[0])
        if new_id in ADMIN_IDS:
            await update.message.reply_text(f"⚠️ `{new_id}` أدمن بالفعل!", parse_mode="Markdown")
            return
        ADMIN_IDS.append(new_id)
        await update.message.reply_text(
            f"✅ تمت إضافة `{new_id}` كأدمن!\n👑 الأدمنز الآن: {len(ADMIN_IDS)}",
            parse_mode="Markdown"
        )
    except (IndexError, ValueError):
        await update.message.reply_text("❌ الصيغة: `/addadmin user_id`", parse_mode="Markdown")

async def cmd_deladmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        del_id = int(ctx.args[0])
        if del_id not in ADMIN_IDS:
            await update.message.reply_text(f"❌ `{del_id}` مش أدمن!", parse_mode="Markdown")
            return
        if len(ADMIN_IDS) == 1:
            await update.message.reply_text("❌ لازم يفضل أدمن واحد على الأقل!", parse_mode="Markdown")
            return
        ADMIN_IDS.remove(del_id)
        await update.message.reply_text(
            f"✅ تم حذف `{del_id}` من الأدمنز!", parse_mode="Markdown"
        )
    except (IndexError, ValueError):
        await update.message.reply_text("❌ الصيغة: `/deladmin user_id`", parse_mode="Markdown")

async def cmd_list_admins(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    lines = ["👑 *الأدمنز الحاليون:*\n"]
    for aid in ADMIN_IDS:
        lines.append(f"• `{aid}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /dashboard — يبني رابط الداشبورد بالبيانات الحقيقية ويبعته للمستخدم
    الرابط شكله: https://site.com/dashboard.html#d=BASE64_JSON
    """
    uid  = update.effective_user.id
    user = await db_get("SELECT * FROM users WHERE user_id=?", (uid,))
    if not user:
        await update.message.reply_text("❌ سجّل الدخول أولاً بـ /start")
        return
    if not DASHBOARD_URL:
        await update.message.reply_text("⚠️ الداشبورد غير مفعّل.\nالأدمن يستخدم: /setdashboard https://...")
        return

    msg = await update.message.reply_text("⏳ جاري تجهيز الداشبورد...")
    is_admin = uid in ADMIN_IDS

    # بيانات المستخدم
    my_offers = await db_all(
        "SELECT * FROM offers WHERE user_id=? ORDER BY created_at DESC LIMIT 10", (uid,)
    )
    my_trades_raw = await db_all("""
        SELECT t.*, u1.username as n1, u2.username as n2
        FROM trades t
        LEFT JOIN users u1 ON t.user1_id=u1.user_id
        LEFT JOIN users u2 ON t.user2_id=u2.user_id
        WHERE t.user1_id=? OR t.user2_id=?
        ORDER BY t.created_at DESC LIMIT 20
    """, (uid, uid))
    my_trades = []
    for t in my_trades_raw:
        is1 = t["user1_id"] == uid
        my_trades.append({
            "partner": t["n2"] if is1 else t["n1"],
            "gave":    t["val1"] if is1 else t["val2"],
            "got":     t["val2"] if is1 else t["val1"],
            "status":  t["status"],
            "created_at": t["created_at"],
        })
    my_units = user.get("card_units", 0)
    market = await db_all("""
        SELECT o.*, u.username FROM offers o JOIN users u ON o.user_id=u.user_id
        WHERE o.status='active' AND o.user_id!=?
          AND datetime(o.expires_at)>datetime('now')
        ORDER BY ABS(o.card_units - ?) ASC LIMIT 20
    """, (uid, my_units))

    data = {
        "is_admin":  is_admin,
        "my_user":   dict(user),
        "my_offers": my_offers,
        "my_trades": my_trades,
        "market":    market,
    }

    # بيانات الأدمن الإضافية
    if is_admin:
        stats     = await api_stats()
        all_users = await db_all("""
            SELECT user_id, username, phone, card_value, card_units,
                   trades_done, banned, last_seen
            FROM users ORDER BY trades_done DESC LIMIT 100
        """)
        for u in all_users:
            p = u.get("phone", "")
            u["phone"] = p[:4] + "****" + p[-2:] if len(p) >= 6 else "****"
        all_trades = await db_all("""
            SELECT t.trade_id, t.val1, t.val2, t.status, t.created_at,
                   u1.username as user1, u2.username as user2
            FROM trades t
            LEFT JOIN users u1 ON t.user1_id=u1.user_id
            LEFT JOIN users u2 ON t.user2_id=u2.user_id
            ORDER BY t.created_at DESC LIMIT 50
        """)
        data["stats"]      = stats
        data["all_users"]  = all_users
        data["all_trades"] = all_trades

    # تحويل البيانات لـ base64 وإضافتها للرابط
    import base64 as b64
    json_str = json.dumps(data, ensure_ascii=False, default=str)
    encoded  = b64.b64encode(json_str.encode()).decode()
    url_encoded = encoded.replace('+', '-').replace('/', '_')

    # الرابط النهائي مع البيانات
    dashboard_link = f"{DASHBOARD_URL}#d={url_encoded}"

    # ✅ حفظ الرابط في قاعدة البيانات عشان يظهر تلقائياً في القائمة الرئيسية
    await db_run("UPDATE users SET dashboard_url=? WHERE user_id=?", (dashboard_link, uid))

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 فتح الداشبورد", web_app=WebAppInfo(url=dashboard_link))
    ]])

    role = "👑 أدمن" if is_admin else "👤 عضو"
    await msg.edit_text(
        f"🌙 *داشبورد رمضان* 🌙\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📱 `{user['phone']}`\n"
        f"💰 `{user.get('card_value',0):.1f}` جنيه  ·  `{my_units:.0f}` وحدة\n"
        f"🎭 {role}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"✅ تم حفظ الداشبورد — زرار 📊 في القائمة جاهز!\n"
        f"لتحديث البيانات ابعت /dashboard مجدداً",
        parse_mode="Markdown",
        reply_markup=kb
    )

    # تحديث القائمة الرئيسية عشان يظهر فيها زرار الداشبورد فوراً
    await ctx.bot.send_message(
        uid,
        "📊 *زرار لوحة التحكم جاهز في القائمة!* 👇",
        parse_mode="Markdown",
        reply_markup=await get_main_kb(uid)
    )


async def cmd_setdashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """تعيين رابط الداشبورد — /setdashboard https://example.com/dashboard.html"""
    global DASHBOARD_URL
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"الاستخدام: `/setdashboard https://...`\n\nالرابط الحالي: `{DASHBOARD_URL or 'غير محدد'}`",
            parse_mode="Markdown"
        )
        return
    DASHBOARD_URL = ctx.args[0].strip()
    await update.message.reply_text(
        f"✅ *تم تعيين رابط الداشبورد!*\n\n`{DASHBOARD_URL}`\n\n"
        f"الآن زر 📊 لوحة التحكم سيظهر في القائمة.",
        parse_mode="Markdown"
    )

# ══════════════════════════════════════════════════════
#                    تشغيل البوت
# ══════════════════════════════════════════════════════
async def _main():
    await init_db()

    app = (Application.builder()
           .token(BOT_TOKEN)
           .concurrent_updates(True)
           .build())

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            ST_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_phone),
            ],
            ST_PASSWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_password),
            ],
            ST_MAIN: [
                MessageHandler(filters.ALL & ~filters.COMMAND, text_router),
            ],
            ST_RANGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_range),
            ],
            ST_GIFT_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, gift_phone),
            ],
            ST_GIFT_PASS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, gift_pass),
            ],
            ST_GIFT_CONFIRM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, gift_confirm),
            ],
            ST_BROADCAST: [
                MessageHandler(filters.ALL & ~filters.COMMAND, admin_broadcast_send),
            ],
            ST_ADD_CHANNEL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_channel),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        allow_reentry=True,
    )

    app.add_handler(conv)

    # WebApp data handler — قلب الداشبورد
    app.add_handler(
        MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data),
        group=0
    )

    app.add_handler(CallbackQueryHandler(cb_login,          pattern="^(do_login|about|check_sub)$"), group=1)
    app.add_handler(CallbackQueryHandler(main_menu_cb,      pattern="^menu_"),                      group=1)
    app.add_handler(CallbackQueryHandler(admin_menu_cb,     pattern="^adm_"),                       group=1)
    app.add_handler(CallbackQueryHandler(cb_cancel_offers,  pattern="^cancel_my_offers$"),          group=1)
    app.add_handler(CallbackQueryHandler(handle_range,      pattern="^range_"),                     group=1)
    app.add_handler(CallbackQueryHandler(cb_market,         pattern="^(pick_|exec_|offer_next|offer_prev|noop|main_menu|trade_|confirm_trade)"), group=1)
    app.add_handler(CallbackQueryHandler(cb_channel_actions,pattern="^(add_channel|del_channel|rmch_|admin_refresh_stats|back_admin)"), group=1)
    app.add_handler(CallbackQueryHandler(gift_confirm,      pattern="^gift_confirm$"),              group=1)

    app.add_handler(CommandHandler("admin",        cmd_admin))
    app.add_handler(CommandHandler("stats",        admin_stats))
    app.add_handler(CommandHandler("ban",          admin_ban_cmd))
    app.add_handler(CommandHandler("unban",        admin_unban_cmd))
    app.add_handler(CommandHandler("debug",        admin_debug))
    app.add_handler(CommandHandler("refresh",      cmd_refresh))
    app.add_handler(CommandHandler("addadmin",     cmd_addadmin))
    app.add_handler(CommandHandler("deladmin",     cmd_deladmin))
    app.add_handler(CommandHandler("admins",       cmd_list_admins))
    app.add_handler(CommandHandler("setdashboard", cmd_setdashboard))
    app.add_handler(CommandHandler("dashboard",    cmd_dashboard))

    log.info("💎 البوت يعمل — النسخة 4.0")

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    import signal
    stop_event = asyncio.Event()

    def _stop(*_):
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, _stop)
        except Exception:
            signal.signal(sig, _stop)

    await stop_event.wait()
    await app.updater.stop()
    await app.stop()
    await app.shutdown()


def main():
    asyncio.run(_main())


if __name__ == "__main__":
    main()
