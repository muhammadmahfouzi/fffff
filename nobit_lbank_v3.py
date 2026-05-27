#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LBank Radar
v2: fix 30m/4h candle percent calculation so displayed candle percentages use
    the latest analysis candle open -> latest/current price instead of close vs
    a multi-candle average or a stale closed-only 4h candle.
"""

import json
import math
import time
import sys
import traceback
import statistics
import os
import re
import threading
import queue
import random
import http.client
import concurrent.futures
import mimetypes
import uuid
import importlib
import socket
import ssl
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, parse, error

def load_env_file(filename=".env"):
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            os.environ[key] = value


load_env_file()

# =========================
# CONFIGURATION
# =========================

BASE_URL = "https://api.lbkex.com"
EXCHANGE_NAME = "LBank"
LBANK_USER_AGENT = os.getenv("LBANK_USER_AGENT", "TraderBot/LBankPumpRadar").strip() or "TraderBot/LBankPumpRadar"
# Keep the old variable name as a compatibility alias for the shared HTTP helpers.
NOBITEX_USER_AGENT = LBANK_USER_AGENT
NOBITEX_MAX_OHLC_COUNTBACK = 2000
LBANK_KLINE_TYPE_BY_RESOLUTION = {
    "1": "minute1",
    "5": "minute5",
    "15": "minute15",
    "30": "minute30",
    "60": "hour1",
    "240": "hour4",
    "480": "hour8",
    "720": "hour12",
    "D": "day1",
    "1D": "day1",
    "W": "week1",
    "1W": "week1",
    "M": "month1",
    "1M": "month1",
}
LBANK_KLINE_INTERVAL_SECONDS = {
    "1": 60,
    "5": 5 * 60,
    "15": 15 * 60,
    "30": 30 * 60,
    "60": 60 * 60,
    "240": 4 * 60 * 60,
    "480": 8 * 60 * 60,
    "720": 12 * 60 * 60,
    "D": 24 * 60 * 60,
    "1D": 24 * 60 * 60,
    "W": 7 * 24 * 60 * 60,
    "1W": 7 * 24 * 60 * 60,
    "M": 30 * 24 * 60 * 60,
    "1M": 30 * 24 * 60 * 60,
}
NOBITEX_VALID_OHLC_RESOLUTIONS = set(LBANK_KLINE_TYPE_BY_RESOLUTION.keys())


# Stats filters
MIN_VOLUME_USDT = 0.0            # LBank USDT-only scan; no IRT/RLS market filter
MIN_DAYCHANGE_PCT = 10.0        # same as signaler_lbank_v2: minimum 24h % change to be interesting
MAX_DAYCHANGE_PCT = 280.0       # same as signaler_lbank_v2: maximum 24h % change accepted
MAX_CANDIDATE_MARKETS = 500     # same as signaler_lbank_v2 MAX_CANDIDATES

SKIP_SYMBOLS_BY_TAG = {
    "Meme": {
        "CGPT"
    },
    "WhaleFavorites": {
        "BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "LINK", "DOGE", "TRX",
        "AVAX", "DOT", "XMR", "BCH", "LTC", "XLM", "SUI", "HBAR", "ARB"
    },
}
DEFAULT_SKIP_TAGS = {"Meme", "WhaleFavorites"}


# Pump detection thresholds
PUMP_MIN_30M_LAST_VS_AVG_PCT = 1.2
PUMP_MIN_4H_PCT = 1.2
PUMP_ALT_30M_PCT = 2.4
MIN_Z_SCORE = 1.618

RECENT_PUMP_DIFF_PCT = 2.4

# Top header red-circle display filter.
# This affects only the compact header lines such as "🔴 ATHUSDT +2.97%".
# It does not remove the 🔴 marker from the detailed body section below.
TOP_RED_CIRCLE_MIN_30M_PCT = 0.90

# Request timing & loop cadence
OHLC_SLEEP_BETWEEN_SYMBOLS = 0.1
QUIET_TERMINAL = False

# Native LBank GET behavior: no third-party dependency.
# Retry count means extra attempts, so 0 = exactly one HTTP attempt.
HTTP_TIMEOUT = 2.0
HTTP_RETRY_COUNT = 0
HTTP_BACKOFF_FACTOR = 2
HTTP_BACKOFF_CAP_SECONDS = 4.0

# OHLC prefetch: fetch the scan's OHLC data first, then analyze from memory.
OHLC_PREFETCH_BUDGET_SECONDS = 55.0
OHLC_PREFETCH_WORKERS = 16
OHLC_PREFETCH_REQUEST_TIMEOUT = 0.2
OHLC_PREFETCH_FIRST_REQUEST_TIMEOUT = 7.0
OHLC_PREFETCH_REQUEST_GAP_SECONDS = 0.02
OHLC_PREFETCH_MAX_PLAN_ITEMS = 240
OHLC_PREFETCH_STRICT = True
# Failed prefetch keys get exactly one retry during the prefetch phase only.
OHLC_PREFETCH_FAILED_RETRY_COUNT = 1


# 30m candles logic
CANDLE_30M_FETCH = 12
CANDLE_30M_PREV_FOR_AVG = 6

# 4h candles logic
CANDLE_4H_FETCH = 4

# v4 candle percent logic:
# - Message fields labelled 30m/4h must show actual candle body movement
#   (candle open -> latest/current price), not last close vs an older average.
# - Include the current forming candle when LBank provides it. When the current
#   candle close is stale, use the LBank ticker latest price as the live close.
# - Keep the old avg-vs-prev-closes value only as an internal momentum/z helper.
CANDLE_PERCENT_USE_LIVE_CANDLE = True
CANDLE_PERCENT_USE_STATS_LATEST_PRICE = True

# v4 safety for displayed candle percentages.
# Live ticker prices are allowed for DISPLAY only when they are on the same
# scale as the OHLC candles, or can be safely normalized by a simple decimal
# factor. This prevents fake +900% candles when Nobitex IRT latest is in Rial
# while OHLC candles are effectively in Toman. It also hardens LBank against
# any future live/OHLC scale mismatch.
CANDLE_PERCENT_LIVE_SCALE_REPAIR_ENABLED = True
CANDLE_PERCENT_LIVE_SCALE_REPAIR_FACTORS = (1.0, 0.1, 10.0, 0.01, 100.0, 0.001, 1000.0)
CANDLE_PERCENT_LIVE_MAX_REASONABLE_ABS_PCT = 120.0
CANDLE_PERCENT_LIVE_SCALE_MAX_DEVIATION_FROM_OHLC_CLOSE_PCT = 35.0

# Cache for 4h candles (Symbol -> last known pct_4h)
CACHE_4H = {}

# Native HTTP keep-alive and per-scan OHLC prefetch runtime state.
NATIVE_HTTP_LOCAL = threading.local()
OHLC_PREFETCH_CONTEXT = threading.local()
OHLC_PREFETCH_WORKER_LOCAL = threading.local()
OHLC_PREFETCH_HTTP_LOG_LOCAL = threading.local()
OHLC_PREFETCH_GATE_LOCK = threading.Lock()
OHLC_PREFETCH_NEXT_REQUEST_MONO = 0.0


def _record_suppressed_ohlc_prefetch_http_warning(url, reason):
    collector = getattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector", None)
    if not isinstance(collector, dict):
        return False

    lock = collector.get("lock")
    if lock is None:
        collector["count"] = int(collector.get("count", 0)) + 1
        return True

    with lock:
        collector["count"] = int(collector.get("count", 0)) + 1
    return True


def _set_ohlc_prefetch_http_warning_collector(collector):
    old = getattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector", None)
    if collector is None:
        if hasattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector"):
            delattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector")
    else:
        OHLC_PREFETCH_HTTP_LOG_LOCAL.collector = collector
    return old


def _restore_ohlc_prefetch_http_warning_collector(old_collector):
    if old_collector is None:
        if hasattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector"):
            delattr(OHLC_PREFETCH_HTTP_LOG_LOCAL, "collector")
    else:
        OHLC_PREFETCH_HTTP_LOG_LOCAL.collector = old_collector

# Local storage files
STAR_STATE_FILE = "pump_star_state.json"
SCAN_HISTORY_FILE = "pump_scan_history.json"
DEMAND_STATE_FILE = "pump_demand_state.json"
BALE_RUNTIME_FILE = "pump_bale_runtime.json"
BALE_TODAY_SENT_FILE = "pump_bale_today_sent.json"
LOCAL_CHAT_STORE_FILE = "pump_local_chat_store.json"

# Scheduler configuration
SCAN_START_HOUR = 0
SCAN_START_MINUTE = 0
SCAN_END_HOUR = 23
SCAN_END_MINUTE = 45
SCHEDULER_CHECK_SECONDS = 120
SCHEDULER_GRACE_SECONDS = 120

# 2-minute demanded-symbol scan config
DEMAND_SCAN_INTERVAL_MINUTES = 2
SCAN_COOLDOWN_SECONDS = 60
BALE_UPDATES_TIMEOUT = 30
BALE_UPDATE_RETRY_SLEEP = 5

# Local web chat config
LOCAL_WEB_HOST = "0.0.0.0"
LOCAL_WEB_PORT = int(os.environ.get("PORT", "8765"))
LOCAL_WEB_DEFAULT_CHAT_ID = "local-web"
LOCAL_WEB_FETCH_LIMIT_DEFAULT = 200
LOCAL_WEB_FETCH_LIMIT_MAX = 1000
LOCAL_WEB_BODY_LIMIT = 64 * 1024
LOCAL_WEB_SSE_QUEUE_SIZE = 200
LOCAL_WEB_SSE_HEARTBEAT_SECONDS = 20
LOCAL_CHAT_RETENTION_HOURS = 48
LOCAL_CHAT_PRUNE_INTERVAL_SECONDS = 300

# Runtime scan coordination
WAKEUP_EVENT = threading.Event()
SCAN_COORDINATION_LOCK = threading.Lock()
ACTIVE_SCAN_KIND = None
LAST_SCAN_FINISHED_MONO = 0.0

# Demanded symbol runtime state
DEMAND_STATE_LOCK = threading.Lock()
DEMAND_SYMBOL_SUBSCRIBERS = {}
DEMAND_CHAT_SUBSCRIPTIONS = {}
DEMAND_PRICE_TRACKING = {}
DEMAND_LAST_SCAN_SLOT_KEY = None
DEMAND_SCAN_START_NOT_BEFORE = None
BALE_UPDATES_OFFSET = None

# Local web runtime state
LOCAL_CHAT_LOCK = threading.Lock()
LOCAL_SSE_CLIENTS_LOCK = threading.Lock()
LOCAL_SSE_CLIENTS = set()
OUTGOING_REPLY_CONTEXT = threading.local()
TODAY_CHART_RESOLUTION_CACHE_LOCK = threading.Lock()
TODAY_CHART_RESOLUTION_CACHE = {}
TODAY_CHART_RESOLUTION_CACHE_TTL = 12 * 3600
TODAY_CHART_PROBE_TIMEOUT_SECONDS = 2.4
TODAY_CHART_BUTTONS_PER_ROW = 2
TODAY_CHART_MAX_BUTTONS_PER_MESSAGE = 60
TODAY_CHART_WINDOW_HOURS = 3
TODAY_CHART_PRECOMPUTE_WORKERS = 6
TODAY_CHART_PRECOMPUTE_LOCK = threading.Lock()
TODAY_CHART_PRECOMPUTE_RUNNING = False
PERSIAN_DIGITS_TRANSLATION = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")

BALE_OUTGOING_QUEUE = queue.Queue()
BALE_OUTGOING_WORKER_LOCK = threading.Lock()
BALE_OUTGOING_WORKER_STARTED = False
ADMIN_ADVER_SESSIONS = {}
ADMIN_ADLIST_SESSIONS = {}
ADMIN_ADSCHEDULE_SESSIONS = {}
ADVER_SCHEDULER_LOCK = threading.Lock()
ADVER_SCHEDULER_THREAD_STARTED = False

# =========================
# BALE MESSENGER CONFIG
# =========================

BALE_BOT_TOKEN = os.getenv("BALE_BOT_TOKEN", "").strip()
BALE_CHAT_IDS = ["808704022", "1270763934"]
BALE_CHAT_ID_SET = {str(chat_id).strip() for chat_id in BALE_CHAT_IDS if str(chat_id).strip()}
BALE_API_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/sendMessage"
BALE_GET_UPDATES_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/getUpdates"
BALE_SEND_INVOICE_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/sendInvoice"
BALE_ANSWER_PRECHECKOUT_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/answerPreCheckoutQuery"
BALE_SEND_DOCUMENT_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/sendDocument"
BALE_SEND_PHOTO_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/sendPhoto"
BALE_GET_FILE_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/getFile"
BALE_FILE_DOWNLOAD_BASE_URL = f"https://tapi.bale.ai/file/bot{BALE_BOT_TOKEN}"

BALE_TEXT_CHUNK_MAX_CHARS = 4000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ADVER_BACKUP_DIR = os.getenv("ADVER_BACKUP_DIR", os.path.join(BASE_DIR, "adver_backups"))
ADVER_INDEX_FILE = os.path.join(ADVER_BACKUP_DIR, "adver_index.json")
ADVER_SEND_THROTTLE_SECONDS = max(0.0, float(os.getenv("ADVER_SEND_THROTTLE_SECONDS", "0.35") or "0.35"))
ADVER_SCHEDULER_CHECK_SECONDS = max(5, int(float(os.getenv("ADVER_SCHEDULER_CHECK_SECONDS", "30") or "30")))
FILES_DIR = os.path.join(BASE_DIR, "files")
AUTO_TRADING_BUTTON_TEXT = "اتومات کردن معاملات"
AUTO_TRADING_FILES_PROMPT_TEXT = "برای دریافت فایل‌های اتومات کردن معاملات از دکمه زیر استفاده کنید."
AUTO_TRADING_FILES_MESSAGE_TEXT = "راه اندازی ربات معامله گر یعنی پذیرش ریسک زیان هم می‌باشد، و مسئولیت مالی و رفتارهای مالی شما با خود شما است و این ربات هیچ مسئولیتی نسبت به زیان در معاملات شما را ندارد.\n\nقبل از شروع\nاین موارد را آماده داشته باشید:\nمرورگر مناسب دستگاه\nفایل ZIP اکستنشن Violentmonkey\nفایل TXT معامله‌گر (LBankTrader)\nحساب Bale\nحساب LBank با API فعال\nنکتهٔ مهم: هر دو مرحلهٔ ثبت API Key و دریافت توکن session باید در مرورگر انجام شود، نه در اپ.\n\n1) دانلود دو فایل قرار داده شده در انتهای این راهنما و انتخاب مرورگر بسته به دستگاه شما\n\nاندروید\nاز Kiwi Browser استفاده کنید، برای نصب نام آن را در مایکت یا بازار یا فروشگاه پلی استور سرچ کرده و نصب نمایید.\nآیفون / آیپد\nاز Orion Browser استفاده کنید. Orion روی iPhone و iPad بخش Extensions دارد و با دکمهٔ + اجازه می‌دهد اکستنشن نصب شود.\nویندوز / لینوکس / مک\nاز Google Chrome استفاده کنید.\n\n2) نصب افزونهٔ Violentmonkey\n\nدر شرایط نت ملی از طریق فایل zip اقدام نمایید، در غیر اینصورت مستقیماً نام violentmonkey را در فروشگاه اکستنشن گوگل کروم(chrome web store) سرچ کرده و آن را مستقیماً مثل بقیه اکستنشن ها نصب نمایید.\n\nاندروید — Kiwi Browser\nمرورگر Kiwi را باز کنید.\nروی سه‌نقطهٔ بالا بزنید.\nاز منو، Extensions را باز کنید و اوکی بزنید .\nDeveloper mode را روشن کنید.\nروی دکمهٔ +(from .zip/.crx/...) بزنید.\nفایل ZIP اکستنشن Violentmonkey که در زیر این راهنما قرار دادیم و دانلود کردید را انتخاب کنید.\nصبر کنید تا افزونه نصب شود.\nمطمئن شوید Violentmonkey در لیست افزونه‌ها دیده می‌شود و فعال است.\n\nویندوز / لینوکس / مک — Chrome\nدر Chrome باید فایل ZIP را اول از حالت فشرده خارج کنید و بعد پوشه را بارگذاری کنید.\nمراحل\nفایل ZIP افزونهٔ Violentmonkey را دانلود کنید.\nآن را Extract کنید.\nمرورگر Chrome را باز کنید.\nصفحه extensions را باز کنید و Developer mode را روشن کنید.\nروی Load unpacked بزنید.\nفایل زیپ را از حالت زیپ خارج کرده و پوشهٔ استخراج‌شدهٔ Violentmonkey را انتخاب کنید.\nصبر کنید تا افزونه نصب شود.\nمطمئن شوید افزونه فعال است.\n\nآیفون / آیپد — Orion\nمراحل\nمرورگر Orion را باز کنید.\nروی سه‌نقطه بزنید.\nوارد Settings شوید.\nدر بخش Extensions، پشتیبانی افزونه‌ها را فعال کنید.\nبرگردید.\nدوباره روی سه‌نقطه بزنید.\nوارد Extensions شوید.\nروی دکمهٔ + بزنید.\nگزینهٔ نصب file-based extension را انتخاب کنید.\nفایل افزونهٔ Violentmonkey را از داخل Files انتخاب کنید.\nصبر کنید تا افزونه اضافه شود.\n\n3) بازکردن Violentmonkey و اضافه‌کردن ربات معامله‌گر\nاین مرحله روی همهٔ دستگاه‌ها تقریباً یکسان است.\nفایل TXT آماده است و لازم نیست چیزی به آن اضافه یا از آن کم شود.\nمراحل دقیق\nViolentmonkey را باز کنید.\nروی دکمهٔ + بزنید.\nگزینهٔ New script را انتخاب کنید.\nادیتور باز می‌شود.\nهمهٔ متن داخل ادیتور را کامل پاک کنید و مطمئن شوید هیچ خطی از متن نمونه داخل ادیتور باقی نگذارید.\nفایل TXT را باز کنید.\nکل متن فایل TXT را انتخاب کنید.\nمتن را Copy کنید.\nبه ادیتور Violentmonkey برگردید.\nکل متن فایل TXT را داخل ادیتور Paste کنید.\nروی Save بزنید.\n\n4) بازکردن Bale و ربات\nبعد از ذخیره‌کردن اسکریپت:\nدر همان مرورگری که Violentmonkey داخلش نصب شده، web.bale.ai را باز کنید.\nوارد حساب Bale شوید.\nچت lbankbot@ (چت رادار LBank) را باز کنید.\nصبر کنید تا پنل کامل بالا بیاید.\n\n5) ساختن API Key در LBank و کپی کردن آن\nاین مرحله را در مرورگر انجام دهید.\nمراحل\nدر مرورگر سایت lbank.com را باز کنید.\nوارد حساب خود شوید.\nروی آیکون پروفایل (آدمک) کلیک کنید.\nوارد بخش API Management شوید.\nیک API Key جدید بسازید یا از API موجود استفاده کنید.\nمطمئن شوید مجوز Spot Trading برای این API فعال است.\nAPI Key و Secret Key را کپی کنید و در جای امنی ذخیره نمایید.\n\n6) واردکردن API Key داخل ربات معامله‌گر\nبه چت رادار LBank (lbankbot@) در Bale بروید.\nروی دکمه تنظیم API اصلی بزنید.\nروش امضا: HmacSHA256 را انتخاب کنید.\nAPI Key را در فیلد LBank API Key وارد کنید.\nSecret Key را در فیلد Secret Key وارد کنید.\nروی ذخیره و بررسی اتصال بزنید و منتظر تأیید بمانید.\n\n7) دریافت خودکار توکن session از LBank (مرحله مهم)\nدر همان مرورگری که Violentmonkey نصب است:\nبه سایت lbank.com بروید.\nاگر وارد حساب نشده‌اید وارد شوید.\nاسکریپت به‌صورت خودکار توکن session شما را از مرورگر می‌خواند و ذخیره می‌کند.\nیک پنجرهٔ تأیید با پیام توکن session ذخیره شد ظاهر می‌شود.\nروی بستن بزنید و به Bale برگردید.\nنکته: این توکن session برای اجرای دستورات معاملاتی از طریق مسیر داخلی LBank استفاده می‌شود و تکمیل‌کنندهٔ API Key است. هر بار که از حساب خارج شوید باید این مرحله را تکرار کنید.\n\n8) شروع معاملهٔ واقعی\n\nدکمه اجرای واقعی را روشن کنید.\nدکمه حالت آزمایشی را خاموش کنید.\nاگر می‌خواهید ربات در پس‌زمینه هم ادامه بدهد، اجرای در پس‌زمینه را هم روشن کنید.\nاز اینجا به بعد، وقتی سیگنال خرید برسد خرید انجام می‌شود و وقتی سیگنال فروش برسد فروش انجام می‌شود.\n\nتقسیم‌بندی خرید طبق طبقه بندی ارزهای با بالاترین پامپ انجام می‌گیرد و درصد خرید هر یک ارز با دیگری متفاوت است و بر اساس میزان پامپ ارز ها اولویت بندی میکند و همچنین میزان درصد خرید برای هر کدام را بالاتر و پایین‌تر و بطور خودکار انتخاب میکند و معاملات را از جانب شما، برای شما انجام میدهد.\n\n9) موجودی لازم\nبرای شروع کار:\nحداقل ۱۵ تتر (USDT) موجودی در حساب LBank خود داشته باشید.\nاین ربات فقط در بازار تتر (USDT) معامله می‌کند.\n اگر سوالی دارید به پشتیبانی پیام بدهید: @smartabz"
AUTO_TRADING_XPI_PATH = os.path.join(FILES_DIR, "violentmonkey.zip")
AUTO_TRADING_TXT_PATH = os.path.join(FILES_DIR, "LBankTrader.txt")


BALE_ADMIN_CHAT_IDS = ["808704022"]
BALE_ADMIN_CHAT_ID = BALE_ADMIN_CHAT_IDS[0]
BALE_ADMIN_CHAT_ID_SET = {str(chat_id).strip() for chat_id in BALE_ADMIN_CHAT_IDS if str(chat_id).strip()}
BALE_ADMIN_CONTACT_USERNAME = os.getenv("BALE_ADMIN_USERNAME", "smartabz").strip().lstrip("@") or "smartabz"


def is_admin_chat(chat_id):
    return str(chat_id or "").strip() in BALE_ADMIN_CHAT_ID_SET


def is_configured_bale_chat(chat_id):
    return str(chat_id or "").strip() in BALE_CHAT_ID_SET


def get_admin_contact_username():
    return (BALE_ADMIN_CONTACT_USERNAME or "smartabz").strip().lstrip("@") or "smartabz"


def build_activation_or_support_line(action_text="برای تمدید"):
    return f"{action_text}، دستور /activate را ارسال کنید یا به پشتیبانی پیام بدهید: @{get_admin_contact_username()}"


def build_first_month_support_line():
    return (
        "اگر این ماه، اولین ماه استفاده شماست، "
        f"برای فعال‌سازی ماه اول با قیمت {FIRST_MONTH_SUPPORT_PRICE_TEXT} "
        f"به پشتیبانی پیام بدهید: @{get_admin_contact_username()}"
    )
SUBSCRIBERS_FILE = "bale_subscribers.json"
FREE_USAGE_HOURS = 36
FREE_USAGE_SECONDS = FREE_USAGE_HOURS * 3600
SUBSCRIPTION_DAYS = 30
ACTIVATION_COMMANDS = {"/activate", "activate"}
ACTIVATION_SUBSCRIPTION_DAYS = SUBSCRIPTION_DAYS
ACTIVATION_PRICE_TOMAN = 3_900_000
ACTIVATION_PRICE_IRR = ACTIVATION_PRICE_TOMAN * 10
ACTIVATION_INVOICE_PAYLOAD = "activate_subscription_30d"
ACTIVATION_INVOICE_TITLE = "اشتراک 30 روزه"
ACTIVATION_INVOICE_DESCRIPTION = "پرداخت برای فعال‌سازی یا تمدید اشتراک 30 روزه"
FIRST_MONTH_SUPPORT_PRICE_TEXT = "۱ و ۹۰۰ هزار تومن"
BALE_WALLET_PROVIDER_TOKEN = os.getenv("BALE_PROVIDER_TOKEN", "").strip()
SUBSCRIPTION_WARNING_LOOKAHEAD_HOURS = 48
SUBSCRIBER_STATE_LOCK = threading.Lock()
PHONE_NUMBER_REQUIRED = False
AUTO_ACCESS_PHONE_PLACEHOLDER = "auto-start"

DONE_COMMAND_EN = re.escape(os.getenv("DONE_COMMAND_EN", "").strip())
DONE_COMMAND_FA = re.escape(os.getenv("DONE_COMMAND_FA", "").strip())

def get_subscribers_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), SUBSCRIBERS_FILE)


def _parse_access_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _format_access_dt(value):
    if value is None:
        return None
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def normalize_phone_number(phone_number):
    raw = str(phone_number or "").strip()
    digits = re.sub(r"[^0-9+]", "", raw)
    if digits.startswith("0098"):
        digits = "+98" + digits[4:]
    elif digits.startswith("98") and not digits.startswith("+98"):
        digits = "+" + digits
    elif digits.startswith("09"):
        digits = "+98" + digits[1:]
    elif digits.startswith("9") and len(digits) == 10:
        digits = "+98" + digits
    return digits or raw


def mask_phone_number(phone_number):
    digits = re.sub(r"\D", "", str(phone_number or ""))
    if len(digits) < 4:
        return str(phone_number or "-")
    return f"***{digits[-4:]}"


def get_default_subscriber_state():
    return {
        "schema_version": 1,
        "next_order_id": 1,
        "users": {},
        "last_messages": {
            "pump_radar": {
                "text": None,
                "generated_at": None,
                "meta": {}
            },
            "strategy": {
                "text": None,
                "generated_at": None,
                "meta": {}
            }
        }
    }


def normalize_subscriber_state(state):
    default_state = get_default_subscriber_state()
    if not isinstance(state, dict):
        state = {}

    normalized = {
        "schema_version": 1,
        "next_order_id": int(state.get("next_order_id", 1) or 1),
        "users": state.get("users") if isinstance(state.get("users"), dict) else {},
        "last_messages": state.get("last_messages") if isinstance(state.get("last_messages"), dict) else {},
    }

    for key in ("pump_radar", "strategy"):
        item = normalized["last_messages"].get(key)
        if not isinstance(item, dict):
            item = {}
        normalized["last_messages"][key] = {
            "text": item.get("text"),
            "generated_at": item.get("generated_at"),
            "meta": item.get("meta") if isinstance(item.get("meta"), dict) else {}
        }

    cleaned_users = {}
    highest_order = 0

    for raw_chat_id, raw_user in normalized["users"].items():
        chat_id = str(raw_chat_id).strip()
        if not chat_id:
            continue
        if not isinstance(raw_user, dict):
            raw_user = {}

        try:
            order_id = int(raw_user.get("order_id", 0) or 0)
        except Exception:
            order_id = 0
        if order_id <= 0:
            order_id = normalized["next_order_id"]
            normalized["next_order_id"] += 1

        highest_order = max(highest_order, order_id)

        subscriptions = raw_user.get("subscriptions")
        if not isinstance(subscriptions, list):
            subscriptions = []

        clean_subscriptions = []
        for entry in subscriptions:
            if not isinstance(entry, dict):
                continue
            clean_subscriptions.append({
                "granted_at": entry.get("granted_at"),
                "start_at": entry.get("start_at"),
                "end_at": entry.get("end_at"),
                "days": int(entry.get("days", SUBSCRIPTION_DAYS) or SUBSCRIPTION_DAYS),
                "granted_by": str(entry.get("granted_by") or BALE_ADMIN_CHAT_ID),
            })

        free_trial = raw_user.get("free_trial")
        if not isinstance(free_trial, dict):
            free_trial = {}

        notification_state = raw_user.get("notification_state")
        if not isinstance(notification_state, dict):
            notification_state = {}

        sent_slots = notification_state.get("subscription_warning_slots_sent")
        if not isinstance(sent_slots, list):
            sent_slots = []

        access_lock_notice_keys_sent = notification_state.get("access_lock_notice_keys_sent")
        if not isinstance(access_lock_notice_keys_sent, list):
            access_lock_notice_keys_sent = []

        cleaned_users[chat_id] = {
            "chat_id": chat_id,
            "order_id": order_id,
            "created_at": raw_user.get("created_at"),
            "updated_at": raw_user.get("updated_at"),
            "first_name": raw_user.get("first_name"),
            "last_name": raw_user.get("last_name"),
            "username": raw_user.get("username"),
            "phone_number": raw_user.get("phone_number"),
            "interaction_count": int(raw_user.get("interaction_count", 0) or 0),
            "last_interaction_at": raw_user.get("last_interaction_at"),
            "free_trial": {
                "started_at": free_trial.get("started_at"),
                "expires_at": free_trial.get("expires_at"),
                "ended_at": free_trial.get("ended_at"),
                "lock_notice_sent_at": free_trial.get("lock_notice_sent_at"),
                "used_scan_seconds": float(free_trial.get("used_scan_seconds", 0.0) or 0.0),
                "allocated_seconds": int(free_trial.get("allocated_seconds", FREE_USAGE_SECONDS) or FREE_USAGE_SECONDS),
            },
            "notification_state": {
                "subscription_warning_end_at": notification_state.get("subscription_warning_end_at"),
                "subscription_warning_slots_sent": [str(item) for item in sent_slots if str(item).strip()],
                "access_lock_notice_keys_sent": [str(item) for item in access_lock_notice_keys_sent if str(item).strip()],
            },
            "subscriptions": clean_subscriptions,
        }

    normalized["users"] = cleaned_users
    normalized["next_order_id"] = max(int(normalized.get("next_order_id", 1) or 1), highest_order + 1)
    return normalized


def load_subscriber_state_file():
    path = get_subscribers_file_path()
    default_state = get_default_subscriber_state()
    if not os.path.exists(path):
        save_subscriber_state_file(default_state)
        return default_state
    try:
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        return normalize_subscriber_state(state)
    except Exception as exc:
        print(f"{COL_WARN}[ACCESS]{RESET} Failed to load subscriber state, resetting: {exc}")
        save_subscriber_state_file(default_state)
        return default_state


def save_subscriber_state_file(state):
    path = get_subscribers_file_path()
    tmp_path = path + ".tmp"
    normalized = normalize_subscriber_state(state)
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(normalized, fh, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception as exc:
        print(f"{COL_WARN}[ACCESS]{RESET} Failed to save subscriber state: {exc}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def load_subscriber_state():
    with SUBSCRIBER_STATE_LOCK:
        return load_subscriber_state_file()


def save_subscriber_state(state):
    with SUBSCRIBER_STATE_LOCK:
        save_subscriber_state_file(state)


def get_or_create_user_record(state, chat_id, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    chat_id = str(chat_id).strip()
    users = state.setdefault("users", {})
    user = users.get(chat_id)
    if isinstance(user, dict):
        return user
    user = {
        "chat_id": chat_id,
        "order_id": int(state.get("next_order_id", 1) or 1),
        "created_at": _format_access_dt(now_dt),
        "updated_at": _format_access_dt(now_dt),
        "first_name": None,
        "last_name": None,
        "username": None,
        "phone_number": None,
        "interaction_count": 0,
        "last_interaction_at": None,
        "free_trial": {
            "started_at": None,
            "expires_at": None,
            "ended_at": None,
            "lock_notice_sent_at": None,
            "used_scan_seconds": 0.0,
            "allocated_seconds": FREE_USAGE_SECONDS,
        },
        "notification_state": {
            "subscription_warning_end_at": None,
            "subscription_warning_slots_sent": [],
            "access_lock_notice_keys_sent": [],
        },
        "subscriptions": [],
    }
    users[chat_id] = user
    state["next_order_id"] = int(state.get("next_order_id", 1) or 1) + 1
    return user


def update_user_profile_from_message(state, chat_id, message):
    if not isinstance(message, dict):
        return get_or_create_user_record(state, chat_id)
    now_dt = get_tehran_now()
    user = get_or_create_user_record(state, chat_id, now_dt=now_dt)
    sender = message.get("from") or {}
    user["first_name"] = sender.get("first_name") or user.get("first_name")
    user["last_name"] = sender.get("last_name") or user.get("last_name")
    user["username"] = sender.get("username") or user.get("username")
    user["updated_at"] = _format_access_dt(now_dt)
    return user


def touch_user_interaction(state, chat_id, message=None, increment=True):
    now_dt = get_tehran_now()
    user = update_user_profile_from_message(state, chat_id, message)
    if increment:
        user["interaction_count"] = int(user.get("interaction_count", 0) or 0) + 1
    user["last_interaction_at"] = _format_access_dt(now_dt)
    user["updated_at"] = _format_access_dt(now_dt)
    return user


def get_user_display_name(user):
    if not isinstance(user, dict):
        return "-"
    full_name = " ".join(part for part in [user.get("first_name"), user.get("last_name")] if part).strip()
    if full_name:
        return full_name
    username = user.get("username")
    if username:
        return f"@{username}"
    return "-"


def get_free_trial_allocated_seconds(user):
    if not isinstance(user, dict):
        return float(FREE_USAGE_SECONDS)
    free_trial = user.get("free_trial") if isinstance(user.get("free_trial"), dict) else {}
    try:
        allocated = float(free_trial.get("allocated_seconds", FREE_USAGE_SECONDS) or FREE_USAGE_SECONDS)
    except Exception:
        allocated = float(FREE_USAGE_SECONDS)
    if allocated <= 0:
        allocated = float(FREE_USAGE_SECONDS)
    return allocated


def get_scan_window_bounds_for_day(day_dt):
    if day_dt is None:
        return None, None
    window_start = day_dt.replace(hour=SCAN_START_HOUR, minute=SCAN_START_MINUTE, second=0, microsecond=0)
    window_end = day_dt.replace(hour=SCAN_END_HOUR, minute=SCAN_END_MINUTE, second=0, microsecond=0)
    if window_end <= window_start:
        window_end = window_start + timedelta(days=1)
    return window_start, window_end


def get_free_trial_started_dt(user):
    if not isinstance(user, dict):
        return None
    free_trial = user.get("free_trial") if isinstance(user.get("free_trial"), dict) else {}
    return _parse_access_dt(free_trial.get("started_at"))


def get_scheduled_scan_seconds_between(start_dt, end_dt):
    if start_dt is None or end_dt is None or end_dt <= start_dt:
        return 0.0

    total_seconds = 0.0
    current_day = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    last_day = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    while current_day <= last_day:
        window_start, window_end = get_scan_window_bounds_for_day(current_day)
        if window_start is None or window_end is None:
            current_day += timedelta(days=1)
            continue

        overlap_start = max(start_dt, window_start)
        overlap_end = min(end_dt, window_end)
        if overlap_end > overlap_start:
            total_seconds += (overlap_end - overlap_start).total_seconds()

        current_day += timedelta(days=1)

    return max(0.0, total_seconds)


def add_scheduled_scan_seconds(start_dt, seconds_value):
    if start_dt is None:
        return None
    try:
        remaining_seconds = float(seconds_value or 0.0)
    except Exception:
        remaining_seconds = 0.0
    if remaining_seconds <= 0:
        return start_dt

    current_dt = start_dt
    safety_counter = 0

    while remaining_seconds > 0 and safety_counter < 4000:
        safety_counter += 1
        window_start, window_end = get_scan_window_bounds_for_day(current_dt)
        if window_start is None or window_end is None:
            current_dt = (current_dt + timedelta(days=1)).replace(hour=SCAN_START_HOUR, minute=SCAN_START_MINUTE, second=0, microsecond=0)
            continue

        if current_dt < window_start:
            current_dt = window_start
        elif current_dt >= window_end:
            current_dt = (window_start + timedelta(days=1)).replace(hour=SCAN_START_HOUR, minute=SCAN_START_MINUTE, second=0, microsecond=0)
            continue

        available_seconds = (window_end - current_dt).total_seconds()
        if remaining_seconds <= available_seconds:
            return current_dt + timedelta(seconds=remaining_seconds)

        remaining_seconds -= available_seconds
        current_dt = (window_start + timedelta(days=1)).replace(hour=SCAN_START_HOUR, minute=SCAN_START_MINUTE, second=0, microsecond=0)

    return current_dt


def get_free_trial_expires_dt(user):
    started_dt = get_free_trial_started_dt(user)
    if started_dt is None:
        return None
    return add_scheduled_scan_seconds(started_dt, get_free_trial_allocated_seconds(user))


def get_free_trial_used_scan_seconds(user, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    if not isinstance(user, dict):
        return 0.0

    started_dt = get_free_trial_started_dt(user)
    if started_dt is None:
        free_trial = user.get("free_trial") if isinstance(user.get("free_trial"), dict) else {}
        try:
            used_seconds = float(free_trial.get("used_scan_seconds", 0.0) or 0.0)
        except Exception:
            used_seconds = 0.0
        return max(0.0, used_seconds)

    elapsed_seconds = get_scheduled_scan_seconds_between(started_dt, now_dt)
    allocated_seconds = get_free_trial_allocated_seconds(user)
    return max(0.0, min(allocated_seconds, elapsed_seconds))


def get_free_trial_remaining_seconds(user, now_dt=None):
    allocated = get_free_trial_allocated_seconds(user)
    remaining = allocated - get_free_trial_used_scan_seconds(user, now_dt=now_dt)
    if remaining < 0:
        remaining = 0.0
    return remaining


def normalize_command_input(text):
    if not isinstance(text, str):
        return ""

    cleaned = text
    for ch in ("‎", "‏", "؜", "‪", "‫", "‬", "‭", "‮", "⁦", "⁧", "⁨", "⁩"):
        cleaned = cleaned.replace(ch, "")

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    if cleaned.endswith("/") and not cleaned.startswith("/") and " " not in cleaned:
        command_name = cleaned[:-1].strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", command_name):
            cleaned = f"/{command_name}"

    return cleaned


def format_duration_persian(seconds_value):
    try:
        total_seconds = int(max(0, round(float(seconds_value))))
    except Exception:
        total_seconds = 0
    total_minutes = (total_seconds + 59) // 60 if total_seconds > 0 else 0
    hours = total_minutes // 60
    minutes = total_minutes % 60
    parts = []
    if hours > 0:
        parts.append(f"{hours} ساعت")
    if minutes > 0:
        parts.append(f"{minutes} دقیقه")
    if not parts:
        parts.append("کمتر از ۱ دقیقه")
    return " و ".join(parts)


def to_persian_digits(value):
    return str(value).translate(PERSIAN_DIGITS_TRANSLATION)


def get_subscription_warning_slot_key(now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    half_day = "AM" if int(now_dt.hour) < 12 else "PM"
    return f"{now_dt.strftime('%Y-%m-%d')}|{half_day}"


def get_last_subscription_end_dt(user):
    if not isinstance(user, dict):
        return None
    latest = None
    for entry in user.get("subscriptions", []):
        if not isinstance(entry, dict):
            continue
        end_dt = _parse_access_dt(entry.get("end_at"))
        if end_dt is None:
            continue
        if latest is None or end_dt > latest:
            latest = end_dt
    return latest


def get_user_access_snapshot_from_record(user, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    if not isinstance(user, dict):
        return {
            "active": False,
            "stage": "missing",
            "reason": "missing",
            "expires_at": None,
            "is_admin": False,
            "remaining_scan_seconds": 0.0,
        }
    chat_id = str(user.get("chat_id") or "")
    if is_admin_chat(chat_id):
        return {
            "active": True,
            "stage": "admin",
            "reason": "admin",
            "expires_at": None,
            "is_admin": True,
            "remaining_scan_seconds": None,
        }

    if is_configured_bale_chat(chat_id):
        return {
            "active": True,
            "stage": "configured_chat",
            "reason": "configured_chat",
            "expires_at": None,
            "is_admin": False,
            "remaining_scan_seconds": None,
        }
    phone_number = user.get("phone_number")
    free_trial = user.get("free_trial") if isinstance(user.get("free_trial"), dict) else {}
    free_expires_dt = get_free_trial_expires_dt(user)
    subscription_end_dt = get_last_subscription_end_dt(user)
    remaining_scan_seconds = get_free_trial_remaining_seconds(user, now_dt=now_dt)
    free_started = get_free_trial_started_dt(user) is not None

    if subscription_end_dt is not None and subscription_end_dt > now_dt:
        return {
            "active": True,
            "stage": "subscribed",
            "reason": "subscription_active",
            "expires_at": subscription_end_dt,
            "is_admin": False,
            "remaining_scan_seconds": remaining_scan_seconds,
        }

    if free_started and remaining_scan_seconds > 0:
        return {
            "active": True,
            "stage": "free",
            "reason": "free_trial_active",
            "expires_at": free_expires_dt,
            "is_admin": False,
            "remaining_scan_seconds": remaining_scan_seconds,
        }

    # Phone numbers are no longer required. The free-trial clock is started
    # automatically from the first user interaction instead of blocking on
    # an awaiting_phone stage. If an old record reaches this point without
    # a started trial, it falls through to normal locked/no-access handling.

    if subscription_end_dt is not None and subscription_end_dt <= now_dt:
        return {
            "active": False,
            "stage": "locked",
            "reason": "subscription_expired",
            "expires_at": subscription_end_dt,
            "is_admin": False,
            "remaining_scan_seconds": remaining_scan_seconds,
        }

    if free_started and remaining_scan_seconds <= 0:
        return {
            "active": False,
            "stage": "locked",
            "reason": "free_trial_expired",
            "expires_at": free_expires_dt,
            "is_admin": False,
            "remaining_scan_seconds": 0.0,
        }

    return {
        "active": False,
        "stage": "locked",
        "reason": "no_access",
        "expires_at": None,
        "is_admin": False,
        "remaining_scan_seconds": remaining_scan_seconds,
    }

def build_locked_access_message(access):
    reason = (access or {}).get("reason")
    if reason == "subscription_expired":
        return (
            "اشتراک شما به پایان رسیده است.\n"
            + build_activation_or_support_line("برای تمدید")
        )
    return (
        "استفاده رایگان شما به پایان رسیده است.\n"
        + build_activation_or_support_line("برای ادامه استفاده")
        + "\n"
        + build_first_month_support_line()
    )



def build_user_commands_text():
    return (
        "راهنما\n\n"
        "اسکن کامل رادار:\n"
        "• 🔴 یعنی نماد در اسکن اخیر پامپ کوتاه‌مدت یا حرکت غیرعادی داشته است.\n"
        "• ⭐ یعنی نماد در چند اسکن پشت‌سرهم تکرار شده و x تعداد تکرار را نشان می‌دهد.\n"
        "• 4h و 30m و 1d درصد تغییرها هستند.\n"
        "USDT قیمت و بازار LBank همان نماد را نشان می‌دهد.\n"
        "• خط Market وضعیت کلی بازار را با تغییر ۲۴ساعته BTC و ETH نشان می‌دهد.\n\n"
        "سیگنال‌های استراتیژی:\n"
        "• 🔸 احتمال خرید هنگام پامپ: منتظر تایید نهایی میباشد.\n"
        "• ✴️ سیگنال خرید هنگام پامپ: هنگام پامپ سیگنال خرید میدهد.\n"
        "• 🟢 خرید: ورود تأیید شده است.\n"
        "• 🔻 فروش: خروج ثبت شده است.\n"
        "• وضعیت خریدهای فعال: قیمت فعلی، درصد سود/زیان، و مقدار مقاومت های شکسته شده را با x1/x2 نشان می‌دهد.\n\n"
        "دکمه‌ها و گزارش‌ها:\n"
        "• /today گزارش امروز را با دکمه‌های نمودار نشان می‌دهد.\n"
        "• /pnl سود/زیان معاملات بسته‌شدهٔ ۲۴ ساعت اخیر را نشان می‌دهد.\n"
        "• /my نمادهای فعال شما را نشان می‌دهد.\n\n"
        "دستورات قابل استفاده:\n\n"
        "برای ردیابی دو دقیقه‌ای یک نماد، فقط نام کوین را بفرستید:\n"
        "BTC\n"
        "برای توقف ردیابی:\n"
        "-BTC\n\n"
        "نمادهای فعال شما:\n"
        "/my\n\n"
        "گزارش امروز:\n"
        "/today\n\n"
        "سود/زیان معاملات بسته‌شده:\n"
        "/pnl\n\n"
        "راهنما:\n"
        "/help\n\n"
    )

def persist_last_bot_message_snapshot(message_key, text, generated_at=None, meta=None):
    if generated_at is None:
        generated_at = get_tehran_now()
    state = load_subscriber_state()
    last_messages = state.setdefault("last_messages", {})
    current = last_messages.get(message_key)
    if not isinstance(current, dict):
        current = {}
    current["text"] = str(text or "").strip() or None
    current["generated_at"] = _format_access_dt(generated_at)
    current["meta"] = meta if isinstance(meta, dict) else {}
    last_messages[message_key] = current
    save_subscriber_state(state)


def get_last_bot_message_snapshot(message_key):
    state = load_subscriber_state()
    last_messages = state.get("last_messages") if isinstance(state.get("last_messages"), dict) else {}
    item = last_messages.get(message_key)
    if not isinstance(item, dict):
        return None
    return item


def get_active_broadcast_chat_ids(now_dt=None, include_admin=True):
    if now_dt is None:
        now_dt = get_tehran_now()
    state = load_subscriber_state()
    active_chat_ids = []
    if include_admin:
        for admin_chat_id in BALE_ADMIN_CHAT_IDS:
            admin_chat_id = str(admin_chat_id).strip()
            if admin_chat_id and admin_chat_id not in active_chat_ids:
                active_chat_ids.append(admin_chat_id)

    for configured_chat_id in BALE_CHAT_IDS:
        configured_chat_id = str(configured_chat_id).strip()
        if configured_chat_id and configured_chat_id not in active_chat_ids:
            active_chat_ids.append(configured_chat_id)

    for user in state.get("users", {}).values():
        access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
        if not access.get("active"):
            continue
        chat_id = str(user.get("chat_id") or "").strip()
        if chat_id and chat_id not in active_chat_ids:
            active_chat_ids.append(chat_id)
    return active_chat_ids


def build_access_granted_message(user, access):
    return ""

def grant_free_trial_to_user(state, chat_id, phone_number=None, message=None):
    now_dt = get_tehran_now()
    user = touch_user_interaction(state, chat_id, message=message, increment=False)
    free_trial = user.get("free_trial") if isinstance(user.get("free_trial"), dict) else {}
    if user.get("phone_number") and free_trial.get("started_at"):
        access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
        return user, access, False
    normalized_phone = normalize_phone_number(phone_number)
    if not normalized_phone:
        normalized_phone = AUTO_ACCESS_PHONE_PLACEHOLDER
    user["phone_number"] = normalized_phone
    user["free_trial"] = {
        "started_at": _format_access_dt(now_dt),
        "expires_at": None,
        "ended_at": None,
        "lock_notice_sent_at": None,
        "used_scan_seconds": 0.0,
        "allocated_seconds": FREE_USAGE_SECONDS,
    }
    user["free_trial"]["expires_at"] = _format_access_dt(get_free_trial_expires_dt(user))
    user["updated_at"] = _format_access_dt(now_dt)
    access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
    return user, access, True


def ensure_free_trial_started_without_phone(state, chat_id, user=None, message=None):
    """Start access immediately without requesting a phone/contact share."""
    now_dt = get_tehran_now()
    if user is None:
        user = touch_user_interaction(state, chat_id, message=message, increment=False)
    if not isinstance(user, dict):
        return user, get_user_access_snapshot_from_record(user, now_dt=now_dt), False

    chat_id_text = str(user.get("chat_id") or chat_id or "").strip()
    if is_admin_chat(chat_id_text) or is_configured_bale_chat(chat_id_text):
        return user, get_user_access_snapshot_from_record(user, now_dt=now_dt), False

    free_started = get_free_trial_started_dt(user) is not None
    if free_started:
        return user, get_user_access_snapshot_from_record(user, now_dt=now_dt), False

    return grant_free_trial_to_user(
        state,
        chat_id_text or chat_id,
        phone_number=AUTO_ACCESS_PHONE_PLACEHOLDER,
        message=message,
    )

def build_admin_users_message(state):
    users = []
    now_dt = get_tehran_now()
    for user in state.get("users", {}).values():
        if is_admin_chat(user.get("chat_id")):
            continue
        access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
        users.append((
            -int(user.get("interaction_count", 0) or 0),
            int(user.get("order_id", 0) or 0),
            user,
            access,
        ))

    users.sort(key=lambda item: (item[0], item[1]))
    if not users:
        return "هنوز هیچ کاربری ثبت نشده است."

    lines = ["فهرست کاربران (مرتب‌شده بر اساس بیشترین تعامل):", ""]
    for rank, (_, _, user, access) in enumerate(users, 1):
        lines.append(
            f"{rank}. شناسه سفارش {user.get('order_id')} | چت‌آیدی {user.get('chat_id')} | "
            f"نام {get_user_display_name(user)} | موبایل {mask_phone_number(user.get('phone_number'))} | "
            f"تعامل {int(user.get('interaction_count', 0) or 0)} | وضعیت {access.get('stage')}"
        )
    return "\n".join(lines).strip()

def build_admin_subscriptions_message(state):
    now_dt = get_tehran_now()
    rows = []
    for user in state.get("users", {}).values():
        if is_admin_chat(user.get("chat_id")):
            continue
        rows.append((int(user.get("order_id", 0) or 0), user, get_user_access_snapshot_from_record(user, now_dt=now_dt)))
    rows.sort(key=lambda item: item[0])

    if not rows:
        return "هنوز هیچ اشتراکی ثبت نشده است."

    lines = ["وضعیت اشتراک کاربران:", ""]
    for _, user, access in rows:
        lines.append(
            f"شناسه سفارش {user.get('order_id')} | چت‌آیدی {user.get('chat_id')} | موبایل {mask_phone_number(user.get('phone_number'))}"
        )
        if access.get("stage") == "subscribed" and access.get("expires_at") is not None:
            lines.append(f"وضعیت فعلی: فعال تا {_format_access_dt(access.get('expires_at'))}")
        elif access.get("stage") == "free" and access.get("active"):
            lines.append(f"وضعیت فعلی: دسترسی رایگان فعال | باقی‌مانده زمان رایگان: {format_duration_persian(access.get('remaining_scan_seconds') or 0)}")
        elif access.get("reason") == "free_trial_expired":
            lines.append("وضعیت فعلی: استفاده رایگان تمام شده")
        elif access.get("reason") == "subscription_expired":
            lines.append("وضعیت فعلی: اشتراک تمام شده")
        else:
            lines.append("وضعیت فعلی: بدون دسترسی")

        subscriptions = user.get("subscriptions", [])
        if subscriptions:
            for idx, entry in enumerate(subscriptions, 1):
                lines.append(
                    f"  {idx}) ثبت: {entry.get('granted_at') or '-'} | شروع: {entry.get('start_at') or '-'} | پایان: {entry.get('end_at') or '-'}"
                )
        else:
            lines.append("  اشتراکی ثبت نشده است.")
        lines.append("")

    return "\n".join(lines).strip()

def grant_subscription_to_chat_id(chat_id, granted_by_chat_id=None, days=SUBSCRIPTION_DAYS, state=None, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()

    own_state = state is None
    if state is None:
        state = load_subscriber_state()

    user = get_or_create_user_record(state, chat_id, now_dt=now_dt)
    last_end_dt = get_last_subscription_end_dt(user)
    start_dt = last_end_dt if last_end_dt is not None and last_end_dt > now_dt else now_dt
    end_dt = start_dt + timedelta(days=days)

    user.setdefault("subscriptions", []).append({
        "granted_at": _format_access_dt(now_dt),
        "start_at": _format_access_dt(start_dt),
        "end_at": _format_access_dt(end_dt),
        "days": int(days),
        "granted_by": str(granted_by_chat_id or BALE_ADMIN_CHAT_ID),
    })
    user.setdefault("free_trial", {})["lock_notice_sent_at"] = None
    user["updated_at"] = _format_access_dt(now_dt)

    if own_state:
        save_subscriber_state(state)

    return user, start_dt, end_dt


def grant_subscription_by_order_id(order_id, granted_by_chat_id=None, days=SUBSCRIPTION_DAYS):
    now_dt = get_tehran_now()
    state = load_subscriber_state()
    target_user = None
    for user in state.get("users", {}).values():
        if int(user.get("order_id", 0) or 0) == int(order_id):
            target_user = user
            break

    if target_user is None:
        return False, f"کاربری با شناسه سفارش {order_id} پیدا نشد."

    last_end_dt = get_last_subscription_end_dt(target_user)
    start_dt = last_end_dt if last_end_dt is not None and last_end_dt > now_dt else now_dt
    end_dt = start_dt + timedelta(days=days)
    target_user.setdefault("subscriptions", []).append({
        "granted_at": _format_access_dt(now_dt),
        "start_at": _format_access_dt(start_dt),
        "end_at": _format_access_dt(end_dt),
        "days": int(days),
        "granted_by": str(granted_by_chat_id or BALE_ADMIN_CHAT_ID),
    })
    target_user.setdefault("free_trial", {})["lock_notice_sent_at"] = None
    target_user["updated_at"] = _format_access_dt(now_dt)
    save_subscriber_state(state)
    return True, (
        f"اشتراک کاربر با شناسه سفارش {order_id} تا {_format_access_dt(end_dt)} تمدید شد.\n"
        f"چت‌آیدی: {target_user.get('chat_id')} | موبایل: {mask_phone_number(target_user.get('phone_number'))}"
    )


def get_admin_contact_username():
    username = str(BALE_ADMIN_CONTACT_USERNAME or "").strip().lstrip("@")
    return username or "smartabz"


def build_admin_adver_footer(admin_username=None):
    username = str(admin_username or get_admin_contact_username()).strip().lstrip("@")
    return f"راه ارتباط @{username}" if username else "راه ارتباط"


def append_admin_adver_footer(message_text, admin_username=None):
    body = str(message_text or "").strip()
    footer = build_admin_adver_footer(admin_username)
    if not body:
        return footer
    return f"{body}\n\n{footer}"


def build_admin_adver_prompt():
    return (
        "پیام تبلیغ را ارسال کنید.\n\n"
        "می‌توانید فقط متن بفرستید یا یک عکس همراه کپشن بفرستید.\n"
        "متن می‌تواند لینک و یوزرنیم داشته باشد.\n"
        "عکس اختیاری است.\n"
        "بعد از ذخیره، ربات حالت ارسال فوری یا زمان‌بندی را می‌پرسد.\n\n"
        "برای لغو: /cancel"
    )


def is_admin_adver_cancel_text(text):
    low = normalize_command_input(text).lower()
    return low in {"/cancel", "cancel", "لغو", "/لغو"}


def is_admin_adver_session_active(chat_id):
    return str(chat_id or "").strip() in ADMIN_ADVER_SESSIONS


def start_admin_adver_session(chat_id):
    normalized_chat_id = str(chat_id or "").strip()
    if not normalized_chat_id:
        return False
    ADMIN_ADVER_SESSIONS[normalized_chat_id] = {
        "started_at": _format_access_dt(get_tehran_now()),
        "stage": "awaiting_content",
    }
    send_bale_message(normalized_chat_id, build_admin_adver_prompt(), message_kind="admin_adver_prompt")
    return True


def clear_admin_adver_session(chat_id):
    normalized_chat_id = str(chat_id or "").strip()
    if normalized_chat_id:
        ADMIN_ADVER_SESSIONS.pop(normalized_chat_id, None)


def get_adver_photo_file_id(message):
    if not isinstance(message, dict):
        return None

    photos = message.get("photo")
    if isinstance(photos, list) and photos:
        candidates = []
        for photo in photos:
            if not isinstance(photo, dict):
                continue
            file_id = str(photo.get("file_id") or "").strip()
            if not file_id:
                continue
            try:
                score = int(photo.get("file_size") or 0)
            except Exception:
                score = 0
            try:
                score = max(score, int(photo.get("width") or 0) * int(photo.get("height") or 0))
            except Exception:
                pass
            candidates.append((score, file_id))
        if candidates:
            candidates.sort(key=lambda item: item[0], reverse=True)
            return candidates[0][1]

    document = message.get("document")
    if isinstance(document, dict):
        mime_type = str(document.get("mime_type") or "").lower()
        file_name = str(document.get("file_name") or "").lower()
        if mime_type.startswith("image/") or file_name.endswith((".jpg", ".jpeg", ".png", ".webp")):
            file_id = str(document.get("file_id") or "").strip()
            return file_id or None

    return None


def extract_admin_adver_text(message, fallback_text=None):
    if isinstance(message, dict):
        caption = message.get("caption")
        if isinstance(caption, str) and caption.strip():
            return caption.strip()
        text_value = message.get("text")
        if isinstance(text_value, str) and text_value.strip():
            return text_value.strip()

    if isinstance(fallback_text, str) and fallback_text.strip():
        return fallback_text.strip()

    return ""


def get_admin_username_from_message(message):
    if isinstance(message, dict):
        sender = message.get("from")
        if isinstance(sender, dict):
            username = str(sender.get("username") or "").strip().lstrip("@")
            if username:
                return username
    return get_admin_contact_username()


def ensure_adver_backup_dir():
    os.makedirs(ADVER_BACKUP_DIR, exist_ok=True)
    return ADVER_BACKUP_DIR


def get_default_adver_index():
    return {
        "schema_version": 1,
        "next_id": 1,
        "ads": [],
    }


def normalize_adver_index(state):
    default_state = get_default_adver_index()
    if not isinstance(state, dict):
        return default_state

    ads = state.get("ads")
    if not isinstance(ads, list):
        ads = []

    cleaned_ads = []
    highest_id = 0
    for raw_ad in ads:
        if not isinstance(raw_ad, dict):
            continue
        try:
            ad_id = int(raw_ad.get("id", 0) or 0)
        except Exception:
            ad_id = 0
        if ad_id <= 0:
            continue
        highest_id = max(highest_id, ad_id)

        schedule = raw_ad.get("schedule")
        if not isinstance(schedule, dict):
            schedule = {}

        cleaned_ads.append({
            "id": ad_id,
            "created_at": raw_ad.get("created_at"),
            "created_by_chat_id": str(raw_ad.get("created_by_chat_id") or "").strip(),
            "created_by_username": str(raw_ad.get("created_by_username") or "").strip().lstrip("@"),
            "text": str(raw_ad.get("text") or ""),
            "final_text": str(raw_ad.get("final_text") or ""),
            "photo_file_id": str(raw_ad.get("photo_file_id") or "").strip() or None,
            "photo_backup_path": raw_ad.get("photo_backup_path"),
            "text_backup_path": raw_ad.get("text_backup_path"),
            "json_backup_path": raw_ad.get("json_backup_path"),
            "target_count": int(raw_ad.get("target_count", 0) or 0),
            "last_sent_at": raw_ad.get("last_sent_at"),
            "last_send_result": raw_ad.get("last_send_result") if isinstance(raw_ad.get("last_send_result"), dict) else {},
            "schedule": {
                "enabled": bool(schedule.get("enabled")),
                "cadence": str(schedule.get("cadence") or "daily").strip().lower() or "daily",
                "start_time": str(schedule.get("start_time") or "").strip(),
                "every_hours": float(schedule.get("every_hours", 0) or 0),
                "times_per_day": int(schedule.get("times_per_day", 0) or 0),
                "week_days": normalize_adver_week_days(schedule.get("week_days")),
                "month_days": normalize_adver_month_days(schedule.get("month_days")),
                "next_run_at": schedule.get("next_run_at"),
                "last_run_at": schedule.get("last_run_at"),
                "runs_today_date": schedule.get("runs_today_date"),
                "runs_today": int(schedule.get("runs_today", 0) or 0),
            },
        })

    next_id = int(state.get("next_id", highest_id + 1) or highest_id + 1)
    return {
        "schema_version": 1,
        "next_id": max(next_id, highest_id + 1),
        "ads": cleaned_ads,
    }


def load_adver_index():
    ensure_adver_backup_dir()
    if not os.path.exists(ADVER_INDEX_FILE):
        state = get_default_adver_index()
        save_adver_index(state)
        return state

    try:
        with open(ADVER_INDEX_FILE, "r", encoding="utf-8") as fh:
            return normalize_adver_index(json.load(fh))
    except Exception as exc:
        print(f"{COL_WARN}[ADVER]{RESET} Failed to load adver index, resetting: {exc}")
        state = get_default_adver_index()
        save_adver_index(state)
        return state


def save_adver_index(state):
    ensure_adver_backup_dir()
    normalized = normalize_adver_index(state)
    tmp_path = ADVER_INDEX_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(normalized, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_path, ADVER_INDEX_FILE)


def build_adver_backup_slug(ad_id, created_dt):
    if created_dt is None:
        created_dt = get_tehran_now()
    return f"{created_dt.strftime('%Y%m%d-%H%M%S')}-ad-{int(ad_id):04d}"


def safe_adver_filename(value):
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return normalized.strip("._") or "adver"


def download_adver_photo_backup(photo_file_id, ad_dir):
    normalized_photo_file_id = str(photo_file_id or "").strip()
    if not normalized_photo_file_id:
        return None, {"ok": False, "description": "missing photo_file_id"}

    try:
        result = http_post_json_absolute(
            BALE_GET_FILE_URL,
            {"file_id": normalized_photo_file_id},
            timeout=20,
            retries=2,
            backoff_factor=1.5,
        )
        if not result or not result.get("ok"):
            return None, result or {"ok": False, "description": "getFile failed"}

        file_info = result.get("result")
        if not isinstance(file_info, dict):
            return None, {"ok": False, "description": "getFile result is not a File object", "result": result}

        file_path = str(file_info.get("file_path") or "").strip()
        if not file_path:
            return None, {"ok": False, "description": "getFile did not return file_path", "result": result}

        extension = os.path.splitext(file_path)[1].lower()
        if extension not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            extension = ".jpg"

        local_path = os.path.join(ad_dir, "image" + extension)
        download_url = f"{BALE_FILE_DOWNLOAD_BASE_URL}/{parse.quote(file_path, safe='/')}"
        req = request.Request(download_url, headers={"User-Agent": NOBITEX_USER_AGENT}, method="GET")

        with request.urlopen(req, timeout=30) as resp:
            with open(local_path, "wb") as fh:
                fh.write(resp.read())

        return local_path, {"ok": True, "file": file_info, "download_url_used": True}
    except Exception as exc:
        return None, {"ok": False, "description": str(exc)}


def create_adver_backup_record(adver_text, final_text, photo_file_id=None, admin_chat_id=None, admin_username=None, message=None):
    created_dt = get_tehran_now()
    state = load_adver_index()
    ad_id = int(state.get("next_id", 1) or 1)
    state["next_id"] = ad_id + 1

    slug = build_adver_backup_slug(ad_id, created_dt)
    ad_dir = os.path.join(ensure_adver_backup_dir(), slug)
    os.makedirs(ad_dir, exist_ok=True)

    text_backup_path = os.path.join(ad_dir, "message.txt")
    with open(text_backup_path, "w", encoding="utf-8") as fh:
        fh.write(str(final_text or ""))

    raw_text_backup_path = os.path.join(ad_dir, "message.original.txt")
    with open(raw_text_backup_path, "w", encoding="utf-8") as fh:
        fh.write(str(adver_text or ""))

    photo_backup_path = None
    photo_backup_result = None
    if photo_file_id:
        photo_backup_path, photo_backup_result = download_adver_photo_backup(photo_file_id, ad_dir)

    record = {
        "id": ad_id,
        "created_at": _format_access_dt(created_dt),
        "created_by_chat_id": str(admin_chat_id or "").strip(),
        "created_by_username": str(admin_username or get_admin_contact_username()).strip().lstrip("@"),
        "text": str(adver_text or ""),
        "final_text": str(final_text or ""),
        "photo_file_id": str(photo_file_id or "").strip() or None,
        "photo_backup_path": photo_backup_path,
        "text_backup_path": text_backup_path,
        "json_backup_path": os.path.join(ad_dir, "ad.json"),
        "target_count": 0,
        "last_sent_at": None,
        "last_send_result": {},
        "photo_backup_result": photo_backup_result if isinstance(photo_backup_result, dict) else {},
        "schedule": {
            "enabled": False,
            "cadence": "daily",
            "start_time": "",
            "every_hours": 0.0,
            "times_per_day": 0,
            "week_days": [],
            "month_days": [],
            "next_run_at": None,
            "last_run_at": None,
            "runs_today_date": None,
            "runs_today": 0,
        },
    }

    with open(record["json_backup_path"], "w", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False, indent=2)

    state.setdefault("ads", []).append(record)
    save_adver_index(state)
    return record


def update_adver_record(record):
    if not isinstance(record, dict):
        return False

    ad_id = int(record.get("id", 0) or 0)
    if ad_id <= 0:
        return False

    state = load_adver_index()
    updated = False
    for idx, existing in enumerate(state.get("ads", [])):
        if int(existing.get("id", 0) or 0) == ad_id:
            state["ads"][idx] = record
            updated = True
            break

    if not updated:
        state.setdefault("ads", []).append(record)

    json_backup_path = record.get("json_backup_path")
    if json_backup_path:
        try:
            os.makedirs(os.path.dirname(json_backup_path), exist_ok=True)
            with open(json_backup_path, "w", encoding="utf-8") as fh:
                json.dump(record, fh, ensure_ascii=False, indent=2)
        except Exception as exc:
            print(f"{COL_WARN}[ADVER]{RESET} Failed to update backup json for ad {ad_id}: {exc}")

    save_adver_index(state)
    return True


def get_adver_record_by_number(number):
    try:
        desired = int(str(number).strip())
    except Exception:
        return None

    if desired <= 0:
        return None

    ads = load_adver_index().get("ads", [])
    ads.sort(key=lambda item: int(item.get("id", 0) or 0))
    if desired <= len(ads):
        return ads[desired - 1]

    for ad in ads:
        if int(ad.get("id", 0) or 0) == desired:
            return ad
    return None


def get_all_known_adver_target_chat_ids(state=None):
    if state is None:
        state = load_subscriber_state()

    excluded_chat_ids = set(BALE_CHAT_ID_SET)
    target_chat_ids = []
    seen = set()

    def add_chat_id(value):
        chat_id = str(value or "").strip()
        if not chat_id or chat_id in excluded_chat_ids or chat_id in seen:
            return
        if chat_id.lower() in {"system", LOCAL_WEB_DEFAULT_CHAT_ID.lower()}:
            return
        target_chat_ids.append(chat_id)
        seen.add(chat_id)

    users = state.get("users") if isinstance(state, dict) else {}
    if isinstance(users, dict):
        for raw_chat_id, user in users.items():
            if isinstance(user, dict):
                add_chat_id(user.get("chat_id") or raw_chat_id)
            else:
                add_chat_id(raw_chat_id)

    try:
        local_state = load_local_chat_state()
        messages = local_state.get("messages", []) if isinstance(local_state, dict) else []
        if isinstance(messages, list):
            for message in messages:
                if not isinstance(message, dict):
                    continue
                add_chat_id(message.get("chat_id"))
                meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
                add_chat_id(meta.get("original_chat_id"))
    except Exception as exc:
        print(f"{COL_WARN}[ADVER]{RESET} Failed to read local chat ids: {exc}")

    return target_chat_ids



ADVER_WEEKDAY_LABELS = {
    1: "شنبه",
    2: "یکشنبه",
    3: "دوشنبه",
    4: "سه‌شنبه",
    5: "چهارشنبه",
    6: "پنجشنبه",
    7: "جمعه",
}

ADVER_WEEKDAY_NAME_TO_NUMBER = {
    "sat": 1,
    "saturday": 1,
    "شنبه": 1,
    "sun": 2,
    "sunday": 2,
    "یکشنبه": 2,
    "یک‌شنبه": 2,
    "mon": 3,
    "monday": 3,
    "دوشنبه": 3,
    "دو‌شنبه": 3,
    "tue": 4,
    "tues": 4,
    "tuesday": 4,
    "سه‌شنبه": 4,
    "سهشنبه": 4,
    "wed": 5,
    "wednesday": 5,
    "چهارشنبه": 5,
    "چهار‌شنبه": 5,
    "thu": 6,
    "thur": 6,
    "thurs": 6,
    "thursday": 6,
    "پنجشنبه": 6,
    "پنج‌شنبه": 6,
    "fri": 7,
    "friday": 7,
    "جمعه": 7,
}


def normalize_adver_week_days(value):
    if value is None:
        return []

    if isinstance(value, str):
        parts = re.split(r"[,\s،]+", value.strip())
    elif isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        parts = [value]

    cleaned = []
    for item in parts:
        if item is None:
            continue
        raw = normalize_command_input(str(item).strip()).lower()
        if not raw:
            continue
        try:
            day_num = int(raw)
        except Exception:
            day_num = ADVER_WEEKDAY_NAME_TO_NUMBER.get(raw)
        if day_num is None or day_num < 1 or day_num > 7:
            continue
        if day_num not in cleaned:
            cleaned.append(day_num)

    cleaned.sort()
    return cleaned


def normalize_adver_month_days(value):
    if value is None:
        return []

    if isinstance(value, str):
        parts = re.split(r"[,\s،]+", value.strip())
    elif isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        parts = [value]

    cleaned = []
    for item in parts:
        try:
            day_num = int(str(item).strip())
        except Exception:
            continue
        if 1 <= day_num <= 31 and day_num not in cleaned:
            cleaned.append(day_num)

    cleaned.sort()
    return cleaned


def get_adver_week_day_number(dt):
    """Return Iranian week day number: 1=Saturday ... 7=Friday."""
    return ((dt.weekday() + 2) % 7) + 1


def get_adver_schedule_day_allowed(dt, cadence="daily", week_days=None, month_days=None):
    cadence = str(cadence or "daily").strip().lower()
    if cadence == "weekly":
        allowed_week_days = normalize_adver_week_days(week_days)
        return bool(allowed_week_days) and get_adver_week_day_number(dt) in allowed_week_days
    if cadence == "monthly":
        allowed_month_days = normalize_adver_month_days(month_days)
        return bool(allowed_month_days) and dt.day in allowed_month_days
    return True


def format_adver_week_days(week_days):
    values = normalize_adver_week_days(week_days)
    if not values:
        return "-"
    return "، ".join(f"{day}:{ADVER_WEEKDAY_LABELS.get(day, day)}" for day in values)


def format_adver_month_days(month_days):
    values = normalize_adver_month_days(month_days)
    if not values:
        return "-"
    return "، ".join(str(day) for day in values)


def describe_adver_schedule(schedule):
    if not isinstance(schedule, dict) or not schedule.get("enabled"):
        return "بدون زمان‌بندی"

    cadence = str(schedule.get("cadence") or "daily").strip().lower()
    start_time = schedule.get("start_time") or "-"
    times_per_day = int(schedule.get("times_per_day", 1) or 1)
    next_run = schedule.get("next_run_at") or "-"

    if cadence == "weekly":
        base = f"هفتگی | روزها: {format_adver_week_days(schedule.get('week_days'))}"
    elif cadence == "monthly":
        base = f"ماهانه | روزهای ماه: {format_adver_month_days(schedule.get('month_days'))}"
    else:
        base = "روزانه"

    return f"{base} | شروع: {start_time} | {times_per_day} بار در روز | اجرای بعدی: {next_run}"


def parse_adver_number_list(text):
    values = []
    for part in re.split(r"[,،\s]+", str(text or "").strip()):
        if not part:
            continue
        try:
            values.append(int(part))
        except Exception:
            continue
    return values


def extract_adver_times_per_day(raw, hour, minute):
    times_match = re.search(r"(\d+)\s*(?:times|بار)", raw)
    if times_match:
        return int(times_match.group(1))

    raw_without_time = re.sub(r"\d{1,2}\s*:\s*\d{2}", " ", raw)
    raw_without_lists = re.sub(r"(?:every|هر)\s+(?:month|ماه)\s+[0-9,\s،]+", " ", raw_without_time)
    raw_without_lists = re.sub(r"(?:every|هر)\s+[0-9,\s،]+", " ", raw_without_lists)
    numbers = [int(item) for item in re.findall(r"\b\d+\b", raw_without_lists)]
    for value in numbers:
        if value not in {hour, minute}:
            return value
    return 1


def parse_adver_schedule_text(text, now_dt=None):
    raw = normalize_command_input(str(text or "").strip()).lower()
    if not raw:
        return None

    if raw in {"now", "send", "ارسال", "الان", "همین الان"}:
        return {"mode": "now"}

    if raw in {"/cancel", "cancel", "لغو", "/لغو"}:
        return {"mode": "cancel"}

    if now_dt is None:
        now_dt = get_tehran_now()

    time_match = re.search(r"(\d{1,2})\s*:\s*(\d{2})", raw)
    if not time_match:
        return None

    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None

    times_per_day = max(1, min(24, int(extract_adver_times_per_day(raw, hour, minute) or 1)))
    every_hours = max(0.25, min(24.0, 24.0 / float(times_per_day)))
    cadence = "daily"
    week_days = []
    month_days = []

    monthly_match = re.search(r"(?:every|هر)\s+(?:month|ماه|monthly|ماهیانه|ماهانه)\s+([0-9,\s،]+)", raw)
    if monthly_match:
        month_days = normalize_adver_month_days(monthly_match.group(1))
        if not month_days:
            return None
        cadence = "monthly"
    else:
        weekday_values = []
        every_match = re.search(r"(?:every|هر)\s+([a-zآ-ی0-9,\s،‌\-]+?)(?=\s+(?:at|در|ساعت)\s+\d{1,2}\s*:\s*\d{2}|\s+\d{1,2}\s*:\s*\d{2}|$)", raw)
        if every_match and "hour" not in every_match.group(1) and "ساعت" not in every_match.group(1):
            candidate = every_match.group(1).strip()
            numeric_candidate = parse_adver_number_list(candidate)
            if numeric_candidate and all(1 <= value <= 7 for value in numeric_candidate):
                weekday_values = numeric_candidate
            else:
                for token in re.split(r"[,،\s]+", candidate):
                    token = token.strip()
                    if not token:
                        continue
                    day_num = ADVER_WEEKDAY_NAME_TO_NUMBER.get(token)
                    if day_num is not None:
                        weekday_values.append(day_num)
            week_days = normalize_adver_week_days(weekday_values)
            if week_days:
                cadence = "weekly"

    start_time = f"{hour:02d}:{minute:02d}"
    next_run_at = compute_next_adver_run_at(
        start_time,
        every_hours,
        now_dt=now_dt,
        cadence=cadence,
        week_days=week_days,
        month_days=month_days,
        times_per_day=times_per_day,
    )

    return {
        "mode": "schedule",
        "cadence": cadence,
        "start_time": start_time,
        "every_hours": every_hours,
        "times_per_day": times_per_day,
        "week_days": week_days,
        "month_days": month_days,
        "next_run_at": _format_access_dt(next_run_at),
    }


def compute_next_adver_run_at(
    start_time,
    every_hours,
    now_dt=None,
    after_dt=None,
    cadence="daily",
    week_days=None,
    month_days=None,
    times_per_day=1,
):
    if now_dt is None:
        now_dt = get_tehran_now()
    if after_dt is None:
        after_dt = now_dt

    try:
        hour, minute = [int(part) for part in str(start_time or "00:00").split(":", 1)]
    except Exception:
        hour, minute = 0, 0

    cadence = str(cadence or "daily").strip().lower()
    if cadence not in {"daily", "weekly", "monthly"}:
        cadence = "daily"

    times_per_day = max(1, min(24, int(times_per_day or 1)))
    interval = timedelta(hours=max(0.25, float(every_hours or (24.0 / times_per_day))))

    search_start = after_dt - timedelta(days=1)
    for day_offset in range(0, 400):
        day = (search_start + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        if day.date() < after_dt.date():
            continue
        if not get_adver_schedule_day_allowed(day, cadence=cadence, week_days=week_days, month_days=month_days):
            continue

        base = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
        for slot_index in range(times_per_day):
            candidate = base + (interval * slot_index)
            if candidate.date() != day.date():
                break
            if candidate > after_dt:
                return candidate

    return after_dt + timedelta(days=1)
def configure_adver_schedule(record, schedule_info):
    if not isinstance(record, dict) or not isinstance(schedule_info, dict):
        return record

    schedule = record.setdefault("schedule", {})
    cadence = str(schedule_info.get("cadence") or "daily").strip().lower()
    if cadence not in {"daily", "weekly", "monthly"}:
        cadence = "daily"

    schedule.update({
        "enabled": True,
        "cadence": cadence,
        "start_time": schedule_info.get("start_time"),
        "every_hours": float(schedule_info.get("every_hours") or 24.0),
        "times_per_day": int(schedule_info.get("times_per_day") or 1),
        "week_days": normalize_adver_week_days(schedule_info.get("week_days")),
        "month_days": normalize_adver_month_days(schedule_info.get("month_days")),
        "next_run_at": schedule_info.get("next_run_at"),
        "last_run_at": None,
        "runs_today_date": None,
        "runs_today": 0,
    })
    update_adver_record(record)
    return record



def mark_adver_send_result(record, result, sent_dt=None):
    if sent_dt is None:
        sent_dt = get_tehran_now()
    if not isinstance(record, dict):
        return

    record["target_count"] = int(result.get("total", 0) or 0) if isinstance(result, dict) else 0
    record["last_sent_at"] = _format_access_dt(sent_dt)
    record["last_send_result"] = result if isinstance(result, dict) else {}
    update_adver_record(record)


def send_adver_record_to_targets(record, target_chat_ids=None, message_kind="admin_adver_broadcast"):
    if not isinstance(record, dict):
        return {"total": 0, "success": 0, "failed": 0, "success_chat_ids": [], "failed_chat_ids": []}

    if target_chat_ids is None:
        target_chat_ids = get_all_known_adver_target_chat_ids(load_subscriber_state())

    result = send_bale_adver_to_many_sync(
        target_chat_ids,
        record.get("final_text") or record.get("text") or "",
        photo_file_id=record.get("photo_file_id"),
        message_kind=message_kind,
    )
    mark_adver_send_result(record, result)
    return result


def should_run_adver_schedule(record, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    if not isinstance(record, dict):
        return False

    schedule = record.get("schedule") if isinstance(record.get("schedule"), dict) else {}
    if not schedule.get("enabled"):
        return False

    next_run_at = _parse_access_dt(schedule.get("next_run_at"))
    if next_run_at is None or next_run_at > now_dt:
        return False

    today_key = now_dt.strftime("%Y-%m-%d")
    if schedule.get("runs_today_date") != today_key:
        schedule["runs_today_date"] = today_key
        schedule["runs_today"] = 0

    try:
        runs_today = int(schedule.get("runs_today", 0) or 0)
        times_per_day = int(schedule.get("times_per_day", 1) or 1)
    except Exception:
        runs_today, times_per_day = 0, 1

    return runs_today < max(1, times_per_day)


def advance_adver_schedule(record, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    if not isinstance(record, dict):
        return record

    schedule = record.get("schedule") if isinstance(record.get("schedule"), dict) else {}
    today_key = now_dt.strftime("%Y-%m-%d")
    if schedule.get("runs_today_date") != today_key:
        schedule["runs_today_date"] = today_key
        schedule["runs_today"] = 0

    schedule["runs_today"] = int(schedule.get("runs_today", 0) or 0) + 1
    schedule["last_run_at"] = _format_access_dt(now_dt)

    times_per_day = max(1, int(schedule.get("times_per_day", 1) or 1))
    every_hours = max(0.25, float(schedule.get("every_hours", 24.0) or 24.0))
    start_time = schedule.get("start_time") or now_dt.strftime("%H:%M")

    next_run = compute_next_adver_run_at(
        start_time,
        every_hours,
        now_dt=now_dt,
        after_dt=now_dt,
        cadence=schedule.get("cadence") or "daily",
        week_days=schedule.get("week_days"),
        month_days=schedule.get("month_days"),
        times_per_day=times_per_day,
    )

    schedule["next_run_at"] = _format_access_dt(next_run)
    record["schedule"] = schedule
    update_adver_record(record)
    return record



def adver_scheduler_loop():
    while True:
        try:
            now_dt = get_tehran_now()
            state = load_adver_index()
            ads = state.get("ads", []) if isinstance(state, dict) else []
            for record in ads:
                if not should_run_adver_schedule(record, now_dt=now_dt):
                    continue
                result = send_adver_record_to_targets(record, message_kind="admin_adver_scheduled")
                advance_adver_schedule(record, now_dt=now_dt)
                print(f"{COL_INFO}[ADVER]{RESET} Scheduled ad {record.get('id')} sent: {result}")
        except Exception as exc:
            print(f"{COL_WARN}[ADVER]{RESET} Scheduler error: {exc}")
            traceback.print_exc()
        time.sleep(ADVER_SCHEDULER_CHECK_SECONDS)


def ensure_adver_scheduler_started():
    global ADVER_SCHEDULER_THREAD_STARTED
    if ADVER_SCHEDULER_THREAD_STARTED:
        return
    with ADVER_SCHEDULER_LOCK:
        if ADVER_SCHEDULER_THREAD_STARTED:
            return
        worker = threading.Thread(target=adver_scheduler_loop, daemon=True)
        worker.start()
        ADVER_SCHEDULER_THREAD_STARTED = True


def build_adver_schedule_prompt(record):
    return (
        f"تبلیغ شماره {record.get('id')} ذخیره شد.\n\n"
        "حالت ارسال را انتخاب کنید:\n"
        "• الان\n"
        "• schedule 09:00 3  ← روزانه از 09:00، سه بار در روز\n"
        "• every 1,2 at 09:00 2 times  ← هفتگی؛ 1=شنبه تا 7=جمعه\n"
        "• every wednesday at 09:00  ← هفتگی با نام روز\n"
        "• every month 1,15 at 09:00  ← ماهانه در روزهای 1 و 15\n\n"
        "برای ویرایش زمان‌بندی تبلیغ‌های قبلی: /adedit\n"
        "برای لغو: /cancel"
    )



def build_adver_list_message():
    ads = load_adver_index().get("ads", [])
    ads.sort(key=lambda item: int(item.get("id", 0) or 0))
    if not ads:
        return "هنوز هیچ تبلیغی ذخیره نشده است."

    lines = ["فهرست تبلیغ‌های ذخیره‌شده:", ""]
    for idx, ad in enumerate(ads, 1):
        schedule = ad.get("schedule") if isinstance(ad.get("schedule"), dict) else {}
        status = describe_adver_schedule(schedule)
        text_preview = re.sub(r"\s+", " ", str(ad.get("text") or ad.get("final_text") or "")).strip()
        if len(text_preview) > 70:
            text_preview = text_preview[:67] + "..."
        lines.append(
            f"{idx}. #{ad.get('id')} | {ad.get('created_at') or '-'} | "
            f"{'عکس‌دار' if ad.get('photo_file_id') else 'متنی'} | {status}\n"
            f"   {text_preview or '-'}"
        )

    lines.append("\nبرای پیش‌نمایش، عدد تبلیغ را بفرستید.")
    return "\n".join(lines).strip()


def send_adver_preview_to_admin(chat_id, record):
    if not isinstance(record, dict):
        send_bale_message(chat_id, "تبلیغ پیدا نشد.", message_kind="admin_adlist_missing")
        return False

    caption = (
        f"پیش‌نمایش تبلیغ #{record.get('id')}\n"
        f"ساخته‌شده: {record.get('created_at') or '-'}\n\n"
        f"{record.get('final_text') or record.get('text') or ''}"
    )

    if record.get("photo_file_id"):
        sent_ok, result = send_bale_photo_by_file_id_sync(
            chat_id,
            record.get("photo_file_id"),
            caption=caption,
            message_kind="admin_adlist_preview",
            extra_meta={"admin_adlist_preview": True, "ad_id": record.get("id")},
        )
        if sent_ok:
            return True
        send_bale_message(chat_id, f"ارسال عکس پیش‌نمایش ناموفق بود، متن ارسال می‌شود.\n{result}", message_kind="admin_adlist_photo_failed")

    send_bale_message(chat_id, caption, message_kind="admin_adlist_preview")
    return True


def start_admin_adlist_session(chat_id):
    normalized_chat_id = str(chat_id or "").strip()
    if not normalized_chat_id:
        return False
    ADMIN_ADLIST_SESSIONS[normalized_chat_id] = {
        "started_at": _format_access_dt(get_tehran_now()),
    }
    send_bale_message(normalized_chat_id, build_adver_list_message(), message_kind="admin_adlist")
    return True


def is_admin_adlist_session_active(chat_id):
    return str(chat_id or "").strip() in ADMIN_ADLIST_SESSIONS


def clear_admin_adlist_session(chat_id):
    ADMIN_ADLIST_SESSIONS.pop(str(chat_id or "").strip(), None)


def handle_admin_adlist_selection(chat_id, text):
    stripped = str(text or "").strip()
    if is_admin_adver_cancel_text(stripped):
        clear_admin_adlist_session(chat_id)
        send_bale_message(chat_id, "نمایش فهرست تبلیغ‌ها بسته شد.", message_kind="admin_adlist_cancelled")
        return True

    if not re.fullmatch(r"\d+", stripped):
        send_bale_message(chat_id, "برای پیش‌نمایش، فقط عدد تبلیغ را بفرستید. برای لغو: /cancel", message_kind="admin_adlist_invalid")
        return True

    record = get_adver_record_by_number(stripped)
    send_adver_preview_to_admin(chat_id, record)
    clear_admin_adlist_session(chat_id)
    return True





def start_admin_adschedule_session(chat_id, ad_number=None):
    normalized_chat_id = str(chat_id or "").strip()
    if not normalized_chat_id:
        return False

    if ad_number:
        record = get_adver_record_by_number(ad_number)
        if not isinstance(record, dict):
            send_bale_message(normalized_chat_id, "تبلیغ پیدا نشد. /adedit را اجرا کنید و شماره درست را بفرستید.", message_kind="admin_adschedule_missing")
            return True
        ADMIN_ADSCHEDULE_SESSIONS[normalized_chat_id] = {
            "stage": "awaiting_schedule",
            "ad_id": record.get("id"),
            "started_at": _format_access_dt(get_tehran_now()),
        }
        send_bale_message(
            normalized_chat_id,
            (
                f"ویرایش زمان‌بندی تبلیغ #{record.get('id')}\n"
                f"وضعیت فعلی: {describe_adver_schedule(record.get('schedule'))}\n\n"
                "فرمت جدید را بفرستید:\n"
                "• الان\n"
                "• schedule 09:00 3\n"
                "• every 1,2 at 09:00 2 times\n"
                "• every month 1,15 at 09:00\n\n"
                "برای لغو: /cancel"
            ),
            message_kind="admin_adschedule_prompt",
        )
        return True

    ADMIN_ADSCHEDULE_SESSIONS[normalized_chat_id] = {
        "stage": "awaiting_ad_number",
        "started_at": _format_access_dt(get_tehran_now()),
    }
    send_bale_message(
        normalized_chat_id,
        build_adver_list_message() + "\n\nبرای ویرایش زمان‌بندی، شماره تبلیغ را بفرستید.",
        message_kind="admin_adschedule_list",
    )
    return True


def is_admin_adschedule_session_active(chat_id):
    return str(chat_id or "").strip() in ADMIN_ADSCHEDULE_SESSIONS


def clear_admin_adschedule_session(chat_id):
    ADMIN_ADSCHEDULE_SESSIONS.pop(str(chat_id or "").strip(), None)


def handle_admin_adschedule_input(chat_id, text):
    normalized_chat_id = str(chat_id or "").strip()
    stripped = str(text or "").strip()
    session = ADMIN_ADSCHEDULE_SESSIONS.get(normalized_chat_id)
    if not isinstance(session, dict):
        return False

    if is_admin_adver_cancel_text(stripped):
        clear_admin_adschedule_session(normalized_chat_id)
        send_bale_message(normalized_chat_id, "ویرایش زمان‌بندی تبلیغ لغو شد.", message_kind="admin_adschedule_cancelled")
        return True

    if session.get("stage") == "awaiting_ad_number":
        if not re.fullmatch(r"\d+", stripped):
            send_bale_message(normalized_chat_id, "فقط شماره تبلیغ را بفرستید. برای لغو: /cancel", message_kind="admin_adschedule_invalid_number")
            return True
        record = get_adver_record_by_number(stripped)
        if not isinstance(record, dict):
            send_bale_message(normalized_chat_id, "تبلیغ پیدا نشد. شماره دیگری بفرستید یا /cancel.", message_kind="admin_adschedule_missing")
            return True
        session["stage"] = "awaiting_schedule"
        session["ad_id"] = record.get("id")
        ADMIN_ADSCHEDULE_SESSIONS[normalized_chat_id] = session
        send_bale_message(
            normalized_chat_id,
            (
                f"ویرایش زمان‌بندی تبلیغ #{record.get('id')}\n"
                f"وضعیت فعلی: {describe_adver_schedule(record.get('schedule'))}\n\n"
                "فرمت جدید را بفرستید:\n"
                "• الان\n"
                "• schedule 09:00 3\n"
                "• every 1,2 at 09:00 2 times\n"
                "• every month 1,15 at 09:00\n\n"
                "برای لغو: /cancel"
            ),
            message_kind="admin_adschedule_prompt",
        )
        return True

    schedule_info = parse_adver_schedule_text(stripped)
    if not schedule_info:
        send_bale_message(
            normalized_chat_id,
            "فرمت زمان‌بندی معتبر نیست. نمونه: الان یا every 1,2 at 09:00 2 times یا every month 1,15 at 09:00",
            message_kind="admin_adschedule_invalid",
        )
        return True

    if schedule_info.get("mode") == "cancel":
        clear_admin_adschedule_session(normalized_chat_id)
        send_bale_message(normalized_chat_id, "ویرایش زمان‌بندی تبلیغ لغو شد.", message_kind="admin_adschedule_cancelled")
        return True

    record = get_adver_record_by_number(session.get("ad_id"))
    if not isinstance(record, dict):
        clear_admin_adschedule_session(normalized_chat_id)
        send_bale_message(normalized_chat_id, "تبلیغ ذخیره‌شده پیدا نشد.", message_kind="admin_adschedule_missing_record")
        return True

    if schedule_info.get("mode") == "now":
        target_chat_ids = get_admin_adver_target_chat_ids(load_subscriber_state())
        record["target_count"] = len(target_chat_ids)
        update_adver_record(record)
        clear_admin_adschedule_session(normalized_chat_id)

        if not target_chat_ids:
            send_bale_message(
                normalized_chat_id,
                "هیچ کاربری برای ارسال پیام عمومی پیدا نشد.",
                message_kind="admin_adschedule_no_targets",
            )
            return True

        queued_count, _ = send_bale_adver_to_many(
            target_chat_ids,
            record.get("final_text") or record.get("text") or "",
            photo_file_id=record.get("photo_file_id"),
            message_kind="admin_adschedule_broadcast_now",
        )
        send_bale_message(
            normalized_chat_id,
            (
                "تبلیغ انتخاب‌شده همین الان در صف ارسال قرار گرفت.\n"
                f"شماره تبلیغ: {record.get('id')}\n"
                f"تعداد مقصدها: {queued_count}\n"
                f"عکس: {'دارد' if record.get('photo_file_id') else 'ندارد'}"
            ),
            message_kind="admin_adschedule_queued_now",
        )
        return True

    configure_adver_schedule(record, schedule_info)
    ensure_adver_scheduler_started()
    clear_admin_adschedule_session(normalized_chat_id)
    send_bale_message(
        normalized_chat_id,
        (
            "زمان‌بندی تبلیغ ویرایش شد.\n"
            f"شماره تبلیغ: {record.get('id')}\n"
            f"{describe_adver_schedule(record.get('schedule'))}"
        ),
        message_kind="admin_adschedule_saved",
    )
    return True


def get_admin_adver_target_chat_ids(state=None):
    """Return literally all known Bale users, paid or free, excluding fixed script chat IDs."""
    return get_all_known_adver_target_chat_ids(state=state)


def send_bale_photo_by_file_id_sync(chat_id, photo_file_id, caption=None, message_kind="photo_outgoing", extra_meta=None):
    normalized_photo_file_id = str(photo_file_id or "").strip()
    if not normalized_photo_file_id:
        return False, {"ok": False, "description": "missing photo file_id"}

    payload = {
        "photo": normalized_photo_file_id,
        "caption": str(caption or ""),
    }
    return send_bale_payload_to_url(
        chat_id,
        BALE_SEND_PHOTO_URL,
        payload,
        local_text=("[عکس]\n" + str(caption or "").strip()).strip(),
        message_kind=message_kind,
        extra_meta=extra_meta,
    )


def send_bale_adver_to_many_sync(chat_ids, message_text, photo_file_id=None, message_kind="admin_adver_broadcast"):
    normalized_chat_ids = []
    seen = set()

    for chat_id in chat_ids:
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id or normalized_chat_id in seen:
            continue
        normalized_chat_ids.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    success_chat_ids = []
    failed_chat_ids = []

    for index, target_chat_id in enumerate(normalized_chat_ids):
        if photo_file_id:
            sent_ok, result = send_bale_photo_by_file_id_sync(
                target_chat_id,
                photo_file_id,
                caption=message_text,
                message_kind=message_kind,
                extra_meta={
                    "admin_adver": True,
                    "has_photo": True,
                },
            )
        else:
            result = send_bale_message_raw(target_chat_id, message_text)
            sent_ok = bool(result and result.get("ok"))

            message_meta = {
                "message_kind": message_kind,
                "bale_ok": sent_ok,
                "fallback_used": not sent_ok,
                "original_chat_id": target_chat_id,
                "result": result.get("result") if isinstance(result, dict) else result,
                "admin_adver": True,
                "has_photo": False,
            }
            message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(target_chat_id, message_meta)
            append_local_chat_message(
                chat_id=target_chat_id,
                text=message_text,
                direction="outgoing",
                transport="bale" if sent_ok else "local_fallback",
                source="bot",
                meta=message_meta,
            )
            if should_mirror_to_local_web:
                append_local_web_reply_mirror(
                    original_chat_id=target_chat_id,
                    text=message_text,
                    direction="outgoing",
                    transport="bale" if sent_ok else "local_fallback",
                    source="bot",
                    meta=message_meta,
                )

        if sent_ok:
            success_chat_ids.append(target_chat_id)
        else:
            failed_chat_ids.append(target_chat_id)
            print(f"{COL_WARN}[ADVER]{RESET} Failed sending adver to {target_chat_id}: {result}")

        if index < len(normalized_chat_ids) - 1 and ADVER_SEND_THROTTLE_SECONDS > 0:
            time.sleep(ADVER_SEND_THROTTLE_SECONDS)

    log_bale_delivery_summary(
        len(success_chat_ids),
        len(normalized_chat_ids),
        failed_count=len(failed_chat_ids),
        local_web_backup=False,
        label="[ADVER]",
    )

    return {
        "total": len(normalized_chat_ids),
        "success": len(success_chat_ids),
        "failed": len(failed_chat_ids),
        "success_chat_ids": success_chat_ids,
        "failed_chat_ids": failed_chat_ids,
    }


def send_bale_adver_to_many(chat_ids, message_text, photo_file_id=None, message_kind="admin_adver_broadcast"):
    normalized_chat_ids = []
    seen = set()

    for chat_id in chat_ids:
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id or normalized_chat_id in seen:
            continue
        normalized_chat_ids.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    ensure_bale_outgoing_worker_started()
    BALE_OUTGOING_QUEUE.put({
        "kind": "multi_adver",
        "chat_ids": normalized_chat_ids,
        "message_text": message_text,
        "photo_file_id": str(photo_file_id or "").strip() or None,
        "message_kind": message_kind,
    })
    return len(normalized_chat_ids), normalized_chat_ids


def handle_admin_adver_content(chat_id, message=None, fallback_text=None):
    normalized_chat_id = str(chat_id or "").strip()
    session = ADMIN_ADVER_SESSIONS.get(normalized_chat_id)
    if not isinstance(session, dict):
        session = {"stage": "awaiting_content"}
        ADMIN_ADVER_SESSIONS[normalized_chat_id] = session

    incoming_text = fallback_text if isinstance(fallback_text, str) else ""

    if is_admin_adver_cancel_text(incoming_text):
        clear_admin_adver_session(normalized_chat_id)
        send_bale_message(normalized_chat_id, "ارسال پیام عمومی لغو شد.", message_kind="admin_adver_cancelled")
        return True

    if session.get("stage") == "awaiting_schedule":
        schedule_info = parse_adver_schedule_text(incoming_text)
        if not schedule_info:
            send_bale_message(
                normalized_chat_id,
                "فرمت زمان‌بندی معتبر نیست.\n"
                "نمونه‌ها:\n"
                "الان\n"
                "schedule 09:00 3\n"
                "every 1,2 at 09:00 2 times\n"
                "every month 1,15 at 09:00\n\n"
                "برای لغو: /cancel",
                message_kind="admin_adver_schedule_invalid",
            )
            return True

        ad_id = session.get("pending_ad_id")
        record = get_adver_record_by_number(ad_id)
        if not isinstance(record, dict):
            clear_admin_adver_session(normalized_chat_id)
            send_bale_message(normalized_chat_id, "تبلیغ ذخیره‌شده پیدا نشد. دوباره /adver را اجرا کنید.", message_kind="admin_adver_missing_record")
            return True

        if schedule_info.get("mode") == "cancel":
            clear_admin_adver_session(normalized_chat_id)
            send_bale_message(normalized_chat_id, "ارسال پیام عمومی لغو شد.", message_kind="admin_adver_cancelled")
            return True

        target_chat_ids = get_admin_adver_target_chat_ids(load_subscriber_state())
        record["target_count"] = len(target_chat_ids)
        update_adver_record(record)
        clear_admin_adver_session(normalized_chat_id)

        if not target_chat_ids:
            send_bale_message(
                normalized_chat_id,
                "هیچ کاربری برای ارسال پیام عمومی پیدا نشد.",
                message_kind="admin_adver_no_targets",
            )
            return True

        if schedule_info.get("mode") == "now":
            queued_count, _ = send_bale_adver_to_many(
                target_chat_ids,
                record.get("final_text") or record.get("text") or "",
                photo_file_id=record.get("photo_file_id"),
                message_kind="admin_adver_broadcast",
            )
            send_bale_message(
                normalized_chat_id,
                (
                    "پیام عمومی در صف ارسال قرار گرفت.\n"
                    f"شماره تبلیغ: {record.get('id')}\n"
                    f"تعداد مقصدها: {queued_count}\n"
                    f"عکس: {'دارد' if record.get('photo_file_id') else 'ندارد'}\n"
                    f"بکاپ: {record.get('json_backup_path') or '-'}"
                ),
                message_kind="admin_adver_queued",
            )
            return True

        configure_adver_schedule(record, schedule_info)
        ensure_adver_scheduler_started()
        send_bale_message(
            normalized_chat_id,
            (
                "زمان‌بندی تبلیغ ذخیره شد.\n"
                f"شماره تبلیغ: {record.get('id')}\n"
                f"{describe_adver_schedule(record.get('schedule'))}\n"
                f"تعداد مقصدها: {len(target_chat_ids)}"
            ),
            message_kind="admin_adver_scheduled",
        )
        return True

    photo_file_id = get_adver_photo_file_id(message)
    adver_text = extract_admin_adver_text(message, fallback_text=fallback_text)

    if not adver_text:
        send_bale_message(
            normalized_chat_id,
            "متن پیام خالی است. لطفاً متن تبلیغ را بفرستید یا عکس را همراه کپشن ارسال کنید.\nبرای لغو: /cancel",
            message_kind="admin_adver_missing_text",
        )
        return True

    admin_username = get_admin_username_from_message(message)
    final_text = append_admin_adver_footer(adver_text, admin_username=admin_username)
    record = create_adver_backup_record(
        adver_text,
        final_text,
        photo_file_id=photo_file_id,
        admin_chat_id=normalized_chat_id,
        admin_username=admin_username,
        message=message,
    )

    session["stage"] = "awaiting_schedule"
    session["pending_ad_id"] = record.get("id")
    ADMIN_ADVER_SESSIONS[normalized_chat_id] = session

    send_bale_message(
        normalized_chat_id,
        build_adver_schedule_prompt(record),
        message_kind="admin_adver_schedule_prompt",
    )
    return True


def handle_admin_command(chat_id, text, message=None):
    stripped = (text or "").strip()
    low = normalize_command_input(stripped).lower()
    state = load_subscriber_state()
    touch_user_interaction(state, chat_id, message=message, increment=True)
    save_subscriber_state(state)

    if is_admin_adver_session_active(chat_id):
        return handle_admin_adver_content(chat_id, message=message, fallback_text=stripped)

    if is_admin_adlist_session_active(chat_id):
        return handle_admin_adlist_selection(chat_id, stripped)

    if is_admin_adschedule_session_active(chat_id):
        return handle_admin_adschedule_input(chat_id, stripped)

    adedit_match = re.match(r"^/(?:adedit|adschedule|adset)\s+(\d+)\s*$|^(?:adedit|adschedule|adset)\s+(\d+)\s*$", low)
    if adedit_match:
        clear_admin_adver_session(chat_id)
        clear_admin_adlist_session(chat_id)
        ad_number = adedit_match.group(1) or adedit_match.group(2)
        start_admin_adschedule_session(chat_id, ad_number=ad_number)
        return True

    if low in {"adedit", "/adedit", "adschedule", "/adschedule", "adset", "/adset", "ویرایش تبلیغ", "/ویرایش تبلیغ", "زمان‌بندی تبلیغ", "/زمان‌بندی تبلیغ"}:
        clear_admin_adver_session(chat_id)
        clear_admin_adlist_session(chat_id)
        start_admin_adschedule_session(chat_id)
        return True

    if low in {"adver", "/adver", "تبلیغ", "/تبلیغ"}:
        clear_admin_adschedule_session(chat_id)
        clear_admin_adlist_session(chat_id)
        start_admin_adver_session(chat_id)
        return True

    if low in {"adlist", "/adlist", "تبلیغ‌ها", "/تبلیغ‌ها", "تبلیغها", "/تبلیغها"}:
        clear_admin_adschedule_session(chat_id)
        clear_admin_adver_session(chat_id)
        start_admin_adlist_session(chat_id)
        return True

    if low in {"admin", "/admin", "پشتیبانی", "/پشتیبانی"}:
        send_bale_message(
            chat_id,
            (
                "دستورات پشتیبانی:\n"
                "users یا کاربران\n"
                "subscriptions یا subs\n"
                "/adver ساخت تبلیغ متنی/عکس‌دار، بکاپ، ارسال فوری یا زمان‌بندی\n"
                "/adlist فهرست تبلیغ‌های قبلی و پیش‌نمایش با ارسال شماره\n"
                "/adedit ویرایش زمان‌بندی تبلیغ‌های قبلی\n"
                "زمان‌بندی: الان، schedule 09:00 3، every 1,2 at 09:00، every month 1,15 at 09:00"
            ),
            message_kind="admin_help",
        )
        return True

    if low in {"users", "کاربران"}:
        send_bale_message(chat_id, build_admin_users_message(state), message_kind="admin_users")
        return True

    if low in {"subscriptions", "subs", "اشتراک", "اشتراک‌ها", "اشتراکها"}:
        send_bale_message(chat_id, build_admin_subscriptions_message(state), message_kind="admin_subscriptions")
        return True

    match = re.match(
    rf"^(?:{DONE_COMMAND_EN}|{DONE_COMMAND_FA})\s+(\d+)\s*$",
    stripped,
    flags=re.I,
)
    if match:
        ok, response_text = grant_subscription_by_order_id(int(match.group(1)), granted_by_chat_id=chat_id)
        send_bale_message(chat_id, response_text, message_kind="admin_subscribe")
        return True

    return False


def send_custom_bale_payload_sync(chat_id, payload, local_text, message_kind="generic_outgoing", extra_meta=None):
    normalized_chat_id = str(chat_id).strip()
    final_payload = dict(payload or {})
    final_payload["chat_id"] = normalized_chat_id
    result = http_post_json_absolute(BALE_API_URL, final_payload, timeout=15)
    bale_ok = bool(result and result.get("ok"))
    message_text = str(local_text or final_payload.get("text") or "")
    message_meta = {
        "message_kind": str(message_kind or "generic_outgoing"),
        "bale_ok": bale_ok,
        "fallback_used": not bale_ok,
        "original_chat_id": normalized_chat_id,
        "result": result,
        "custom_payload": True,
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(normalized_chat_id, message_meta)
    transport_name = "bale" if bale_ok else "local_fallback"

    append_local_chat_message(
        chat_id=normalized_chat_id,
        text=message_text,
        direction="outgoing",
        transport=transport_name,
        source="bot",
        meta=message_meta,
    )

    if should_mirror_to_local_web:
        append_local_web_reply_mirror(
            original_chat_id=normalized_chat_id,
            text=message_text,
            direction="outgoing",
            transport=transport_name,
            source="bot",
            meta=message_meta,
        )

    return bale_ok


def send_custom_bale_payload(chat_id, payload, local_text, message_kind="generic_outgoing", extra_meta=None):
    ensure_bale_outgoing_worker_started()
    BALE_OUTGOING_QUEUE.put({
        "kind": "custom_payload",
        "chat_id": chat_id,
        "payload": dict(payload or {}),
        "local_text": local_text,
        "message_kind": message_kind,
        "extra_meta": extra_meta if isinstance(extra_meta, dict) else extra_meta,
    })
    return True

def send_contact_request_prompt(chat_id):
    # Legacy fallback: do not ask for a phone number anymore. Start access and
    # show the normal welcome/menu flow immediately.
    state = load_subscriber_state()
    user = get_or_create_user_record(state, chat_id)
    user, access, _granted = ensure_free_trial_started_without_phone(state, chat_id, user=user)
    save_subscriber_state(state)
    send_access_welcome_bundle(
        chat_id,
        intro_text=(
            f"دسترسی شما فعال است.\nباقی‌مانده زمان رایگان: {format_duration_persian(access.get('remaining_scan_seconds') or 0)}"
            if access.get("stage") == "free" else
            "دسترسی شما فعال است."
        ),
    )
    return True


def send_keyboard_remove_message(chat_id, text):
    return send_custom_bale_payload(
        chat_id,
        {
            "text": text,
            "reply_markup": {"remove_keyboard": True},
            "parse_mode": "Markdown",
        },
        local_text=text,
        message_kind="access_granted",
    )


def build_persistent_main_menu_reply_markup():
    return {
        "keyboard": [[{"text": AUTO_TRADING_BUTTON_TEXT}]],
        "resize_keyboard": True,
    }


def send_persistent_main_menu(chat_id):
    return send_custom_bale_payload(
        chat_id,
        {
            "text": AUTO_TRADING_FILES_PROMPT_TEXT,
            "reply_markup": build_persistent_main_menu_reply_markup(),
        },
        local_text=AUTO_TRADING_FILES_PROMPT_TEXT,
        message_kind="auto_trading_menu",
    )


def send_bale_payload_to_url(chat_id, api_url, payload, local_text, message_kind="generic_outgoing", extra_meta=None):
    normalized_chat_id = str(chat_id).strip()
    final_payload = dict(payload or {})
    final_payload["chat_id"] = normalized_chat_id
    result = http_post_json_absolute(api_url, final_payload, timeout=15)
    bale_ok = bool(result and result.get("ok"))
    message_text = str(local_text or final_payload.get("text") or "")
    message_meta = {
        "message_kind": str(message_kind or "generic_outgoing"),
        "bale_ok": bale_ok,
        "fallback_used": not bale_ok,
        "original_chat_id": normalized_chat_id,
        "result": result,
        "custom_payload": True,
        "api_url": api_url,
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(normalized_chat_id, message_meta)
    transport_name = "bale" if bale_ok else "local_fallback"

    append_local_chat_message(
        chat_id=normalized_chat_id,
        text=message_text,
        direction="outgoing",
        transport=transport_name,
        source="bot",
        meta=message_meta,
    )

    if should_mirror_to_local_web:
        append_local_web_reply_mirror(
            original_chat_id=normalized_chat_id,
            text=message_text,
            direction="outgoing",
            transport=transport_name,
            source="bot",
            meta=message_meta,
        )

    return bale_ok, result


def build_activation_invoice_local_text():
    return "درخواست خرید یا تمدید اشتراک ارسال شد."


def send_activation_invoice(chat_id):
    return send_bale_payload_to_url(
        chat_id,
        BALE_SEND_INVOICE_URL,
        {
            "title": ACTIVATION_INVOICE_TITLE,
            "description": ACTIVATION_INVOICE_DESCRIPTION,
            "payload": ACTIVATION_INVOICE_PAYLOAD,
            "provider_token": BALE_WALLET_PROVIDER_TOKEN,
            "prices": [
                {
                    "label": ACTIVATION_INVOICE_TITLE,
                    "amount": ACTIVATION_PRICE_IRR,
                }
            ],
        },
        local_text=build_activation_invoice_local_text(),
        message_kind="activation_invoice"
    )


def answer_bale_pre_checkout_query(pre_checkout_query_id, ok=True, error_message=None):
    payload = {
        "pre_checkout_query_id": str(pre_checkout_query_id),
        "ok": bool(ok),
    }
    if not ok and error_message:
        payload["error_message"] = str(error_message)

    result = http_post_json_absolute(
        BALE_ANSWER_PRECHECKOUT_URL,
        payload,
        timeout=10,
        retries=1,
        backoff_factor=1,
    )
    return bool(result and result.get("ok")), result


def is_activation_pre_checkout_query_valid(pre_checkout_query):
    if not isinstance(pre_checkout_query, dict):
        return False

    payload = str(pre_checkout_query.get("invoice_payload") or "").strip()
    currency = str(pre_checkout_query.get("currency") or "").strip().upper()

    try:
        total_amount = int(pre_checkout_query.get("total_amount", 0) or 0)
    except Exception:
        total_amount = 0

    return (
        payload == ACTIVATION_INVOICE_PAYLOAD
        and currency == "IRR"
        and total_amount == ACTIVATION_PRICE_IRR
    )


def handle_activation_command(chat_id, access=None):
    if access and access.get("stage") == "subscribed" and access.get("active"):
        current_end = access.get("expires_at")
        prefix = (
            f"اشتراک شما هم‌اکنون تا {_format_access_dt(current_end)} فعال است.\n"
            if current_end is not None else
            "اشتراک شما هم‌اکنون فعال است.\n"
        )
    else:
        prefix = ""

    bale_ok, result = send_activation_invoice(chat_id)
    if bale_ok:
        if prefix:
            send_bale_message(
                chat_id,
                prefix + f"برای خرید یا تمدید، صورتحساب {ACTIVATION_PRICE_TOMAN:,} تومان برای شما ارسال شد.",
                message_kind="activation_invoice_notice"
            )
        return True

    send_bale_message(
        chat_id,
        prefix + "ارسال صورتحساب پرداخت ناموفق بود. لطفاً دوباره تلاش کنید.",
        message_kind="activation_invoice_failed",
        extra_meta={"invoice_result": result},
    )
    return True


def handle_successful_payment_message(chat_id, message, successful_payment):
    if not isinstance(successful_payment, dict):
        return False

    invoice_payload = str(successful_payment.get("invoice_payload") or "").strip()
    if invoice_payload != ACTIVATION_INVOICE_PAYLOAD:
        return False

    state = load_subscriber_state()
    user = touch_user_interaction(state, chat_id, message=message, increment=False)
    previous_access = get_user_access_snapshot_from_record(user)
    user, start_dt, end_dt = grant_subscription_to_chat_id(
        chat_id,
        granted_by_chat_id=chat_id,
        days=ACTIVATION_SUBSCRIPTION_DAYS,
        state=state,
        now_dt=get_tehran_now(),
    )
    save_subscriber_state(state)

    append_local_chat_message(
        chat_id=chat_id,
        text="پرداخت اشتراک با موفقیت انجام شد.",
        direction="incoming",
        transport="bale",
        source="bale",
        meta={
            "message_kind": "incoming_bale_successful_payment",
        }
    )

    if previous_access.get("active"):
        send_bale_message(
            chat_id,
            f"✅ پرداخت شما با موفقیت انجام شد.\nاشتراک شما تا {_format_access_dt(end_dt)} تمدید شد.",
            message_kind="activation_payment_success"
        )
    else:
        send_access_welcome_bundle(
            chat_id,
            intro_text=f"✅ پرداخت شما با موفقیت انجام شد.\nاشتراک شما تا {_format_access_dt(end_dt)} فعال شد."
        )

    return True


def get_latest_onboarding_messages():
    messages = []
    pump_snapshot = get_last_bot_message_snapshot("pump_radar") or {}
    strategy_snapshot = get_last_bot_message_snapshot("strategy") or {}

    pump_text = pump_snapshot.get("text")
    strategy_text = strategy_snapshot.get("text")

    if pump_text:
        messages.append(pump_text)
    else:
        messages.append("آخرین پیام رادار پامپ هنوز ثبت نشده است.")

    if strategy_text:
        messages.append(strategy_text)
    else:
        messages.append("آخرین پیام استراتژی هنوز ثبت نشده است.")

    return messages


def send_access_welcome_bundle(chat_id, intro_text=None):
    if intro_text:
        send_keyboard_remove_message(chat_id, intro_text)
    for message_text in get_latest_onboarding_messages():
        send_bale_message(chat_id, message_text, message_kind="access_snapshot")
    send_bale_message(chat_id, build_user_commands_text(), message_kind="access_commands")
    send_persistent_main_menu(chat_id)


def handle_contact_message(chat_id, message, incoming_transport="bale"):
    contact = message.get("contact") or {}
    sender = message.get("from") or {}
    contact_user_id = contact.get("user_id")
    sender_id = sender.get("id")
    if contact_user_id is not None and sender_id is not None and str(contact_user_id) != str(sender_id):
        send_bale_message(chat_id, "لطفاً فقط شماره موبایل خودتان را ارسال کنید.", message_kind="contact_rejected")
        return True

    phone_number = normalize_phone_number(contact.get("phone_number"))
    if not phone_number:
        send_bale_message(chat_id, "شماره موبایل معتبر دریافت نشد.", message_kind="contact_invalid")
        return True

    state = load_subscriber_state()
    update_user_profile_from_message(state, chat_id, message)
    user, access, granted = grant_free_trial_to_user(state, chat_id, phone_number, message=message)
    save_subscriber_state(state)

    if granted:
        send_access_welcome_bundle(chat_id, intro_text=build_access_granted_message(user, access))
    else:
        access = get_user_access_snapshot_from_record(user)
        if access.get("active"):
            send_access_welcome_bundle(chat_id, intro_text=(f"دسترسی شما فعال است.\nباقی‌مانده زمان رایگان: {format_duration_persian(access.get('remaining_scan_seconds') or 0)}" if access.get("stage") == "free" else "دسترسی شما فعال است."))
        else:
            send_bale_message(chat_id, build_locked_access_message(access), message_kind="contact_locked")
    return True


def apply_scan_runtime_usage_to_free_trials(duration_seconds, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    state = load_subscriber_state()
    changed = False
    affected_users = 0

    for user in state.get("users", {}).values():
        chat_id = str(user.get("chat_id") or "").strip()
        if not chat_id or is_admin_chat(chat_id):
            continue

        free_trial = user.setdefault("free_trial", {})
        started_dt = get_free_trial_started_dt(user)
        if started_dt is None:
            continue

        local_changed = False
        expires_dt = get_free_trial_expires_dt(user)
        expires_at_text = _format_access_dt(expires_dt)
        if free_trial.get("expires_at") != expires_at_text:
            free_trial["expires_at"] = expires_at_text
            local_changed = True

        used_seconds = get_free_trial_used_scan_seconds(user, now_dt=now_dt)
        previous_used_seconds = float(free_trial.get("used_scan_seconds", 0.0) or 0.0)
        if abs(previous_used_seconds - used_seconds) > 0.5:
            free_trial["used_scan_seconds"] = used_seconds
            local_changed = True

        if expires_dt is not None and now_dt >= expires_dt:
            if not free_trial.get("ended_at"):
                free_trial["ended_at"] = expires_at_text
                local_changed = True
                affected_users += 1
        elif free_trial.get("ended_at"):
            free_trial["ended_at"] = None
            local_changed = True

        if local_changed:
            user["updated_at"] = _format_access_dt(now_dt)
            changed = True

    if changed:
        save_subscriber_state(state)
    return affected_users

def ensure_subscription_expiry_warnings(notify=True, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    state = load_subscriber_state()
    changed = False
    pending_messages = []
    slot_key = get_subscription_warning_slot_key(now_dt)
    lookahead_seconds = SUBSCRIPTION_WARNING_LOOKAHEAD_HOURS * 3600
    for user in state.get("users", {}).values():
        chat_id = str(user.get("chat_id") or "").strip()
        if not chat_id or is_admin_chat(chat_id):
            continue
        access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
        if access.get("stage") != "subscribed" or not access.get("active"):
            continue
        expires_dt = access.get("expires_at")
        if expires_dt is None:
            continue
        remaining_seconds = (expires_dt - now_dt).total_seconds()
        notification_state = user.setdefault("notification_state", {})
        current_end_at = _format_access_dt(expires_dt)
        stored_end_at = notification_state.get("subscription_warning_end_at")
        sent_slots = notification_state.get("subscription_warning_slots_sent")
        if not isinstance(sent_slots, list):
            sent_slots = []
        if stored_end_at != current_end_at:
            notification_state["subscription_warning_end_at"] = current_end_at
            notification_state["subscription_warning_slots_sent"] = []
            sent_slots = []
            changed = True
        if remaining_seconds <= 0 or remaining_seconds > lookahead_seconds:
            continue
        if slot_key in sent_slots:
            continue
        if notify:
            pending_messages.append((
                chat_id,
                "هشدار پایان اشتراک:\n"
                f"اشتراک شما در {current_end_at} به پایان می‌رسد.\n"
                f"زمان باقی‌مانده: {format_duration_persian(remaining_seconds)}\n"
                + build_activation_or_support_line("برای تمدید")
            ))
        sent_slots = list(sent_slots)
        sent_slots.append(slot_key)
        notification_state["subscription_warning_slots_sent"] = sent_slots[-8:]
        changed = True
    if changed:
        save_subscriber_state(state)
    if notify:
        for chat_id, message_text in pending_messages:
            send_bale_message(chat_id, message_text, message_kind="subscription_expiry_warning")

def ensure_access_lock_notices(notify=True, now_dt=None):
    if now_dt is None:
        now_dt = get_tehran_now()
    state = load_subscriber_state()
    changed = False
    pending_messages = []

    for user in state.get("users", {}).values():
        chat_id = str(user.get("chat_id") or "").strip()
        if not chat_id or is_admin_chat(chat_id):
            continue
        access = get_user_access_snapshot_from_record(user, now_dt=now_dt)
        free_trial = user.setdefault("free_trial", {})
        notification_state = user.setdefault("notification_state", {})

        if access.get("active"):
            if free_trial.get("lock_notice_sent_at"):
                free_trial["lock_notice_sent_at"] = None
                changed = True
            if notification_state.get("access_lock_notice_keys_sent"):
                notification_state["access_lock_notice_keys_sent"] = []
                changed = True
            continue

        reason = access.get("reason")
        if reason not in {"free_trial_expired", "subscription_expired"}:
            continue

        expires_dt = access.get("expires_at")
        if expires_dt is None:
            expires_dt = now_dt
        expires_key = _format_access_dt(expires_dt) or _format_access_dt(now_dt)

        if reason == "free_trial_expired" and not free_trial.get("ended_at"):
            free_trial["ended_at"] = expires_key
            changed = True

        sent_keys = notification_state.get("access_lock_notice_keys_sent")
        if not isinstance(sent_keys, list):
            sent_keys = []

        base_key = f"{reason}|{expires_key}"
        first_key = f"{base_key}|expired"
        second_key = f"{base_key}|expired_plus_2h"
        selected_key = None

        if first_key not in sent_keys:
            selected_key = first_key
        elif now_dt >= expires_dt + timedelta(hours=2) and second_key not in sent_keys:
            selected_key = second_key

        if selected_key is None:
            continue

        if notify:
            pending_messages.append((chat_id, build_locked_access_message(access)))

        sent_keys = list(sent_keys)
        sent_keys.append(selected_key)
        notification_state["access_lock_notice_keys_sent"] = sent_keys[-12:]
        free_trial["lock_notice_sent_at"] = _format_access_dt(now_dt)
        changed = True

    if changed:
        save_subscriber_state(state)

    if notify:
        for chat_id, message_text in pending_messages:
            send_bale_message(chat_id, message_text, message_kind="access_locked")

def should_ignore_locked_user_message(user, access, stripped_text):
    if access.get("active"):
        return False
    return True


# =========================
# ANSI COLOR HELPERS
# =========================

def color_256(code):
    return f"\033[38;5;{code}m"

RESET = "\033[0m"
BOLD = "\033[1m"

COL_HDR      = color_256(24)
COL_INFO     = color_256(240)
COL_VOLUME   = color_256(136)
COL_PRICE    = color_256(30)
COL_POS      = color_256(28)
COL_NEG      = color_256(88)
COL_NEUTRAL  = color_256(238)
COL_PUMP     = color_256(90)
COL_WARN     = color_256(130)
COL_SYMBOL   = color_256(31)

def log_info(*args, **kwargs):
    if not QUIET_TERMINAL:
        print(*args, **kwargs)


def import_lbank_signaler_module():
    """Import the matching LBank signaler module without falling back to Nobitex signaler.py."""
    last_exc = None
    for module_name in ("signaler_lbank", "signaler_lbank_v45"):
        try:
            return importlib.import_module(module_name)
        except Exception as exc:
            last_exc = exc
    raise ImportError("Could not import signaler_lbank or signaler_lbank_v45; refusing to import Nobitex signaler.py") from last_exc


def get_json_headers(extra=None):
    headers = {
        "Accept": "application/json",
        "User-Agent": NOBITEX_USER_AGENT,
    }
    if extra:
        headers.update(extra)
    return headers


def get_post_json_headers(extra=None):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": NOBITEX_USER_AGENT,
    }
    if extra:
        headers.update(extra)
    return headers


def _safe_float_default(value, default=0.0):
    try:
        number = float(value)
    except Exception:
        return default
    if not math.isfinite(number):
        return default
    return number


def _extract_http_error_payload(exc):
    try:
        raw = exc.read()
    except Exception:
        return None
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _http_attempt_count(retries):
    try:
        retry_count = max(0, int(retries))
    except Exception:
        retry_count = 0
    return retry_count + 1


def _http_error_code(exc):
    if isinstance(exc, error.HTTPError):
        try:
            return int(getattr(exc, "code", 0) or 0)
        except Exception:
            return None
    return None


def _is_retryable_http_code(code):
    try:
        code = int(code)
    except Exception:
        return False
    return code in {429, 500, 502, 503, 504}


def _http_error_reason(exc):
    if isinstance(exc, error.HTTPError):
        code = getattr(exc, "code", None)
        reason = getattr(exc, "reason", None)
        if code is not None and reason:
            return f"HTTP {code} {reason}"
        if code is not None:
            return f"HTTP {code}"

    reason = getattr(exc, "reason", None)
    if reason is not None:
        return reason

    return str(exc)


def _retry_sleep_seconds(exc, attempt, backoff_factor):
    if isinstance(exc, error.HTTPError) and getattr(exc, "code", None) == 429:
        payload = _extract_http_error_payload(exc)
        if payload:
            backoff_value = payload.get("backOff", payload.get("backoff"))
            try:
                backoff_seconds = float(backoff_value)
            except Exception:
                backoff_seconds = None
            if backoff_seconds is not None and math.isfinite(backoff_seconds) and backoff_seconds >= 0:
                return min(backoff_seconds, 3.0)

    try:
        sleep_seconds = float(backoff_factor) ** int(attempt)
    except Exception:
        sleep_seconds = 1.0

    if not math.isfinite(sleep_seconds) or sleep_seconds < 0:
        sleep_seconds = 1.0

    return min(sleep_seconds, 3.0)

def _get_stats_markets(stats_data, feed_name=""):
    if not isinstance(stats_data, dict):
        return {}

    status = stats_data.get("status")
    if status is not None and status != "ok":
        log_info(f"{COL_WARN}[STATS]{RESET} Ignoring {feed_name or 'market'} feed with status={status!r}.")
        return {}

    markets = stats_data.get("stats")
    return markets if isinstance(markets, dict) else {}


def _first_market_entry(markets):
    if not isinstance(markets, dict):
        return {}
    for entry in markets.values():
        if isinstance(entry, dict):
            return entry
    return {}


def normalize_nobitex_ohlc_resolution(resolution):
    try:
        resolution_value = str(resolution).strip().upper()
    except Exception:
        return None
    if not resolution_value:
        return None
    return resolution_value if resolution_value in NOBITEX_VALID_OHLC_RESOLUTIONS else None


def normalize_nobitex_countback(countback):
    if countback is None:
        return None
    try:
        count = int(float(countback))
    except Exception:
        return None
    if count <= 0:
        return None
    return min(count, NOBITEX_MAX_OHLC_COUNTBACK)


QUOTE_CURRENCY_ALIASES = {
    "usdt": "USDT",
}
# LBank port is deliberately USDT-only. Do not infer or accept IRT/RLS markets.
DEFAULT_NOBITEX_QUOTE_SUFFIXES = ("USDT",)


def _sanitize_nobitex_symbol_fragment(value, allow_underscore=True):
    """Return a LBank-safe uppercase symbol fragment without losing token underscores."""
    text = str(value or "").strip().upper()
    if not text:
        return None

    if allow_underscore:
        text = re.sub(r"[^A-Z0-9_]+", "", text)
        text = re.sub(r"_+", "_", text).strip("_")
    else:
        text = re.sub(r"[^A-Z0-9]+", "", text)

    return text or None


def _repair_nobitex_multiplier_base_underscore(base_symbol):
    """
    Repair compact LBank multiplier bases that were previously flattened.

    LBank UDF symbols can contain underscores inside the base asset, for example
    100K_FLOKIUSDT. Older normalization removed the underscore and produced
    100KFLOKIUSDT, which caused Nobitex UDF bad requests; preserved here for multiplier-base compatibility.
    """
    text = _sanitize_nobitex_symbol_fragment(base_symbol, allow_underscore=True)
    if not text:
        return None
    if "_" in text:
        return text

    multiplier_match = re.match(r"^(\d+(?:K|M|B|T))([A-Z][A-Z0-9]{1,})$", text)
    if multiplier_match:
        return f"{multiplier_match.group(1)}_{multiplier_match.group(2)}"

    return text


def normalize_nobitex_base_symbol(value):
    return _repair_nobitex_multiplier_base_underscore(value)


def normalize_nobitex_symbol_text(value):
    text = str(value or "").strip().upper()
    if not text:
        return None

    # LBank symbols are USDT-only here. Keep underscores inside multiplier bases
    # such as 1000_PEPE, but do not rewrite RLS/RIAL/IRT aliases.
    return _sanitize_nobitex_symbol_fragment(text, allow_underscore=True)


def split_nobitex_market_symbol(symbol):
    normalized_symbol = normalize_nobitex_symbol_text(symbol)
    if not normalized_symbol:
        return None, None

    for quote in get_known_quote_suffixes():
        if normalized_symbol.endswith(quote) and len(normalized_symbol) > len(quote):
            raw_base = normalized_symbol[:-len(quote)].rstrip("_")
            base = normalize_nobitex_base_symbol(raw_base)
            if base:
                return base, quote

    return normalize_nobitex_base_symbol(normalized_symbol), None


def normalize_nobitex_udf_symbol(value, default_quote_currency=None):
    base, quote = split_nobitex_market_symbol(value)

    if quote is None and default_quote_currency:
        quote = normalize_quote_currency(default_quote_currency)

    if base and quote:
        return f"{base}{quote}"
    if base:
        return base

    return normalize_nobitex_symbol_text(value)


def compact_nobitex_symbol_alias(value):
    normalized = normalize_nobitex_udf_symbol(value)
    if not normalized:
        return None
    alias = normalized.replace("_", "")
    return alias or None


def _split_csv_tokens(value):
    if value is None:
        return []
    return [part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()]


def normalize_quote_currency(value):
    quote = str(value or "").strip()
    if not quote:
        return None
    return QUOTE_CURRENCY_ALIASES.get(quote.lower(), quote.upper())


def get_known_quote_suffixes():
    suffixes = {normalize_quote_currency(item) for item in DEFAULT_NOBITEX_QUOTE_SUFFIXES}
    suffixes.update(normalize_quote_currency(item) for item in _split_csv_tokens(os.getenv("LBANK_QUOTE_CURRENCIES")))
    return tuple(sorted({item for item in suffixes if item}, key=len, reverse=True))


def quote_currency_from_market_symbol(symbol):
    if not isinstance(symbol, str):
        return None
    _, quote = split_nobitex_market_symbol(symbol)
    return quote


def get_configured_default_skip_tags():
    env_value = os.getenv("LBANK_SKIP_TAGS")
    if env_value is None:
        return set(DEFAULT_SKIP_TAGS)

    value = str(env_value).strip()
    if not value or value.lower() in {"0", "false", "off", "none", "disabled", "no"}:
        return set()

    return {tag for tag in _split_csv_tokens(value) if tag in SKIP_SYMBOLS_BY_TAG}


def get_extra_skip_symbols():
    return {token.upper() for token in _split_csv_tokens(os.getenv("LBANK_EXTRA_SKIP_SYMBOLS"))}


def build_skip_symbol_set(enabled_tags=None):
    if enabled_tags is None:
        enabled_tags = get_configured_default_skip_tags()

    combined = set()
    for tag in enabled_tags:
        combined.update(SKIP_SYMBOLS_BY_TAG.get(tag, set()))
    combined.update(get_extra_skip_symbols())
    return {str(symbol).upper() for symbol in combined if str(symbol).strip()}

DEFAULT_SKIP_SYMBOLS = build_skip_symbol_set()

def fmt_pct_plain(pct):
    return f"{pct:+.2f}%"

def fmt_pct(pct):
    if pct > 0:
        return f"{COL_POS}{pct:+.2f}%{RESET}"
    elif pct < 0:
        return f"{COL_NEG}{pct:+.2f}%{RESET}"
    else:
        return f"{COL_NEUTRAL}{pct:+.2f}%{RESET}"

def fmt_volume(vol):
    return f"{COL_VOLUME}{vol:,.0f}{RESET}"

def fmt_price(price):
    return f"{COL_PRICE}{price}{RESET}"

def fmt_symbol(sym):
    return f"{BOLD}{COL_SYMBOL}{sym}{RESET}"

# =========================
# TIMEZONE / SCHEDULER TIME HELPERS
# =========================

TEHRAN_TZ_NAME = "Asia/Tehran"
TEHRAN_FIXED_OFFSET = timezone(timedelta(hours=3, minutes=30), TEHRAN_TZ_NAME)
TEHRAN_ZONEINFO = None
TEHRAN_ZONEINFO_LOOKUP_DONE = False

def get_tehran_tzinfo():
    """Return Asia/Tehran tzinfo using only Python stdlib/local tzdata.

    This never calls external time websites and never changes the device/system timezone.
    If local IANA tzdata is unavailable, fall back to Iran's fixed UTC+03:30
    offset so the bot still runs without external packages or network time.
    """
    global TEHRAN_ZONEINFO, TEHRAN_ZONEINFO_LOOKUP_DONE

    if not TEHRAN_ZONEINFO_LOOKUP_DONE:
        TEHRAN_ZONEINFO_LOOKUP_DONE = True
        if ZoneInfo is not None:
            try:
                TEHRAN_ZONEINFO = ZoneInfo(TEHRAN_TZ_NAME)
            except Exception:
                TEHRAN_ZONEINFO = None

    return TEHRAN_ZONEINFO if TEHRAN_ZONEINFO is not None else TEHRAN_FIXED_OFFSET

def get_current_tehran_offset():
    try:
        now_aware = datetime.now(get_tehran_tzinfo())
        offset = now_aware.utcoffset()
        return offset if offset is not None else timedelta(hours=3, minutes=30)
    except Exception:
        return timedelta(hours=3, minutes=30)

def get_tehran_now():
    """Current Tehran wall-clock time as a naive datetime.

    Existing scheduler/storage code expects naive datetimes.  The conversion is
    done with Asia/Tehran tzinfo locally, then tzinfo is stripped intentionally.
    """
    try:
        return datetime.now(get_tehran_tzinfo()).replace(tzinfo=None, microsecond=0)
    except Exception:
        # Last-resort stdlib fallback: UTC +03:30, still no network and no system clock mutation.
        return (datetime.utcnow() + timedelta(hours=3, minutes=30)).replace(microsecond=0)

def format_offset(td):
    total_seconds = int(td.total_seconds())
    sign = "+" if total_seconds >= 0 else "-"
    total_seconds = abs(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"UTC{sign}{hours:02d}:{minutes:02d}"

def build_scan_slots_for_day(ref_dt):
    slots = []
    start_dt = ref_dt.replace(hour=SCAN_START_HOUR, minute=SCAN_START_MINUTE, second=0, microsecond=0)
    end_dt = ref_dt.replace(hour=SCAN_END_HOUR, minute=SCAN_END_MINUTE, second=0, microsecond=0)

    cursor = start_dt
    while cursor <= end_dt:
        slots.append(cursor)
        cursor += timedelta(minutes=15)

    return slots

def get_scan_type_for_slot(slot_dt):
    if slot_dt.minute in (0, 15, 30, 45):
        return "full"
    return None

def get_scan_slot_key(slot_dt, scan_kind=None):
    if scan_kind is None:
        scan_kind = get_scan_type_for_slot(slot_dt)
    return f"{slot_dt.strftime('%Y-%m-%d %H:%M')}|{scan_kind}"

def get_due_scan_slot(now_local=None, grace_seconds=SCHEDULER_GRACE_SECONDS):
    if now_local is None:
        now_local = get_tehran_now()

    history_state = load_scan_history(scan_dt=now_local)

    if history_state.get("day_closed"):
        return None, None

    executed_slots = history_state.get("scheduled_slots", {})

    latest_due_slot = None
    latest_due_type = None

    for slot_dt in build_scan_slots_for_day(now_local):
        if slot_dt <= now_local:
            latest_due_slot = slot_dt
            latest_due_type = get_scan_type_for_slot(slot_dt)
        else:
            break

    if latest_due_slot is None or latest_due_type is None:
        return None, None

    slot_key = get_scan_slot_key(latest_due_slot, latest_due_type)
    if slot_key in executed_slots:
        return None, None

    return latest_due_slot, latest_due_type

def format_scan_time(dt=None):
    if dt is None:
        dt = get_tehran_now()
    return dt.strftime("%m_%d::%I_%M%p")

def floor_to_even_two_minute_slot(dt):
    minute = (dt.minute // DEMAND_SCAN_INTERVAL_MINUTES) * DEMAND_SCAN_INTERVAL_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)

def ceil_to_even_two_minute_slot(dt):
    floored = floor_to_even_two_minute_slot(dt)
    if floored >= dt:
        return floored
    return floored + timedelta(minutes=DEMAND_SCAN_INTERVAL_MINUTES)

def get_demand_slot_key(slot_dt):
    return slot_dt.strftime("%Y-%m-%d %H:%M")

def get_active_demand_count():
    with DEMAND_STATE_LOCK:
        return sum(len(v) for v in DEMAND_SYMBOL_SUBSCRIBERS.values())

def get_demand_subscriber_snapshot():
    with DEMAND_STATE_LOCK:
        return {sym: set(chat_ids) for sym, chat_ids in DEMAND_SYMBOL_SUBSCRIBERS.items() if chat_ids}

def get_user_subscription_snapshot(chat_id):
    chat_id = str(chat_id)
    with DEMAND_STATE_LOCK:
        return sorted(DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set()))

def get_latest_due_demand_slot(now_local=None):
    global DEMAND_LAST_SCAN_SLOT_KEY

    if now_local is None:
        now_local = get_tehran_now()

    with DEMAND_STATE_LOCK:
        has_demands = any(DEMAND_SYMBOL_SUBSCRIBERS.values())
        start_not_before = DEMAND_SCAN_START_NOT_BEFORE
        last_slot_key = DEMAND_LAST_SCAN_SLOT_KEY

    if not has_demands:
        return None

    slot_dt = floor_to_even_two_minute_slot(now_local)

    if start_not_before is not None and slot_dt < start_not_before:
        return None

    slot_key = get_demand_slot_key(slot_dt)
    if slot_key == last_slot_key:
        return None

    return slot_dt

def get_next_demand_run_time(now_local=None):
    if now_local is None:
        now_local = get_tehran_now()

    with DEMAND_STATE_LOCK:
        has_demands = any(DEMAND_SYMBOL_SUBSCRIBERS.values())
        start_not_before = DEMAND_SCAN_START_NOT_BEFORE
        last_slot_key = DEMAND_LAST_SCAN_SLOT_KEY

    if not has_demands:
        return None

    current_slot = floor_to_even_two_minute_slot(now_local)
    current_key = get_demand_slot_key(current_slot)

    if start_not_before is not None:
        if current_slot >= start_not_before and current_key != last_slot_key:
            return current_slot
        return start_not_before

    if current_key != last_slot_key:
        return current_slot

    return current_slot + timedelta(minutes=DEMAND_SCAN_INTERVAL_MINUTES)

# =========================
# LOCAL WEB CHAT STORAGE / LIVE EVENTS
# =========================

def get_chat_retention_cutoff_ts(now_ts=None):
    if now_ts is None:
        now_ts = time.time()
    return now_ts - (LOCAL_CHAT_RETENTION_HOURS * 3600)

def get_default_local_chat_state():
    return {
        "next_id": 1,
        "messages": []
    }

def save_local_chat_state(state):
    try:
        with open(LOCAL_CHAT_STORE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{COL_WARN}[LOCAL CHAT]{RESET} Failed to save local chat store: {e}")

def prune_local_chat_state_inplace(state, now_ts=None):
    if now_ts is None:
        now_ts = time.time()

    cutoff_ts = get_chat_retention_cutoff_ts(now_ts)
    messages = state.get("messages", [])
    if not isinstance(messages, list):
        messages = []

    pruned_messages = []
    max_id = 0

    for item in messages:
        if not isinstance(item, dict):
            continue

        try:
            created_ts = float(item.get("created_ts", 0.0) or 0.0)
        except Exception:
            created_ts = 0.0

        if created_ts < cutoff_ts:
            continue

        try:
            msg_id = int(item.get("id", 0) or 0)
        except Exception:
            msg_id = 0

        if msg_id > max_id:
            max_id = msg_id

        pruned_messages.append(item)

    state["messages"] = pruned_messages

    try:
        next_id = int(state.get("next_id", max_id + 1) or (max_id + 1))
    except Exception:
        next_id = max_id + 1

    if next_id <= max_id:
        next_id = max_id + 1

    state["next_id"] = next_id
    return state

def load_local_chat_state():
    default_state = get_default_local_chat_state()

    if not os.path.exists(LOCAL_CHAT_STORE_FILE):
        save_local_chat_state(default_state)
        return default_state

    try:
        with open(LOCAL_CHAT_STORE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            save_local_chat_state(default_state)
            return default_state

        if "next_id" not in state:
            state["next_id"] = 1
        if not isinstance(state.get("messages"), list):
            state["messages"] = []

        prune_local_chat_state_inplace(state)
        return state
    except Exception as e:
        print(f"{COL_WARN}[LOCAL CHAT]{RESET} Failed to load local chat store, resetting: {e}")
        save_local_chat_state(default_state)
        return default_state

def register_local_sse_client():
    q = queue.Queue(maxsize=LOCAL_WEB_SSE_QUEUE_SIZE)
    with LOCAL_SSE_CLIENTS_LOCK:
        LOCAL_SSE_CLIENTS.add(q)
    return q

def unregister_local_sse_client(q):
    with LOCAL_SSE_CLIENTS_LOCK:
        if q in LOCAL_SSE_CLIENTS:
            LOCAL_SSE_CLIENTS.remove(q)

def broadcast_local_event(event_name, payload):
    with LOCAL_SSE_CLIENTS_LOCK:
        clients = list(LOCAL_SSE_CLIENTS)

    for q in clients:
        try:
            q.put_nowait((event_name, payload))
        except queue.Full:
            try:
                q.get_nowait()
            except Exception:
                pass
            try:
                q.put_nowait((event_name, payload))
            except Exception:
                pass

def is_demand_outgoing_message_kind(message_kind):
    normalized = str(message_kind or "").strip().lower()
    if not normalized:
        return False

    if normalized == "demand_scan":
        return False

    if normalized == "full_scan":
        return False

    if "scan" in normalized and not normalized.startswith("demand_"):
        return False

    return True


def resolve_local_chat_endpoint_bucket(direction, message_kind):
    direction_str = str(direction or "").strip().lower()
    normalized_kind = str(message_kind or "").strip().lower()

    if direction_str == "incoming":
        return "incoming"

    if direction_str == "outgoing":
        if normalized_kind == "demand_scan":
            return "trackings"
        if is_demand_outgoing_message_kind(normalized_kind):
            return "demand_outgoing"

    return "main"


def get_current_outgoing_reply_context():
    stack = getattr(OUTGOING_REPLY_CONTEXT, "stack", None)
    if not stack:
        return None

    current = stack[-1]
    if not isinstance(current, dict):
        return None

    return dict(current)


@contextmanager
def outgoing_reply_context(chat_id=None, incoming_transport=None, endpoint_bucket=None, reply_scope=None, mirror_to_local_web=False):
    stack = getattr(OUTGOING_REPLY_CONTEXT, "stack", None)
    if stack is None:
        stack = []
        OUTGOING_REPLY_CONTEXT.stack = stack

    context = {
        "chat_id": str(chat_id).strip() if chat_id is not None else None,
        "incoming_transport": str(incoming_transport).strip().lower() if incoming_transport is not None else None,
        "endpoint_bucket": str(endpoint_bucket).strip().lower() if endpoint_bucket is not None else None,
        "reply_scope": str(reply_scope).strip() if reply_scope is not None else None,
        "mirror_to_local_web": bool(mirror_to_local_web),
    }

    stack.append(context)
    try:
        yield context
    finally:
        if stack:
            stack.pop()
        if not stack:
            try:
                del OUTGOING_REPLY_CONTEXT.stack
            except AttributeError:
                pass


def enrich_outgoing_message_meta_for_reply_context(chat_id, message_meta):
    if not isinstance(message_meta, dict):
        message_meta = {}

    context = get_current_outgoing_reply_context()
    if not isinstance(context, dict):
        return message_meta, False

    normalized_chat_id = str(chat_id).strip()
    source_chat_id = str(context.get("chat_id") or "").strip()
    incoming_transport = str(context.get("incoming_transport") or "").strip().lower()
    endpoint_bucket = str(context.get("endpoint_bucket") or "").strip().lower()
    reply_scope = str(context.get("reply_scope") or "").strip()

    if endpoint_bucket and not str(message_meta.get("endpoint_bucket") or "").strip():
        message_meta["endpoint_bucket"] = endpoint_bucket

    if reply_scope and not str(message_meta.get("reply_scope") or "").strip():
        message_meta["reply_scope"] = reply_scope

    if incoming_transport and not str(message_meta.get("incoming_transport") or "").strip():
        message_meta["incoming_transport"] = incoming_transport

    if source_chat_id and not str(message_meta.get("reply_source_chat_id") or "").strip():
        message_meta["reply_source_chat_id"] = source_chat_id

    should_mirror = bool(context.get("mirror_to_local_web")) and normalized_chat_id != str(LOCAL_WEB_DEFAULT_CHAT_ID).strip()
    return message_meta, should_mirror


def append_local_web_reply_mirror(original_chat_id, text, direction, transport, source, meta=None, created_dt=None):
    normalized_original_chat_id = str(original_chat_id).strip()
    if not normalized_original_chat_id or normalized_original_chat_id == str(LOCAL_WEB_DEFAULT_CHAT_ID).strip():
        return None

    mirror_meta = dict(meta) if isinstance(meta, dict) else {}
    mirror_meta["endpoint_bucket"] = "demand_outgoing"
    mirror_meta["mirrored_to_local_web"] = True
    mirror_meta["mirror_of_chat_id"] = normalized_original_chat_id
    mirror_meta.setdefault("original_chat_id", normalized_original_chat_id)

    return append_local_chat_message(
        chat_id=LOCAL_WEB_DEFAULT_CHAT_ID,
        text=text,
        direction=direction,
        transport=transport,
        source=source,
        meta=mirror_meta,
        created_dt=created_dt,
    )


def normalize_local_chat_message_inplace(message):
    if not isinstance(message, dict):
        return None, False

    changed = False

    meta = message.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        message["meta"] = meta
        changed = True

    direction_str = str(message.get("direction", "")).strip().lower()
    message_kind = str(meta.get("message_kind", "")).strip().lower()

    if not message_kind:
        if direction_str == "incoming":
            message_kind = "incoming"
        elif direction_str == "outgoing":
            message_kind = "generic_outgoing"
        elif direction_str:
            message_kind = direction_str
        else:
            message_kind = "unknown"
        meta["message_kind"] = message_kind
        changed = True

    endpoint_bucket = str(meta.get("endpoint_bucket", "")).strip().lower()
    if endpoint_bucket not in {"main", "incoming", "demand_outgoing", "trackings"}:
        endpoint_bucket = resolve_local_chat_endpoint_bucket(direction_str, message_kind)
        meta["endpoint_bucket"] = endpoint_bucket
        changed = True

    if message.get("endpoint_bucket") != endpoint_bucket:
        message["endpoint_bucket"] = endpoint_bucket
        changed = True

    return message, changed


def append_local_chat_message(chat_id, text, direction, transport, source, meta=None, created_dt=None):
    if created_dt is None:
        created_dt = get_tehran_now()

    if meta is None:
        meta = {}

    text = str(text)
    chat_id = str(chat_id)
    direction_str = str(direction).strip().lower()
    source_str = str(source).strip().lower()

    if chat_id.strip().lower() == "system" or direction_str == "system" or source_str == "system":
        return None

    now_ts = time.time()

    with LOCAL_CHAT_LOCK:
        state = load_local_chat_state()
        prune_local_chat_state_inplace(state, now_ts=now_ts)

        msg_id = int(state.get("next_id", 1) or 1)
        message = {
            "id": msg_id,
            "chat_id": chat_id,
            "text": text,
            "direction": str(direction),
            "transport": str(transport),
            "source": str(source),
            "created_at": created_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "created_ts": now_ts,
            "meta": dict(meta) if isinstance(meta, dict) else {}
        }

        normalize_local_chat_message_inplace(message)

        state["messages"].append(message)
        state["next_id"] = msg_id + 1
        save_local_chat_state(state)

    broadcast_local_event("message", message)
    return message


def local_chat_message_matches_bucket(message, endpoint_bucket):
    normalized_message, _ = normalize_local_chat_message_inplace(message)
    if not isinstance(normalized_message, dict):
        return False

    resolved_bucket = str(normalized_message.get("endpoint_bucket", "")).strip().lower()
    desired_bucket = str(endpoint_bucket or "main").strip().lower()

    if desired_bucket == "main":
        return resolved_bucket == "main"

    return resolved_bucket == desired_bucket


def get_local_chat_messages(chat_id=None, limit=LOCAL_WEB_FETCH_LIMIT_DEFAULT, after_id=None, endpoint_bucket="main", message_kinds=None):
    if chat_id is not None:
        chat_id = str(chat_id)

    normalized_message_kinds = None
    if message_kinds is not None:
        normalized_message_kinds = {
            str(item).strip().lower()
            for item in message_kinds
            if str(item).strip()
        }
        if not normalized_message_kinds:
            normalized_message_kinds = None

    try:
        limit = int(limit)
    except Exception:
        limit = LOCAL_WEB_FETCH_LIMIT_DEFAULT

    limit = max(1, min(LOCAL_WEB_FETCH_LIMIT_MAX, limit))

    try:
        after_id = int(after_id) if after_id is not None else None
    except Exception:
        after_id = None

    with LOCAL_CHAT_LOCK:
        state = load_local_chat_state()
        prune_local_chat_state_inplace(state)
        messages = state.get("messages", [])
        state_changed = False

        filtered = []
        for item in messages:
            if not isinstance(item, dict):
                continue

            _, item_changed = normalize_local_chat_message_inplace(item)
            if item_changed:
                state_changed = True

            if chat_id is not None and str(item.get("chat_id")) != chat_id:
                continue
            if not local_chat_message_matches_bucket(item, endpoint_bucket):
                continue

            message_kind = str((item.get("meta") or {}).get("message_kind") or "").strip().lower()
            if normalized_message_kinds is not None and message_kind not in normalized_message_kinds:
                continue

            try:
                msg_id = int(item.get("id", 0) or 0)
            except Exception:
                msg_id = 0
            if after_id is not None and msg_id <= after_id:
                continue
            filtered.append(item)

        save_local_chat_state(state)

    if len(filtered) > limit:
        filtered = filtered[-limit:]

    return filtered

def prune_local_chat_store_and_notify():
    removed = 0
    cutoff_ts = get_chat_retention_cutoff_ts()

    with LOCAL_CHAT_LOCK:
        state = load_local_chat_state()
        before = len(state.get("messages", []))
        prune_local_chat_state_inplace(state)
        after = len(state.get("messages", []))
        removed = max(0, before - after)
        save_local_chat_state(state)

    if removed > 0:
        broadcast_local_event("pruned", {
            "removed": removed,
            "retention_hours": LOCAL_CHAT_RETENTION_HOURS,
            "cutoff_ts": cutoff_ts
        })

    return removed

def local_chat_cleanup_loop():
    log_info(f"{COL_INFO}[LOCAL CHAT]{RESET} 48h retention cleanup thread started.")
    while True:
        try:
            prune_local_chat_store_and_notify()
        except Exception as e:
            print(f"{COL_WARN}[LOCAL CHAT]{RESET} Cleanup loop error: {e}")
            traceback.print_exc()
        time.sleep(LOCAL_CHAT_PRUNE_INTERVAL_SECONDS)

def build_local_chat_status(now_local=None):
    if now_local is None:
        now_local = get_tehran_now()

    next_full = get_next_run_time(now_local=now_local)
    next_demand = get_next_demand_run_time(now_local=now_local)
    active_scan = get_active_scan_kind()

    with LOCAL_CHAT_LOCK:
        state = load_local_chat_state()
        prune_local_chat_state_inplace(state)
        save_local_chat_state(state)
        message_count = len(state.get("messages", []))
        last_message = state.get("messages", [])[-1] if state.get("messages") else None

    return {
        "ok": True,
        "now": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "active_scan_kind": active_scan,
        "active_demand_count": get_active_demand_count(),
        "local_web": {
            "host": LOCAL_WEB_HOST,
            "port": LOCAL_WEB_PORT,
            "retention_hours": LOCAL_CHAT_RETENTION_HOURS,
            "message_count": message_count
        },
        "next_full_run": next_full.strftime("%Y-%m-%d %H:%M:%S") if next_full else None,
        "next_demand_run": next_demand.strftime("%Y-%m-%d %H:%M:%S") if next_demand else None,
        "last_message": last_message
    }

class PumpLocalWebHandler(BaseHTTPRequestHandler):
    server_version = "PumpLocalWeb/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        if not QUIET_TERMINAL:
            super().log_message(format, *args)

    def _send_json(self, status_code, payload):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except Exception:
            length = 0

        if length <= 0:
            return {}

        if length > LOCAL_WEB_BODY_LIMIT:
            raise ValueError("Request body too large")

        raw = self.rfile.read(length)
        if not raw:
            return {}

        return json.loads(raw.decode("utf-8"))

    def _serve_index_file(self):
        try:
            file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
            with open(file_path, "rb") as fh:
                raw = fh.read()

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
        except FileNotFoundError:
            self._send_json(404, {"ok": False, "error": "index.html not found"})
        except Exception as e:
            self._send_json(500, {"ok": False, "error": str(e)})

    def _write_sse_packet(self, event_name, payload):
        packet = f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")
        self.wfile.write(packet)
        self.wfile.flush()

    def _handle_module_sse_stream(self, mod):
        client_queue = mod.register_local_sse_client()

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            self._write_sse_packet("connected", mod.build_local_chat_status())

            while True:
                try:
                    event_name, payload = client_queue.get(timeout=mod.LOCAL_WEB_SSE_HEARTBEAT_SECONDS)
                    self._write_sse_packet(event_name, payload)
                except queue.Empty:
                    self._write_sse_packet("heartbeat", {
                        "now": mod.get_tehran_now().strftime("%Y-%m-%d %H:%M:%S")
                    })
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            print(f"{COL_WARN}[LOCAL WEB]{RESET} SSE stream error: {e}")
        finally:
            mod.unregister_local_sse_client(client_queue)

    def _route_module_get(self, mod, path, query):
        if path == "/health":
            self._send_json(200, {"ok": True})
            return True

        if path == "/api/state":
            self._send_json(200, mod.build_local_chat_status())
            return True

        if path in ("/api/today", "/api/today-pumps"):
            if not hasattr(mod, "build_bale_today_api_payload"):
                self._send_json(404, {
                    "ok": False,
                    "error": "Endpoint not available for this module"
                })
                return True

            self._send_json(200, mod.build_bale_today_api_payload())
            return True

        endpoint_bucket = None
        message_kinds = None
        endpoint_name = None
        if path == "/api/messages":
            endpoint_bucket = "main"
            endpoint_name = "messages"
        elif path == "/api/messages/incoming":
            endpoint_bucket = "incoming"
            endpoint_name = "messages/incoming"
        elif path == "/api/messages/demand-outgoing":
            endpoint_bucket = "demand_outgoing"
            endpoint_name = "messages/demand-outgoing"
        elif path == "/api/messages/trackings":
            endpoint_bucket = "trackings"
            message_kinds = {"demand_scan"}
            endpoint_name = "messages/trackings"

        if endpoint_bucket is not None:
            chat_id = query.get("chat_id", [None])[0]
            limit = query.get("limit", [mod.LOCAL_WEB_FETCH_LIMIT_DEFAULT])[0]
            after_id = query.get("after_id", [None])[0]

            try:
                messages = mod.get_local_chat_messages(
                    chat_id=chat_id,
                    limit=limit,
                    after_id=after_id,
                    endpoint_bucket=endpoint_bucket,
                    message_kinds=message_kinds,
                )
            except TypeError:
                if endpoint_bucket != "main" or message_kinds is not None:
                    self._send_json(404, {
                        "ok": False,
                        "error": "Endpoint not available for this module"
                    })
                    return True
                messages = mod.get_local_chat_messages(chat_id=chat_id, limit=limit, after_id=after_id)

            self._send_json(200, {
                "ok": True,
                "endpoint": endpoint_name,
                "endpoint_bucket": endpoint_bucket,
                "messages": messages,
                "count": len(messages)
            })
            return True

        if path == "/events":
            self._handle_module_sse_stream(mod)
            return True

        return False

    def _route_module_post(self, mod, path):
        if path != "/api/send":
            return False

        try:
            payload = self._read_json_body()
        except Exception as e:
            self._send_json(400, {
                "ok": False,
                "error": f"بدنه JSON نامعتبر است: {e}"
            })
            return True

        text = payload.get("text")
        chat_id = payload.get("chat_id") or mod.LOCAL_WEB_DEFAULT_CHAT_ID

        if not isinstance(text, str) or not text.strip():
            self._send_json(400, {
                "ok": False,
                "error": "فیلد text الزامی است"
            })
            return True

        try:
            incoming_msg = mod.append_local_chat_message(
                chat_id=chat_id,
                text=text.strip(),
                direction="incoming",
                transport="web",
                source="web",
                meta={
                    "api": "/api/send",
                    "message_kind": "incoming_web"
                }
            )
            mod.process_incoming_text_message(chat_id, text.strip(), incoming_transport="web")
            self._send_json(200, {
                "ok": True,
                "accepted": incoming_msg
            })
        except Exception as e:
            traceback.print_exc()
            self._send_json(500, {
                "ok": False,
                "error": str(e)
            })

        return True

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = parse.urlparse(self.path)
        path = parsed.path
        query = parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._serve_index_file()
            return

        if path.startswith("/nobit"):
            subpath = path[len("/nobit"):] or "/"
            if self._route_module_get(sys.modules[__name__], subpath, query):
                return

        if path.startswith("/signaler"):
            signaler = import_lbank_signaler_module()
            subpath = path[len("/signaler"):] or "/"
            if self._route_module_get(signaler, subpath, query):
                return

        if self._route_module_get(sys.modules[__name__], path, query):
            return

        self._send_json(404, {
            "ok": False,
            "error": "یافت نشد",
            "available_endpoints": [
                "/",
                "/index.html",
                "/health",
                "/api/state",
                "/api/today",
                "/api/today-pumps",
                "/api/messages",
                "/api/messages/incoming",
                "/api/messages/demand-outgoing",
                "/api/messages/trackings",
                "/api/send",
                "/events",
                "/nobit/health",
                "/nobit/api/state",
                "/nobit/api/today",
                "/nobit/api/today-pumps",
                "/nobit/api/messages",
                "/nobit/api/messages/incoming",
                "/nobit/api/messages/demand-outgoing",
                "/nobit/api/messages/trackings",
                "/nobit/api/send",
                "/nobit/events",
                "/signaler/health",
                "/signaler/api/state",
                "/signaler/api/messages",
                "/signaler/api/send",
                "/signaler/events"
            ]
        })

    def do_POST(self):
        parsed = parse.urlparse(self.path)
        path = parsed.path

        if path.startswith("/nobit"):
            subpath = path[len("/nobit"):] or "/"
            if self._route_module_post(sys.modules[__name__], subpath):
                return

        if path.startswith("/signaler"):
            signaler = import_lbank_signaler_module()
            subpath = path[len("/signaler"):] or "/"
            if self._route_module_post(signaler, subpath):
                return

        if self._route_module_post(sys.modules[__name__], path):
            return

        self._send_json(404, {
            "ok": False,
            "error": "یافت نشد"
        })

def local_web_server_loop():
    server = ThreadingHTTPServer((LOCAL_WEB_HOST, LOCAL_WEB_PORT), PumpLocalWebHandler)
    log_info(f"{COL_INFO}[LOCAL WEB]{RESET} Server started on http://{LOCAL_WEB_HOST}:{LOCAL_WEB_PORT}")
    try:
        server.serve_forever()
    except Exception as e:
        print(f"{COL_WARN}[LOCAL WEB]{RESET} Server error: {e}")
        traceback.print_exc()
    finally:
        try:
            server.server_close()
        except Exception:
            pass

# =========================
# LOCAL STAR STATE / HISTORY HELPERS
# =========================

def get_today_str(dt=None):
    if dt is None:
        dt = get_tehran_now()
    return dt.strftime("%Y-%m-%d")

def get_default_scan_history(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    return {
        "date": get_today_str(scan_dt),
        "scheduled_slots": {},
        "day_closed": False,
        "closed_at": None,
        "last_closed_slot": None
    }

def save_scan_history(state):
    try:
        with open(SCAN_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{COL_WARN}[SCAN HISTORY]{RESET} Failed to save history file: {e}")

def load_scan_history(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    today_str = get_today_str(scan_dt)
    default_state = get_default_scan_history(scan_dt)

    if not os.path.exists(SCAN_HISTORY_FILE):
        save_scan_history(default_state)
        return default_state

    try:
        with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            print(f"{COL_WARN}[SCAN HISTORY]{RESET} Invalid history structure. Resetting.")
            save_scan_history(default_state)
            return default_state

        stored_date = state.get("date")
        if stored_date != today_str:
            log_info(f"{COL_INFO}[SCAN HISTORY]{RESET} History date is old ({stored_date}). Resetting for new day and rechecking.")
            save_scan_history(default_state)
            return default_state

        if not isinstance(state.get("scheduled_slots"), dict):
            state["scheduled_slots"] = {}

        if "day_closed" not in state:
            state["day_closed"] = False

        if "closed_at" not in state:
            state["closed_at"] = None

        if "last_closed_slot" not in state:
            state["last_closed_slot"] = None

        return state

    except Exception as e:
        print(f"{COL_WARN}[SCAN HISTORY]{RESET} Failed to load history file, resetting: {e}")
        save_scan_history(default_state)
        return default_state

def record_scheduled_scan_history(scheduled_slot_dt, scan_kind, actual_scan_dt=None, source_label="SCHEDULER"):
    if actual_scan_dt is None:
        actual_scan_dt = get_tehran_now()

    state = load_scan_history(scan_dt=actual_scan_dt)
    slot_key = get_scan_slot_key(scheduled_slot_dt, scan_kind)

    state["date"] = get_today_str(actual_scan_dt)
    state["scheduled_slots"][slot_key] = {
        "scheduled_for": scheduled_slot_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "scan_kind": scan_kind,
        "executed_at": actual_scan_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "source": source_label
    }

    save_scan_history(state)

def close_scan_history_for_day(scan_dt=None, closed_slot_dt=None, scan_kind="full"):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state = get_default_scan_history(scan_dt)
    state["day_closed"] = True
    state["closed_at"] = scan_dt.strftime("%Y-%m-%d %H:%M:%S")

    if closed_slot_dt is not None:
        state["last_closed_slot"] = get_scan_slot_key(closed_slot_dt, scan_kind)

    save_scan_history(state)
    log_info(f"{COL_INFO}[SCAN HISTORY]{RESET} History reset for the day and marked closed at {state['closed_at']}.")

def get_display_symbol(symbol):
    if isinstance(symbol, str):
        base, quote = split_nobitex_market_symbol(symbol)
        if base and quote == "USDT":
            return base
        if base:
            return base
    return symbol

def clear_star_state():
    try:
        if os.path.exists(STAR_STATE_FILE):
            os.remove(STAR_STATE_FILE)
            log_info(f"{COL_INFO}[STAR STATE]{RESET} Cleared local star state.")
    except Exception as e:
        print(f"{COL_WARN}[STAR STATE]{RESET} Failed to clear state file: {e}")

def get_streak_key_for_coin(coin):
    base_symbol = coin.get("symbol", "")
    if not isinstance(base_symbol, str):
        return ""
    return get_display_symbol(base_symbol)

def get_notification_symbol_for_coin(coin):
    base_symbol = coin.get("symbol", "")
    if not isinstance(base_symbol, str):
        return ""
    return f"{base_symbol}USDT"


def _top_red_circle_market_change_pct(market_result):
    if not isinstance(market_result, dict):
        return None
    try:
        return float(market_result.get("30m_pct", 0.0) or 0.0)
    except Exception:
        return None


def choose_top_red_circle_market_result(coin):
    """Pick the market for the compact header red-circle line.

    LBank is USDT-only. If the USDT 30m display percentage is below
    TOP_RED_CIRCLE_MIN_30M_PCT, skip only the compact header line.
    The detailed body red marker is intentionally unaffected.
    """
    if not isinstance(coin, dict):
        return None
    market_result = coin.get("usdt_market")
    change_pct = _top_red_circle_market_change_pct(market_result)
    if (
        isinstance(market_result, dict)
        and market_result.get("is_recent_pump")
        and change_pct is not None
        and change_pct >= TOP_RED_CIRCLE_MIN_30M_PCT
    ):
        return market_result
    return None


def get_top_red_circle_symbol_for_coin(coin, market_result=None):
    return get_notification_symbol_for_coin(coin)


def get_top_red_circle_change_pct(coin, market_result=None):
    market_change = _top_red_circle_market_change_pct(market_result)
    if market_change is not None:
        return market_change
    try:
        return float(coin.get("recent_pump_30m_pct", coin.get("30m_pct", 0.0)) or 0.0)
    except Exception:
        return 0.0


def load_star_state(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    today_str = get_today_str(scan_dt)
    default_state = {
        "date": today_str,
        "active_streaks": {}
    }

    if not os.path.exists(STAR_STATE_FILE):
        return default_state

    try:
        with open(STAR_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            return default_state

        stored_date = state.get("date")
        if stored_date != today_str:
            log_info(f"{COL_INFO}[STAR STATE]{RESET} Old date detected in local storage. Resetting for new day.")
            clear_star_state()
            return default_state

        if not isinstance(state.get("active_streaks"), dict):
            state["active_streaks"] = {}

        return state

    except Exception as e:
        print(f"{COL_WARN}[STAR STATE]{RESET} Failed to load state file, resetting: {e}")
        return default_state

def save_star_state(state):
    try:
        with open(STAR_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{COL_WARN}[STAR STATE]{RESET} Failed to save state file: {e}")

def update_starred_symbols(pumped_list, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state = load_star_state(scan_dt=scan_dt)
    prev_active = state.get("active_streaks", {})

    current_keys_ordered = []
    current_visible_symbols = {}
    seen = set()

    for coin in pumped_list:
        streak_key = get_streak_key_for_coin(coin)
        visible_symbol = get_notification_symbol_for_coin(coin)

        if streak_key and streak_key not in seen:
            current_keys_ordered.append(streak_key)
            current_visible_symbols[streak_key] = visible_symbol
            seen.add(streak_key)

    new_active = {}
    starred_formatted = {}
    streak_counts = {}

    for streak_key in current_keys_ordered:
        prev_info = prev_active.get(streak_key, {})
        prev_count = int(prev_info.get("count", 0) or 0)

        count = prev_count + 1

        new_active[streak_key] = {
            "count": count
        }
        streak_counts[streak_key] = count

        if count >= 2:
            visible_symbol = current_visible_symbols.get(streak_key, streak_key)
            starred_formatted[streak_key] = f"⭐ *{visible_symbol}* x{count}"

    state["date"] = get_today_str(scan_dt)
    state["active_streaks"] = new_active
    save_star_state(state)

    return starred_formatted, streak_counts

# =========================
# BALE TODAY-SENT DAILY JSON HELPERS
# =========================

def get_default_bale_today_sent_state(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    return {
        "date": get_today_str(scan_dt),
        "coins": {},
        "day_closed": False,
        "closed_at": None,
        "last_closed_slot": None
    }


def normalize_bale_today_chart_resolution(value):
    if not isinstance(value, dict):
        return None

    url = str(value.get("url") or "").strip()
    symbol = str(value.get("symbol") or "").strip()
    if not url or not symbol:
        return None

    normalized = {
        "exchange": str(value.get("exchange") or "").strip() or "-",
        "pair": str(value.get("pair") or "").strip() or "-",
        "symbol": symbol,
        "url": url,
        "verified_at": value.get("verified_at"),
    }
    return normalized


def normalize_bale_today_coin_state_entry(coin_name, entry):
    normalized_coin = normalize_bale_today_coin_name(coin_name)
    if normalized_coin is None:
        return None

    source = entry if isinstance(entry, dict) else {}
    normalized = {
        "coin": normalized_coin,
        "first_sent_at": source.get("first_sent_at"),
        "latest_sent_at": source.get("latest_sent_at"),
        "send_count": int(source.get("send_count", 0) or 0),
        "last_source": source.get("last_source"),
        "last_message_kind": source.get("last_message_kind"),
        "has_full_scan": bool(source.get("has_full_scan")),
        "chart": normalize_bale_today_chart_resolution(source.get("chart")),
    }
    return normalized


def save_bale_today_sent_state(state):
    try:
        with open(BALE_TODAY_SENT_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

        try:
            broadcast_local_event("today_pumps", build_bale_today_api_payload())
        except Exception:
            pass
    except Exception as e:
        print(f"{COL_WARN}[BALE TODAY]{RESET} Failed to save today-sent file: {e}")


def load_bale_today_sent_state(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    today_str = get_today_str(scan_dt)
    default_state = get_default_bale_today_sent_state(scan_dt)

    if not os.path.exists(BALE_TODAY_SENT_FILE):
        save_bale_today_sent_state(default_state)
        return default_state

    try:
        with open(BALE_TODAY_SENT_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            print(f"{COL_WARN}[BALE TODAY]{RESET} Invalid today-sent structure. Resetting.")
            save_bale_today_sent_state(default_state)
            return default_state

        stored_date = state.get("date")
        if stored_date != today_str:
            log_info(f"{COL_INFO}[BALE TODAY]{RESET} Old date detected ({stored_date}). Resetting for new day.")
            save_bale_today_sent_state(default_state)
            return default_state

        raw_coins = state.get("coins") if isinstance(state.get("coins"), dict) else {}
        normalized_coins = {}
        for raw_coin_name, raw_entry in raw_coins.items():
            normalized_entry = normalize_bale_today_coin_state_entry(raw_coin_name, raw_entry)
            if normalized_entry is not None:
                normalized_coins[normalized_entry["coin"]] = normalized_entry
        state["coins"] = normalized_coins

        if "day_closed" not in state:
            state["day_closed"] = False

        if "closed_at" not in state:
            state["closed_at"] = None

        if "last_closed_slot" not in state:
            state["last_closed_slot"] = None

        return state

    except Exception as e:
        print(f"{COL_WARN}[BALE TODAY]{RESET} Failed to load today-sent file, resetting: {e}")
        save_bale_today_sent_state(default_state)
        return default_state


def clear_bale_today_sent_state():
    try:
        if os.path.exists(BALE_TODAY_SENT_FILE):
            os.remove(BALE_TODAY_SENT_FILE)
            log_info(f"{COL_INFO}[BALE TODAY]{RESET} Cleared today-sent state file.")
    except Exception as e:
        print(f"{COL_WARN}[BALE TODAY]{RESET} Failed to clear today-sent state file: {e}")


def close_bale_today_sent_for_day(scan_dt=None, closed_slot_dt=None, scan_kind="full"):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state = get_default_bale_today_sent_state(scan_dt)
    state["day_closed"] = True
    state["closed_at"] = scan_dt.strftime("%Y-%m-%d %H:%M:%S")

    if closed_slot_dt is not None:
        state["last_closed_slot"] = get_scan_slot_key(closed_slot_dt, scan_kind)

    save_bale_today_sent_state(state)
    log_info(f"{COL_INFO}[BALE TODAY]{RESET} Today-sent ledger reset and marked closed at {state['closed_at']}.")


def normalize_bale_today_coin_name(symbol):
    if symbol is None:
        return None
    clean = str(symbol).strip().upper()
    if not clean:
        return None
    return get_display_symbol(clean)


def extract_today_coin_names_from_pumped_list(pumped_list):
    seen = set()
    ordered = []

    for coin in pumped_list:
        coin_name = normalize_bale_today_coin_name(coin.get("symbol", ""))
        if coin_name and coin_name not in seen:
            ordered.append(coin_name)
            seen.add(coin_name)

    return ordered


def extract_today_coin_names_from_demand_results(results_list):
    seen = set()
    ordered = []

    for result in results_list:
        if not isinstance(result, dict):
            continue
        if result.get("not_found"):
            continue
        if not result.get("had_any_30m_data"):
            continue

        coin_name = normalize_bale_today_coin_name(result.get("symbol", ""))
        if coin_name and coin_name not in seen:
            ordered.append(coin_name)
            seen.add(coin_name)

    return ordered


def record_bale_today_sent_coin_names(coin_names, sent_dt=None, source_label="UNKNOWN", message_kind="UNKNOWN"):
    if sent_dt is None:
        sent_dt = get_tehran_now()

    state = load_bale_today_sent_state(scan_dt=sent_dt)

    if state.get("day_closed"):
        log_info(f"{COL_INFO}[BALE TODAY]{RESET} Today-sent ledger is already closed for today. New entries ignored.")
        return False

    sent_at_str = sent_dt.strftime("%Y-%m-%d %H:%M:%S")
    unique_coin_names = []
    seen = set()

    for coin_name in coin_names:
        normalized = normalize_bale_today_coin_name(coin_name)
        if normalized and normalized not in seen:
            unique_coin_names.append(normalized)
            seen.add(normalized)

    if not unique_coin_names:
        return False

    state["date"] = get_today_str(sent_dt)

    is_full_scan_entry = str(message_kind or "").strip().lower() == "full_scan"

    for coin_name in unique_coin_names:
        existing = normalize_bale_today_coin_state_entry(coin_name, state["coins"].get(coin_name, {})) or {
            "coin": coin_name,
            "first_sent_at": sent_at_str,
            "latest_sent_at": sent_at_str,
            "send_count": 0,
            "last_source": None,
            "last_message_kind": None,
            "has_full_scan": False,
            "chart": None,
        }
        previous_count = int(existing.get("send_count", 0) or 0)
        first_sent_at = existing.get("first_sent_at") or sent_at_str

        state["coins"][coin_name] = {
            "coin": coin_name,
            "first_sent_at": first_sent_at,
            "latest_sent_at": sent_at_str,
            "send_count": previous_count + 1,
            "last_source": source_label,
            "last_message_kind": message_kind,
            "has_full_scan": bool(existing.get("has_full_scan")) or is_full_scan_entry,
            "chart": normalize_bale_today_chart_resolution(existing.get("chart")),
        }

    save_bale_today_sent_state(state)
    return True

def _parse_bale_today_sent_dt(value):
    if not value:
        return datetime.min
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.min


def is_bale_today_visible_entry(entry):
    if not isinstance(entry, dict):
        return False

    if entry.get("has_full_scan"):
        return True

    last_message_kind = str(entry.get("last_message_kind") or "").strip().lower()
    if last_message_kind == "demand_scan":
        return False

    return True


def get_bale_today_sorted_entries(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state = load_bale_today_sent_state(scan_dt=scan_dt)
    coins_map = state.get("coins", {}) if isinstance(state.get("coins"), dict) else {}

    sorted_entries = []
    for entry in coins_map.values():
        if not isinstance(entry, dict):
            continue
        normalized_entry = {
            "coin": entry.get("coin", "-"),
            "first_sent_at": entry.get("first_sent_at"),
            "latest_sent_at": entry.get("latest_sent_at"),
            "send_count": int(entry.get("send_count", 0) or 0),
            "last_source": entry.get("last_source"),
            "last_message_kind": entry.get("last_message_kind"),
            "has_full_scan": bool(entry.get("has_full_scan")),
            "chart": normalize_bale_today_chart_resolution(entry.get("chart")),
        }
        if is_bale_today_visible_entry(normalized_entry):
            sorted_entries.append(normalized_entry)

    sorted_entries.sort(
        key=lambda item: _parse_bale_today_sent_dt(item.get("latest_sent_at")),
        reverse=True
    )

    return state, sorted_entries


def split_today_coin_symbol_for_chart(value):
    raw = normalize_nobitex_symbol_text(value)

    if not raw:
        return {"raw": "", "base": "", "quote": ""}

    base, quote = split_nobitex_market_symbol(raw)
    if quote == "IRR":
        quote = "USDT"

    if base and quote:
        return {
            "raw": f"{base}{quote}",
            "base": base,
            "quote": quote,
        }

    base = normalize_nobitex_base_symbol(base or raw) or raw
    return {
        "raw": base,
        "base": base,
        "quote": "",
    }


def build_bitycle_chart_candidates_for_coin(coin_value):
    parts = split_today_coin_symbol_for_chart(coin_value)
    base = str(parts.get("base") or "").strip().upper()
    if not base:
        return []

    candidates = [
        {"exchange": "LBank", "pair": f"{base}USDT", "symbol": f"lbank_spot:{base}USDT"},
        {"exchange": "Binance", "pair": f"{base}USDT", "symbol": f"binance_spot:{base}USDT"},
        {"exchange": "Bybit", "pair": f"{base}USDT", "symbol": f"bybit_spot:{base}USDT"},
        {"exchange": "MEXC", "pair": f"{base}USDT", "symbol": f"mexc_spot:{base}USDT"},
    ]

    return [
        {
            "exchange": item["exchange"],
            "pair": item["pair"],
            "symbol": item["symbol"],
            "url": f"https://app.bitycle.com/terminal?symbol={parse.quote(item['symbol'])}",
        }
        for item in candidates
    ]


def get_today_chart_resolution_cache_key(candidates):
    return "|".join(str(item.get("symbol") or "") for item in candidates if isinstance(item, dict))


def get_today_chart_resolution_from_cache(cache_key):
    if not cache_key:
        return None, False

    now_mono = time.monotonic()
    with TODAY_CHART_RESOLUTION_CACHE_LOCK:
        entry = TODAY_CHART_RESOLUTION_CACHE.get(cache_key)
        if not isinstance(entry, dict):
            return None, False

        stored_mono = float(entry.get("stored_mono", 0.0) or 0.0)
        if stored_mono <= 0 or (now_mono - stored_mono) > TODAY_CHART_RESOLUTION_CACHE_TTL:
            TODAY_CHART_RESOLUTION_CACHE.pop(cache_key, None)
            return None, False

        resolved = entry.get("resolved")
        if isinstance(resolved, dict):
            return dict(resolved), True
        return None, True


def save_today_chart_resolution_to_cache(cache_key, resolved_candidate):
    if not cache_key:
        return

    payload = {
        "stored_mono": time.monotonic(),
        "resolved": dict(resolved_candidate) if isinstance(resolved_candidate, dict) else None,
    }

    with TODAY_CHART_RESOLUTION_CACHE_LOCK:
        TODAY_CHART_RESOLUTION_CACHE[cache_key] = payload


def extract_bitycle_symbol_from_url(url):
    safe_url = str(url or "").strip()
    if not safe_url:
        return ""
    try:
        parsed_url = parse.urlsplit(safe_url)
        query_params = parse.parse_qs(parsed_url.query)
        symbol_value = query_params.get("symbol", [""])[0]
        return parse.unquote(str(symbol_value or "")).strip()
    except Exception:
        return ""


def probe_today_chart_candidate(candidate, timeout_seconds=TODAY_CHART_PROBE_TIMEOUT_SECONDS):
    if not isinstance(candidate, dict):
        return False

    safe_url = str(candidate.get("url") or "").strip()
    expected_symbol = str(candidate.get("symbol") or "").strip()
    if not safe_url or not expected_symbol:
        return False

    try:
        timeout_value = max(0.8, float(timeout_seconds or TODAY_CHART_PROBE_TIMEOUT_SECONDS))
    except Exception:
        timeout_value = TODAY_CHART_PROBE_TIMEOUT_SECONDS

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Mobile Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
    }

    for method in ("HEAD", "GET"):
        try:
            req = request.Request(safe_url, headers=headers, method=method)
        except TypeError:
            req = request.Request(safe_url, headers=headers)
            req.get_method = lambda method_name=method: method_name

        try:
            with request.urlopen(req, timeout=timeout_value) as resp:
                status_code = 0
                try:
                    status_code = int(getattr(resp, "status", 0) or 0)
                except Exception:
                    status_code = 0
                if not status_code:
                    try:
                        status_code = int(resp.getcode() or 0)
                    except Exception:
                        status_code = 0
                if status_code and status_code >= 400:
                    continue

                final_url = ""
                try:
                    final_url = str(resp.geturl() or "").strip()
                except Exception:
                    final_url = ""

                final_symbol = extract_bitycle_symbol_from_url(final_url)
                if final_symbol and final_symbol == expected_symbol:
                    return True

                if method == "GET":
                    try:
                        body_text = resp.read(65536).decode("utf-8", errors="ignore")
                    except Exception:
                        body_text = ""
                    if expected_symbol in body_text or parse.quote(expected_symbol) in body_text:
                        return True
        except error.HTTPError:
            continue
        except (error.URLError, TimeoutError, ValueError):
            continue
        except Exception:
            continue

    return False


def resolve_working_today_chart_candidate(coin_value):
    candidates = build_bitycle_chart_candidates_for_coin(coin_value)
    if not candidates:
        return None

    cache_key = get_today_chart_resolution_cache_key(candidates)
    cached_candidate, cache_hit = get_today_chart_resolution_from_cache(cache_key)
    if cache_hit:
        return cached_candidate

    resolved_candidate = None
    for candidate in candidates:
        if probe_today_chart_candidate(candidate, TODAY_CHART_PROBE_TIMEOUT_SECONDS):
            resolved_candidate = dict(candidate)
            resolved_candidate["verified_at"] = get_tehran_now().strftime("%Y-%m-%d %H:%M:%S")
            break

    save_today_chart_resolution_to_cache(cache_key, resolved_candidate)
    return resolved_candidate


def refresh_bale_today_chart_resolutions(scan_dt=None, coins_to_refresh=None, force_refresh=False):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state = load_bale_today_sent_state(scan_dt=scan_dt)
    coins_map = state.get("coins", {}) if isinstance(state.get("coins"), dict) else {}
    if not coins_map:
        return {
            "attempted": 0,
            "resolved": 0,
            "updated": 0,
        }

    requested_coins = []
    seen = set()

    if coins_to_refresh:
        for coin_name in coins_to_refresh:
            normalized = normalize_bale_today_coin_name(coin_name)
            if normalized and normalized in coins_map and normalized not in seen:
                requested_coins.append(normalized)
                seen.add(normalized)
    else:
        ordered_entries = sorted(
            coins_map.values(),
            key=lambda item: _parse_bale_today_sent_dt(item.get("latest_sent_at")),
            reverse=True,
        )
        for entry in ordered_entries:
            coin_name = normalize_bale_today_coin_name(entry.get("coin"))
            if coin_name and coin_name not in seen:
                requested_coins.append(coin_name)
                seen.add(coin_name)

    targets = []
    for coin_name in requested_coins:
        existing_entry = coins_map.get(coin_name, {})
        existing_chart = normalize_bale_today_chart_resolution(existing_entry.get("chart"))
        if force_refresh or existing_chart is None:
            targets.append(coin_name)

    if not targets:
        return {
            "attempted": 0,
            "resolved": 0,
            "updated": 0,
        }

    results_by_coin = {}
    work_queue = queue.Queue()
    for coin_name in targets:
        work_queue.put(coin_name)

    def worker():
        while True:
            try:
                current_coin = work_queue.get_nowait()
            except queue.Empty:
                break
            try:
                results_by_coin[current_coin] = resolve_working_today_chart_candidate(current_coin)
            except Exception:
                results_by_coin[current_coin] = None
            finally:
                work_queue.task_done()

    worker_count = min(TODAY_CHART_PRECOMPUTE_WORKERS, len(targets))
    workers = []
    for _ in range(max(1, worker_count)):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        workers.append(thread)

    work_queue.join()

    updated_count = 0
    resolved_count = 0
    verified_at = scan_dt.strftime("%Y-%m-%d %H:%M:%S")

    for coin_name in targets:
        resolved_candidate = results_by_coin.get(coin_name)
        if isinstance(resolved_candidate, dict):
            resolved_count += 1
            normalized_chart = normalize_bale_today_chart_resolution({
                **resolved_candidate,
                "verified_at": resolved_candidate.get("verified_at") or verified_at,
            })
            if normalized_chart is not None and coin_name in coins_map:
                coins_map[coin_name]["chart"] = normalized_chart
                updated_count += 1

    state["coins"] = coins_map
    save_bale_today_sent_state(state)
    return {
        "attempted": len(targets),
        "resolved": resolved_count,
        "updated": updated_count,
    }


def ensure_bale_today_chart_resolutions(scan_dt=None, coins_to_refresh=None, force_refresh=False):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    with TODAY_CHART_PRECOMPUTE_LOCK:
        return refresh_bale_today_chart_resolutions(
            scan_dt=scan_dt,
            coins_to_refresh=coins_to_refresh,
            force_refresh=force_refresh,
        )


def precompute_bale_today_chart_resolutions_if_idle(scan_dt=None, coins_to_refresh=None, force_refresh=False):
    global TODAY_CHART_PRECOMPUTE_RUNNING

    if scan_dt is None:
        scan_dt = get_tehran_now()

    with TODAY_CHART_PRECOMPUTE_LOCK:
        if TODAY_CHART_PRECOMPUTE_RUNNING:
            return False
        TODAY_CHART_PRECOMPUTE_RUNNING = True

    def runner():
        global TODAY_CHART_PRECOMPUTE_RUNNING
        try:
            ensure_bale_today_chart_resolutions(
                scan_dt=scan_dt,
                coins_to_refresh=coins_to_refresh,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            print(f"{COL_WARN}[BALE TODAY]{RESET} Chart precompute failed: {exc}")
            traceback.print_exc()
        finally:
            with TODAY_CHART_PRECOMPUTE_LOCK:
                TODAY_CHART_PRECOMPUTE_RUNNING = False

    threading.Thread(target=runner, daemon=True).start()
    return True

def build_bale_today_chart_button_text(entry):
    coin_name = str((entry or {}).get("coin") or "-").strip() or "-"
    send_count = int((entry or {}).get("send_count", 0) or 0)
    if send_count > 1:
        return f"{coin_name} • {send_count}x"
    return coin_name


def chunk_list(items, chunk_size):
    safe_items = list(items or [])
    safe_chunk_size = max(1, int(chunk_size or 1))
    return [safe_items[idx:idx + safe_chunk_size] for idx in range(0, len(safe_items), safe_chunk_size)]


def get_today_entry_window_index(entry_dt, now_dt):
    if not isinstance(entry_dt, datetime):
        return 0
    diff_seconds = max(0.0, (now_dt - entry_dt).total_seconds())
    window_seconds = max(1, int(TODAY_CHART_WINDOW_HOURS * 3600))
    return int(diff_seconds // window_seconds)


def get_bale_today_window_title(window_index, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    if int(window_index or 0) <= 0:
        total_today = len(get_bale_today_sorted_entries(scan_dt=scan_dt)[1])
        return (
            f"کوین‌های امروز {scan_dt.strftime('%Y-%m-%d')} | {to_persian_digits(total_today)}\n\n"
            "روی هر دکمه بزنید تا چارت باز شود.\n\n"
            "توجه: هنگام باز شدن چارت، برای استفاده کامل از امکانات و اندیکاتورها وارد حساب کاربری بایتیکل خود شوید."
        )

    hours_ago = int(window_index) * int(TODAY_CHART_WINDOW_HOURS)
    return f"کوین های {to_persian_digits(hours_ago)} ساعت پیش"


def build_bale_today_grouped_windows(sorted_entries, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    windows = {}
    for entry in sorted_entries:
        entry_dt = _parse_bale_today_sent_dt(entry.get("latest_sent_at"))
        if entry_dt == datetime.min:
            continue
        window_index = get_today_entry_window_index(entry_dt, scan_dt)
        windows.setdefault(window_index, []).append(entry)

    ordered_windows = []
    for window_index in sorted(windows.keys()):
        entries = sorted(
            windows.get(window_index, []),
            key=lambda item: _parse_bale_today_sent_dt(item.get("latest_sent_at")),
            reverse=True,
        )
        grouped = []
        grouped_map = {}
        for entry in entries:
            entry_dt = _parse_bale_today_sent_dt(entry.get("latest_sent_at"))
            minute_label = entry_dt.strftime("%H:%M") if entry_dt != datetime.min else "-"
            if minute_label not in grouped_map:
                grouped_map[minute_label] = {
                    "minute_label": minute_label,
                    "entries": [],
                }
                grouped.append(grouped_map[minute_label])
            grouped_map[minute_label]["entries"].append(entry)
        ordered_windows.append({
            "window_index": window_index,
            "title": get_bale_today_window_title(window_index, scan_dt=scan_dt),
            "groups": grouped,
            "entries": entries,
        })
    return ordered_windows


def split_bale_today_window_into_chunks(window_payload):
    groups = list((window_payload or {}).get("groups") or [])
    if not groups:
        return []

    chunks = []
    current_groups = []
    current_button_count = 0
    max_buttons = max(1, int(TODAY_CHART_MAX_BUTTONS_PER_MESSAGE or 1))

    def flush_current():
        nonlocal current_groups, current_button_count
        if current_groups:
            chunks.append({
                "window_index": window_payload.get("window_index", 0),
                "title": window_payload.get("title") or "",
                "groups": current_groups,
            })
            current_groups = []
            current_button_count = 0

    for group in groups:
        group_entries = list(group.get("entries") or [])
        if not group_entries:
            continue

        max_entries_for_group_chunk = max(1, max_buttons - 1)
        group_entry_chunks = chunk_list(group_entries, max_entries_for_group_chunk)

        for entry_chunk in group_entry_chunks:
            synthetic_group = {
                "minute_label": group.get("minute_label") or "-",
                "entries": entry_chunk,
            }
            synthetic_button_count = 1 + len(entry_chunk)

            if current_groups and (current_button_count + synthetic_button_count) > max_buttons:
                flush_current()

            current_groups.append(synthetic_group)
            current_button_count += synthetic_button_count

    flush_current()
    return chunks


def build_bale_today_message_chunks(sorted_entries, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    windows = build_bale_today_grouped_windows(sorted_entries, scan_dt=scan_dt)
    chunks = []
    for window_payload in windows:
        chunks.extend(split_bale_today_window_into_chunks(window_payload))
    return chunks

def build_bale_today_summary_message(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state, sorted_entries = get_bale_today_sorted_entries(scan_dt=scan_dt)

    if not sorted_entries:
        if state.get("day_closed"):
            return (
                "در حال حاضر هیچ کوینی برای امروز ثبت نشده است.\n"
                "دفتر امروز بعد از پاکسازی ساعت ۲۲:۳۰ بسته شده است."
            )
        return "هنوز امروز هیچ کوینی ارسال نشده است."

    lines = [
        f"کوین‌های ارسال‌شده امروز ({state.get('date')})",
        "جدیدترین‌ها در ابتدا:",
        ""
    ]

    for idx, entry in enumerate(sorted_entries, 1):
        coin_name = entry.get("coin", "-")
        latest_sent_at = entry.get("latest_sent_at", "-")
        send_count = int(entry.get("send_count", 0) or 0)
        time_part = latest_sent_at.split(" ", 1)[1] if " " in latest_sent_at else latest_sent_at
        lines.append(f"{idx}. *{coin_name}* — {time_part} (ارسال {send_count} بار)")

    lines.append("")
    lines.append(f"تعداد کوین‌های یکتا امروز: {len(sorted_entries)}")

    if state.get("day_closed"):
        lines.append("ثبت امروز بسته شده است.")

    return "\n".join(lines).strip()

def build_bale_today_api_payload(scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    state, sorted_entries = get_bale_today_sorted_entries(scan_dt=scan_dt)

    return {
        "ok": True,
        "source": BALE_TODAY_SENT_FILE,
        "generated_at": scan_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "refresh_seconds": 60,
        "date": state.get("date"),
        "day_closed": bool(state.get("day_closed")),
        "closed_at": state.get("closed_at"),
        "last_closed_slot": state.get("last_closed_slot"),
        "unique_coins": len(sorted_entries),
        "coins": sorted_entries,
        "today_pumps": sorted_entries,
        "summary_text": build_bale_today_summary_message(scan_dt=scan_dt)
    }

# =========================
# BASIC HTTP UTILITIES W/ RETRY
# =========================

def close_native_http_connection():
    conn = getattr(NATIVE_HTTP_LOCAL, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    NATIVE_HTTP_LOCAL.conn = None
    NATIVE_HTTP_LOCAL.host = None


def _apply_native_http_timeout(conn, timeout):
    try:
        timeout_value = float(timeout)
    except Exception:
        timeout_value = float(HTTP_TIMEOUT)
    if timeout_value <= 0:
        timeout_value = float(HTTP_TIMEOUT)

    try:
        conn.timeout = timeout_value
    except Exception:
        pass

    sock = getattr(conn, "sock", None)
    if sock is not None:
        try:
            sock.settimeout(timeout_value)
        except Exception:
            pass

    return timeout_value


def get_native_http_connection(timeout=HTTP_TIMEOUT):
    parsed = parse.urlsplit(BASE_URL)
    if parsed.scheme != "https":
        raise ValueError(f"Only HTTPS BASE_URL is supported by native keep-alive GET: {BASE_URL}")

    host = parsed.netloc
    conn = getattr(NATIVE_HTTP_LOCAL, "conn", None)
    old_host = getattr(NATIVE_HTTP_LOCAL, "host", None)

    if conn is not None and old_host == host:
        _apply_native_http_timeout(conn, timeout)
        return conn

    close_native_http_connection()
    context = ssl.create_default_context()
    conn = http.client.HTTPSConnection(
        host,
        timeout=float(timeout or HTTP_TIMEOUT),
        context=context,
    )
    NATIVE_HTTP_LOCAL.conn = conn
    NATIVE_HTTP_LOCAL.host = host
    _apply_native_http_timeout(conn, timeout)
    return conn


def build_native_http_target(path, params=None):
    path = str(path or "").strip()
    if not path.startswith("/"):
        path = "/" + path
    if params:
        return path + "?" + parse.urlencode(params)
    return path


def build_native_http_log_url(target):
    return BASE_URL.rstrip("/") + str(target)


def _native_http_sleep_from_body(raw_body, attempt, backoff_factor=HTTP_BACKOFF_FACTOR, status_code=None):
    sleep_seconds = None
    if status_code == 429 and raw_body:
        try:
            text = raw_body.decode("utf-8", errors="replace") if isinstance(raw_body, bytes) else str(raw_body)
            payload = json.loads(text) if text else {}
            if isinstance(payload, dict):
                backoff_value = payload.get("backOff", payload.get("backoff"))
                if backoff_value is not None:
                    sleep_seconds = float(backoff_value)
        except Exception:
            sleep_seconds = None

    if sleep_seconds is None:
        try:
            sleep_seconds = float(backoff_factor) ** int(attempt)
        except Exception:
            sleep_seconds = 1.0

    if not math.isfinite(sleep_seconds) or sleep_seconds < 0:
        sleep_seconds = 1.0

    sleep_seconds = min(float(sleep_seconds), float(HTTP_BACKOFF_CAP_SECONDS))
    time.sleep(sleep_seconds + random.uniform(0.05, 0.30))


def _is_stale_keepalive_error(exc, reason=None):
    """
    Return True only for stale keep-alive socket failures. This allows one
    reconnect-once redo without changing normal retry or timeout settings.
    """
    stale_types = (
        http.client.RemoteDisconnected,
        http.client.CannotSendRequest,
        http.client.ResponseNotReady,
        http.client.BadStatusLine,
    )
    if isinstance(exc, stale_types):
        return True

    text = f"{reason if reason is not None else ''} {exc}".lower()
    stale_phrases = (
        "remote end closed connection without response",
        "remote disconnected",
        "connection reset by peer",
        "broken pipe",
        "cannot send request",
        "response not ready",
        "bad status line",
    )
    return any(phrase in text for phrase in stale_phrases)


def _native_http_get_json_once(target, url, headers, timeout):
    """
    Single native keep-alive GET attempt. Used by http_get_json and the
    reconnect-once path after LBank closes an idle/reused socket.
    """
    conn = get_native_http_connection(timeout=timeout)
    conn.request("GET", target, body=None, headers=headers)
    resp = conn.getresponse()
    raw_body = resp.read()
    status_code = int(resp.status)
    connection_header = str(resp.getheader("Connection") or "").lower()
    should_close = connection_header == "close"

    if status_code >= 400:
        return {
            "ok": False,
            "status_code": status_code,
            "raw_body": raw_body,
            "should_close": should_close,
            "data": None,
        }

    if should_close:
        close_native_http_connection()

    if not raw_body:
        return {
            "ok": True,
            "status_code": status_code,
            "raw_body": raw_body,
            "should_close": should_close,
            "data": None,
        }

    data = raw_body.decode("utf-8", errors="replace")
    if not data:
        return {
            "ok": True,
            "status_code": status_code,
            "raw_body": raw_body,
            "should_close": should_close,
            "data": None,
        }

    return {
        "ok": True,
        "status_code": status_code,
        "raw_body": raw_body,
        "should_close": should_close,
        "data": json.loads(data),
    }


def _native_http_reconnect_once(target, url, headers, timeout):
    close_native_http_connection()
    response = _native_http_get_json_once(target, url, headers, timeout)

    if not response.get("ok"):
        status_code = response.get("status_code")
        if not _record_suppressed_ohlc_prefetch_http_warning(url, f"HTTP {status_code}"):
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} reconnect-once failed for {url} -> HTTP {status_code}")
        if response.get("should_close") or _is_retryable_http_code(status_code):
            close_native_http_connection()
        return None

    return response.get("data")


def http_get_json(path, params=None, timeout=HTTP_TIMEOUT, retries=HTTP_RETRY_COUNT, backoff_factor=HTTP_BACKOFF_FACTOR):
    target = build_native_http_target(path, params=params)
    url = build_native_http_log_url(target)
    attempts = _http_attempt_count(retries)
    headers = get_json_headers({"Connection": "keep-alive"})

    for attempt in range(1, attempts + 1):
        try:
            response = _native_http_get_json_once(target, url, headers, timeout)

            if not response.get("ok"):
                status_code = response.get("status_code")
                raw_body = response.get("raw_body")
                should_close = bool(response.get("should_close"))

                if not _record_suppressed_ohlc_prefetch_http_warning(url, f"HTTP {status_code}"):
                    print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for {url} -> HTTP {status_code}")
                if should_close or _is_retryable_http_code(status_code):
                    close_native_http_connection()
                if attempt >= attempts or not _is_retryable_http_code(status_code):
                    print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for {url}.")
                    return None
                _native_http_sleep_from_body(raw_body, attempt, backoff_factor=backoff_factor, status_code=status_code)
                continue

            return response.get("data")

        except json.JSONDecodeError as e:
            print(f"{COL_WARN}[HTTP ERROR]{RESET} Invalid JSON from {url} -> {e}")
            return None

        except (
            http.client.HTTPException,
            OSError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
        ) as e:
            reason = _http_error_reason(e)
            stale_keepalive = _is_stale_keepalive_error(e, reason=reason)
            close_native_http_connection()
            if not _record_suppressed_ohlc_prefetch_http_warning(url, reason):
                print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for {url} -> {reason}")

            # This does not increase normal retries. It only repairs a reused
            # keep-alive socket that LBank closed remotely.
            if stale_keepalive:
                try:
                    return _native_http_reconnect_once(target, url, headers, timeout)
                except json.JSONDecodeError as e2:
                    print(f"{COL_WARN}[HTTP ERROR]{RESET} reconnect-once invalid JSON from {url} -> {e2}")
                    return None
                except Exception as e2:
                    close_native_http_connection()
                    reason2 = _http_error_reason(e2)
                    if not _record_suppressed_ohlc_prefetch_http_warning(url, reason2):
                        print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} reconnect-once failed for {url} -> {reason2}")
                    return None

            if attempt >= attempts:
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for {url}.")
                return None
            _native_http_sleep_from_body(None, attempt, backoff_factor=backoff_factor)

        except Exception as e:
            close_native_http_connection()
            print(f"{COL_WARN}[ERROR]{RESET} {url} -> {e}")
            traceback.print_exc()
            return None

    return None

def http_get_json_absolute(url, params=None, timeout=2, retries=1, backoff_factor=2):
    if params:
        query = parse.urlencode(params)
        final_url = url + ("&" if "?" in url else "?") + query
    else:
        final_url = url

    req = request.Request(final_url, headers=get_json_headers())
    attempts = _http_attempt_count(retries)

    for attempt in range(1, attempts + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="replace")
                if not data:
                    return None
                return json.loads(data)

        except error.HTTPError as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for {final_url} -> {reason}")
            code = _http_error_code(e)
            if attempt >= attempts or not _is_retryable_http_code(code):
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for {final_url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except (error.URLError, TimeoutError, socket.timeout, ssl.SSLError) as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for {final_url} -> {reason}")
            if attempt >= attempts:
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for {final_url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except json.JSONDecodeError as e:
            print(f"{COL_WARN}[HTTP ERROR]{RESET} Invalid JSON from {final_url} -> {e}")
            return None

        except Exception as e:
            print(f"{COL_WARN}[ERROR]{RESET} {final_url} -> {e}")
            traceback.print_exc()
            return None

    return None


def http_post_json_absolute(url, payload, timeout=15, retries=3, backoff_factor=2):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=get_post_json_headers(), method="POST")
    attempts = _http_attempt_count(retries)

    for attempt in range(1, attempts + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return None
                return json.loads(raw)

        except error.HTTPError as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for POST {url} -> {reason}")
            code = _http_error_code(e)
            if attempt >= attempts or not _is_retryable_http_code(code):
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for POST {url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except (error.URLError, TimeoutError, socket.timeout, ssl.SSLError) as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for POST {url} -> {reason}")
            if attempt >= attempts:
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for POST {url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except json.JSONDecodeError as e:
            print(f"{COL_WARN}[HTTP ERROR]{RESET} Invalid JSON from POST {url} -> {e}")
            return None

        except Exception as e:
            print(f"{COL_WARN}[ERROR]{RESET} POST {url} -> {e}")
            traceback.print_exc()
            return None

    return None


def http_post_multipart_absolute(url, fields=None, files=None, timeout=30, retries=3, backoff_factor=2):
    fields = fields or {}
    files = files or []
    attempts = _http_attempt_count(retries)

    for attempt in range(1, attempts + 1):
        boundary = "----BaleBoundary" + uuid.uuid4().hex
        body = bytearray()

        try:
            for name, value in fields.items():
                body.extend(f"--{boundary}\r\n".encode("utf-8"))
                body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
                body.extend(str(value).encode("utf-8"))
                body.extend(b"\r\n")

            for item in files:
                field_name = str(item.get("field_name") or "").strip()
                file_path = str(item.get("file_path") or "").strip()
                if not field_name or not file_path:
                    continue

                filename = item.get("filename") or os.path.basename(file_path)
                content_type = item.get("content_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream"

                with open(file_path, "rb") as fh:
                    file_bytes = fh.read()

                body.extend(f"--{boundary}\r\n".encode("utf-8"))
                body.extend(f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode("utf-8"))
                body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
                body.extend(file_bytes)
                body.extend(b"\r\n")

            body.extend(f"--{boundary}--\r\n".encode("utf-8"))

            req = request.Request(url, data=bytes(body), headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(len(body)),
                "User-Agent": NOBITEX_USER_AGENT,
            }, method="POST")

            with request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return None
                return json.loads(raw)

        except error.HTTPError as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for MULTIPART POST {url} -> {reason}")
            code = _http_error_code(e)
            if attempt >= attempts or not _is_retryable_http_code(code):
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for MULTIPART POST {url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except (error.URLError, TimeoutError, socket.timeout, ssl.SSLError) as e:
            reason = _http_error_reason(e)
            print(f"{COL_WARN}[HTTP/SSL WARN]{RESET} Attempt {attempt}/{attempts} failed for MULTIPART POST {url} -> {reason}")
            if attempt >= attempts:
                print(f"{COL_WARN}[HTTP ERROR]{RESET} Max retries reached for MULTIPART POST {url}.")
                return None
            time.sleep(_retry_sleep_seconds(e, attempt, backoff_factor))

        except json.JSONDecodeError as e:
            print(f"{COL_WARN}[HTTP ERROR]{RESET} Invalid JSON from MULTIPART POST {url} -> {e}")
            return None

        except Exception as e:
            print(f"{COL_WARN}[ERROR]{RESET} MULTIPART POST {url} -> {e}")
            traceback.print_exc()
            return None

    return None


def send_bale_document(chat_id, file_path, caption=None, reply_markup=None, message_kind="document_outgoing", extra_meta=None):
    normalized_chat_id = str(chat_id).strip()
    normalized_file_path = os.path.abspath(file_path)

    if not os.path.isfile(normalized_file_path):
        result = {
            "ok": False,
            "description": f"File not found: {normalized_file_path}",
        }
        bale_ok = False
    else:
        fields = {"chat_id": normalized_chat_id}
        if caption:
            fields["caption"] = caption
        if reply_markup is not None:
            fields["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

        result = http_post_multipart_absolute(
            BALE_SEND_DOCUMENT_URL,
            fields=fields,
            files=[
                {
                    "field_name": "document",
                    "file_path": normalized_file_path,
                    "filename": os.path.basename(normalized_file_path),
                }
            ],
            timeout=60,
        )
        bale_ok = bool(result and result.get("ok"))

    message_text = os.path.basename(normalized_file_path)
    message_meta = {
        "message_kind": str(message_kind or "document_outgoing"),
        "bale_ok": bale_ok,
        "fallback_used": not bale_ok,
        "original_chat_id": normalized_chat_id,
        "result": result,
        "custom_payload": True,
        "api_url": BALE_SEND_DOCUMENT_URL,
        "file_path": normalized_file_path,
        "caption": caption,
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(normalized_chat_id, message_meta)
    transport_name = "bale" if bale_ok else "local_fallback"

    append_local_chat_message(
        chat_id=normalized_chat_id,
        text=message_text,
        direction="outgoing",
        transport=transport_name,
        source="bot",
        meta=message_meta,
    )

    if should_mirror_to_local_web:
        append_local_web_reply_mirror(
            original_chat_id=normalized_chat_id,
            text=message_text,
            direction="outgoing",
            transport=transport_name,
            source="bot",
            meta=message_meta,
        )

    return bale_ok, result


def send_auto_trading_files_bundle(chat_id):
    send_bale_message(chat_id, AUTO_TRADING_FILES_MESSAGE_TEXT, message_kind="auto_trading_files_text")

    ok_xpi, result_xpi = send_bale_document(
        chat_id,
        AUTO_TRADING_XPI_PATH,
        message_kind="auto_trading_xpi",
    )
    if not ok_xpi:
        send_bale_message(
            chat_id,
            f"ارسال فایل violentmonkey.zip ناموفق بود.\n{result_xpi}",
            message_kind="auto_trading_xpi_failed",
        )

    ok_txt, result_txt = send_bale_document(
        chat_id,
        AUTO_TRADING_TXT_PATH,
        message_kind="auto_trading_txt",
    )
    if not ok_txt:
        send_bale_message(
            chat_id,
            f"ارسال فایل ناموفق بود.\n{result_txt}",
            message_kind="auto_trading_txt_failed",
        )


def split_bale_message_text(message_text, max_chars=BALE_TEXT_CHUNK_MAX_CHARS):
    text = "" if message_text is None else str(message_text)
    try:
        limit = int(max_chars or BALE_TEXT_CHUNK_MAX_CHARS)
    except Exception:
        limit = BALE_TEXT_CHUNK_MAX_CHARS

    if limit < 1:
        limit = BALE_TEXT_CHUNK_MAX_CHARS

    if len(text) <= limit:
        return [text]

    chunks = []
    remaining = text

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n\n", 0, limit + 1)
        split_len = 2

        if split_at <= 0:
            split_at = remaining.rfind("\n", 0, limit + 1)
            split_len = 1
        if split_at <= 0:
            split_at = remaining.rfind(" ", 0, limit + 1)
            split_len = 1
        if split_at <= 0:
            split_at = limit
            split_len = 0

        chunk = remaining[:split_at]
        if not chunk:
            chunk = remaining[:limit]
            split_at = len(chunk)
            split_len = 0

        chunks.append(chunk)
        remaining = remaining[split_at + split_len:]

    return chunks or [text]


def send_bale_message_raw(chat_id, message_text):
    normalized_chat_id = str(chat_id).strip()
    text_chunks = split_bale_message_text(message_text)
    chunk_results = []
    bale_ok = True

    for chunk_text in text_chunks:
        payload = {
            "chat_id": normalized_chat_id,
            "text": chunk_text,
            "parse_mode": "Markdown"
        }

        result = http_post_json_absolute(BALE_API_URL, payload, timeout=15)
        chunk_ok = bool(result and result.get("ok"))
        if not chunk_ok:
            bale_ok = False

        chunk_results.append({
            "ok": chunk_ok,
            "text": chunk_text,
            "result": result,
        })

    return {
        "ok": bale_ok,
        "chat_id": normalized_chat_id,
        "result": chunk_results[-1]["result"] if chunk_results else None,
        "chunk_count": len(chunk_results),
        "chunks": chunk_results,
    }


def log_bale_delivery_summary(success_count, total_count, failed_count=0, local_web_backup=False, chunk_count=None, label="[BALE]"):
    try:
        success_count = int(success_count or 0)
    except Exception:
        success_count = 0
    try:
        total_count = int(total_count or 0)
    except Exception:
        total_count = 0
    try:
        failed_count = int(failed_count or 0)
    except Exception:
        failed_count = 0

    if total_count <= 0:
        return

    failed_part = f" | failed={failed_count}" if failed_count else ""
    local_part = " | local_web_backup=yes" if local_web_backup else ""
    chunk_part = f" | chunks={int(chunk_count)}" if chunk_count is not None else ""
    color = COL_POS if success_count > 0 else COL_WARN
    log_info(
        f"{color}{label}{RESET} Delivery summary: sent to {success_count}/{total_count} Bale chat ids"
        f"{failed_part}{local_part}{chunk_part}"
    )


def send_bale_message_sync(chat_id, message_text, message_kind="generic_outgoing", extra_meta=None):
    send_result = send_bale_message_raw(chat_id, message_text)
    bale_ok = send_result["ok"]
    normalized_chat_id = send_result["chat_id"]

    message_meta = {
        "message_kind": str(message_kind or "generic_outgoing"),
        "bale_ok": bale_ok,
        "fallback_used": not bale_ok,
        "original_chat_id": normalized_chat_id,
        "result": send_result.get("result")
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(normalized_chat_id, message_meta)
    transport_name = "bale" if bale_ok else "local_fallback"

    append_local_chat_message(
        chat_id=normalized_chat_id,
        text=message_text,
        direction="outgoing",
        transport=transport_name,
        source="bot",
        meta=message_meta
    )

    if should_mirror_to_local_web:
        append_local_web_reply_mirror(
            original_chat_id=normalized_chat_id,
            text=message_text,
            direction="outgoing",
            transport=transport_name,
            source="bot",
            meta=message_meta,
        )

    if bale_ok:
        log_bale_delivery_summary(
            1,
            1,
            failed_count=0,
            local_web_backup=bool(should_mirror_to_local_web),
            chunk_count=send_result.get("chunk_count"),
        )
        return True

    print(f"{COL_WARN}[BALE] Failed sending message to {normalized_chat_id}: {send_result.get('result')}{RESET}")
    print(f"{COL_WARN}[BALE] Message was still written to local live chat fallback for {normalized_chat_id}.{RESET}")
    log_bale_delivery_summary(
        0,
        1,
        failed_count=1,
        local_web_backup=True,
        chunk_count=send_result.get("chunk_count"),
    )
    return True

def send_bale_message(chat_id, message_text, message_kind="generic_outgoing", extra_meta=None):
    ensure_bale_outgoing_worker_started()
    BALE_OUTGOING_QUEUE.put({
        "kind": "single_text",
        "chat_id": chat_id,
        "message_text": message_text,
        "message_kind": message_kind,
        "extra_meta": extra_meta if isinstance(extra_meta, dict) else extra_meta,
    })
    return True


def send_bale_message_to_many_sync(chat_ids, message_text, store_local_web_once=False, local_web_chat_id=LOCAL_WEB_DEFAULT_CHAT_ID, message_kind="generic_outgoing", extra_meta=None):
    normalized_chat_ids = []
    seen = set()

    for chat_id in chat_ids:
        normalized_chat_id = str(chat_id).strip()
        if not normalized_chat_id or normalized_chat_id in seen:
            continue
        normalized_chat_ids.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    success_chat_ids = []
    failed_chat_ids = []
    per_chat_results = {}

    for chat_id in normalized_chat_ids:
        send_result = send_bale_message_raw(chat_id, message_text)
        per_chat_results[chat_id] = send_result
        if send_result["ok"]:
            success_chat_ids.append(chat_id)
        else:
            failed_chat_ids.append(chat_id)
            print(f"{COL_WARN}[BALE] Failed sending message to {chat_id}: {send_result.get('result')}{RESET}")

    log_bale_delivery_summary(
        len(success_chat_ids),
        len(normalized_chat_ids),
        failed_count=len(failed_chat_ids),
        local_web_backup=bool(store_local_web_once),
    )

    if store_local_web_once:
        message_meta = {
            "message_kind": str(message_kind or "generic_outgoing"),
            "original_chat_ids": normalized_chat_ids,
            "bale_success_chat_ids": success_chat_ids,
            "bale_failed_chat_ids": failed_chat_ids,
            "saved_as_local_web": True,
            "duplicate_storage_prevented": True,
            "per_chat_results": per_chat_results,
        }
        if isinstance(extra_meta, dict):
            message_meta.update(extra_meta)

        message_meta, _ = enrich_outgoing_message_meta_for_reply_context(local_web_chat_id, message_meta)

        append_local_chat_message(
            chat_id=local_web_chat_id,
            text=message_text,
            direction="outgoing",
            transport="bale+web" if success_chat_ids else "local_fallback",
            source="bot",
            meta=message_meta
        )
    else:
        for chat_id in normalized_chat_ids:
            send_result = per_chat_results.get(chat_id, {"ok": False, "result": None})
            message_meta = {
                "message_kind": str(message_kind or "generic_outgoing"),
                "bale_ok": bool(send_result.get("ok")),
                "fallback_used": not bool(send_result.get("ok")),
                "original_chat_id": chat_id,
                "result": send_result.get("result")
            }
            if isinstance(extra_meta, dict):
                message_meta.update(extra_meta)

            message_meta, should_mirror_to_local_web = enrich_outgoing_message_meta_for_reply_context(chat_id, message_meta)
            transport_name = "bale" if send_result.get("ok") else "local_fallback"

            append_local_chat_message(
                chat_id=chat_id,
                text=message_text,
                direction="outgoing",
                transport=transport_name,
                source="bot",
                meta=message_meta
            )

            if should_mirror_to_local_web:
                append_local_web_reply_mirror(
                    original_chat_id=chat_id,
                    text=message_text,
                    direction="outgoing",
                    transport=transport_name,
                    source="bot",
                    meta=message_meta,
                )

    return len(success_chat_ids), success_chat_ids


def send_bale_message_to_many(chat_ids, message_text, store_local_web_once=False, local_web_chat_id=LOCAL_WEB_DEFAULT_CHAT_ID, message_kind="generic_outgoing", extra_meta=None):
    normalized_chat_ids = []
    seen = set()

    for chat_id in chat_ids:
        normalized_chat_id = str(chat_id).strip()
        if not normalized_chat_id or normalized_chat_id in seen:
            continue
        normalized_chat_ids.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    ensure_bale_outgoing_worker_started()
    BALE_OUTGOING_QUEUE.put({
        "kind": "multi_text",
        "chat_ids": normalized_chat_ids,
        "message_text": message_text,
        "store_local_web_once": bool(store_local_web_once),
        "local_web_chat_id": local_web_chat_id,
        "message_kind": message_kind,
        "extra_meta": extra_meta if isinstance(extra_meta, dict) else extra_meta,
    })
    return len(normalized_chat_ids), normalized_chat_ids


def bale_outgoing_worker_loop():
    while True:
        job = BALE_OUTGOING_QUEUE.get()
        try:
            if not isinstance(job, dict):
                continue

            job_kind = str(job.get("kind") or "").strip().lower()
            if job_kind == "single_text":
                send_bale_message_sync(
                    job.get("chat_id"),
                    job.get("message_text"),
                    message_kind=job.get("message_kind", "generic_outgoing"),
                    extra_meta=job.get("extra_meta"),
                )
            elif job_kind == "multi_text":
                send_bale_message_to_many_sync(
                    job.get("chat_ids") or [],
                    job.get("message_text"),
                    store_local_web_once=bool(job.get("store_local_web_once")),
                    local_web_chat_id=job.get("local_web_chat_id", LOCAL_WEB_DEFAULT_CHAT_ID),
                    message_kind=job.get("message_kind", "generic_outgoing"),
                    extra_meta=job.get("extra_meta"),
                )
            elif job_kind == "custom_payload":
                send_custom_bale_payload_sync(
                    job.get("chat_id"),
                    job.get("payload") or {},
                    job.get("local_text"),
                    message_kind=job.get("message_kind", "generic_outgoing"),
                    extra_meta=job.get("extra_meta"),
                )
            elif job_kind == "multi_adver":
                send_bale_adver_to_many_sync(
                    job.get("chat_ids") or [],
                    job.get("message_text"),
                    photo_file_id=job.get("photo_file_id"),
                    message_kind=job.get("message_kind", "admin_adver_broadcast"),
                )
            else:
                print(f"{COL_WARN}[BALE]{RESET} Unknown outgoing Bale job kind: {job_kind}")
        except Exception as exc:
            print(f"{COL_WARN}[BALE]{RESET} Outgoing Bale worker error: {exc}")
            traceback.print_exc()
        finally:
            BALE_OUTGOING_QUEUE.task_done()


def ensure_bale_outgoing_worker_started():
    global BALE_OUTGOING_WORKER_STARTED

    if BALE_OUTGOING_WORKER_STARTED:
        return

    with BALE_OUTGOING_WORKER_LOCK:
        if BALE_OUTGOING_WORKER_STARTED:
            return
        worker = threading.Thread(target=bale_outgoing_worker_loop, daemon=True)
        worker.start()
        BALE_OUTGOING_WORKER_STARTED = True


def send_bale_notification(message_text):
    persist_last_bot_message_snapshot(
        "pump_radar",
        message_text,
        generated_at=get_tehran_now(),
        meta={"source": "nobit_full_scan", "delivery": "async_queue"}
    )
    target_chat_ids = get_active_broadcast_chat_ids(now_dt=get_tehran_now(), include_admin=True)
    return send_bale_message_to_many(
        target_chat_ids,
        message_text,
        store_local_web_once=True,
        local_web_chat_id=LOCAL_WEB_DEFAULT_CHAT_ID,
        message_kind="full_scan",
        extra_meta={"delivery": "async_queue"}
    )

def bale_get_updates(offset=None, timeout=BALE_UPDATES_TIMEOUT):
    params = {
        "timeout": str(timeout)
    }
    if offset is not None:
        params["offset"] = str(offset)

    return http_get_json_absolute(
        BALE_GET_UPDATES_URL,
        params=params,
        timeout=timeout + 15,
        retries=3,
        backoff_factor=2
    )

# =========================
# SYMBOL & FIELD HANDLING
# =========================

def nobitex_stats_to_symbol(stats_key):
    symbol = stats_key_to_market_symbol(stats_key)
    if quote_currency_from_market_symbol(symbol) == "USDT":
        return symbol
    return None


def stats_key_to_market_symbol(stats_key):
    """Convert an LBank pair key like btc_usdt or 1000_pepe_usdt to BTCUSDT."""
    if not isinstance(stats_key, str):
        return None

    key = stats_key.strip().lower().replace("-", "_")
    if "_" not in key:
        return None

    base, quote = key.rsplit("_", 1)
    base = normalize_nobitex_base_symbol(base.upper())
    quote = normalize_quote_currency(quote)
    if not base or quote != "USDT":
        return None
    return f"{base}{quote}"


def base_symbol_from_market_symbol(symbol):
    if not isinstance(symbol, str):
        return None
    base, _ = split_nobitex_market_symbol(symbol)
    return base


def market_type_from_symbol(symbol):
    return quote_currency_from_market_symbol(symbol)


def discover_change_and_volume_keys(sample_market):
    change_key = "dayChange"
    volume_key = "volumeDst"

    if not isinstance(sample_market, dict):
        return change_key, volume_key

    numeric_keys = []
    for key, value in sample_market.items():
        if isinstance(value, (int, float)):
            if math.isfinite(float(value)):
                numeric_keys.append(key)
        elif isinstance(value, str):
            try:
                if math.isfinite(float(value)):
                    numeric_keys.append(key)
            except Exception:
                pass

    for candidate in ("dayChange", "change24h", "change", "changePercent", "percentChange"):
        if candidate in numeric_keys:
            change_key = candidate
            break

    for candidate in ("volumeDst", "volume", "volume24h", "quoteVolume", "volumeQuote", "dayVolume"):
        if candidate in numeric_keys:
            volume_key = candidate
            break

    return change_key, volume_key


def fmt_usdt_price_for_message(price):
    try:
        val = float(price)
    except Exception:
        return "0"
    s = f"{val:,.2f}"
    s = s.rstrip("0").rstrip(".")
    return s


def maybe_bold(text, should_bold):
    return f"*{text}*" if should_bold else text

def choose_notification_market_result(coin):
    return coin.get("usdt_market")


def normalize_requested_base_symbol(text):
    if not isinstance(text, str):
        return None

    token = text.strip()
    if not token:
        return None

    if token.startswith("/"):
        return None

    if token.startswith("-#"):
        token = token[2:].strip()
    elif token.startswith("#"):
        token = token[1:].strip()
    elif token.startswith("-"):
        token = token[1:].strip()

    if not token:
        return None

    if " " in token or "\n" in token or "\t" in token:
        return None

    if not re.fullmatch(r"[A-Za-z0-9_]+", token):
        return None

    base_symbol, _ = split_nobitex_market_symbol(token)
    return base_symbol or None


def build_all_coin_market_map(skip_default_tags=True):
    skip_symbols = build_skip_symbol_set() if skip_default_tags else set()
    all_coins = {}

    for raw_symbol, ticker in fetch_lbank_usdt_tickers():
        symbol_udf = stats_key_to_market_symbol(raw_symbol)
        if not symbol_udf:
            continue
        base_symbol = base_symbol_from_market_symbol(symbol_udf)
        if not base_symbol:
            continue
        if skip_default_tags and base_symbol in skip_symbols:
            continue

        change_pct = _safe_float_default(ticker.get("change"), 0.0)
        volume_val = _safe_float_default(ticker.get("vol"), 0.0)
        latest = _safe_float_default(ticker.get("latest"), 0.0)
        if latest <= 0:
            continue

        all_coins[base_symbol] = {
            "base_symbol": base_symbol,
            "markets": {
                "USDT": {
                    "raw_symbol": raw_symbol,
                    "symbol_udf": symbol_udf,
                    "stats_entry": ticker,
                    "change_key": "change",
                    "volume_key": "vol",
                    "change_pct": change_pct,
                    "volume": volume_val,
                    "latest": latest,
                    "turnover": _safe_float_default(ticker.get("turnover"), 0.0),
                }
            }
        }

    return all_coins


def symbol_exists_for_demand(base_symbol):
    if not base_symbol:
        return False
    all_coins = build_all_coin_market_map(skip_default_tags=False)
    return base_symbol in all_coins

# =========================
# DEMAND / BALE RUNTIME PERSISTENCE
# =========================

def _parse_runtime_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _format_runtime_dt(value):
    if value is None:
        return None
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def get_default_demand_runtime_state():
    return {
        "symbol_subscribers": {},
        "chat_subscriptions": {},
        "price_tracking": {},
        "last_scan_slot_key": None,
        "scan_start_not_before": None
    }

def load_demand_runtime_state_file():
    default_state = get_default_demand_runtime_state()

    if not os.path.exists(DEMAND_STATE_FILE):
        save_demand_runtime_state_file(default_state)
        return default_state

    try:
        with open(DEMAND_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            save_demand_runtime_state_file(default_state)
            return default_state

        if not isinstance(state.get("symbol_subscribers"), dict):
            state["symbol_subscribers"] = {}

        if not isinstance(state.get("chat_subscriptions"), dict):
            state["chat_subscriptions"] = {}

        if not isinstance(state.get("price_tracking"), dict):
            state["price_tracking"] = {}

        if "last_scan_slot_key" not in state:
            state["last_scan_slot_key"] = None

        if "scan_start_not_before" not in state:
            state["scan_start_not_before"] = None

        return state
    except Exception as e:
        print(f"{COL_WARN}[DEMAND STATE]{RESET} Failed to load demand runtime file, resetting: {e}")
        save_demand_runtime_state_file(default_state)
        return default_state

def save_demand_runtime_state_file(state):
    try:
        with open(DEMAND_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{COL_WARN}[DEMAND STATE]{RESET} Failed to save demand runtime file: {e}")

def persist_demand_runtime_state():
    with DEMAND_STATE_LOCK:
        state = {
            "symbol_subscribers": {
                str(sym): sorted(set(str(x) for x in chat_ids))
                for sym, chat_ids in DEMAND_SYMBOL_SUBSCRIBERS.items() if chat_ids
            },
            "chat_subscriptions": {
                str(chat_id): sorted(set(str(x) for x in symbols))
                for chat_id, symbols in DEMAND_CHAT_SUBSCRIPTIONS.items() if symbols
            },
            "price_tracking": json.loads(json.dumps(DEMAND_PRICE_TRACKING, ensure_ascii=False)),
            "last_scan_slot_key": DEMAND_LAST_SCAN_SLOT_KEY,
            "scan_start_not_before": _format_runtime_dt(DEMAND_SCAN_START_NOT_BEFORE)
        }
    save_demand_runtime_state_file(state)

def load_demand_runtime_into_memory():
    global DEMAND_LAST_SCAN_SLOT_KEY, DEMAND_SCAN_START_NOT_BEFORE

    state = load_demand_runtime_state_file()

    with DEMAND_STATE_LOCK:
        DEMAND_SYMBOL_SUBSCRIBERS.clear()
        DEMAND_CHAT_SUBSCRIPTIONS.clear()
        DEMAND_PRICE_TRACKING.clear()

        for sym, chat_ids in state.get("symbol_subscribers", {}).items():
            if not isinstance(chat_ids, list):
                continue
            clean_sym = str(sym).upper().strip()
            if not clean_sym:
                continue
            DEMAND_SYMBOL_SUBSCRIBERS[clean_sym] = set(str(x) for x in chat_ids if str(x).strip())

        for chat_id, symbols in state.get("chat_subscriptions", {}).items():
            if not isinstance(symbols, list):
                continue
            clean_chat_id = str(chat_id).strip()
            if not clean_chat_id:
                continue
            DEMAND_CHAT_SUBSCRIPTIONS[clean_chat_id] = set(str(x).upper().strip() for x in symbols if str(x).strip())

        raw_tracking = state.get("price_tracking", {})
        if isinstance(raw_tracking, dict):
            for chat_id, symbol_map in raw_tracking.items():
                clean_chat_id = str(chat_id).strip()
                if not clean_chat_id or not isinstance(symbol_map, dict):
                    continue
                DEMAND_PRICE_TRACKING[clean_chat_id] = {}
                for base_symbol, market_map in symbol_map.items():
                    clean_base_symbol = str(base_symbol).upper().strip()
                    if not clean_base_symbol or not isinstance(market_map, dict):
                        continue
                    DEMAND_PRICE_TRACKING[clean_chat_id][clean_base_symbol] = {}
                    for market_key, tracking in market_map.items():
                        clean_market_key = str(market_key).upper().strip()
                        if clean_market_key != "USDT":
                            continue
                        if not isinstance(tracking, dict):
                            tracking = {}
                        DEMAND_PRICE_TRACKING[clean_chat_id][clean_base_symbol][clean_market_key] = {
                            "entry_price": tracking.get("entry_price"),
                            "last_price": tracking.get("last_price"),
                            "last_sent_price": tracking.get("last_sent_price"),
                            "entry_captured_at": tracking.get("entry_captured_at"),
                            "last_seen_at": tracking.get("last_seen_at"),
                            "last_sent_at": tracking.get("last_sent_at")
                        }

        DEMAND_LAST_SCAN_SLOT_KEY = state.get("last_scan_slot_key")
        DEMAND_SCAN_START_NOT_BEFORE = _parse_runtime_dt(state.get("scan_start_not_before"))

def get_default_bale_runtime_state():
    return {
        "updates_offset": None
    }

def load_bale_runtime_state_file():
    default_state = get_default_bale_runtime_state()

    if not os.path.exists(BALE_RUNTIME_FILE):
        save_bale_runtime_state_file(default_state)
        return default_state

    try:
        with open(BALE_RUNTIME_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            save_bale_runtime_state_file(default_state)
            return default_state

        if "updates_offset" not in state:
            state["updates_offset"] = None

        return state
    except Exception as e:
        print(f"{COL_WARN}[BALE RUNTIME]{RESET} Failed to load Bale runtime file, resetting: {e}")
        save_bale_runtime_state_file(default_state)
        return default_state

def save_bale_runtime_state_file(state):
    try:
        with open(BALE_RUNTIME_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{COL_WARN}[BALE RUNTIME]{RESET} Failed to save Bale runtime file: {e}")

def persist_bale_runtime_state():
    save_bale_runtime_state_file({
        "updates_offset": BALE_UPDATES_OFFSET
    })

def load_bale_runtime_into_memory():
    global BALE_UPDATES_OFFSET
    state = load_bale_runtime_state_file()
    BALE_UPDATES_OFFSET = state.get("updates_offset")

# =========================
# DEMAND PRICE TRACKING HELPERS
# =========================

def get_market_price_from_coin_info(coin_info, market_key):
    try:
        market = coin_info.get("markets", {}).get(market_key)
        if not market:
            return None
        return float(market.get("latest", 0.0) or 0.0)
    except Exception:
        return None

def ensure_tracking_market_entry(chat_id, base_symbol, market_key):
    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    market_key = str(market_key).upper().strip()

    if chat_id not in DEMAND_PRICE_TRACKING:
        DEMAND_PRICE_TRACKING[chat_id] = {}
    if base_symbol not in DEMAND_PRICE_TRACKING[chat_id]:
        DEMAND_PRICE_TRACKING[chat_id][base_symbol] = {}
    if market_key not in DEMAND_PRICE_TRACKING[chat_id][base_symbol]:
        DEMAND_PRICE_TRACKING[chat_id][base_symbol][market_key] = {
            "entry_price": None,
            "last_price": None,
            "last_sent_price": None,
            "entry_captured_at": None,
            "last_seen_at": None,
            "last_sent_at": None
        }
    return DEMAND_PRICE_TRACKING[chat_id][base_symbol][market_key]

def seed_demand_entry_prices(chat_id, base_symbol):
    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    now_str = _format_runtime_dt(get_tehran_now())

    all_coins = build_all_coin_market_map(skip_default_tags=False)
    coin_info = all_coins.get(base_symbol)

    with DEMAND_STATE_LOCK:
        for market_key in ("USDT",):
            tracking = ensure_tracking_market_entry(chat_id, base_symbol, market_key)
            current_price = get_market_price_from_coin_info(coin_info, market_key) if coin_info else None
            tracking["entry_price"] = current_price
            tracking["last_price"] = current_price
            tracking["last_sent_price"] = None
            tracking["entry_captured_at"] = now_str
            tracking["last_seen_at"] = now_str if current_price is not None else None
            tracking["last_sent_at"] = None

    persist_demand_runtime_state()

def remove_demand_tracking_entry_if_unused(chat_id, base_symbol):
    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()

    with DEMAND_STATE_LOCK:
        user_symbols = DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set())
        if base_symbol in user_symbols:
            return

        if chat_id in DEMAND_PRICE_TRACKING:
            DEMAND_PRICE_TRACKING[chat_id].pop(base_symbol, None)
            if not DEMAND_PRICE_TRACKING[chat_id]:
                DEMAND_PRICE_TRACKING.pop(chat_id, None)

def capture_demand_scan_snapshot(chat_id, base_symbol, result, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    now_str = _format_runtime_dt(scan_dt)

    with DEMAND_STATE_LOCK:
        for market_key, market_result in (("USDT", result.get("usdt_market")),):
            tracking = ensure_tracking_market_entry(chat_id, base_symbol, market_key)
            if market_result is None:
                continue

            try:
                current_price = float(market_result.get("price", 0.0) or 0.0)
            except Exception:
                current_price = None

            if tracking.get("entry_price") is None and current_price is not None:
                tracking["entry_price"] = current_price
                tracking["entry_captured_at"] = now_str

            tracking["last_price"] = current_price
            tracking["last_seen_at"] = now_str

    persist_demand_runtime_state()

def mark_demand_notification_sent(chat_id, base_symbol, result, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    now_str = _format_runtime_dt(scan_dt)

    with DEMAND_STATE_LOCK:
        for market_key, market_result in (("USDT", result.get("usdt_market")),):
            tracking = ensure_tracking_market_entry(chat_id, base_symbol, market_key)
            if market_result is None:
                continue
            try:
                tracking["last_sent_price"] = float(market_result.get("price", 0.0) or 0.0)
            except Exception:
                tracking["last_sent_price"] = None
            tracking["last_sent_at"] = now_str

    persist_demand_runtime_state()

def get_demand_tracking_snapshot(chat_id, base_symbol):
    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()

    with DEMAND_STATE_LOCK:
        base_tracking = DEMAND_PRICE_TRACKING.get(chat_id, {}).get(base_symbol, {})
        return json.loads(json.dumps(base_tracking, ensure_ascii=False)) if isinstance(base_tracking, dict) else {}

# =========================
# INCOMING COMMAND HANDLING (BALE + WEB)
# =========================

def add_demand_subscription(chat_id, base_symbol):
    global DEMAND_SCAN_START_NOT_BEFORE

    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    if not chat_id or not base_symbol:
        return False, 0, 0

    with DEMAND_STATE_LOCK:
        already_had_demands = any(DEMAND_SYMBOL_SUBSCRIBERS.values())

        if base_symbol not in DEMAND_SYMBOL_SUBSCRIBERS:
            DEMAND_SYMBOL_SUBSCRIBERS[base_symbol] = set()

        if chat_id not in DEMAND_CHAT_SUBSCRIPTIONS:
            DEMAND_CHAT_SUBSCRIPTIONS[chat_id] = set()

        if base_symbol in DEMAND_CHAT_SUBSCRIPTIONS[chat_id]:
            total_for_symbol = len(DEMAND_SYMBOL_SUBSCRIBERS.get(base_symbol, set()))
            total_for_user = len(DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set()))
            return False, total_for_symbol, total_for_user

        DEMAND_SYMBOL_SUBSCRIBERS[base_symbol].add(chat_id)
        DEMAND_CHAT_SUBSCRIPTIONS[chat_id].add(base_symbol)
        ensure_tracking_market_entry(chat_id, base_symbol, "USDT")
        
        if not already_had_demands:
            DEMAND_SCAN_START_NOT_BEFORE = ceil_to_even_two_minute_slot(get_tehran_now())

        total_for_symbol = len(DEMAND_SYMBOL_SUBSCRIBERS.get(base_symbol, set()))
        total_for_user = len(DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set()))

    persist_demand_runtime_state()
    seed_demand_entry_prices(chat_id, base_symbol)
    WAKEUP_EVENT.set()
    return True, total_for_symbol, total_for_user

def remove_demand_subscription(chat_id, base_symbol):
    chat_id = str(chat_id)
    base_symbol = str(base_symbol).upper().strip()
    removed = False

    with DEMAND_STATE_LOCK:
        user_set = DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set())
        symbol_set = DEMAND_SYMBOL_SUBSCRIBERS.get(base_symbol, set())

        if base_symbol in user_set:
            user_set.discard(base_symbol)
            removed = True

        if chat_id in symbol_set:
            symbol_set.discard(chat_id)
            removed = True

        if not user_set and chat_id in DEMAND_CHAT_SUBSCRIPTIONS:
            DEMAND_CHAT_SUBSCRIPTIONS.pop(chat_id, None)

        if not symbol_set and base_symbol in DEMAND_SYMBOL_SUBSCRIBERS:
            DEMAND_SYMBOL_SUBSCRIBERS.pop(base_symbol, None)

        remaining_for_symbol = len(DEMAND_SYMBOL_SUBSCRIBERS.get(base_symbol, set()))
        remaining_for_user = len(DEMAND_CHAT_SUBSCRIPTIONS.get(chat_id, set()))

    if removed:
        remove_demand_tracking_entry_if_unused(chat_id, base_symbol)
        persist_demand_runtime_state()
        WAKEUP_EVENT.set()

    return removed, remaining_for_symbol, remaining_for_user

def format_user_subscription_list(chat_id):
    current = get_user_subscription_snapshot(chat_id)
    if not current:
        return "در حال حاضر هیچ نماد فعالی برای شما ثبت نشده است."
    return "نمادهای فعال شما: " + ", ".join(f"#{sym}" for sym in current)


def send_demand_command_reply(chat_id, message_text, command_name="reply", extra_meta=None):
    normalized_command_name = re.sub(r"[^a-z0-9_]+", "_", str(command_name or "reply").strip().lower())
    normalized_command_name = normalized_command_name.strip("_") or "reply"

    message_meta = {
        "endpoint_bucket": "demand_outgoing",
        "command_name": normalized_command_name,
        "reply_scope": "demand_command"
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    return send_bale_message(
        chat_id,
        message_text,
        message_kind=f"demand_command_{normalized_command_name}",
        extra_meta=message_meta
    )

def send_demand_command_payload(chat_id, payload, local_text, command_name="reply", extra_meta=None):
    normalized_command_name = re.sub(r"[^a-z0-9_]+", "_", str(command_name or "reply").strip().lower())
    normalized_command_name = normalized_command_name.strip("_") or "reply"

    message_meta = {
        "endpoint_bucket": "demand_outgoing",
        "command_name": normalized_command_name,
        "reply_scope": "demand_command"
    }
    if isinstance(extra_meta, dict):
        message_meta.update(extra_meta)

    return send_custom_bale_payload(
        chat_id,
        payload,
        local_text=local_text,
        message_kind=f"demand_command_{normalized_command_name}",
        extra_meta=message_meta
    )

def send_bale_today_buttons(chat_id, scan_dt=None):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    send_demand_command_reply(chat_id, "لطفاً چند لحظه صبر کنید", command_name="today")

    state, sorted_entries = get_bale_today_sorted_entries(scan_dt=scan_dt)

    if not sorted_entries:
        return send_demand_command_reply(
            chat_id,
            build_bale_today_summary_message(scan_dt=scan_dt),
            command_name="today"
        )

    ensure_bale_today_chart_resolutions(scan_dt=scan_dt)
    state, sorted_entries = get_bale_today_sorted_entries(scan_dt=scan_dt)

    resolved_entries = []
    unresolved_coins = []
    newly_resolved_charts = {}

    for entry in sorted_entries:
        chart = normalize_bale_today_chart_resolution(entry.get("chart"))
        if chart is None:
            coin_name = str(entry.get("coin") or "").strip()
            chart = resolve_working_today_chart_candidate(coin_name)
            if isinstance(chart, dict):
                entry["chart"] = chart
                newly_resolved_charts[coin_name] = chart
            else:
                unresolved_coins.append(coin_name or "-")
                continue
        resolved_entries.append({
            **entry,
            "chart": chart,
        })

    if newly_resolved_charts:
        refresh_bale_today_chart_resolutions(
            scan_dt=scan_dt,
            coins_to_refresh=list(newly_resolved_charts.keys()),
            force_refresh=False,
        )

    if not resolved_entries:
        return send_demand_command_reply(
            chat_id,
            build_bale_today_summary_message(scan_dt=scan_dt) + "\n\nهیچ لینک تأییدشده‌ای برای چارت‌های امروز پیدا نشد.",
            command_name="today",
            extra_meta={
                "today_buttons": True,
                "today_resolved_buttons": 0,
                "today_unresolved_buttons": len(unresolved_coins),
            }
        )

    chunks = build_bale_today_message_chunks(resolved_entries, scan_dt=scan_dt)
    total_chunks = len(chunks)
    total_coins = len(sorted_entries)
    resolved_total = len(resolved_entries)
    unresolved_total = len(unresolved_coins)
    any_success = False

    for chunk_index, chunk in enumerate(chunks, 1):
        inline_keyboard = []
        local_lines = []

        for group in chunk.get("groups", []):
            minute_label = str(group.get("minute_label") or "-").strip() or "-"
            inline_keyboard.append([
                {
                    "text": minute_label,
                    "callback_data": f"today_time_{minute_label.replace(':', '')}_{chunk_index}",
                }
            ])

            current_row = []
            for entry in group.get("entries", []):
                chart = normalize_bale_today_chart_resolution(entry.get("chart"))
                if chart is None:
                    continue
                button_text = build_bale_today_chart_button_text(entry)
                chart_url = str(chart.get("url") or "").strip()
                if not chart_url:
                    continue

                current_row.append({
                    "text": button_text,
                    "url": chart_url,
                })

                local_lines.append(
                    f"[{minute_label}] {button_text} -> {chart.get('exchange', '-')} {chart.get('pair', '-')} -> {chart_url}"
                )

                if len(current_row) >= TODAY_CHART_BUTTONS_PER_ROW:
                    inline_keyboard.append(current_row)
                    current_row = []

            if current_row:
                inline_keyboard.append(current_row)

        message_text = str(chunk.get("title") or "").strip()
        if not message_text:
            continue

        if unresolved_total > 0 and chunk_index == 1:
            message_text = message_text + f"\n\nلینک تأییدنشده: {to_persian_digits(unresolved_total)}"

        local_text = message_text
        if local_lines:
            local_text = message_text + "\n\n" + "\n".join(local_lines)

        send_ok = send_demand_command_payload(
            chat_id,
            {
                "text": message_text,
                "reply_markup": {
                    "inline_keyboard": inline_keyboard
                },
            },
            local_text=local_text,
            command_name="today",
            extra_meta={
                "today_buttons": True,
                "today_total_coins": total_coins,
                "today_resolved_buttons": resolved_total,
                "today_unresolved_buttons": unresolved_total,
                "today_chunk_index": chunk_index,
                "today_chunk_total": total_chunks,
                "today_window_index": chunk.get("window_index", 0),
            }
        )
        any_success = bool(send_ok) or any_success

    return any_success


def process_incoming_text_message(chat_id, text, incoming_transport="bale", message=None):
    stripped = (text or "").strip()
    if not stripped:
        return

    normalized_input = normalize_command_input(stripped)

    reply_context = nullcontext()
    normalized_incoming_transport = str(incoming_transport or "").strip().lower()
    normalized_chat_id = str(chat_id).strip()

    if normalized_incoming_transport == "web":
        reply_context = outgoing_reply_context(
            chat_id=normalized_chat_id,
            incoming_transport="web",
            endpoint_bucket="demand_outgoing",
            reply_scope="incoming_web_reply",
            mirror_to_local_web=False,
        )

    with reply_context:
        state = load_subscriber_state()
        user = touch_user_interaction(state, chat_id, message=message, increment=True)
        user, access, _auto_granted = ensure_free_trial_started_without_phone(
            state,
            chat_id,
            user=user,
            message=message,
        )
        save_subscriber_state(state)

        if is_admin_chat(chat_id):
            if handle_admin_command(chat_id, normalized_input or stripped, message=message):
                return

        low = (normalized_input or stripped).lower()

        if low in ACTIVATION_COMMANDS:
            handle_activation_command(chat_id, access=access)
            return

        if should_ignore_locked_user_message(user, access, normalized_input or stripped):
            return

        if not access.get("active"):
            return

        if (normalized_input or stripped) == AUTO_TRADING_BUTTON_TEXT:
            send_auto_trading_files_bundle(chat_id)
            return

        if low in {"/start", "start", "شروع"}:
            send_access_welcome_bundle(chat_id, intro_text=(f"دسترسی شما فعال است.\nباقی‌مانده زمان رایگان: {format_duration_persian(access.get('remaining_scan_seconds') or 0)}" if access.get("stage") == "free" else "دسترسی شما فعال است."))
            return

        if low in ("/today", "today"):
            send_bale_today_buttons(chat_id)
            return

        if low in ("/pnl", "pnl"):
            try:
                signaler = import_lbank_signaler_module()
                report_text = signaler.build_realized_pnl_report(hours=24, now_dt=get_tehran_now())
            except Exception as exc:
                log_info(f"{COL_WARN}[PNL]{RESET} Failed to build pnl report: {exc}")
                report_text = "گزارش PnL فعلاً در دسترس نیست."
            send_demand_command_reply(chat_id, report_text, command_name="pnl")
            return

        if low in ("/my", "/list", "my", "list"):
            send_demand_command_reply(chat_id, format_user_subscription_list(chat_id), command_name="list")
            return

        if low in ("/help", "help"):
            send_demand_command_reply(chat_id, build_user_commands_text(), command_name="help")
            return

        if low.startswith("/stop "):
            base_symbol = normalize_requested_base_symbol((normalized_input or stripped).split(" ", 1)[1])
            if not base_symbol:
                send_demand_command_reply(chat_id, "نماد واردشده برای توقف معتبر نیست.", command_name="stop_invalid")
                return

            removed, remaining_for_symbol, remaining_for_user = remove_demand_subscription(chat_id, base_symbol)
            if removed:
                send_demand_command_reply(
                    chat_id,
                    f"ردیابی دو دقیقه‌ای *{base_symbol}* متوقف شد.\n"
                    f"تعداد نمادهای فعال شما: {remaining_for_user}",
                    command_name="stop_removed"
                )
            else:
                send_demand_command_reply(chat_id, f"*{base_symbol}* در فهرست فعال شما نبود.", command_name="stop_missing")
            return

        if (normalized_input or stripped).startswith("-#") or ((normalized_input or stripped).startswith("-") and not low.startswith("/stop ")):
            base_symbol = normalize_requested_base_symbol(normalized_input or stripped)
            if not base_symbol:
                send_demand_command_reply(chat_id, "فرمت نماد برای حذف معتبر نیست.", command_name="remove_invalid")
                return

            removed, remaining_for_symbol, remaining_for_user = remove_demand_subscription(chat_id, base_symbol)
            if removed:
                send_demand_command_reply(
                    chat_id,
                    f"ردیابی دو دقیقه‌ای *{base_symbol}* متوقف شد.\n"
                    f"تعداد نمادهای فعال شما: {remaining_for_user}",
                    command_name="stop_removed"
                )
            else:
                send_demand_command_reply(chat_id, f"*{base_symbol}* در فهرست فعال شما نبود.", command_name="remove_missing")
            return

        base_symbol = normalize_requested_base_symbol(normalized_input or stripped)
        if not base_symbol:
            return

        if not symbol_exists_for_demand(base_symbol):
            send_demand_command_reply(
                chat_id,
                f"نماد *{base_symbol}USDT* الان در LBank پیدا نشد.",
                command_name="symbol_not_found"
            )
            return

        added, total_for_symbol, total_for_user = add_demand_subscription(chat_id, base_symbol)

        if added:
            send_demand_command_reply(
                chat_id,
                f"ردیابی دو دقیقه‌ای برای *{base_symbol}* فعال شد.\n"
                f"بازار *{base_symbol}USDT* برای شما بررسی می‌شود.\n"
                f"تعداد نمادهای فعال شما: {total_for_user}\n\n"
                f"برای توقف ردیابی این را بفرستید:\n"
                f"-{base_symbol}",
                command_name="start_added"
            )
        else:
            send_demand_command_reply(
                chat_id,
                f"*{base_symbol}* از قبل در فهرست ردیابی شما وجود دارد.\n"
                f"تعداد نمادهای فعال شما: {total_for_user}",
                command_name="start_exists"
            )

def process_bale_update(update):
    if not isinstance(update, dict):
        return

    pre_checkout_query = update.get("pre_checkout_query")
    if isinstance(pre_checkout_query, dict):
        try:
            query_id = pre_checkout_query.get("id")
            if query_id is None:
                return

            if is_activation_pre_checkout_query_valid(pre_checkout_query):
                ok, result = answer_bale_pre_checkout_query(query_id, ok=True)
            else:
                ok, result = answer_bale_pre_checkout_query(
                    query_id,
                    ok=False,
                    error_message="اطلاعات پرداخت نامعتبر است."
                )

            if not ok:
                print(f"{COL_WARN}[BALE PAYMENT]{RESET} Failed answering pre-checkout query: {result}")
        except Exception as e:
            print(f"{COL_WARN}[BALE PAYMENT]{RESET} Failed processing pre-checkout query: {e}")
            traceback.print_exc()
        return

    message = update.get("message") or update.get("edited_message")
    if not isinstance(message, dict):
        return

    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return

    state = load_subscriber_state()
    update_user_profile_from_message(state, chat_id, message)
    save_subscriber_state(state)

    successful_payment = message.get("successful_payment")
    contact = message.get("contact")
    text_value = message.get("text")
    caption_value = message.get("caption")
    photo_file_id = get_adver_photo_file_id(message)

    try:
        if isinstance(successful_payment, dict):
            if handle_successful_payment_message(chat_id, message, successful_payment):
                return

        if isinstance(contact, dict):
            masked_phone = mask_phone_number(contact.get("phone_number"))
            append_local_chat_message(
                chat_id=chat_id,
                text=f"شماره تماس دریافت شد: {masked_phone}",
                direction="incoming",
                transport="bale",
                source="bale",
                meta={
                    "message_kind": "incoming_bale_contact",
                    "update_id": update.get("update_id"),
                    "message_id": message.get("message_id"),
                }
            )
            handle_contact_message(chat_id, message, incoming_transport="bale")
            return

        if is_admin_chat(chat_id) and is_admin_adver_session_active(chat_id):
            incoming_adver_text = text_value if isinstance(text_value, str) else caption_value
            append_local_chat_message(
                chat_id=chat_id,
                text=incoming_adver_text if isinstance(incoming_adver_text, str) and incoming_adver_text.strip() else ("[عکس]" if photo_file_id else ""),
                direction="incoming",
                transport="bale",
                source="bale",
                meta={
                    "message_kind": "incoming_bale_adver_content",
                    "update_id": update.get("update_id"),
                    "message_id": message.get("message_id"),
                    "has_photo": bool(photo_file_id),
                }
            )
            handle_admin_adver_content(chat_id, message=message, fallback_text=incoming_adver_text)
            return

        if not isinstance(text_value, str):
            return

        append_local_chat_message(
            chat_id=chat_id,
            text=text_value,
            direction="incoming",
            transport="bale",
            source="bale",
            meta={
                "message_kind": "incoming_bale",
                "update_id": update.get("update_id"),
                "message_id": message.get("message_id")
            }
        )
        process_incoming_text_message(chat_id, text_value, incoming_transport="bale", message=message)
    except Exception as e:
        print(f"{COL_WARN}[BALE INPUT]{RESET} Failed processing incoming update from {chat_id}: {e}")
        traceback.print_exc()

def bale_update_listener():
    global BALE_UPDATES_OFFSET

    log_info(f"{COL_INFO}[BALE INPUT]{RESET} Incoming Bale update listener started.")

    while True:
        try:
            result = bale_get_updates(offset=BALE_UPDATES_OFFSET, timeout=BALE_UPDATES_TIMEOUT)

            if not result or not result.get("ok"):
                print(f"{COL_WARN}[BALE INPUT]{RESET} getUpdates failed: {result}")
                time.sleep(BALE_UPDATE_RETRY_SLEEP)
                continue

            updates = result.get("result", [])
            if not isinstance(updates, list):
                updates = []

            for upd in updates:
                if not isinstance(upd, dict):
                    continue

                upd_id = upd.get("update_id")
                if isinstance(upd_id, int):
                    BALE_UPDATES_OFFSET = upd_id + 1
                    persist_bale_runtime_state()

                process_bale_update(upd)

        except Exception as e:
            print(f"{COL_WARN}[BALE INPUT]{RESET} Listener error: {e}")
            traceback.print_exc()
            time.sleep(BALE_UPDATE_RETRY_SLEEP)

# =========================
# SCAN COORDINATION HELPERS
# =========================

def get_scan_cooldown_remaining():
    with SCAN_COORDINATION_LOCK:
        elapsed = time.monotonic() - LAST_SCAN_FINISHED_MONO
    remaining = SCAN_COOLDOWN_SECONDS - elapsed
    if remaining < 0:
        return 0.0
    return remaining

def set_active_scan_kind(scan_kind):
    global ACTIVE_SCAN_KIND
    with SCAN_COORDINATION_LOCK:
        ACTIVE_SCAN_KIND = scan_kind

def clear_active_scan_kind():
    global ACTIVE_SCAN_KIND, LAST_SCAN_FINISHED_MONO
    with SCAN_COORDINATION_LOCK:
        ACTIVE_SCAN_KIND = None
        LAST_SCAN_FINISHED_MONO = time.monotonic()
    broadcast_local_event("state", build_local_chat_status())

def get_active_scan_kind():
    with SCAN_COORDINATION_LOCK:
        return ACTIVE_SCAN_KIND

def mark_demand_slot_executed(slot_dt):
    global DEMAND_LAST_SCAN_SLOT_KEY, DEMAND_SCAN_START_NOT_BEFORE
    with DEMAND_STATE_LOCK:
        DEMAND_LAST_SCAN_SLOT_KEY = get_demand_slot_key(slot_dt)
        DEMAND_SCAN_START_NOT_BEFORE = None
    persist_demand_runtime_state()
    broadcast_local_event("state", build_local_chat_status())

# =========================
# OHLC PREFETCH HELPERS
# =========================


def _ohlc_prefetch_key(symbol, resolution, countback):
    normalized_symbol = normalize_nobitex_udf_symbol(symbol)
    normalized_resolution = normalize_nobitex_ohlc_resolution(resolution)
    normalized_countback = normalize_nobitex_countback(countback)
    if not normalized_symbol or normalized_resolution is None or normalized_countback is None:
        return None
    return (normalized_symbol, normalized_resolution, int(normalized_countback))


CACHE_4H_LOCK = threading.Lock()


def _hourly_4h_cache_key(scan_dt=None):
    if not isinstance(scan_dt, datetime):
        scan_dt = get_tehran_now()
    return scan_dt.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")


def _ensure_4h_cache_hour(scan_dt=None):
    hour_key = _hourly_4h_cache_key(scan_dt)
    with CACHE_4H_LOCK:
        if CACHE_4H.get("_hour_key") != hour_key:
            CACHE_4H.clear()
            CACHE_4H["_hour_key"] = hour_key
            CACHE_4H["data"] = {}
        elif not isinstance(CACHE_4H.get("data"), dict):
            CACHE_4H["data"] = {}
    return hour_key


def _get_4h_cached_ohlc_by_key(key, scan_dt=None):
    if key is None:
        return False, None
    symbol, resolution, countback = key
    if str(resolution) != "240":
        return False, None
    hour_key = _ensure_4h_cache_hour(scan_dt)
    with CACHE_4H_LOCK:
        if CACHE_4H.get("_hour_key") != hour_key:
            return False, None
        data = CACHE_4H.get("data") if isinstance(CACHE_4H.get("data"), dict) else {}
        if key in data:
            return True, data.get(key)
    return False, None


def _store_4h_cached_ohlc_by_key(key, value, scan_dt=None):
    if key is None or not value:
        return False
    symbol, resolution, countback = key
    if str(resolution) != "240":
        return False
    hour_key = _ensure_4h_cache_hour(scan_dt)
    with CACHE_4H_LOCK:
        if CACHE_4H.get("_hour_key") != hour_key:
            CACHE_4H.clear()
            CACHE_4H["_hour_key"] = hour_key
            CACHE_4H["data"] = {}
        CACHE_4H.setdefault("data", {})[key] = value
    return True


def _get_prefetched_ohlc(symbol, resolution, countback):
    data = getattr(OHLC_PREFETCH_CONTEXT, "data", None)
    if not isinstance(data, dict):
        return False, None
    key = _ohlc_prefetch_key(symbol, resolution, countback)
    if key is None:
        return False, None
    if key in data:
        return True, data.get(key)
    cached_hit, cached_data = _get_4h_cached_ohlc_by_key(key)
    if cached_hit:
        return True, cached_data
    strict = bool(getattr(OHLC_PREFETCH_CONTEXT, "strict", False))
    if strict:
        return True, None
    return False, None


@contextmanager
def use_ohlc_prefetch_context(prefetch_data, strict=OHLC_PREFETCH_STRICT):
    old_data = getattr(OHLC_PREFETCH_CONTEXT, "data", None)
    old_strict = getattr(OHLC_PREFETCH_CONTEXT, "strict", None)
    OHLC_PREFETCH_CONTEXT.data = prefetch_data if isinstance(prefetch_data, dict) else {}
    OHLC_PREFETCH_CONTEXT.strict = bool(strict)
    try:
        yield
    finally:
        if old_data is None:
            try:
                delattr(OHLC_PREFETCH_CONTEXT, "data")
            except Exception:
                pass
        else:
            OHLC_PREFETCH_CONTEXT.data = old_data

        if old_strict is None:
            try:
                delattr(OHLC_PREFETCH_CONTEXT, "strict")
            except Exception:
                pass
        else:
            OHLC_PREFETCH_CONTEXT.strict = old_strict


def _ohlc_prefetch_request_gate():
    global OHLC_PREFETCH_NEXT_REQUEST_MONO
    gap = max(0.0, float(OHLC_PREFETCH_REQUEST_GAP_SECONDS or 0.0))
    if gap <= 0:
        return
    with OHLC_PREFETCH_GATE_LOCK:
        now = time.monotonic()
        wait_seconds = OHLC_PREFETCH_NEXT_REQUEST_MONO - now
        if wait_seconds > 0:
            time.sleep(wait_seconds)
            now = time.monotonic()
        OHLC_PREFETCH_NEXT_REQUEST_MONO = now + gap


def _reset_ohlc_prefetch_worker_state():
    OHLC_PREFETCH_WORKER_LOCAL.did_first_request = False


def _next_ohlc_prefetch_request_timeout():
    did_first = bool(getattr(OHLC_PREFETCH_WORKER_LOCAL, "did_first_request", False))
    if not did_first:
        OHLC_PREFETCH_WORKER_LOCAL.did_first_request = True
        return float(OHLC_PREFETCH_FIRST_REQUEST_TIMEOUT)
    return float(OHLC_PREFETCH_REQUEST_TIMEOUT)


def build_ohlc_prefetch_plan_from_coin_infos(coin_infos):
    plan = []
    seen = set()

    for coin_info in coin_infos or []:
        if not isinstance(coin_info, dict):
            continue
        markets = coin_info.get("markets") if isinstance(coin_info.get("markets"), dict) else {}
        for market in markets.values():
            if not isinstance(market, dict):
                continue
            symbol_udf = market.get("symbol_udf")
            for resolution, countback in (("30", CANDLE_30M_FETCH), ("240", CANDLE_4H_FETCH)):
                key = _ohlc_prefetch_key(symbol_udf, resolution, countback)
                if key is None or key in seen:
                    continue
                if str(resolution) == "240":
                    cached_hit, _cached_data = _get_4h_cached_ohlc_by_key(key)
                    if cached_hit:
                        continue
                seen.add(key)
                plan.append({
                    "key": key,
                    "symbol": key[0],
                    "resolution": key[1],
                    "countback": key[2],
                })

    try:
        max_items = max(1, int(OHLC_PREFETCH_MAX_PLAN_ITEMS))
    except Exception:
        max_items = 240
    return plan[:max_items]


def _fetch_ohlc_prefetch_job(job, to_ts, retry_count=0, http_warning_collector=None):
    started = time.monotonic()
    key = job.get("key")
    timeout = _next_ohlc_prefetch_request_timeout()
    _ohlc_prefetch_request_gate()
    old_collector = _set_ohlc_prefetch_http_warning_collector(http_warning_collector)
    try:
        data = fetch_ohlc(
            job.get("symbol"),
            resolution=job.get("resolution"),
            countback=job.get("countback"),
            to_ts=to_ts,
            timeout=timeout,
            retries=retry_count,
        )
    finally:
        _restore_ohlc_prefetch_http_warning_collector(old_collector)
    elapsed = time.monotonic() - started
    return key, data, elapsed, timeout


def prefetch_ohlc_for_coin_infos(coin_infos, label="OHLC"):
    _ensure_4h_cache_hour()
    plan = build_ohlc_prefetch_plan_from_coin_infos(coin_infos)
    if not plan:
        return {}

    started_mono = time.monotonic()

    try:
        budget_seconds = max(0.1, float(OHLC_PREFETCH_BUDGET_SECONDS))
    except Exception:
        budget_seconds = 60.0

    try:
        workers = max(1, int(OHLC_PREFETCH_WORKERS))
    except Exception:
        workers = 8
    workers = min(workers, len(plan))

    try:
        failed_retry_count = max(0, int(OHLC_PREFETCH_FAILED_RETRY_COUNT))
    except Exception:
        failed_retry_count = 1

    to_ts = int(time.time())
    deadline = started_mono + budget_seconds
    results = {}
    cancelled_keys = set()
    http_warning_collector = {"count": 0, "lock": threading.Lock()}

    def run_prefetch_batch(batch_jobs, retry_count, timeout_seconds):
        if not batch_jobs or timeout_seconds <= 0:
            return set()

        failed_keys = set()
        batch_workers = min(workers, len(batch_jobs))
        future_to_job = {}
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=batch_workers,
            thread_name_prefix="ohlc-prefetch",
            initializer=_reset_ohlc_prefetch_worker_state,
        )

        try:
            pending = set()
            for job in batch_jobs:
                future = executor.submit(_fetch_ohlc_prefetch_job, job, to_ts, retry_count, http_warning_collector)
                pending.add(future)
                future_to_job[future] = job

            while pending:
                remaining = min(timeout_seconds, max(0.0, deadline - time.monotonic()))
                if remaining <= 0:
                    break
                done, pending = concurrent.futures.wait(
                    pending,
                    timeout=remaining,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if not done:
                    break
                for future in done:
                    job = future_to_job.get(future, {})
                    key = job.get("key")
                    try:
                        result_key, data, elapsed, timeout_used = future.result()
                        key = result_key or key
                        if key is not None and data:
                            results[key] = data
                        elif key is not None:
                            failed_keys.add(key)
                    except Exception:
                        if key is not None:
                            failed_keys.add(key)

            for future in pending:
                job = future_to_job.get(future, {})
                key = job.get("key")
                if key is not None:
                    cancelled_keys.add(key)
                future.cancel()

        finally:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)

        return failed_keys

    first_failed_keys = run_prefetch_batch(plan, retry_count=0, timeout_seconds=budget_seconds)
    retry_jobs = [job for job in plan if job.get("key") in first_failed_keys and job.get("key") not in results and job.get("key") not in cancelled_keys]
    retried_failed = 0

    remaining_seconds = max(0.0, deadline - time.monotonic())
    if failed_retry_count > 0 and retry_jobs and remaining_seconds > 0:
        retried_failed = len(retry_jobs)
        run_prefetch_batch(retry_jobs, retry_count=failed_retry_count, timeout_seconds=remaining_seconds)

    for key, value in list(results.items()):
        _store_4h_cached_ohlc_by_key(key, value)

    missing = sum(1 for job in plan if job.get("key") not in results and job.get("key") not in cancelled_keys)
    cancelled = len(cancelled_keys)
    duration = time.monotonic() - started_mono
    suppressed_http_warnings = int(http_warning_collector.get("count", 0))
    suppressed_suffix = (
        f" http_warn_suppressed={suppressed_http_warnings}"
        if suppressed_http_warnings > 0
        else ""
    )
    print(
        f"{COL_INFO}[OHLC PREFETCH]{RESET} {label}: "
        f"fetched={len(results)}/{len(plan)} missing={missing} cancelled={cancelled} "
        f"retried_failed={retried_failed} budget={budget_seconds:.1f}s "
        f"workers={workers} retry_failed={failed_retry_count} duration={duration:.1f}s"
        f"{suppressed_suffix}"
    )
    return results


# =========================
# OHLC FETCHERS
# =========================



def lbank_api_symbol(symbol, default_quote_currency="USDT"):
    base, quote = split_nobitex_market_symbol(symbol)
    if not base:
        base = normalize_nobitex_base_symbol(symbol)
    quote = normalize_quote_currency(quote or default_quote_currency) or "USDT"
    if not base or quote != "USDT":
        return None
    return f"{base.lower()}_{quote.lower()}"


def _lbank_response_ok(payload):
    if not isinstance(payload, dict):
        return False
    result = payload.get("result")
    if isinstance(result, bool):
        result_ok = result
    else:
        result_ok = str(result).strip().lower() in {"true", "ok", "success", "1"}
    try:
        error_code_ok = int(payload.get("error_code", 0) or 0) == 0
    except Exception:
        error_code_ok = payload.get("error_code") in (None, "0", 0)
    return bool(result_ok and error_code_ok)


def _lbank_data_list(payload):
    if not _lbank_response_ok(payload):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def fetch_lbank_available_usdt_pairs():
    payload = http_get_json("/v2/currencyPairs.do")
    pairs = set()
    for item in _lbank_data_list(payload):
        raw = str(item or "").strip().lower()
        if not raw.endswith("_usdt"):
            continue
        symbol = stats_key_to_market_symbol(raw)
        if symbol:
            pairs.add(symbol)
    return pairs


def fetch_lbank_usdt_tickers():
    available_pairs = fetch_lbank_available_usdt_pairs()
    ticker_payload = http_get_json("/v2/ticker/24hr.do", params={"symbol": "all"})
    tickers = _lbank_data_list(ticker_payload)
    if not tickers:
        return []

    rows = []
    for item in tickers:
        if not isinstance(item, dict):
            continue
        raw_symbol = str(item.get("symbol") or "").strip().lower()
        if not raw_symbol.endswith("_usdt"):
            continue
        symbol_udf = stats_key_to_market_symbol(raw_symbol)
        if not symbol_udf or quote_currency_from_market_symbol(symbol_udf) != "USDT":
            continue
        if available_pairs and symbol_udf not in available_pairs:
            continue
        ticker = item.get("ticker") if isinstance(item.get("ticker"), dict) else {}
        if not isinstance(ticker, dict):
            continue
        latest = _safe_float_default(ticker.get("latest"), 0.0)
        if latest <= 0:
            continue
        rows.append((raw_symbol, ticker))
    return rows


def lbank_kline_type_from_resolution(resolution):
    key = str(resolution or "30").strip().upper()
    return LBANK_KLINE_TYPE_BY_RESOLUTION.get(key)


def lbank_kline_interval_seconds(resolution):
    key = str(resolution or "30").strip().upper()
    try:
        return int(LBANK_KLINE_INTERVAL_SECONDS.get(key) or 30 * 60)
    except Exception:
        return 30 * 60


def _convert_lbank_kline_payload_to_udf(payload):
    rows = _lbank_data_list(payload)
    if not rows:
        return {"s": "no_data", "t": [], "o": [], "h": [], "l": [], "c": [], "v": []}

    candles = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            ts = int(float(row[0]))
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])
            v = float(row[5])
        except Exception:
            continue
        candles.append((ts, o, h, l, c, v))

    candles.sort(key=lambda item: item[0])
    if not candles:
        return {"s": "no_data", "t": [], "o": [], "h": [], "l": [], "c": [], "v": []}

    return {
        "s": "ok",
        "t": [item[0] for item in candles],
        "o": [item[1] for item in candles],
        "h": [item[2] for item in candles],
        "l": [item[3] for item in candles],
        "c": [item[4] for item in candles],
        "v": [item[5] for item in candles],
    }


def fetch_ohlc(
    symbol,
    resolution,
    countback=None,
    from_ts=None,
    to_ts=None,
    timeout=None,
    retries=None,
    allow_live_fallback=False,
    live_timeout=None,
    live_retries=None,
):
    normalized_symbol = normalize_nobitex_udf_symbol(symbol, default_quote_currency="USDT")
    if not normalized_symbol or quote_currency_from_market_symbol(normalized_symbol) != "USDT":
        print(f"{COL_WARN}[OHLC]{RESET} {symbol} -> invalid LBank USDT symbol.")
        return None

    normalized_resolution = normalize_nobitex_ohlc_resolution(resolution)
    if normalized_resolution is None:
        print(f"{COL_WARN}[OHLC]{RESET} {normalized_symbol} res={resolution} -> invalid LBank resolution.")
        return None

    normalized_countback = normalize_nobitex_countback(countback)
    prefetch_key = _ohlc_prefetch_key(normalized_symbol, normalized_resolution, normalized_countback)

    found_prefetch, prefetched_data = _get_prefetched_ohlc(normalized_symbol, normalized_resolution, normalized_countback)
    if found_prefetch:
        return prefetched_data

    cached_hit, cached_data = _get_4h_cached_ohlc_by_key(prefetch_key)
    if cached_hit:
        return cached_data

    lbank_symbol = lbank_api_symbol(normalized_symbol)
    kline_type = lbank_kline_type_from_resolution(normalized_resolution)
    if not lbank_symbol or not kline_type:
        return None

    if to_ts is None:
        to_ts = int(time.time())

    interval_seconds = lbank_kline_interval_seconds(normalized_resolution)
    if normalized_countback is None:
        normalized_countback = CANDLE_30M_FETCH if str(normalized_resolution) == "30" else CANDLE_4H_FETCH

    try:
        start_ts = int(to_ts) - (int(normalized_countback) * int(interval_seconds))
    except Exception:
        start_ts = int(time.time()) - (int(normalized_countback) * int(interval_seconds))
    start_ts = max(0, start_ts)

    params = {
        "symbol": lbank_symbol,
        "size": str(int(normalized_countback)),
        "type": kline_type,
        "time": str(int(start_ts)),
    }

    request_timeout = HTTP_TIMEOUT if timeout is None else timeout
    request_retries = HTTP_RETRY_COUNT if retries is None else retries
    payload = http_get_json("/v2/kline.do", params=params, timeout=request_timeout, retries=request_retries)
    if not payload:
        return None

    data = _convert_lbank_kline_payload_to_udf(payload)
    if data.get("s") == "ok" and int(to_ts or 0) > 0:
        max_ts = int(to_ts) + int(interval_seconds)
        keep_indices = [i for i, ts in enumerate(data.get("t") or []) if int(ts) <= max_ts]
        if len(keep_indices) != len(data.get("t") or []):
            data = {
                "s": "ok",
                "t": [data["t"][i] for i in keep_indices],
                "o": [data["o"][i] for i in keep_indices],
                "h": [data["h"][i] for i in keep_indices],
                "l": [data["l"][i] for i in keep_indices],
                "c": [data["c"][i] for i in keep_indices],
                "v": [data["v"][i] for i in keep_indices],
            }
        if data.get("t"):
            for field in ("t", "o", "h", "l", "c", "v"):
                data[field] = data[field][-int(normalized_countback):]
        else:
            data = {"s": "no_data", "t": [], "o": [], "h": [], "l": [], "c": [], "v": []}

    _store_4h_cached_ohlc_by_key(prefetch_key, data)
    return data


def _resolution_to_seconds(resolution):
    resolution_value = normalize_nobitex_ohlc_resolution(resolution)
    if resolution_value is None:
        return None

    if resolution_value.isdigit():
        return int(resolution_value) * 60

    mapping = {
        "D": 86400,
        "1D": 86400,
        "2D": 2 * 86400,
        "3D": 3 * 86400,
        "W": 7 * 86400,
        "1W": 7 * 86400,
    }
    return mapping.get(resolution_value)


def _latest_closed_candle_start_ts(timeframe_seconds, now_ts=None):
    try:
        timeframe_seconds = int(timeframe_seconds)
    except Exception:
        return None
    if timeframe_seconds <= 0:
        return None

    if now_ts is None:
        now_ts = time.time()

    minute_based_now_ts = _floor_timestamp_to_minute(now_ts)
    if minute_based_now_ts is None:
        return None

    slot_index = (minute_based_now_ts // timeframe_seconds) - 1
    if slot_index < 0:
        return None
    return slot_index * timeframe_seconds


def _current_candle_start_ts(timeframe_seconds, now_ts=None):
    try:
        timeframe_seconds = int(timeframe_seconds)
    except Exception:
        return None
    if timeframe_seconds <= 0:
        return None

    if now_ts is None:
        now_ts = time.time()

    minute_based_now_ts = _floor_timestamp_to_minute(now_ts)
    if minute_based_now_ts is None:
        return None

    slot_index = minute_based_now_ts // timeframe_seconds
    if slot_index < 0:
        return None
    return slot_index * timeframe_seconds


def _safe_positive_float(value):
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _safe_non_negative_float(value):
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _safe_int_timestamp(value):
    try:
        ts = int(float(value))
    except Exception:
        return None
    if ts <= 0:
        return None
    while ts > 100000000000:
        ts //= 1000
    return ts


def _floor_timestamp_to_minute(ts):
    ts = _safe_int_timestamp(ts)
    if ts is None:
        return None
    return ts - (ts % 60)


def _align_timestamp_to_resolution(ts, timeframe_seconds=None):
    ts = _safe_int_timestamp(ts)
    if ts is None:
        return None

    if timeframe_seconds is None:
        return _floor_timestamp_to_minute(ts)

    try:
        timeframe_seconds = int(timeframe_seconds)
    except Exception:
        return None

    if timeframe_seconds <= 0:
        return None

    minute_based_ts = _floor_timestamp_to_minute(ts)
    if minute_based_ts is None:
        return None

    return (minute_based_ts // timeframe_seconds) * timeframe_seconds


def _price_seed_from_raw_slot(raw_row):
    if not isinstance(raw_row, dict):
        return None
    for key in ("c", "o", "h", "l"):
        price = _safe_positive_float(raw_row.get(key))
        if price is not None:
            return price
    return None


def _normalize_ohlc_slot(ts, raw_row, prev_close=None, next_seed_price=None):
    raw_row = raw_row if isinstance(raw_row, dict) else {}

    open_p = _safe_positive_float(raw_row.get("o"))
    high_p = _safe_positive_float(raw_row.get("h"))
    low_p = _safe_positive_float(raw_row.get("l"))
    close_p = _safe_positive_float(raw_row.get("c"))
    vol = _safe_non_negative_float(raw_row.get("v"))

    fallback_price = None
    for candidate_price in (close_p, open_p, low_p, high_p, prev_close, next_seed_price):
        if candidate_price is not None and candidate_price > 0:
            fallback_price = candidate_price
            break

    if fallback_price is None:
        return None

    if open_p is None:
        open_p = close_p if close_p is not None else fallback_price
    if close_p is None:
        close_p = open_p if open_p is not None else fallback_price

    if open_p is None or close_p is None or open_p <= 0 or close_p <= 0:
        return None

    range_prices = [price for price in (open_p, high_p, low_p, close_p) if price is not None and price > 0]
    if not range_prices:
        return None

    high_p = max(range_prices)
    low_p = min(range_prices)

    full_price_set_valid = (
        open_p is not None and
        high_p is not None and
        low_p is not None and
        close_p is not None and
        high_p >= low_p and
        high_p >= max(open_p, close_p, low_p) and
        low_p <= min(open_p, close_p, high_p)
    )

    placeholder_reason = None
    if not full_price_set_valid:
        placeholder_reason = "repaired"
    elif vol is None:
        placeholder_reason = "volume_repaired"

    return {
        "t": int(ts),
        "o": float(open_p),
        "h": float(high_p),
        "l": float(low_p),
        "c": float(close_p),
        "v": float(vol) if vol is not None else 0.0,
        "is_placeholder": bool(placeholder_reason),
        "placeholder_reason": placeholder_reason,
    }


def _merge_raw_ohlc_rows(existing_row, new_row):
    if not isinstance(existing_row, dict) or not existing_row:
        return dict(new_row or {})
    if not isinstance(new_row, dict):
        return dict(existing_row)

    merged = dict(existing_row)

    # Keep the first valid open for the candle, but the latest valid close.
    # v2: using the first close from a duplicated/merged slot can freeze a
    # candle percentage and make it disagree with the live chart.
    if merged.get("o") is None and new_row.get("o") is not None:
        merged["o"] = new_row.get("o")
    if new_row.get("c") is not None:
        merged["c"] = new_row.get("c")

    high_candidates = [value for value in (merged.get("h"), new_row.get("h")) if value is not None and value > 0]
    merged["h"] = max(high_candidates) if high_candidates else None

    low_candidates = [value for value in (merged.get("l"), new_row.get("l")) if value is not None and value > 0]
    merged["l"] = min(low_candidates) if low_candidates else None

    if merged.get("v") is None and new_row.get("v") is not None:
        merged["v"] = new_row.get("v")
    elif merged.get("v") is not None and new_row.get("v") is not None:
        try:
            merged["v"] = float(merged.get("v") or 0.0) + float(new_row.get("v") or 0.0)
        except Exception:
            pass

    return merged


def _is_preferred_analysis_candle(candle):
    if not isinstance(candle, dict):
        return False
    close_price = candle.get("c")
    if close_price is None or close_price <= 0:
        return False
    return candle.get("placeholder_reason") in (None, "repaired", "volume_repaired")


def extract_candles_from_ohlc(
    ohlc,
    resolution=None,
    expected_count=None,
    now_ts=None,
    fill_missing=True,
    seed_price=None,
    include_live_current=False,
):
    if not ohlc or not isinstance(ohlc, dict):
        return []

    timeframe_seconds = _resolution_to_seconds(resolution)
    if timeframe_seconds:
        if include_live_current:
            latest_allowed_ts = _current_candle_start_ts(timeframe_seconds, now_ts=now_ts)
        else:
            latest_allowed_ts = _latest_closed_candle_start_ts(timeframe_seconds, now_ts=now_ts)
    else:
        latest_allowed_ts = None

    if ohlc.get("s") == "no_data":
        if not fill_missing or timeframe_seconds is None:
            return []
        try:
            expected_count = min(NOBITEX_MAX_OHLC_COUNTBACK, max(1, int(expected_count))) if expected_count is not None else None
        except Exception:
            expected_count = None
        seed_price = _safe_positive_float(seed_price)
        if not expected_count or seed_price is None or latest_allowed_ts is None:
            return []
        start_ts = latest_allowed_ts - ((expected_count - 1) * int(timeframe_seconds))
        return [{
            "t": int(ts),
            "o": float(seed_price),
            "h": float(seed_price),
            "l": float(seed_price),
            "c": float(seed_price),
            "v": 0.0,
            "is_placeholder": True,
            "placeholder_reason": "no_data_fill",
        } for ts in range(int(start_ts), int(latest_allowed_ts) + 1, int(timeframe_seconds))]

    t = ohlc.get("t") or []
    o = ohlc.get("o") or []
    h = ohlc.get("h") or []
    l = ohlc.get("l") or []
    c = ohlc.get("c") or []
    v = ohlc.get("v") or []

    raw_rows_by_ts = {}

    for idx in range(len(t)):
        raw_ts = _safe_int_timestamp(t[idx])
        if raw_ts is None:
            continue

        ts = _align_timestamp_to_resolution(raw_ts, timeframe_seconds) if timeframe_seconds else raw_ts
        if ts is None:
            continue
        if latest_allowed_ts is not None and ts > latest_allowed_ts:
            continue

        row = {
            "o": _safe_positive_float(o[idx]) if idx < len(o) else None,
            "h": _safe_positive_float(h[idx]) if idx < len(h) else None,
            "l": _safe_positive_float(l[idx]) if idx < len(l) else None,
            "c": _safe_positive_float(c[idx]) if idx < len(c) else None,
            "v": _safe_non_negative_float(v[idx]) if idx < len(v) else None,
        }

        raw_rows_by_ts[ts] = _merge_raw_ohlc_rows(raw_rows_by_ts.get(ts), row)

    if not raw_rows_by_ts:
        if not fill_missing or timeframe_seconds is None:
            return []
        try:
            expected_count = min(NOBITEX_MAX_OHLC_COUNTBACK, max(1, int(expected_count))) if expected_count is not None else None
        except Exception:
            expected_count = None
        seed_price = _safe_positive_float(seed_price)
        if not expected_count or seed_price is None or latest_allowed_ts is None:
            return []
        start_ts = latest_allowed_ts - ((expected_count - 1) * int(timeframe_seconds))
        return [{
            "t": int(ts),
            "o": float(seed_price),
            "h": float(seed_price),
            "l": float(seed_price),
            "c": float(seed_price),
            "v": 0.0,
            "is_placeholder": True,
            "placeholder_reason": "empty_window_fill",
        } for ts in range(int(start_ts), int(latest_allowed_ts) + 1, int(timeframe_seconds))]

    if fill_missing and timeframe_seconds is not None:
        if expected_count is not None:
            try:
                expected_count = min(NOBITEX_MAX_OHLC_COUNTBACK, max(1, int(expected_count)))
            except Exception:
                expected_count = None
        if expected_count and latest_allowed_ts is not None:
            start_ts = latest_allowed_ts - ((expected_count - 1) * int(timeframe_seconds))
            end_ts = latest_allowed_ts
        else:
            start_ts = min(raw_rows_by_ts)
            end_ts = max(raw_rows_by_ts)
            max_span_seconds = (NOBITEX_MAX_OHLC_COUNTBACK - 1) * int(timeframe_seconds)
            if end_ts - start_ts > max_span_seconds:
                start_ts = end_ts - max_span_seconds
        target_timestamps = list(range(int(start_ts), int(end_ts) + 1, int(timeframe_seconds)))
    else:
        target_timestamps = sorted(raw_rows_by_ts)

    slot_seed_by_ts = {ts: _price_seed_from_raw_slot(raw_rows_by_ts.get(ts)) for ts in target_timestamps}
    next_seed_price = None
    next_seed_by_ts = {}
    for ts in reversed(target_timestamps):
        seed_candidate = slot_seed_by_ts.get(ts)
        if seed_candidate is not None:
            next_seed_price = seed_candidate
        next_seed_by_ts[ts] = next_seed_price

    candles = []
    prev_close = _safe_positive_float(seed_price)

    for ts in target_timestamps:
        raw_row = raw_rows_by_ts.get(ts)
        candle = _normalize_ohlc_slot(
            ts,
            raw_row,
            prev_close=prev_close,
            next_seed_price=next_seed_by_ts.get(ts),
        )
        if candle is None:
            continue
        if raw_row is None:
            candle["is_placeholder"] = True
            candle["placeholder_reason"] = "gap_fill"
        candles.append(candle)
        prev_close = candle["c"]

    candles.sort(key=lambda row: row["t"])
    return candles


def extract_prices_from_ohlc(
    ohlc,
    resolution=None,
    expected_count=None,
    now_ts=None,
    fill_missing=True,
    seed_price=None,
    include_live_current=False,
):
    candles = extract_candles_from_ohlc(
        ohlc,
        resolution=resolution,
        expected_count=expected_count,
        now_ts=now_ts,
        fill_missing=fill_missing,
        seed_price=seed_price,
        include_live_current=include_live_current,
    )
    return [candle["c"] for candle in candles if candle.get("c") is not None]

def percent_change(start, end):
    try:
        start = float(start)
        end = float(end)
    except Exception:
        return 0.0
    if not math.isfinite(start) or not math.isfinite(end) or start <= 0:
        return 0.0
    return (end - start) / start * 100.0


def _latest_preferred_analysis_candle(candles):
    if not candles:
        return None
    for candle in reversed(candles):
        if _is_preferred_analysis_candle(candle):
            return candle
    for candle in reversed(candles):
        if isinstance(candle, dict) and _safe_positive_float(candle.get("c")) is not None:
            return candle
    return None


def _normalize_live_price_to_candle_scale(live_price, candle_open, candle_close=None):
    """
    Normalize a live ticker price to the OHLC candle scale for display-only
    candle percentages.

    Returns (normalized_price, factor, reason).  If no safe normalization is
    possible, returns (None, None, reason) so the caller falls back to the OHLC
    close instead of printing impossible +900% candles.
    """
    live_price = _safe_positive_float(live_price)
    candle_open = _safe_positive_float(candle_open)
    candle_close = _safe_positive_float(candle_close)

    if live_price is None or candle_open is None:
        return None, None, "missing_live_or_open"

    factors = (1.0,)
    if bool(globals().get("CANDLE_PERCENT_LIVE_SCALE_REPAIR_ENABLED", True)):
        factors = globals().get("CANDLE_PERCENT_LIVE_SCALE_REPAIR_FACTORS", factors) or factors

    max_abs_pct = float(globals().get("CANDLE_PERCENT_LIVE_MAX_REASONABLE_ABS_PCT", 120.0) or 120.0)
    max_close_deviation = float(globals().get("CANDLE_PERCENT_LIVE_SCALE_MAX_DEVIATION_FROM_OHLC_CLOSE_PCT", 35.0) or 35.0)

    candidates = []
    for raw_factor in factors:
        try:
            factor = float(raw_factor)
        except Exception:
            continue
        if not math.isfinite(factor) or factor <= 0:
            continue
        candidate = live_price * factor
        if not math.isfinite(candidate) or candidate <= 0:
            continue

        pct_from_open = percent_change(candle_open, candidate)
        if not math.isfinite(float(pct_from_open)) or abs(float(pct_from_open)) > max_abs_pct:
            continue

        if candle_close is not None and candle_close > 0:
            deviation_from_close = abs(percent_change(candle_close, candidate))
        else:
            deviation_from_close = abs(float(pct_from_open))

        if deviation_from_close > max_close_deviation:
            continue

        # Prefer the factor closest to the OHLC close, then prefer no scaling.
        scale_penalty = 0.0 if abs(factor - 1.0) < 1e-12 else 0.01
        candidates.append((deviation_from_close + scale_penalty, abs(math.log10(factor)) if factor > 0 else 99.0, candidate, factor))

    if not candidates:
        raw_pct = percent_change(candle_open, live_price)
        return None, None, f"live_price_rejected_scale_mismatch raw_pct={raw_pct:+.4f}%"

    candidates.sort(key=lambda item: (item[0], item[1]))
    _score, _distance, normalized, factor = candidates[0]
    reason = "same_scale" if abs(factor - 1.0) < 1e-12 else f"scale_repaired_x{factor:g}"
    return float(normalized), float(factor), reason


def compute_latest_candle_change_pct_from_ohlc(
    ohlc,
    resolution,
    expected_count=None,
    now_ts=None,
    seed_price=None,
    live_price=None,
    include_live_current=True,
):
    """
    v4 actual candle percentage helper.

    This is DISPLAY logic only. It returns the latest usable candle's open ->
    close/live percentage. A live ticker price may replace the OHLC close only
    after scale validation/repair. If the live price looks incompatible with
    the candle unit, this function falls back to the OHLC close instead of
    printing fake 10x / +900% candles.
    """
    candles = extract_candles_from_ohlc(
        ohlc,
        resolution=resolution,
        expected_count=expected_count,
        now_ts=now_ts,
        fill_missing=True,
        seed_price=seed_price,
        include_live_current=include_live_current,
    )

    candle = _latest_preferred_analysis_candle(candles)
    if not candle:
        return {
            "pct": 0.0,
            "had_data": False,
            "candle": None,
            "open": None,
            "close": None,
            "used_live_price": False,
            "live_scale_factor": None,
            "live_scale_reason": "no_candle",
        }

    open_price = _safe_positive_float(candle.get("o"))
    close_price = _safe_positive_float(candle.get("c"))
    live_price = _safe_positive_float(live_price)

    used_live_price = False
    live_scale_factor = None
    live_scale_reason = "disabled"
    if bool(CANDLE_PERCENT_USE_STATS_LATEST_PRICE) and live_price is not None and open_price is not None:
        normalized_live, live_scale_factor, live_scale_reason = _normalize_live_price_to_candle_scale(
            live_price,
            open_price,
            candle_close=close_price,
        )
        if normalized_live is not None:
            close_price = normalized_live
            used_live_price = True

    if open_price is None or close_price is None:
        return {
            "pct": 0.0,
            "had_data": False,
            "candle": candle,
            "open": open_price,
            "close": close_price,
            "used_live_price": used_live_price,
            "live_scale_factor": live_scale_factor,
            "live_scale_reason": live_scale_reason,
        }

    placeholder_reason = candle.get("placeholder_reason")
    synthetic_only = placeholder_reason in ("no_data_fill", "empty_window_fill")

    return {
        "pct": percent_change(open_price, close_price),
        "had_data": not synthetic_only,
        "candle": candle,
        "open": open_price,
        "close": close_price,
        "used_live_price": used_live_price,
        "live_scale_factor": live_scale_factor,
        "live_scale_reason": live_scale_reason,
    }


def compute_close_vs_previous_average_pct(closes, previous_count=CANDLE_30M_PREV_FOR_AVG):
    """Internal momentum helper kept for z-score/pump strength only."""
    try:
        previous_count = max(1, int(previous_count))
    except Exception:
        previous_count = CANDLE_30M_PREV_FOR_AVG

    min_needed = previous_count + 1
    if not closes or len(closes) < min_needed:
        return 0.0, 0.0, []

    prev_closes = closes[-min_needed:-1]
    last_close = closes[-1]
    avg_prev = sum(prev_closes) / len(prev_closes) if prev_closes else last_close
    pct = percent_change(avg_prev, last_close)

    if len(prev_closes) > 1:
        try:
            stdev_prev = statistics.stdev(prev_closes)
        except statistics.StatisticsError:
            stdev_prev = 0.0
    else:
        stdev_prev = 0.0

    if stdev_prev > 0:
        z_score = (last_close - avg_prev) / stdev_prev
    else:
        z_score = 99.0 if last_close > avg_prev else 0.0

    return pct, z_score, prev_closes


def compute_4h_change_pct_from_240m_ohlc(ohlc_4h, now_ts=None):
    """
    Compute the official last closed 4h candle percent from LBank 240m OHLC.

    This uses the raw 240m candle open and close returned by LBank. It does
    not rebuild a rolling 4h value from 30m closes.
    """
    if not isinstance(ohlc_4h, dict):
        return 0.0

    if now_ts is None:
        now_ts = int(time.time())
    else:
        now_ts = _safe_int_timestamp(now_ts) or int(time.time())

    timeframe_seconds = 4 * 60 * 60
    t_values = ohlc_4h.get("t") or []
    o_values = ohlc_4h.get("o") or []
    c_values = ohlc_4h.get("c") or []

    latest_closed = None

    for idx, raw_ts in enumerate(t_values):
        candle_start = _safe_int_timestamp(raw_ts)
        if candle_start is None:
            continue

        candle_end = candle_start + timeframe_seconds
        if candle_end > now_ts:
            continue

        try:
            open_price = float(o_values[idx])
            close_price = float(c_values[idx])
        except Exception:
            continue

        if open_price <= 0 or close_price <= 0:
            continue

        if latest_closed is None or candle_start > latest_closed[0]:
            latest_closed = (candle_start, open_price, close_price)

    if latest_closed is None:
        return 0.0

    _, open_price, close_price = latest_closed
    return percent_change(open_price, close_price)

# =========================
# PUMP ANALYSIS
# =========================


def analyze_symbol(symbol_udf, stats_entry, change_key, volume_key, sleep_after=True):
    latest = _safe_float_default(stats_entry.get("latest", 0.0), 0.0)

    now_ts = int(time.time())

    ohlc_30m_full = fetch_ohlc(
        symbol_udf,
        resolution="30",
        countback=CANDLE_30M_FETCH,
    )
    closes_30m_full = extract_prices_from_ohlc(
        ohlc_30m_full,
        resolution="30",
        expected_count=CANDLE_30M_FETCH,
        now_ts=now_ts,
        fill_missing=True,
        seed_price=latest,
        include_live_current=False,
    )

    pct_30m_info = compute_latest_candle_change_pct_from_ohlc(
        ohlc_30m_full,
        resolution="30",
        expected_count=CANDLE_30M_FETCH,
        now_ts=now_ts,
        seed_price=latest,
        live_price=latest,
        include_live_current=bool(CANDLE_PERCENT_USE_LIVE_CANDLE),
    )
    pct_30m_candle = float(pct_30m_info.get("pct", 0.0) or 0.0)
    had_30m_candle_data = bool(pct_30m_info.get("had_data", False))

    ohlc_4h = fetch_ohlc(
        symbol_udf,
        resolution="240",
        countback=CANDLE_4H_FETCH,
    )
    # v4: pump/star logic keeps the original closed-4h candle rule.
    # The displayed 4h_pct below may use the current/live candle after scale validation,
    # but is_pump decisions must not use display-only live percentages.
    pct_4h_logic = compute_4h_change_pct_from_240m_ohlc(ohlc_4h, now_ts=now_ts)
    pct_4h_info = compute_latest_candle_change_pct_from_ohlc(
        ohlc_4h,
        resolution="240",
        expected_count=CANDLE_4H_FETCH,
        now_ts=now_ts,
        seed_price=latest,
        live_price=latest,
        include_live_current=bool(CANDLE_PERCENT_USE_LIVE_CANDLE),
    )
    pct_4h = float(pct_4h_info.get("pct", 0.0) or 0.0)

    min_needed_30m = CANDLE_30M_PREV_FOR_AVG + 1
    if not closes_30m_full or len(closes_30m_full) < min_needed_30m:
        if sleep_after:
            time.sleep(OHLC_SLEEP_BETWEEN_SYMBOLS)

        change_pct = _safe_float_default(stats_entry.get(change_key, 0.0), 0.0)
        volume_val = _safe_float_default(stats_entry.get(volume_key, 0.0), 0.0)

        return {
            "symbol": symbol_udf,
            "market_type": market_type_from_symbol(symbol_udf),
            "4h_pct": pct_4h,
            "4h_logic_pct": pct_4h_logic,
            "30m_pct": pct_30m_candle,
            "30m_momentum_pct": 0.0,
            "z_score": 0.0,
            "24h_pct": change_pct,
            "price": latest,
            "volume": volume_val,
            "is_pump": False,
            "recent_pump_diff_pct": -9999.0,
            "is_recent_pump": False,
            "had_30m_data": had_30m_candle_data
        }

    last_vs_avg_pct, z_score, prev_closes = compute_close_vs_previous_average_pct(
        closes_30m_full,
        previous_count=CANDLE_30M_PREV_FOR_AVG,
    )

    change_pct = _safe_float_default(stats_entry.get(change_key, 0.0), 0.0)
    volume_val = _safe_float_default(stats_entry.get(volume_key, 0.0), 0.0)

    # v4: live stats prices are not injected into closes_30m_full.
    # Pump/star decisions remain on the original OHLC close-vs-previous-6-average momentum value.
    # v3: keep the original pump/star decision logic exactly on the old
    # close-vs-previous-6-average momentum value.  Only the displayed/report
    # 30m_pct below is the real latest candle percent.
    if pct_4h_logic == 0.0:
        is_pump = (last_vs_avg_pct >= PUMP_ALT_30M_PCT) and (z_score >= MIN_Z_SCORE)
    else:
        is_pump = (
            last_vs_avg_pct >= PUMP_MIN_30M_LAST_VS_AVG_PCT and
            pct_4h_logic >= PUMP_MIN_4H_PCT and
            z_score >= MIN_Z_SCORE
        )

    recent_pump_diff_pct = abs(change_pct - last_vs_avg_pct)

    is_recent_pump = (
        last_vs_avg_pct >= RECENT_PUMP_DIFF_PCT and
        change_pct > 0 and
        recent_pump_diff_pct >= RECENT_PUMP_DIFF_PCT
    )

    if sleep_after:
        time.sleep(OHLC_SLEEP_BETWEEN_SYMBOLS)

    return {
        "symbol": symbol_udf,
        "market_type": market_type_from_symbol(symbol_udf),
        "4h_pct": pct_4h,
        "4h_logic_pct": pct_4h_logic,
        "30m_pct": pct_30m_candle,
        "30m_momentum_pct": last_vs_avg_pct,
        "z_score": z_score,
        "24h_pct": change_pct,
        "price": latest,
        "volume": volume_val,
        "is_pump": is_pump,
        "recent_pump_diff_pct": recent_pump_diff_pct,
        "is_recent_pump": is_recent_pump,
        "had_30m_data": had_30m_candle_data
    }

def analyze_base_symbol_dual_market(base_symbol, coin_info, sleep_after_each_market=True):

    usdt_result = None

    if "USDT" in coin_info.get("markets", {}):
        usdt_market = coin_info["markets"]["USDT"]
        usdt_result = analyze_symbol(
            usdt_market["symbol_udf"],
            usdt_market["stats_entry"],
            usdt_market["change_key"],
            usdt_market["volume_key"],
            sleep_after=sleep_after_each_market
        )

    usdt_pump = bool(usdt_result and usdt_result.get("is_pump"))
    all_results = [r for r in (usdt_result,) if r is not None]
    recent_results = [r for r in all_results if r.get("is_recent_pump")]

    if usdt_result is not None:
        best_result = usdt_result
    else:
        best_result = {
            "4h_pct": 0.0,
            "4h_logic_pct": 0.0,
            "30m_pct": 0.0,
            "30m_momentum_pct": 0.0,
            "z_score": 0.0,
            "24h_pct": 0.0,
            "price": 0.0,
            "market_type": None,
            "is_pump": False,
            "had_30m_data": False
        }

    best_recent_result = None
    if recent_results:
        best_recent_result = max(recent_results, key=lambda r: r.get("recent_pump_diff_pct", 0.0))

    best_24h = max([r.get("24h_pct", float("-inf")) for r in all_results] or [0.0])
    had_any_30m_data = any(bool(r.get("had_30m_data")) for r in all_results)

    return {
        "symbol": base_symbol,
        "4h_pct": best_result.get("4h_pct", 0.0),
        "4h_logic_pct": best_result.get("4h_logic_pct", 0.0),
        "30m_pct": best_result.get("30m_pct", 0.0),
        "30m_momentum_pct": best_result.get("30m_momentum_pct", 0.0),
        "z_score": best_result.get("z_score", 0.0),
        "24h_pct": best_24h,
        "price": best_result.get("price", 0.0),
        "trigger_market": "USDT" if usdt_result is not None else None,
        "pump_in_irt": False,
        "pump_in_usdt": usdt_pump,
        "irt_market": None,
        "usdt_market": usdt_result,
        "is_recent_pump": bool(best_recent_result),
        "recent_pump_diff_pct": best_recent_result.get("recent_pump_diff_pct", 0.0) if best_recent_result else 0.0,
        "recent_pump_30m_pct": best_recent_result.get("30m_pct", 0.0) if best_recent_result else 0.0,
        "had_any_30m_data": had_any_30m_data
    }

# =========================
# MAIN FULL SCAN
# =========================

def scan_once():
    skip_symbols = build_skip_symbol_set()
    all_coins = {}
    candidate_pairs = []

    for raw_symbol, ticker in fetch_lbank_usdt_tickers():
        symbol_udf = stats_key_to_market_symbol(raw_symbol)
        if not symbol_udf:
            continue

        base_symbol = base_symbol_from_market_symbol(symbol_udf)
        if not base_symbol or base_symbol in skip_symbols:
            continue

        change_pct = _safe_float_default(ticker.get("change"), 0.0)
        volume_val = _safe_float_default(ticker.get("vol"), 0.0)
        latest = _safe_float_default(ticker.get("latest"), 0.0)
        if latest <= 0:
            continue

        all_coins[base_symbol] = {
            "base_symbol": base_symbol,
            "markets": {
                "USDT": {
                    "raw_symbol": raw_symbol,
                    "symbol_udf": symbol_udf,
                    "stats_entry": ticker,
                    "change_key": "change",
                    "volume_key": "vol",
                    "change_pct": change_pct,
                    "volume": volume_val,
                    "latest": latest,
                    "turnover": _safe_float_default(ticker.get("turnover"), 0.0),
                }
            }
        }

        if (
            volume_val >= MIN_VOLUME_USDT
            and MIN_DAYCHANGE_PCT <= change_pct <= MAX_DAYCHANGE_PCT
        ):
            candidate_pairs.append((0, -change_pct, base_symbol, "USDT", change_pct))

    candidate_pairs.sort(key=lambda item: (item[1], item[0], item[2], item[3]))
    selected_pairs = candidate_pairs[:MAX_CANDIDATE_MARKETS]

    candidates = []
    for _priority, neg_change, base_symbol, market_type, change_pct in selected_pairs:
        coin_info = all_coins.get(base_symbol)
        if not coin_info:
            continue
        candidates.append((base_symbol, change_pct, coin_info))

    if not candidates:
        return []

    pumped_coins_data = []
    ohlc_prefetch = prefetch_ohlc_for_coin_infos(
        [coin_info for _, _, coin_info in candidates],
        label="LBANK-FULL",
    )

    with use_ohlc_prefetch_context(ohlc_prefetch, strict=OHLC_PREFETCH_STRICT):
        for base_symbol, rank_change, coin_info in candidates:
            merged_result = analyze_base_symbol_dual_market(
                base_symbol=base_symbol,
                coin_info=coin_info,
                sleep_after_each_market=False
            )

            if merged_result.get("pump_in_usdt"):
                pumped_coins_data.append(merged_result)

    return pumped_coins_data

# =========================
# DEMANDED 2-MINUTE SYMBOL SCAN
# =========================

def scan_requested_symbols_once(requested_base_symbols):
    requested_base_symbols = [str(x).upper().strip() for x in requested_base_symbols if str(x).strip()]
    requested_set = set(requested_base_symbols)

    if not requested_set:
        return {}

    all_coins = build_all_coin_market_map(skip_default_tags=False)
    if not all_coins:
        print(f"{COL_WARN}[DEMAND]{RESET} Could not fetch market snapshot for demanded symbols.")
        return {}

    results = {}
    target_coin_infos = [all_coins.get(base_symbol) for base_symbol in sorted(requested_set) if all_coins.get(base_symbol)]
    ohlc_prefetch = prefetch_ohlc_for_coin_infos(target_coin_infos, label="DEMAND")

    with use_ohlc_prefetch_context(ohlc_prefetch, strict=OHLC_PREFETCH_STRICT):
        for base_symbol in sorted(requested_set):
            coin_info = all_coins.get(base_symbol)
            if not coin_info:
                results[base_symbol] = {
                    "symbol": base_symbol,
                    "not_found": True,
                    "pump_in_irt": False,
                    "pump_in_usdt": False,
                    "irt_market": None,
                    "usdt_market": None,
                    "is_recent_pump": False,
                    "recent_pump_diff_pct": 0.0,
                    "recent_pump_30m_pct": 0.0,
                    "had_any_30m_data": False
                }
                continue

            merged_result = analyze_base_symbol_dual_market(
                base_symbol=base_symbol,
                coin_info=coin_info,
                sleep_after_each_market=False
            )
            merged_result["not_found"] = False
            results[base_symbol] = merged_result

    return results

# =========================
# NOTIFICATION / EXECUTION HELPERS
# =========================


def build_notification_message(scan_time_str, due_scan_type, starred_formatted, streak_counts, pumped_list):
    msg_lines = [
        f"{scan_time_str}",
        ""
    ]

    red_items = []
    for coin in pumped_list:
        if not coin.get("is_recent_pump"):
            continue
        top_market = choose_top_red_circle_market_result(coin)
        if top_market is None:
            continue
        red_items.append((coin, top_market))

    if red_items:
        red_items.sort(
            key=lambda item: (
                item[1].get("recent_pump_diff_pct", 0.0),
                get_top_red_circle_change_pct(item[0], item[1]),
            ),
            reverse=True,
        )
        for coin, top_market in red_items:
            streak_key = get_streak_key_for_coin(coin)
            visible_symbol = get_top_red_circle_symbol_for_coin(coin, top_market)
            streak_count = int(streak_counts.get(streak_key, 1) or 1)
            recent_change = get_top_red_circle_change_pct(coin, top_market)
            streak_suffix = f" x{streak_count}" if streak_count >= 2 else ""
            msg_lines.append(f"🔴 {visible_symbol}{streak_suffix} {recent_change:+.2f}%")
        msg_lines.append("")

    if starred_formatted:
        for streak_key in starred_formatted:
            msg_lines.append(starred_formatted[streak_key])
        msg_lines.append("")

    if pumped_list:
        last_tier = "START"

        def get_tier(pct):
            if pct >= 15:
                return "t15"
            if pct >= 10:
                return "t10"
            if pct >= 5:
                return "t5"
            return "t0"

        for coin in pumped_list:
            stats_market = choose_notification_market_result(coin)
            if stats_market is not None:
                pct = stats_market.get("24h_pct", coin["24h_pct"])
                pct_4h = stats_market.get("4h_pct", coin["4h_pct"])
                pct_30m = stats_market.get("30m_pct", coin["30m_pct"])
                pct_1d = stats_market.get("24h_pct", coin["24h_pct"])
            else:
                pct = coin["24h_pct"]
                pct_4h = coin["4h_pct"]
                pct_30m = coin["30m_pct"]
                pct_1d = coin["24h_pct"]

            tier = get_tier(pct)
            if last_tier != "START" and tier != last_tier:
                msg_lines.append("------------------------------------")
                msg_lines.append("")
            last_tier = tier

            display_symbol = get_display_symbol(coin["symbol"])
            red_marker = " 🔴" if coin.get("is_recent_pump") else ""
            usdt_market = coin.get("usdt_market")
            usd_price_text = "-"
            if usdt_market is not None:
                usd_price_text = fmt_usdt_price_for_message(usdt_market.get("price", 0.0))
                usd_price_text = maybe_bold(usd_price_text, coin.get("pump_in_usdt", False))

            msg_lines.append(f"- *{display_symbol}*{red_marker}")
            msg_lines.append(f"4h: *{pct_4h:+.2f}%* | 30m: *{pct_30m:+.2f}%*")
            msg_lines.append(f"1d: *{pct_1d:+.2f}%* | USDT: {usd_price_text}")
            msg_lines.append("")
    else:
        msg_lines.append("- No matches found")

    return "\n".join(msg_lines).strip()

def pct_is_equal(a, b, tolerance=1e-12):
    if a is None or b is None:
        return False
    return abs(a - b) <= tolerance

def pct_is_zero(v, tolerance=1e-12):
    if v is None:
        return False
    return abs(v) <= tolerance

def get_demand_line_arrow(current_pct, previous_pct):
    if current_pct < 0:
        return "⬇️"

    if pct_is_zero(current_pct):
        if previous_pct is not None and previous_pct > 0:
            return "↘️"
        return "➡️"

    if previous_pct is None:
        return "⬆️"

    if pct_is_equal(current_pct, previous_pct):
        return "➡️"

    if current_pct > previous_pct:
        return "⬆️"

    if current_pct < previous_pct:
        return "↘️"

    return "➡️"

def get_demand_line_color(current_pct, previous_pct):
    if current_pct < 0:
        return "🔴"

    if pct_is_zero(current_pct):
        if previous_pct is not None and previous_pct > 0:
            return "🟠"
        return "⚪"

    if previous_pct is None:
        return "🟢"

    if pct_is_equal(current_pct, previous_pct):
        return "⚪"

    if current_pct > previous_pct:
        return "🟢"

    if current_pct < previous_pct:
        return "🟠"

    return "⚪"

def get_market_display_price_for_demand(market_key, price):
    return fmt_usdt_price_for_message(price)


def build_demand_compact_line(base_symbol, market_key, market_result, tracking):
    if market_result is None:
        return None

    try:
        current_price = float(market_result.get("price", 0.0) or 0.0)
    except Exception:
        current_price = None

    if current_price is None:
        return None

    entry_price = tracking.get("entry_price") if isinstance(tracking, dict) else None
    previous_price = tracking.get("last_sent_price")
    if previous_price is None and isinstance(tracking, dict):
        previous_price = tracking.get("last_price")

    try:
        entry_price = float(entry_price) if entry_price is not None else None
    except Exception:
        entry_price = None

    try:
        previous_price = float(previous_price) if previous_price is not None else None
    except Exception:
        previous_price = None

    if entry_price is not None and entry_price != 0:
        pct_from_entry = percent_change(entry_price, current_price)
    else:
        pct_from_entry = 0.0

    if entry_price is not None and entry_price != 0 and previous_price is not None:
        previous_pct_from_entry = percent_change(entry_price, previous_price)
    else:
        previous_pct_from_entry = None

    color_emoji = get_demand_line_color(pct_from_entry, previous_pct_from_entry)
    arrow = get_demand_line_arrow(pct_from_entry, previous_pct_from_entry)
    market_symbol = f"{base_symbol}{market_key}"
    display_price = get_market_display_price_for_demand(market_key, current_price)

    return f"{color_emoji} {market_symbol} {pct_from_entry:+.2f}% {arrow} {display_price}"

def build_combined_demand_notification_message(results_list, tracking_by_symbol):
    msg_lines = []

    if not results_list:
        return None

    for result in results_list:
        base_symbol = result.get("symbol", "")

        if result.get("not_found"):
            continue

        if not result.get("had_any_30m_data"):
            continue

        symbol_tracking = tracking_by_symbol.get(base_symbol, {}) if isinstance(tracking_by_symbol, dict) else {}
        usdt_line = build_demand_compact_line(
            base_symbol,
            "USDT",
            result.get("usdt_market"),
            symbol_tracking.get("USDT", {})
        )
        block_lines = []
        if usdt_line:
            block_lines.append(usdt_line)

        if not block_lines:
            continue

        if msg_lines:
            msg_lines.append("")
        msg_lines.extend(block_lines)

    if not msg_lines:
        return None

    return "\n".join(msg_lines).strip()

def build_demand_notification_message(result, tracking):
    base_symbol = result.get("symbol", "")

    if result.get("not_found"):
        return None

    if not result.get("had_any_30m_data"):
        return None

    lines = []

    usdt_line = build_demand_compact_line(base_symbol, "USDT", result.get("usdt_market"), tracking.get("USDT", {}))

    if usdt_line:
        lines.append(usdt_line)

    if not lines:
        return None

    return "\n".join(lines).strip()

def execute_scan_cycle(scan_kind, scan_dt=None, scheduled_slot_dt=None, source_label="SCHEDULER"):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    scan_time_str = format_scan_time(scan_dt)

    _ = load_star_state(scan_dt=scan_dt)
    _ = load_scan_history(scan_dt=scan_dt)
    _ = load_bale_today_sent_state(scan_dt=scan_dt)

    pumped_list = scan_once()

    starred_formatted, streak_counts = update_starred_symbols(pumped_list, scan_dt=scan_dt)

    final_bale_message = build_notification_message(
        scan_time_str=scan_time_str,
        due_scan_type=scan_kind,
        starred_formatted=starred_formatted,
        streak_counts=streak_counts,
        pumped_list=pumped_list
    )

    if pumped_list:
        success_count, success_chat_ids = send_bale_notification(final_bale_message)
        if success_count > 0:
            record_bale_today_sent_coin_names(
                extract_today_coin_names_from_pumped_list(pumped_list),
                sent_dt=scan_dt,
                source_label=source_label,
                message_kind="full_scan"
            )
            precompute_bale_today_chart_resolutions_if_idle(scan_dt=scan_dt, force_refresh=False)
        else:
            log_info(f"{COL_INFO}[BALE TODAY]{RESET} Full scan message reached no chats successfully. Today-ledger not updated.")
    else:
        log_info(f"{COL_INFO}[BALE]{RESET} Full scan returned no pumped coins. Bale notification skipped.")

    if scheduled_slot_dt is not None:
        record_scheduled_scan_history(
            scheduled_slot_dt=scheduled_slot_dt,
            scan_kind=scan_kind,
            actual_scan_dt=scan_dt,
            source_label=source_label
        )

    broadcast_local_event("state", build_local_chat_status(scan_dt))
    return pumped_list, starred_formatted

def execute_demand_scan_cycle(scan_dt=None, scheduled_slot_dt=None, source_label="DEMAND"):
    if scan_dt is None:
        scan_dt = get_tehran_now()

    if scheduled_slot_dt is None:
        scheduled_slot_dt = get_latest_due_demand_slot(scan_dt)

    if scheduled_slot_dt is None:
        return {}

    snapshot = get_demand_subscriber_snapshot()
    if not snapshot:
        mark_demand_slot_executed(scheduled_slot_dt)
        return {}

    requested_symbols = sorted(snapshot.keys())
    results = scan_requested_symbols_once(requested_base_symbols=requested_symbols)

    chat_to_symbols = {}
    for base_symbol, chat_ids in snapshot.items():
        for chat_id in chat_ids:
            chat_to_symbols.setdefault(str(chat_id), []).append(base_symbol)

    for chat_id in sorted(chat_to_symbols.keys()):
        user_symbols = chat_to_symbols.get(chat_id, [])
        user_results = []
        tracking_by_symbol = {}

        for base_symbol in sorted(set(user_symbols)):
            result = results.get(base_symbol, {
                "symbol": base_symbol,
                "not_found": True,
                "usdt_market": None,
                "irt_market": None,
                "pump_in_usdt": False,
                "pump_in_irt": False,
                "is_recent_pump": False,
                "recent_pump_diff_pct": 0.0,
                "recent_pump_30m_pct": 0.0,
                "had_any_30m_data": False
            })
            user_results.append(result)
            tracking_by_symbol[base_symbol] = get_demand_tracking_snapshot(chat_id, base_symbol)

        final_msg = build_combined_demand_notification_message(user_results, tracking_by_symbol)

        for result in user_results:
            base_symbol = result.get("symbol", "")
            if not base_symbol or result.get("not_found"):
                continue
            capture_demand_scan_snapshot(chat_id, base_symbol, result, scan_dt=scan_dt)

        if not final_msg:
            continue

        sent_ok = send_bale_message(chat_id, final_msg, message_kind="demand_scan")
        if sent_ok:
            for result in user_results:
                base_symbol = result.get("symbol", "")
                if not base_symbol or result.get("not_found") or not result.get("had_any_30m_data"):
                    continue
                mark_demand_notification_sent(chat_id, base_symbol, result, scan_dt=scan_dt)

    mark_demand_slot_executed(scheduled_slot_dt)
    broadcast_local_event("state", build_local_chat_status(scan_dt))
    return results

# =========================
# SCHEDULER & ENTRY POINT
# =========================

def get_next_run_time(now_local=None):
    if now_local is None:
        now_local = get_tehran_now()

    today_slots = build_scan_slots_for_day(now_local)

    for slot_dt in today_slots:
        if slot_dt > now_local:
            return slot_dt

    tomorrow = now_local + timedelta(days=1)
    tomorrow_slots = build_scan_slots_for_day(tomorrow)
    return tomorrow_slots[0]

def choose_next_due_job(now_local):
    due_full_slot_dt, due_scan_type = get_due_scan_slot(now_local, grace_seconds=SCHEDULER_GRACE_SECONDS)
    due_demand_slot_dt = get_latest_due_demand_slot(now_local)

    if due_full_slot_dt is not None and due_scan_type is not None:
        return "full", due_full_slot_dt, due_scan_type

    if due_demand_slot_dt is not None:
        return "demand", due_demand_slot_dt, "demand"

    return None, None, None

def get_next_scheduler_wakeup(now_local):
    next_full = get_next_run_time(now_local=now_local)
    next_demand = get_next_demand_run_time(now_local=now_local)

    if next_demand is None:
        return next_full

    if next_full is None:
        return next_demand

    return min(next_full, next_demand)

def main():
    print(f"{BOLD}LBank Pump Radar (USDT-only).{RESET}")

    load_demand_runtime_into_memory()
    load_bale_runtime_into_memory()
    _ = load_bale_today_sent_state()
    _ = load_local_chat_state()
    _ = load_subscriber_state()
    ensure_subscription_expiry_warnings(notify=False, now_dt=get_tehran_now())
    ensure_access_lock_notices(notify=False, now_dt=get_tehran_now())

    append_local_chat_message(
        chat_id="system",
        text=(
            f"ربات شروع شد. رابط وب روی پورت {LOCAL_WEB_PORT} در دسترس است. "
            f"مسیرهای نوبیت زیر /nobit و مسیرهای سیگنالر زیر /signaler قرار دارند. "
            f"مدت نگه‌داری چت محلی: {LOCAL_CHAT_RETENTION_HOURS} ساعت."
        ),
        direction="system",
        transport="local",
        source="system",
        meta={
            "event": "startup"
        }
    )

    local_web_thread = threading.Thread(target=local_web_server_loop, daemon=True)
    local_web_thread.start()

    cleanup_thread = threading.Thread(target=local_chat_cleanup_loop, daemon=True)
    cleanup_thread.start()

    ensure_bale_outgoing_worker_started()
    ensure_adver_scheduler_started()

    bale_thread = threading.Thread(target=bale_update_listener, daemon=True)
    bale_thread.start()

    signaler = import_lbank_signaler_module()

    signaler.LOCAL_WEB_HOST = LOCAL_WEB_HOST
    signaler.LOCAL_WEB_PORT = LOCAL_WEB_PORT

    signaler_thread = threading.Thread(target=signaler.main_worker_only, daemon=True)
    signaler_thread.start()

    try:
        while True:
            ensure_subscription_expiry_warnings(notify=True, now_dt=get_tehran_now())
            ensure_access_lock_notices(notify=True, now_dt=get_tehran_now())
            cooldown_remaining = get_scan_cooldown_remaining()
            if cooldown_remaining > 0:
                WAKEUP_EVENT.wait(timeout=min(SCHEDULER_CHECK_SECONDS, max(1, int(cooldown_remaining))))
                WAKEUP_EVENT.clear()
                continue

            current_dt = get_tehran_now()
            due_job_kind, due_slot_dt, due_scan_type = choose_next_due_job(current_dt)

            if due_job_kind == "full":
                actual_scan_dt = get_tehran_now()
                scan_started_mono = time.monotonic()
                set_active_scan_kind("full")
                broadcast_local_event("state", build_local_chat_status(actual_scan_dt))
                try:
                    execute_scan_cycle(
                        scan_kind=due_scan_type,
                        scan_dt=actual_scan_dt,
                        scheduled_slot_dt=due_slot_dt,
                        source_label="SCHEDULER"
                    )
                finally:
                    clear_active_scan_kind()
                    apply_scan_runtime_usage_to_free_trials(time.monotonic() - scan_started_mono, now_dt=get_tehran_now())
                    ensure_access_lock_notices(notify=True, now_dt=get_tehran_now())

                if due_scan_type == "full" and due_slot_dt.hour == 23 and due_slot_dt.minute == 45:
                    clear_star_state()
                    close_scan_history_for_day(scan_dt=actual_scan_dt, closed_slot_dt=due_slot_dt, scan_kind=due_scan_type)
                    close_bale_today_sent_for_day(scan_dt=actual_scan_dt, closed_slot_dt=due_slot_dt, scan_kind=due_scan_type)

                continue

            if due_job_kind == "demand":
                actual_scan_dt = get_tehran_now()
                scan_started_mono = time.monotonic()
                set_active_scan_kind("demand")
                broadcast_local_event("state", build_local_chat_status(actual_scan_dt))
                try:
                    execute_demand_scan_cycle(
                        scan_dt=actual_scan_dt,
                        scheduled_slot_dt=due_slot_dt,
                        source_label="DEMAND"
                    )
                finally:
                    clear_active_scan_kind()
                    apply_scan_runtime_usage_to_free_trials(time.monotonic() - scan_started_mono, now_dt=get_tehran_now())
                    ensure_access_lock_notices(notify=True, now_dt=get_tehran_now())

                continue

            next_run = get_next_scheduler_wakeup(now_local=get_tehran_now())
            sleep_seconds = (next_run - get_tehran_now()).total_seconds()
            next_check_sleep = max(1, min(SCHEDULER_CHECK_SECONDS, int(sleep_seconds) if sleep_seconds > 0 else 1))

            WAKEUP_EVENT.wait(timeout=next_check_sleep)
            WAKEUP_EVENT.clear()

    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
    except Exception as e:
        print("\n[UNHANDLED ERROR]", e)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
