#!/usr/bin/env python3
"""
XAUUSDT Dual-Timeframe Opening Range Midpoint Limit Bot
========================================================
- Symbol: XAUUSDT (Gold USDT-M Perpetual Futures)
- Mode 1 (1H / 5m): Captures the first closed 5m candle of each 1H bar,
  calculates High, Low, and Midpoint = (High + Low) / 2. Within the first
  30 minutes of the 1H bar, waits for a subsequent 5m candle to close above
  High (places Buy Limit at Midpoint) or below Low (places Sell Limit at
  Midpoint). Cancels unfilled limit orders when the 30-minute window ends.
- Mode 2 (4H / 15m): Captures the first closed 15m candle of each 4H bar,
  calculates High, Low, and Midpoint = (High + Low) / 2. Within the first
  2 hours of the 4H bar, waits for a subsequent 15m candle to close above
  High (places Buy Limit at Midpoint) or below Low (places Sell Limit at
  Midpoint). Cancels unfilled limit orders when the 2-hour window ends.
- Exits:
  * Take Profit (TP): Entry +/- $5.00 (default, customizable per mode)
  * Stop Loss (SL): First candle Low (for Long) or High (for Short),
    guarded safely ahead of exchange liquidation price.
- Includes: Mobile-friendly tabbed web dashboard, Admin Login gate (with
  stateless HMAC session tokens + cookie fallback for multi-worker & mobile
  compatibility), Multi-Exchange support (Binance USDT-M, Bybit, Bitget, OKX,
  KuCoin, Gate), Two-Way Telegram bot commands & inline buttons, JSON
  Backup/Restore, and Render Environment Variable persistence.
"""

import os
import sys
import json
import time
import re
import fcntl
import tempfile
import hmac
import hashlib
import base64
import secrets
import threading
import traceback
from datetime import datetime, timezone
from functools import wraps

try:
    import requests
except ImportError:
    import urllib.request as _ureq
    import urllib.parse as _uparse
    import urllib.error as _uerr
    import ssl as _ssl

    class _MiniResp:
        def __init__(self, status, body_bytes):
            self.status_code = status
            self.text = body_bytes.decode("utf-8", errors="replace")
        def json(self):
            return json.loads(self.text) if self.text else {}

    class _MiniRequests:
        @staticmethod
        def request(method, url, params=None, headers=None, json_data=None, data=None, timeout=10, **kwargs):
            if "json" in kwargs and json_data is None:
                json_data = kwargs["json"]
            if params:
                qs = _uparse.urlencode(params)
                url = url + ("&" if "?" in url else "?") + qs
            hdrs = dict(headers or {})
            body = None
            if json_data is not None:
                body = json.dumps(json_data).encode("utf-8")
                hdrs.setdefault("Content-Type", "application/json")
            elif data is not None:
                body = data.encode("utf-8") if isinstance(data, str) else data
            req = _ureq.Request(url, data=body, headers=hdrs, method=method.upper())
            ctx = _ssl.create_default_context()
            try:
                with _ureq.urlopen(req, timeout=timeout, context=ctx) as resp:
                    return _MiniResp(resp.status, resp.read())
            except _uerr.HTTPError as he:
                return _MiniResp(he.code, he.read())

        @classmethod
        def get(cls, url, **kwargs):
            return cls.request("GET", url, **kwargs)

        @classmethod
        def post(cls, url, **kwargs):
            return cls.request("POST", url, **kwargs)

    requests = _MiniRequests()
try:
    import ccxt
except ImportError:
    ccxt = None
from flask import Flask, request, jsonify, render_template_string, make_response

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------
APP_TITLE = "XAUUSDT ORB Midpoint Bot"
SYMBOL = "XAUUSDT"
DATA_FILE = os.environ.get("DATA_FILE", "users_data.json")
PORT = int(os.environ.get("PORT", 8080))
ADMIN_USER = os.environ.get("ADMIN_USERNAME", "admin").strip() or "admin"
DEFAULT_ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "admin123").strip() or "admin123"
SECRET_SALT = os.environ.get("APP_SECRET_KEY", "xau-orb-secret-salt-2026").strip() or "xau-orb-secret-salt-2026"

SESSION_TTL = 86400 * 7  # 7 days

# Global state locks and buffers
STATE_LOCK = threading.RLock()
SCANNER_LOGS = []
MAX_LOGS = 250
PUBLIC_IP_CACHE = {"ip": "Detecting...", "ts": 0}
TICKER_PRICE_CACHE = {"price": 0.0, "ts": 0}
EXCHANGE_CLIENTS = {}
EXCHANGE_MARKETS_LOADED = {}
BOT_PAUSED = False
BINANCE_BAN_UNTIL_MS = 0
CANDLE_CACHE = {}          # timeframe -> {"ts": float, "data": list}
CACHED_POSITIONS = []      # updated by single scanner process
_SCANNER_LOCK_FD = None
RUNTIME_STATE_FILE = os.path.join(tempfile.gettempdir(), "xau_orb_runtime.json")


def save_runtime_snapshot():
    try:
        with STATE_LOCK:
            snap = {
                "mark_price": TICKER_PRICE_CACHE["price"],
                "paused": BOT_PAUSED,
                "cycles": CYCLE_STATE,
                "live_positions": CACHED_POSITIONS,
                "logs": SCANNER_LOGS[-100:],
                "ts": time.time(),
            }
        tmp = RUNTIME_STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        os.replace(tmp, RUNTIME_STATE_FILE)
    except Exception:
        pass


def load_runtime_snapshot() -> dict:
    try:
        if os.path.exists(RUNTIME_STATE_FILE):
            with open(RUNTIME_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

# Cycle runtime trackers for 1H/5m and 4H/15m running independently
CYCLE_STATE = {
    "1H": {
        "cycle_ts": 0,
        "status": "INITIALIZING",
        "ref_high": None,
        "ref_low": None,
        "midpoint": None,
        "buy_entry": None,
        "sell_entry": None,
        "direction": None,       # "BUY" or "SELL"
        "limit_order_id": None,
        "limit_price": None,
        "sl_price": None,
        "tp_price": None,
        "qty": 0.0,
        "in_trade": False,
        "entry_price": None,
        "cycle_done": False,
        "last_checked_bar_ts": 0,
    },
    "4H": {
        "cycle_ts": 0,
        "status": "INITIALIZING",
        "ref_high": None,
        "ref_low": None,
        "midpoint": None,
        "buy_entry": None,
        "sell_entry": None,
        "direction": None,       # "BUY" or "SELL"
        "limit_order_id": None,
        "limit_price": None,
        "sl_price": None,
        "tp_price": None,
        "qty": 0.0,
        "in_trade": False,
        "entry_price": None,
        "cycle_done": False,
        "last_checked_bar_ts": 0,
    },
}


# ---------------------------------------------------------------------------
# Logging Helper
# ---------------------------------------------------------------------------
def log_event(msg: str, level: str = "INFO"):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    with STATE_LOCK:
        SCANNER_LOGS.append(line)
        if len(SCANNER_LOGS) > MAX_LOGS:
            del SCANNER_LOGS[: len(SCANNER_LOGS) - MAX_LOGS]


# ---------------------------------------------------------------------------
# Stateless HMAC Session Tokens (Works Across Multi-Worker Gunicorn & Cookies)
# ---------------------------------------------------------------------------
def create_session_token(username: str) -> str:
    exp = int(time.time()) + SESSION_TTL
    payload = f"{username}:{exp}".encode("utf-8")
    sig = hmac.new(SECRET_SALT.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    raw = f"{username}:{exp}:{sig}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def verify_session_token(token: str) -> bool:
    if not token:
        return False
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        parts = decoded.split(":")
        if len(parts) != 3:
            return False
        username, exp_str, sig = parts
        if int(exp_str) < int(time.time()):
            return False
        payload = f"{username}:{exp_str}".encode("utf-8")
        expected = hmac.new(SECRET_SALT.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, expected)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Encryption & Persistence Helpers
# ---------------------------------------------------------------------------
def _derive_key() -> bytes:
    return hashlib.sha256(SECRET_SALT.encode("utf-8")).digest()


def encrypt_secret(plain: str) -> str:
    if not plain:
        return ""
    key = _derive_key()
    raw = plain.encode("utf-8")
    nonce = secrets.token_bytes(16)
    stream = hashlib.pbkdf2_hmac("sha256", key, nonce, 1000, dklen=len(raw))
    cipher = bytes(a ^ b for a, b in zip(raw, stream))
    mac = hmac.new(key, nonce + cipher, hashlib.sha256).digest()[:16]
    return base64.urlsafe_b64encode(nonce + mac + cipher).decode("ascii")


def decrypt_secret(token: str) -> str:
    if not token:
        return ""
    try:
        blob = base64.urlsafe_b64decode(token.encode("ascii"))
        nonce, mac, cipher = blob[:16], blob[16:32], blob[32:]
        key = _derive_key()
        expected_mac = hmac.new(key, nonce + cipher, hashlib.sha256).digest()[:16]
        if not hmac.compare_digest(mac, expected_mac):
            return ""
        stream = hashlib.pbkdf2_hmac("sha256", key, nonce, 1000, dklen=len(cipher))
        raw = bytes(a ^ b for a, b in zip(cipher, stream))
        return raw.decode("utf-8")
    except Exception:
        return ""


def _hash_password(password: str) -> str:
    return hashlib.sha256((SECRET_SALT + ":" + password).encode("utf-8")).hexdigest()


def default_config() -> dict:
    env_api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    env_api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    env_tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    env_tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    exchanges = []
    if env_api_key and env_api_secret:
        exchanges.append(
            {
                "id": "env_binance",
                "name": "Binance Futures (Env)",
                "exchange_id": "binanceusdm",
                "api_key_enc": encrypt_secret(env_api_key),
                "api_secret_enc": encrypt_secret(env_api_secret),
                "passphrase_enc": "",
                "enabled": True,
                "testnet": os.environ.get("BINANCE_TESTNET", "false").lower() == "true",
            }
        )

    return {
        "admin": {
            "username": ADMIN_USER,
            "password_hash": _hash_password(DEFAULT_ADMIN_PASS),
        },
        "symbol": SYMBOL,
        "modes": {
            "1H": {
                "enabled": True,
                "macro_tf": "1h",
                "exec_tf": "5m",
                "window_minutes": 60,
                "tp_dollars": 5.0,
                "sl_offset_dollars": 0.0,
                "leverage": 50,
                "margin_usdt": 5.0,
                "liq_guard": True,
            },
            "4H": {
                "enabled": True,
                "macro_tf": "4h",
                "exec_tf": "15m",
                "window_minutes": 240,
                "tp_dollars": 5.0,
                "sl_offset_dollars": 0.0,
                "leverage": 50,
                "margin_usdt": 5.0,
                "liq_guard": True,
            },
        },
        "exchanges": exchanges,
        "telegram": {
            "enabled": bool(env_tg_token and env_tg_chat),
            "bot_token": env_tg_token,
            "chat_id": env_tg_chat,
        },
    }


def load_data() -> dict:
    with STATE_LOCK:
        if not os.path.exists(DATA_FILE):
            cfg = default_config()
            save_data(cfg)
            return cfg
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if data.get("modes", {}).get("1H", {}).get("window_minutes") == 30:
                data["modes"]["1H"]["window_minutes"] = 60
            if data.get("modes", {}).get("4H", {}).get("window_minutes") == 120:
                data["modes"]["4H"]["window_minutes"] = 240
            if os.environ.get("ADMIN_USERNAME", "").strip():
                data.setdefault("admin", {})["username"] = os.environ.get("ADMIN_USERNAME", "").strip()
            if os.environ.get("ADMIN_PASSWORD", "").strip():
                data.setdefault("admin", {})["password_hash"] = _hash_password(os.environ.get("ADMIN_PASSWORD", "").strip())

            env_api_key = os.environ.get("BINANCE_API_KEY", "").strip()
            env_api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
            if env_api_key and env_api_secret and not data.get("exchanges"):
                data["exchanges"] = default_config()["exchanges"]
            env_tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
            env_tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            if env_tg_token and not data.get("telegram", {}).get("bot_token"):
                data.setdefault("telegram", {})["bot_token"] = env_tg_token
            if env_tg_chat and not data.get("telegram", {}).get("chat_id"):
                data.setdefault("telegram", {})["chat_id"] = env_tg_chat
                if env_tg_token:
                    data["telegram"]["enabled"] = True
            return data
        except Exception as e:
            log_event(f"Error loading {DATA_FILE}: {e}. Recreating default.", "ERROR")
            cfg = default_config()
            save_data(cfg)
            return cfg


def save_data(data: dict):
    with STATE_LOCK:
        tmp_path = DATA_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, DATA_FILE)


# ---------------------------------------------------------------------------
# Public IP Detection
# ---------------------------------------------------------------------------
def get_public_ip() -> str:
    now = time.time()
    if now - PUBLIC_IP_CACHE["ts"] < 600 and PUBLIC_IP_CACHE["ip"] != "Detecting...":
        return PUBLIC_IP_CACHE["ip"]
    for url in ("https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"):
        try:
            r = requests.get(url, timeout=4)
            if r.status_code == 200 and r.text.strip():
                ip = r.text.strip()
                PUBLIC_IP_CACHE["ip"] = ip
                PUBLIC_IP_CACHE["ts"] = now
                return ip
        except Exception:
            continue
    return PUBLIC_IP_CACHE["ip"]


# ---------------------------------------------------------------------------
# Exchange Client & Market Data Helpers
# ---------------------------------------------------------------------------
def get_enabled_exchange_cfg(data: dict):
    for ex in data.get("exchanges", []):
        if ex.get("enabled"):
            return ex
    return None


class PureBybitClient:
    """Lightweight pure-Python Bybit v5 REST client (no ccxt/cryptography needed on Android APK)."""
    id = "bybit"
    markets = {"XAU/USDT:USDT": {"id": "XAUUSDT", "symbol": "XAU/USDT:USDT"}}

    def __init__(self, api_key: str, secret: str, sandbox: bool = False):
        self.apiKey = api_key
        self.secret = secret
        self.sandbox = sandbox
        self.base = "https://api-testnet.bybit.com" if sandbox else "https://api.bybit.com"

    def _req(self, method: str, path: str, params: dict = None) -> dict:
        ts = str(int(time.time() * 1000))
        recv = "5000"
        params = params or {}
        if method == "GET":
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            payload = ts + self.apiKey + recv + qs
            url = f"{self.base}{path}" + (f"?{qs}" if qs else "")
            body_kwargs = {}
        else:
            body_str = json.dumps(params)
            payload = ts + self.apiKey + recv + body_str
            url = f"{self.base}{path}"
            body_kwargs = {"data": body_str, "headers_extra": {"Content-Type": "application/json"}}
        sig = hmac.new(self.secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.apiKey,
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv,
        }
        if "headers_extra" in body_kwargs:
            headers.update(body_kwargs.pop("headers_extra"))
        r = requests.request(method, url, headers=headers, timeout=10, **body_kwargs)
        data = r.json()
        if data.get("retCode", 0) != 0:
            raise RuntimeError(f"bybit {json.dumps(data)}")
        return data

    def market_id(self, sym):
        return "XAUUSDT"

    def price_to_precision(self, sym, px):
        return f"{round(float(px), 2):.2f}"

    def amount_to_precision(self, sym, amt):
        return f"{round(float(amt), 3):.3f}"

    def fetch_balance(self):
        d = self._req("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        coins = d.get("result", {}).get("list", [{}])[0].get("coin", [])
        usdt = next((c for c in coins if c.get("coin") == "USDT"), {})
        tot = float(usdt.get("walletBalance") or 0)
        avail = float(usdt.get("availableToWithdraw") or usdt.get("equity") or tot)
        return {"USDT": {"total": tot, "free": avail}}

    def fetch_positions(self, syms=None):
        d = self._req("GET", "/v5/position/list", {"category": "linear", "symbol": "XAUUSDT"})
        out = []
        for p in d.get("result", {}).get("list", []):
            sz = float(p.get("size") or 0)
            side = (p.get("side") or "").lower()
            amt = sz if side == "buy" else (-sz if side == "sell" else 0.0)
            out.append({
                "contracts": abs(amt),
                "side": "long" if amt > 0 else ("short" if amt < 0 else ""),
                "entryPrice": float(p.get("avgPrice") or 0),
                "markPrice": float(p.get("markPrice") or 0),
                "liquidationPrice": float(p.get("liqPrice") or 0),
                "unrealizedPnl": float(p.get("unrealisedPnl") or 0),
                "info": {"positionAmt": str(amt)},
            })
        return out

    def set_margin_mode(self, mode, sym):
        pass

    def set_leverage(self, lev, sym):
        self._req("POST", "/v5/position/set-leverage", {
            "category": "linear", "symbol": "XAUUSDT", "buyLeverage": str(lev), "sellLeverage": str(lev)
        })

    def create_order(self, sym, otype, side, amount, price=None, params=None):
        params = params or {}
        body = {
            "category": "linear",
            "symbol": "XAUUSDT",
            "side": "Buy" if side.lower() == "buy" else "Sell",
            "orderType": "Limit" if otype.lower() == "limit" else "Market",
            "qty": str(amount),
        }
        if price is not None and otype.lower() == "limit":
            body["price"] = str(price)
        if params.get("reduceOnly"):
            body["reduceOnly"] = True
        for k in ("takeProfit", "stopLoss", "tpslMode", "tpTriggerBy", "slTriggerBy"):
            if k in params:
                body[k] = params[k]
        d = self._req("POST", "/v5/order/create", body)
        return {"id": d.get("result", {}).get("orderId", "")}

    def fetch_order(self, oid, sym):
        d = self._req("GET", "/v5/order/realtime", {"category": "linear", "symbol": "XAUUSDT", "orderId": oid})
        rows = d.get("result", {}).get("list", [])
        if not rows:
            d = self._req("GET", "/v5/order/history", {"category": "linear", "symbol": "XAUUSDT", "orderId": oid})
            rows = d.get("result", {}).get("list", [])
        if not rows:
            return {"status": "canceled", "filled": 0, "average": 0}
        o = rows[0]
        st_map = {"Filled": "closed", "Cancelled": "canceled", "Rejected": "rejected"}
        return {
            "status": st_map.get(o.get("orderStatus"), "open"),
            "filled": float(o.get("cumExecQty") or 0),
            "average": float(o.get("avgPrice") or o.get("price") or 0),
        }

    def fetch_open_orders(self, sym, params=None):
        d = self._req("GET", "/v5/order/realtime", {"category": "linear", "symbol": "XAUUSDT"})
        return [{"id": o.get("orderId"), "type": o.get("orderType"), "reduceOnly": bool(o.get("reduceOnly"))} for o in d.get("result", {}).get("list", [])]

    def cancel_order(self, oid, sym):
        return self._req("POST", "/v5/order/cancel", {"category": "linear", "symbol": "XAUUSDT", "orderId": oid})

    def privatePostV5PositionTradingStop(self, params):
        return self._req("POST", "/v5/position/trading-stop", params)

    def fetch_my_trades(self, sym, limit=6):
        d = self._req("GET", "/v5/execution/list", {"category": "linear", "symbol": "XAUUSDT", "limit": limit})
        return [{"side": (t.get("side") or "").lower(), "price": float(t.get("execPrice") or 0), "info": {}} for t in d.get("result", {}).get("list", [])]


def build_ccxt_client(ex_cfg: dict):
    ex_id = ex_cfg.get("exchange_id", "binanceusdm")
    cache_key = f"{ex_cfg.get('id')}_{ex_id}_{ex_cfg.get('testnet', False)}"
    if cache_key in EXCHANGE_CLIENTS:
        return EXCHANGE_CLIENTS[cache_key]

    api_key = decrypt_secret(ex_cfg.get("api_key_enc", ""))
    api_secret = decrypt_secret(ex_cfg.get("api_secret_enc", ""))
    passphrase = decrypt_secret(ex_cfg.get("passphrase_enc", ""))

    if ccxt is None:
        if ex_id == "bybit":
            client = PureBybitClient(api_key, api_secret, bool(ex_cfg.get("testnet")))
            EXCHANGE_CLIENTS[cache_key] = client
            return client
        raise RuntimeError("Pure-Python APK uses Bybit Perpetual (ccxt not bundled).")
    klass = getattr(ccxt, ex_id, None)
    if klass is None:
        raise ValueError(f"Unsupported exchange ID: {ex_id}")

    params = {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "fetchCurrencies": False},
    }
    if passphrase:
        params["password"] = passphrase

    client = klass(params)
    if ex_cfg.get("testnet"):
        try:
            client.set_sandbox_mode(True)
        except Exception:
            pass

    client.has["fetchCurrencies"] = False
    EXCHANGE_CLIENTS[cache_key] = client
    return client


def ensure_markets(client, cache_key: str):
    if getattr(client, "id", "") in ("binanceusdm", "bybit"):
        if not getattr(client, "markets", None):
            client.set_markets([
                {
                    "id": "XAUUSDT",
                    "lowercaseId": "xauusdt",
                    "symbol": "XAU/USDT:USDT",
                    "base": "XAU",
                    "quote": "USDT",
                    "settle": "USDT",
                    "baseId": "XAU",
                    "quoteId": "USDT",
                    "settleId": "USDT",
                    "type": "swap",
                    "spot": False,
                    "margin": False,
                    "swap": True,
                    "future": False,
                    "option": False,
                    "contract": True,
                    "linear": True,
                    "inverse": False,
                    "contractSize": 1.0,
                    "active": True,
                    "precision": {"price": 0.01, "amount": 0.001},
                    "limits": {
                        "amount": {"min": 0.001, "max": 1000.0},
                        "price": {"min": 0.01, "max": 1000000.0},
                        "cost": {"min": 5.0, "max": 10000000.0},
                    },
                    "info": {"symbol": "XAUUSDT", "contractType": "PERPETUAL", "status": "TRADING"},
                }
            ])
        return
    now = time.time()
    if now - EXCHANGE_MARKETS_LOADED.get(cache_key, 0) > 43200:
        client.load_markets()
        EXCHANGE_MARKETS_LOADED[cache_key] = now


def resolve_ccxt_symbol(client, raw_sym: str = SYMBOL) -> str:
    clean = raw_sym.replace("/", "").replace(":USDT", "").upper()
    candidates = [
        f"{clean[:-4]}/USDT:USDT",
        clean,
        f"{clean[:-4]}/USDT",
    ]
    if clean == "XAUUSDT":
        candidates.extend(["PAXG/USDT:USDT", "PAXGUSDT", "PAXG/USDT"])

    for cand in candidates:
        if cand in client.markets:
            return cand
    for m_sym, m_info in client.markets.items():
        if m_info.get("id", "").upper() == clean and m_info.get("swap"):
            return m_sym
    return "XAU/USDT:USDT"


def _record_binance_ban_if_any(err_obj):
    global BINANCE_BAN_UNTIL_MS
    msg = str(err_obj)
    m = re.search(r"banned until (\d{13})", msg)
    if m:
        until_ms = int(m.group(1))
        if until_ms > BINANCE_BAN_UNTIL_MS:
            BINANCE_BAN_UNTIL_MS = until_ms
            until_str = datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S UTC")
            log_event(f"Binance IP rate-limit ban detected until {until_str}. Backing off Binance calls until then.", "WARN")
    elif "418" in msg or "-1003" in msg or "Too many requests" in msg:
        BINANCE_BAN_UNTIL_MS = max(BINANCE_BAN_UNTIL_MS, int(time.time() * 1000) + 180_000)


def _is_binance_banned() -> bool:
    return int(time.time() * 1000) < BINANCE_BAN_UNTIL_MS


def fetch_public_xau_candles(timeframe: str = "5m", limit: int = 30) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; XAUORBBot/1.0)"}
    # 1. Binance Futures public mirrors
    mirrors = [
        "https://fapi.binance.com/fapi/v1/klines",
        "https://fapi1.binance.com/fapi/v1/klines",
        "https://fapi2.binance.com/fapi/v1/klines",
        "https://fapi3.binance.com/fapi/v1/klines",
    ]
    for url in mirrors:
        try:
            resp = requests.get(
                url,
                params={"symbol": SYMBOL, "interval": timeframe, "limit": limit},
                headers=headers,
                timeout=6,
            )
            if resp.status_code == 200:
                raw = resp.json()
                if isinstance(raw, list) and len(raw) > 0:
                    candles = [
                        [int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                        for r in raw
                    ]
                    TICKER_PRICE_CACHE["price"] = candles[-1][4]
                    TICKER_PRICE_CACHE["ts"] = time.time()
                    return candles
        except Exception:
            continue

    # 2. Bybit v5 linear public klines fallback (works globally including US Render regions)
    bybit_tf = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240"}.get(timeframe, "5")
    for b_sym in ("XAUUSDT",):
        try:
            resp = requests.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": "linear", "symbol": b_sym, "interval": bybit_tf, "limit": limit},
                headers=headers,
                timeout=6,
            )
            if resp.status_code == 200:
                rows = resp.json().get("result", {}).get("list", [])
                if rows:
                    # Bybit returns newest first -> reverse to chronological order
                    candles = [
                        [int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                        for r in reversed(rows)
                    ]
                    TICKER_PRICE_CACHE["price"] = candles[-1][4]
                    TICKER_PRICE_CACHE["ts"] = time.time()
                    return candles
        except Exception:
            continue



    return []


def get_xau_candles(data: dict, timeframe: str, limit: int = 30) -> list:
    now = time.time()
    ttl = 18.0 if timeframe == "5m" else 28.0
    cached = CANDLE_CACHE.get(timeframe)
    if cached and (now - cached["ts"]) < ttl and cached["data"]:
        return cached["data"]

    headers = {"User-Agent": "Mozilla/5.0 (compatible; XAUORBBot/1.0)"}
    # 1. Bybit v5 linear public klines first (avoids burning Binance IP weight)
    bybit_tf = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240"}.get(timeframe, "5")
    for b_sym in ("XAUUSDT",):
        try:
            resp = requests.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": "linear", "symbol": b_sym, "interval": bybit_tf, "limit": limit},
                headers=headers,
                timeout=6,
            )
            if resp.status_code == 200:
                rows = resp.json().get("result", {}).get("list", [])
                if rows:
                    candles = [
                        [int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                        for r in reversed(rows)
                    ]
                    TICKER_PRICE_CACHE["price"] = candles[-1][4]
                    TICKER_PRICE_CACHE["ts"] = now
                    CANDLE_CACHE[timeframe] = {"ts": now, "data": candles}
                    return candles
        except Exception:
            continue



    # 3. Authenticated exchange fallback only if not in Binance 418 ban window
    ex_cfg = get_enabled_exchange_cfg(data)
    if ex_cfg and not (ex_cfg.get("exchange_id") == "binanceusdm" and _is_binance_banned()):
        try:
            client = build_ccxt_client(ex_cfg)
            ensure_markets(client, ex_cfg.get("id", "default"))
            sym = resolve_ccxt_symbol(client, SYMBOL)
            ohlcv = client.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            if ohlcv:
                TICKER_PRICE_CACHE["price"] = float(ohlcv[-1][4])
                TICKER_PRICE_CACHE["ts"] = now
                CANDLE_CACHE[timeframe] = {"ts": now, "data": ohlcv}
                return ohlcv
        except Exception as e:
            _record_binance_ban_if_any(e)
            log_event(f"CCXT fetch_ohlcv fallback ({timeframe}): {e}", "WARN")

    if not _is_binance_banned():
        res = fetch_public_xau_candles(timeframe=timeframe, limit=limit)
        if res:
            CANDLE_CACHE[timeframe] = {"ts": now, "data": res}
        return res
    return cached["data"] if cached else []


# ---------------------------------------------------------------------------
# Telegram Two-Way Bot & Notifications
# ---------------------------------------------------------------------------
def send_telegram(data: dict, text: str, reply_markup: dict = None) -> bool:
    tg = data.get("telegram", {})
    if not tg.get("enabled"):
        return False
    token = tg.get("bot_token", "").strip()
    chat_id = str(tg.get("chat_id", "")).strip()
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=8)
        return r.status_code == 200
    except Exception as e:
        log_event(f"Telegram send error: {e}", "WARN")
        return False


def format_cycle_summary() -> str:
    lines = ["📊 <b>XAUUSDT ORB Midpoint Cycles</b>", "━━━━━━━━━━━━━━━━━━━"]
    with STATE_LOCK:
        for mode_key in ("1H", "4H"):
            st = CYCLE_STATE[mode_key]
            tf_label = "1H / 5m (Full 1H)" if mode_key == "1H" else "4H / 15m (Full 4H)"
            lines.append(f"<b>• {tf_label}</b>: <code>{st['status']}</code>")
            if st["ref_high"] is not None and st["ref_low"] is not None:
                lines.append(
                    f"  Range: H <code>{st['ref_high']:.2f}</code> | "
                    f"L <code>{st['ref_low']:.2f}</code> | "
                    f"Mid <code>{st['midpoint']:.2f}</code>"
                )
            if st["limit_order_id"] and not st["in_trade"]:
                lines.append(
                    f"  Pending {st['direction']} Limit @ <code>{st['limit_price']:.2f}</code> "
                    f"(SL <code>{st['sl_price']:.2f}</code> | TP <code>{st['tp_price']:.2f}</code>)"
                )
            if st["in_trade"]:
                lines.append(
                    f"  Active {st['direction']} @ <code>{st['entry_price']:.2f}</code> "
                    f"(SL <code>{st['sl_price']:.2f}</code> | TP <code>{st['tp_price']:.2f}</code>)"
                )
    return "\n".join(lines)


def _signed_pos_amt(p: dict) -> float:
    raw_info_amt = p.get("info", {}).get("positionAmt")
    if raw_info_amt is not None and str(raw_info_amt).strip() != "":
        try:
            return float(raw_info_amt)
        except Exception:
            pass
    contracts = float(p.get("contracts") or 0)
    side = (p.get("side") or "").lower()
    if side == "short" and contracts > 0:
        return -contracts
    return contracts


def get_live_status_text(data: dict) -> str:
    ex_cfg = get_enabled_exchange_cfg(data)
    if not ex_cfg:
        return "⚠️ No exchange is currently enabled in the web portal."
    if ex_cfg.get("exchange_id") == "binanceusdm" and _is_binance_banned():
        until_str = datetime.fromtimestamp(BINANCE_BAN_UNTIL_MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return f"⏳ Binance IP cooldown active until <code>{until_str}</code>.\n\n" + format_cycle_summary()

    try:
        client = build_ccxt_client(ex_cfg)
        ensure_markets(client, ex_cfg.get("id", "default"))
        sym = resolve_ccxt_symbol(client, SYMBOL)
        positions = client.fetch_positions([sym])
        active = [
            p for p in positions
            if abs(_signed_pos_amt(p)) > 0
        ]
        if not active:
            return f"ℹ️ No open positions on {ex_cfg.get('name')} for <code>{SYMBOL}</code>.\n\n" + format_cycle_summary()
        out = [f"📈 <b>Live {SYMBOL} Positions ({ex_cfg.get('name')})</b>", "━━━━━━━━━━━━━━━━━━━"]
        for p in active:
            amt = _signed_pos_amt(p)
            side = (p.get("side") or ("long" if amt > 0 else "short")).upper()
            entry = float(p.get("entryPrice") or 0)
            mark = float(p.get("markPrice") or TICKER_PRICE_CACHE["price"] or 0)
            liq = float(p.get("liquidationPrice") or 0)
            upnl = float(p.get("unrealizedPnl") or 0)
            badge = "🟢" if upnl >= 0 else "🔴"
            out.append(
                f"• Side: <b>{side}</b> | Size: <code>{abs(amt)}</code>\n"
                f"• Entry: <code>{entry:.2f}</code> | Mark: <code>{mark:.2f}</code>\n"
                f"• Liq Price: <code>{liq:.2f}</code>\n"
                f"• Unrealized PnL: {badge} <b>{upnl:+.2f} USDT</b>"
            )
        return "\n".join(out)
    except Exception as e:
        return f"⚠️ Error fetching live status: <code>{e}</code>"


def get_balance_text(data: dict) -> str:
    ex_cfg = get_enabled_exchange_cfg(data)
    if not ex_cfg:
        return "⚠️ No exchange is currently enabled."
    try:
        client = build_ccxt_client(ex_cfg)
        bal = client.fetch_balance()
        usdt = bal.get("USDT", {})
        free = float(usdt.get("free") or 0)
        total = float(usdt.get("total") or 0)
        used = float(usdt.get("used") or 0)
        return (
            f"💰 <b>Wallet Balance ({ex_cfg.get('name')})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• Total Equity: <b>{total:.2f} USDT</b>\n"
            f"• Available Margin: <b>{free:.2f} USDT</b>\n"
            f"• Margin in Use: <code>{used:.2f} USDT</code>"
        )
    except Exception as e:
        return f"⚠️ Balance check failed: <code>{e}</code>"


def emergency_close_all(data: dict) -> str:
    ex_cfg = get_enabled_exchange_cfg(data)
    results = []
    if ex_cfg:
        try:
            client = build_ccxt_client(ex_cfg)
            ensure_markets(client, ex_cfg.get("id", "default"))
            sym = resolve_ccxt_symbol(client, SYMBOL)
            try:
                client.cancel_all_orders(sym)
            except Exception:
                pass
            positions = client.fetch_positions([sym])
            closed_any = False
            for p in positions:
                amt = _signed_pos_amt(p)
                if abs(amt) > 0:
                    side = "sell" if amt > 0 else "buy"
                    client.create_order(sym, "market", side, abs(amt), params={"reduceOnly": True})
                    closed_any = True
                    results.append(f"✅ Closed {SYMBOL} ({'LONG' if amt > 0 else 'SHORT'}) qty {abs(amt)} at Market.")
            if not closed_any:
                results.append(f"ℹ️ Canceled pending orders for {SYMBOL}; no open position to close.")
        except Exception as e:
            results.append(f"⚠️ Exchange close error: {e}")
    else:
        results.append("ℹ️ No exchange enabled; reset internal cycle tracking.")

    with STATE_LOCK:
        CACHED_POSITIONS.clear()
        for m in ("1H", "4H"):
            CYCLE_STATE[m]["limit_order_id"] = None
            CYCLE_STATE[m]["in_trade"] = False
            CYCLE_STATE[m]["cycle_done"] = True
            CYCLE_STATE[m]["status"] = "CLOSED_MANUALLY"
    save_runtime_snapshot()
    return "\n".join(results)


def cancel_pending_limits(data: dict, mode_filter: str = None) -> str:
    ex_cfg = get_enabled_exchange_cfg(data)
    canceled = []
    with STATE_LOCK:
        modes = [mode_filter] if mode_filter in ("1H", "4H") else ["1H", "4H"]
        for m in modes:
            st = CYCLE_STATE[m]
            oid = st.get("limit_order_id")
            if oid and not st.get("in_trade"):
                if ex_cfg:
                    try:
                        client = build_ccxt_client(ex_cfg)
                        ensure_markets(client, ex_cfg.get("id", "default"))
                        sym = resolve_ccxt_symbol(client, SYMBOL)
                        client.cancel_order(oid, sym)
                        _cancel_stop_and_tp_orders(client, sym)
                    except Exception as e:
                        log_event(f"Cancel order {oid} note: {e}", "WARN")
                st["limit_order_id"] = None
                st["cycle_done"] = True
                st["status"] = "LIMIT_CANCELED"
                canceled.append(f"🚫 Canceled {m} pending limit order.")
    return "\n".join(canceled) if canceled else "ℹ️ No pending limit orders to cancel."


def telegram_polling_worker():
    offset = 0
    while True:
        try:
            data = load_data()
            tg = data.get("telegram", {})
            token = tg.get("bot_token", "").strip()
            allowed_chat = str(tg.get("chat_id", "")).strip()
            if not tg.get("enabled") or not token:
                time.sleep(5)
                continue

            url = f"https://api.telegram.org/bot{token}/getUpdates"
            resp = requests.get(url, params={"timeout": 20, "offset": offset}, timeout=25)
            if resp.status_code == 409:
                log_event("Telegram 409 Conflict: Another bot instance is polling this same Bot Token!", "WARN")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                time.sleep(4)
                continue

            updates = resp.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                cb = upd.get("callback_query") or {}

                chat_id = ""
                text = ""
                if msg:
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    text = (msg.get("text") or "").strip()
                elif cb:
                    chat_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
                    text = (cb.get("data") or "").strip()
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"]},
                            timeout=4,
                        )
                    except Exception:
                        pass

                if not chat_id or (allowed_chat and chat_id != allowed_chat):
                    continue

                global BOT_PAUSED
                cmd = text.lower().split()[0] if text else ""
                if cmd in ("/start", "/menu"):
                    kb = {
                        "inline_keyboard": [
                            [
                                {"text": "📊 Live Status", "callback_data": "/status"},
                                {"text": "🔄 Cycles", "callback_data": "/cycles"},
                            ],
                            [
                                {"text": "💰 Balance", "callback_data": "/balance"},
                                {"text": "🚫 Cancel Limits", "callback_data": "/cancel"},
                            ],
                            [
                                {"text": "⏸️ Pause Bot", "callback_data": "/pause"},
                                {"text": "▶️ Resume Bot", "callback_data": "/resume"},
                            ],
                            [{"text": "❌ Emergency Close All", "callback_data": "/close"}],
                        ]
                    }
                    send_telegram(
                        data,
                        f"🤖 <b>{APP_TITLE}</b>\nSelect an action or type a command:\n"
                        "<code>/status</code> | <code>/cycles</code> | <code>/balance</code> | "
                        "<code>/cancel</code> | <code>/close</code> | <code>/pause</code> | <code>/resume</code>",
                        reply_markup=kb,
                    )
                elif cmd in ("/status", "/positions"):
                    send_telegram(data, get_live_status_text(data))
                elif cmd == "/cycles":
                    send_telegram(data, format_cycle_summary())
                elif cmd == "/balance":
                    send_telegram(data, get_balance_text(data))
                elif cmd == "/cancel":
                    send_telegram(data, cancel_pending_limits(data))
                elif cmd in ("/close", "/closeall"):
                    send_telegram(data, emergency_close_all(data))
                elif cmd == "/pause":
                    BOT_PAUSED = True
                    send_telegram(data, "⏸️ <b>Scanner Paused</b>. No new limit orders will be placed until resumed.")
                elif cmd == "/resume":
                    BOT_PAUSED = False
                    send_telegram(data, "▶️ <b>Scanner Resumed</b>. Monitoring XAUUSDT 1H & 4H cycles.")
        except Exception:
            time.sleep(5)


# ---------------------------------------------------------------------------
# Order Execution & Exchange Synchronization Helpers
# ---------------------------------------------------------------------------
def _fetch_all_open_orders(client, sym: str) -> list:
    orders = []
    try:
        orders.extend(client.fetch_open_orders(sym))
    except Exception:
        pass
    try:
        cond = client.fetch_open_orders(sym, params={"stop": True})
        seen = {o.get("id") for o in orders}
        for c in cond:
            if c.get("id") not in seen:
                orders.append(c)
    except Exception:
        pass
    return orders


def _binance_signed_fapi(client, method: str, path: str, params: dict) -> dict:
    """Direct HMAC-SHA256 signed request to Binance USDT-M Futures API (used for /fapi/v1/algoOrder)."""
    api_key = getattr(client, "apiKey", "") or ""
    secret = getattr(client, "secret", "") or ""
    if not api_key or not secret:
        raise ValueError("Missing Binance API credentials")
    is_sandbox = bool(getattr(client, "sandbox", False))
    base_url = "https://testnet.binancefuture.com" if is_sandbox else "https://fapi.binance.com"
    q = dict(params)
    q["timestamp"] = int(time.time() * 1000)
    q["recvWindow"] = 5000
    query_str = "&".join(f"{k}={v}" for k, v in q.items())
    sig = hmac.new(secret.encode("utf-8"), query_str.encode("utf-8"), hashlib.sha256).hexdigest()
    full_url = f"{base_url}{path}?{query_str}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}
    resp = requests.request(method.upper(), full_url, headers=headers, timeout=8)
    data = resp.json() if resp.text else {}
    if resp.status_code >= 400:
        err = RuntimeError(f"Binance {path} {resp.status_code}: {data}")
        _record_binance_ban_if_any(err)
        raise err
    return data


def _cancel_stop_and_tp_orders(client, sym: str):
    for o in _fetch_all_open_orders(client, sym):
        otype = (o.get("type") or o.get("info", {}).get("type") or "").upper()
        is_ro = bool(o.get("reduceOnly") or o.get("info", {}).get("reduceOnly"))
        if "STOP" in otype or "TAKE_PROFIT" in otype or (otype == "LIMIT" and is_ro):
            try:
                client.cancel_order(o["id"], sym)
            except Exception:
                pass
    # Also cancel Binance Algo Conditional open orders (/fapi/v1/algoOpenOrders)
    if getattr(client, "id", "") == "binanceusdm":
        raw_sym = client.market_id(sym) if sym in client.markets else SYMBOL
        try:
            _binance_signed_fapi(client, "DELETE", "/fapi/v1/algoOpenOrders", {"symbol": raw_sym})
        except Exception:
            pass


def _place_single_conditional(client, sym: str, order_type: str, close_side: str, qty_clean: float, trigger_px: float, pre_staged: bool) -> str:
    raw_sym = client.market_id(sym) if sym in client.markets else SYMBOL

    # For Take Profit when position is open, a standard LIMIT reduceOnly order works natively on /fapi/v1/order
    if order_type == "TAKE_PROFIT_MARKET" and not pre_staged:
        try:
            lim_tp = client.create_order(
                sym,
                "limit",
                close_side,
                qty_clean,
                trigger_px,
                {"reduceOnly": True, "timeInForce": "GTC"},
            )
            return f"{trigger_px:.2f} (Limit TP #{lim_tp.get('id', 'OK')})"
        except Exception as e_lim:
            log_event(f"Limit TP fallback to Algo TP @ {trigger_px}: {e_lim}", "WARN")

    # For Binance Futures conditional orders, try /fapi/v1/algoOrder variants cleanly
    if getattr(client, "id", "") == "binanceusdm":
        variants = []
        if pre_staged:
            variants.append({
                "algoType": "CONDITIONAL",
                "symbol": raw_sym,
                "side": close_side.upper(),
                "type": order_type,
                "triggerPrice": str(trigger_px),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            })
        else:
            variants.append({
                "algoType": "CONDITIONAL",
                "symbol": raw_sym,
                "side": close_side.upper(),
                "type": order_type,
                "triggerPrice": str(trigger_px),
                "quantity": str(qty_clean),
                "workingType": "MARK_PRICE",
            })
            variants.append({
                "algoType": "CONDITIONAL",
                "symbol": raw_sym,
                "side": close_side.upper(),
                "type": order_type,
                "triggerPrice": str(trigger_px),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            })
        last_err = ""
        for v_params in variants:
            try:
                res = _binance_signed_fapi(client, "POST", "/fapi/v1/algoOrder", v_params)
                algo_id = res.get("algoId") or res.get("clientAlgoId") or "OK"
                return f"{trigger_px:.2f} (Algo #{algo_id})"
            except Exception as e_v:
                last_err = str(e_v)
        log_event(f"Binance Algo {order_type} @ {trigger_px} failed: {last_err}", "ERROR")
        return f"ERR: {last_err}"

    # Non-Binance exchanges (Bybit, Bitget, OKX, KuCoin, Gate)
    try:
        if pre_staged:
            client.create_order(
                sym,
                order_type,
                close_side,
                None,
                None,
                {"stopPrice": trigger_px, "closePosition": True, "workingType": "MARK_PRICE"},
            )
        else:
            client.create_order(
                sym,
                order_type,
                close_side,
                qty_clean,
                None,
                {"stopPrice": trigger_px, "reduceOnly": True, "workingType": "MARK_PRICE"},
            )
        return f"{trigger_px:.2f} (OK)"
    except Exception as e1:
        log_event(f"Failed placing {order_type} @ {trigger_px}: {e1}", "ERROR")
        return f"ERR: {e1}"


def attach_sl_and_tp(client, sym: str, direction: str, qty: float, entry_price: float, sl_price: float, tp_price: float, liq_guard: bool, pre_staged: bool = False):
    close_side = "sell" if direction == "BUY" else "buy"
    guarded_sl = sl_price

    if liq_guard:
        try:
            positions = client.fetch_positions([sym])
            for p in positions:
                amt = _signed_pos_amt(p)
                if abs(amt) > 0:
                    real_liq = float(p.get("liquidationPrice") or 0)
                    if real_liq > 0:
                        if direction == "BUY" and guarded_sl <= real_liq:
                            safe_floor = real_liq + 0.25 * max(entry_price - real_liq, 0.5)
                            guarded_sl = max(guarded_sl, safe_floor)
                            log_event(f"Liq Guard clamped BUY SL from {sl_price:.2f} -> {guarded_sl:.2f} (Liq {real_liq:.2f})", "WARN")
                        elif direction == "SELL" and guarded_sl >= real_liq:
                            safe_ceil = real_liq - 0.25 * max(real_liq - entry_price, 0.5)
                            guarded_sl = min(guarded_sl, safe_ceil)
                            log_event(f"Liq Guard clamped SELL SL from {sl_price:.2f} -> {guarded_sl:.2f} (Liq {real_liq:.2f})", "WARN")
        except Exception as e:
            log_event(f"Liq price check skipped: {e}", "WARN")

    if getattr(client, "id", "") == "binanceusdm":
        guarded_sl = round(float(guarded_sl), 2)
        tp_clean = round(float(tp_price), 2)
        qty_clean = round(float(qty), 3)
    else:
        guarded_sl = float(client.price_to_precision(sym, guarded_sl))
        tp_clean = float(client.price_to_precision(sym, tp_price))
        qty_clean = float(client.amount_to_precision(sym, qty))

    # Clear any stale stop/tp orders before placing fresh ones
    _cancel_stop_and_tp_orders(client, sym)

    if getattr(client, "id", "") == "bybit" and not pre_staged:
        raw_sym = client.market_id(sym) if sym in client.markets else SYMBOL
        try:
            client.privatePostV5PositionTradingStop({
                "category": "linear",
                "symbol": raw_sym,
                "takeProfit": str(tp_clean),
                "stopLoss": str(guarded_sl),
                "tpTriggerBy": "MarkPrice",
                "slTriggerBy": "MarkPrice",
                "tpslMode": "Full",
                "positionIdx": 0,
            })
            return guarded_sl, f"{guarded_sl:.2f} (Bybit Pos SL)", f"{tp_clean:.2f} (Bybit Pos TP)"
        except Exception as e_bybit_ts:
            log_event(f"Bybit trading-stop fallback: {e_bybit_ts}", "WARN")

    sl_status = _place_single_conditional(client, sym, "STOP_MARKET", close_side, qty_clean, guarded_sl, pre_staged)
    tp_status = _place_single_conditional(client, sym, "TAKE_PROFIT_MARKET", close_side, qty_clean, tp_clean, pre_staged)

    return guarded_sl, sl_status, tp_status


def place_midpoint_limit_order(data: dict, mode_key: str, direction: str, midpoint: float, ref_high: float, ref_low: float):
    mode_cfg = data["modes"][mode_key]
    tp_dollars = float(mode_cfg.get("tp_dollars", 5.0))
    sl_offset = float(mode_cfg.get("sl_offset_dollars", 0.0))
    leverage = max(int(mode_cfg.get("leverage", 50)), 1)
    margin_usdt = float(mode_cfg.get("margin_usdt", 5.0))
    liq_guard = bool(mode_cfg.get("liq_guard", True))

    if direction == "BUY":
        raw_sl = ref_low - sl_offset
        raw_tp = midpoint + tp_dollars
    else:
        raw_sl = ref_high + sl_offset
        raw_tp = midpoint - tp_dollars

    if liq_guard:
        max_dist = midpoint * (1.0 / leverage) * 0.75
        if direction == "BUY" and (midpoint - raw_sl) > max_dist:
            raw_sl = midpoint - max_dist
        elif direction == "SELL" and (raw_sl - midpoint) > max_dist:
            raw_sl = midpoint + max_dist

    ex_cfg = get_enabled_exchange_cfg(data)
    if not ex_cfg:
        with STATE_LOCK:
            st = CYCLE_STATE[mode_key]
            st["direction"] = direction
            st["limit_order_id"] = f"SIM_{mode_key}_{int(time.time())}"
            st["limit_price"] = round(midpoint, 2)
            st["sl_price"] = round(raw_sl, 2)
            st["tp_price"] = round(raw_tp, 2)
            st["status"] = "LIMIT_PENDING (SIM)"
        log_event(f"[{mode_key}] Signal-only {direction} Limit @ {midpoint:.2f} | SL {raw_sl:.2f} | TP {raw_tp:.2f}")
        send_telegram(
            data,
            f"🟡 <b>XAUUSDT ORB BREAKOUT ({mode_key}) — SIGNAL ONLY</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• Direction: <b>{direction} LIMIT</b>\n"
            f"• Entry (Midpoint): <code>{midpoint:.2f}</code>\n"
            f"• Take Profit (+${tp_dollars:.2f}): <code>{raw_tp:.2f}</code>\n"
            f"• Stop Loss: <code>{raw_sl:.2f}</code>\n"
            f"• Ref Range: H <code>{ref_high:.2f}</code> / L <code>{ref_low:.2f}</code>\n"
            f"<i>Enable an exchange in the portal for live auto-execution.</i>",
        )
        return

    try:
        client = build_ccxt_client(ex_cfg)
        ensure_markets(client, ex_cfg.get("id", "default"))
        sym = resolve_ccxt_symbol(client, SYMBOL)

        positions = client.fetch_positions([sym])
        for p in positions:
            amt = _signed_pos_amt(p)
            if abs(amt) > 0:
                side_lbl = "LONG" if amt > 0 else "SHORT"
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["status"] = f"WAITING_FLAT ({side_lbl} {abs(amt)})"
                log_event(f"[{mode_key}] Holding {direction} limit: XAUUSDT still has open {side_lbl} ({abs(amt)}) on exchange.", "WARN")
                return

        if getattr(client, "id", "") == "binanceusdm":
            try:
                _binance_signed_fapi(client, "POST", "/fapi/v1/marginType", {"symbol": SYMBOL, "marginType": "ISOLATED"})
            except Exception:
                pass
            try:
                _binance_signed_fapi(client, "POST", "/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": int(leverage)})
            except Exception:
                pass

            limit_px = round(float(midpoint), 2)
            sl_px = round(float(raw_sl), 2)
            tp_px = round(float(raw_tp), 2)

            notional = margin_usdt * leverage
            qty = round(notional / limit_px, 3)
            if qty <= 0:
                raise ValueError(f"Calculated order qty is 0 (margin={margin_usdt}, lev={leverage}). Increase margin.")

            side_str = "BUY" if direction == "BUY" else "SELL"
            order = _binance_signed_fapi(
                client,
                "POST",
                "/fapi/v1/order",
                {
                    "symbol": SYMBOL,
                    "side": side_str,
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "quantity": f"{qty:.3f}",
                    "price": f"{limit_px:.2f}",
                },
            )
            oid = str(order.get("orderId") or order.get("clientOrderId") or "")
        else:
            try:
                client.set_margin_mode("isolated", sym)
            except Exception:
                pass
            try:
                client.set_leverage(leverage, sym)
            except Exception:
                pass

            limit_px = float(client.price_to_precision(sym, midpoint))
            sl_px = float(client.price_to_precision(sym, raw_sl))
            tp_px = float(client.price_to_precision(sym, raw_tp))

            notional = margin_usdt * leverage
            raw_qty = notional / limit_px
            qty = float(client.amount_to_precision(sym, raw_qty))
            if qty <= 0:
                raise ValueError(f"Calculated order qty is 0 (margin={margin_usdt}, lev={leverage}). Increase margin.")

            side_str = "buy" if direction == "BUY" else "sell"
            extra_params = {}
            if getattr(client, "id", "") == "bybit":
                extra_params = {
                    "takeProfit": str(tp_px),
                    "stopLoss": str(sl_px),
                    "tpslMode": "Full",
                    "tpTriggerBy": "MarkPrice",
                    "slTriggerBy": "MarkPrice",
                }
            order = client.create_order(sym, "limit", side_str, qty, limit_px, extra_params)
            oid = str(order.get("id"))

        if getattr(client, "id", "") == "bybit":
            pre_sl_msg = f"{sl_px:.2f} (Bybit Native SL)"
            pre_tp_msg = f"{tp_px:.2f} (Bybit Native TP)"
        else:
            sl_px, pre_sl_msg, pre_tp_msg = attach_sl_and_tp(
                client, sym, direction, qty, limit_px, sl_px, tp_px, liq_guard, pre_staged=True
            )

        with STATE_LOCK:
            st = CYCLE_STATE[mode_key]
            st["direction"] = direction
            st["limit_order_id"] = oid
            st["limit_price"] = limit_px
            st["sl_price"] = sl_px
            st["tp_price"] = tp_px
            st["qty"] = qty
            st["status"] = "LIMIT_PENDING"

        log_event(
            f"[{mode_key}] Placed {direction} LIMIT on {ex_cfg.get('name')}: "
            f"Qty {qty} @ {limit_px:.2f} | Pre-Staged SL: {pre_sl_msg} | TP: {pre_tp_msg} | ID {oid}"
        )
        kb = {
            "inline_keyboard": [
                [
                    {"text": "📊 Live Status", "callback_data": "/status"},
                    {"text": "🚫 Cancel Limit", "callback_data": "/cancel"},
                ]
            ]
        }
        send_telegram(
            data,
            f"{'🟢' if direction == 'BUY' else '🔴'} <b>XAUUSDT ORB MIDPOINT LIMIT ({mode_key})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• Action: <b>{direction} LIMIT</b> ({leverage}x isolated)\n"
            f"• Limit Entry (Midpoint): <code>{limit_px:.2f}</code>\n"
            f"• Take Profit (+${tp_dollars:.2f}): <code>{tp_px:.2f}</code>\n"
            f"• Stop Loss (Ref {'Low' if direction == 'BUY' else 'High'}): <code>{sl_px:.2f}</code>\n"
            f"• Size: <code>{qty}</code> ({margin_usdt:.2f} USDT margin)\n"
            f"• Order ID: <code>{oid}</code>",
            reply_markup=kb,
        )
    except Exception as e:
        _record_binance_ban_if_any(e)
        is_ban = "418" in str(e) or "-1003" in str(e)
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["cycle_done"] = True
            CYCLE_STATE[mode_key]["status"] = "IP_COOLDOWN" if is_ban else "ORDER_ERR"
        log_event(f"[{mode_key}] Order placement failed: {e}", "WARN" if is_ban else "ERROR")
        if not is_ban:
            send_telegram(data, f"⚠️ <b>XAUUSDT ({mode_key}) Order Failed</b>\n<code>{e}</code>")


# ---------------------------------------------------------------------------
# Core Strategy Engine Loop (1H/5m and 4H/15m Independent State Machines)
# ---------------------------------------------------------------------------
def monitor_pending_or_active_trade(data: dict, mode_key: str, window_end_ms: int, now_ms: int, current_mark: float):
    ex_cfg = get_enabled_exchange_cfg(data)
    with STATE_LOCK:
        st = dict(CYCLE_STATE[mode_key])

    if st["limit_order_id"] and not st["in_trade"]:
        if not ex_cfg:
            filled_sim = (
                (st["direction"] == "BUY" and current_mark > 0 and current_mark <= st["limit_price"])
                or (st["direction"] == "SELL" and current_mark > 0 and current_mark >= st["limit_price"])
            )
            if filled_sim:
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["in_trade"] = True
                    CYCLE_STATE[mode_key]["entry_price"] = st["limit_price"]
                    CYCLE_STATE[mode_key]["status"] = "IN_POSITION (SIM)"
                log_event(f"[{mode_key}] Simulated {st['direction']} Limit FILLED @ {st['limit_price']:.2f}")
                return
            if now_ms >= window_end_ms:
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["limit_order_id"] = None
                    CYCLE_STATE[mode_key]["cycle_done"] = True
                    CYCLE_STATE[mode_key]["status"] = "WINDOW_EXPIRED_CANCELED"
                log_event(f"[{mode_key}] Active window ended without fill. Canceled simulated limit order.")
            return

        try:
            client = build_ccxt_client(ex_cfg)
            ensure_markets(client, ex_cfg.get("id", "default"))
            sym = resolve_ccxt_symbol(client, SYMBOL)
            ord_info = client.fetch_order(st["limit_order_id"], sym)
            status = (ord_info.get("status") or "").lower()
            filled_qty = float(ord_info.get("filled") or 0)
            avg_px = float(ord_info.get("average") or ord_info.get("price") or st["limit_price"])

            if status == "closed" or (filled_qty > 0 and now_ms >= window_end_ms):
                if status != "closed":
                    try:
                        client.cancel_order(st["limit_order_id"], sym)
                    except Exception:
                        pass

                tp_dollars = float(data["modes"][mode_key].get("tp_dollars", 5.0))
                actual_tp = (avg_px + tp_dollars) if st["direction"] == "BUY" else (avg_px - tp_dollars)
                liq_guard = bool(data["modes"][mode_key].get("liq_guard", True))

                guarded_sl, sl_msg, tp_msg = attach_sl_and_tp(
                    client, sym, st["direction"], filled_qty, avg_px, st["sl_price"], actual_tp, liq_guard
                )
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["in_trade"] = True
                    CYCLE_STATE[mode_key]["entry_price"] = avg_px
                    CYCLE_STATE[mode_key]["sl_price"] = guarded_sl
                    CYCLE_STATE[mode_key]["tp_price"] = actual_tp
                    CYCLE_STATE[mode_key]["qty"] = filled_qty
                    CYCLE_STATE[mode_key]["status"] = "IN_POSITION"

                log_event(
                    f"[{mode_key}] {st['direction']} Limit FILLED @ {avg_px:.2f} | "
                    f"Qty {filled_qty} | SL: {sl_msg} | TP: {tp_msg}"
                )
                kb = {
                    "inline_keyboard": [
                        [
                            {"text": "📊 Live Status", "callback_data": "/status"},
                            {"text": "❌ Market Close", "callback_data": "/close"},
                        ]
                    ]
                }
                send_telegram(
                    data,
                    f"✅ <b>XAUUSDT LIMIT FILLED ({mode_key})</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"• Side: <b>{st['direction']}</b> | Qty: <code>{filled_qty}</code>\n"
                    f"• Fill Price: <code>{avg_px:.2f}</code>\n"
                    f"• Stop Loss: <code>{sl_msg}</code>\n"
                    f"• Take Profit: <code>{tp_msg}</code>",
                    reply_markup=kb,
                )
                return

            if status in ("canceled", "expired", "rejected"):
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["limit_order_id"] = None
                    CYCLE_STATE[mode_key]["cycle_done"] = True
                    CYCLE_STATE[mode_key]["status"] = f"LIMIT_{status.upper()}"
                log_event(f"[{mode_key}] Limit order {st['limit_order_id']} status: {status}.")
                return

            if now_ms >= window_end_ms:
                try:
                    client.cancel_order(st["limit_order_id"], sym)
                    _cancel_stop_and_tp_orders(client, sym)
                except Exception as ce:
                    log_event(f"[{mode_key}] Cancel order note: {ce}", "WARN")
                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["limit_order_id"] = None
                    CYCLE_STATE[mode_key]["cycle_done"] = True
                    CYCLE_STATE[mode_key]["status"] = "WINDOW_EXPIRED_CANCELED"
                log_event(f"[{mode_key}] Window expired without fill — canceled limit order {st['limit_order_id']}.")
                send_telegram(
                    data,
                    f"🚫 <b>XAUUSDT Limit Canceled ({mode_key})</b>\n"
                    f"Active window ended without price retracing to Midpoint (<code>{st['limit_price']:.2f}</code>).",
                )
        except Exception as e:
            log_event(f"[{mode_key}] Error checking pending order: {e}", "WARN")
        return

    if st["in_trade"]:
        if not ex_cfg:
            if current_mark > 0:
                hit_tp = (st["direction"] == "BUY" and current_mark >= st["tp_price"]) or (
                    st["direction"] == "SELL" and current_mark <= st["tp_price"]
                )
                hit_sl = (st["direction"] == "BUY" and current_mark <= st["sl_price"]) or (
                    st["direction"] == "SELL" and current_mark >= st["sl_price"]
                )
                if hit_tp or hit_sl:
                    exit_px = st["tp_price"] if hit_tp else st["sl_price"]
                    pnl = (exit_px - st["entry_price"]) if st["direction"] == "BUY" else (st["entry_price"] - exit_px)
                    with STATE_LOCK:
                        CYCLE_STATE[mode_key]["in_trade"] = False
                        CYCLE_STATE[mode_key]["limit_order_id"] = None
                        CYCLE_STATE[mode_key]["cycle_done"] = True
                        CYCLE_STATE[mode_key]["status"] = "TP_HIT (SIM)" if hit_tp else "SL_HIT (SIM)"
                    log_event(f"[{mode_key}] Simulated trade closed @ {exit_px:.2f} ({pnl:+.2f} pts)")
            return

        try:
            client = build_ccxt_client(ex_cfg)
            ensure_markets(client, ex_cfg.get("id", "default"))
            sym = resolve_ccxt_symbol(client, SYMBOL)
            positions = client.fetch_positions([sym])
            has_pos = any(
                abs(_signed_pos_amt(p)) > 0
                for p in positions
            )
            if not has_pos:
                _cancel_stop_and_tp_orders(client, sym)
                exit_px = 0.0
                realized_pnl_usdt = None
                try:
                    recent_trades = client.fetch_my_trades(sym, limit=6)
                    close_side_expected = "sell" if st["direction"] == "BUY" else "buy"
                    for tr in reversed(recent_trades):
                        if (tr.get("side") or "").lower() == close_side_expected and float(tr.get("price") or 0) > 0:
                            exit_px = float(tr["price"])
                            rp = tr.get("info", {}).get("realizedPnl")
                            if rp is not None:
                                realized_pnl_usdt = float(rp)
                            break
                except Exception as te:
                    log_event(f"[{mode_key}] fetch_my_trades exit price note: {te}", "WARN")

                if exit_px <= 0:
                    exit_px = current_mark if current_mark > 0 else st["entry_price"]

                pnl_pts = (exit_px - st["entry_price"]) if st["direction"] == "BUY" else (st["entry_price"] - exit_px)
                is_win = (realized_pnl_usdt > 0) if realized_pnl_usdt is not None else (pnl_pts > 0)
                if is_win and abs(exit_px - st.get("tp_price", 0)) <= 0.5:
                    header = "🎯 TAKE PROFIT TARGET HIT"
                elif is_win:
                    header = "💰 POSITION CLOSED IN PROFIT"
                else:
                    header = "🛑 STOP LOSS HIT"

                with STATE_LOCK:
                    CYCLE_STATE[mode_key]["in_trade"] = False
                    CYCLE_STATE[mode_key]["limit_order_id"] = None
                    CYCLE_STATE[mode_key]["cycle_done"] = True
                    CYCLE_STATE[mode_key]["status"] = "CLOSED_PROFIT" if is_win else "CLOSED_LOSS"

                pnl_extra = f" ({realized_pnl_usdt:+.2f} USDT)" if realized_pnl_usdt is not None else ""
                log_event(f"[{mode_key}] Position closed @ {exit_px:.2f} | Move: {pnl_pts:+.2f} pts{pnl_extra}")
                send_telegram(
                    data,
                    f"{header} (<b>{SYMBOL} — {mode_key}</b>)\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"• Side: <b>{st['direction']}</b>\n"
                    f"• Entry Price: <code>{st['entry_price']:.2f}</code>\n"
                    f"• Exit Price: <code>{exit_px:.2f}</code>\n"
                    f"• Move: <b>{pnl_pts:+.2f} pts{pnl_extra}</b>",
                )
        except Exception as e:
            log_event(f"[{mode_key}] Active position check error: {e}", "WARN")


def evaluate_mode_cycle(data: dict, mode_key: str, candles: list, now_ms: int):
    mode_cfg = data["modes"][mode_key]
    if not mode_cfg.get("enabled", True):
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["status"] = "DISABLED"
        return

    macro_ms = 3_600_000 if mode_key == "1H" else 14_400_000
    exec_ms = 300_000 if mode_key == "1H" else 900_000
    window_ms = int(mode_cfg.get("window_minutes", 60 if mode_key == "1H" else 240)) * 60_000

    cycle_ts = (now_ms // macro_ms) * macro_ms
    first_close_ts = cycle_ts + exec_ms
    window_end_ts = cycle_ts + window_ms

    current_mark = float(candles[-1][4]) if candles else TICKER_PRICE_CACHE["price"]

    with STATE_LOCK:
        st = CYCLE_STATE[mode_key]
        if st["cycle_ts"] != cycle_ts:
            old_oid = st.get("limit_order_id")
            old_in_trade = st.get("in_trade")
            if old_oid and not old_in_trade:
                ex_cfg = get_enabled_exchange_cfg(data)
                if ex_cfg:
                    try:
                        client = build_ccxt_client(ex_cfg)
                        ensure_markets(client, ex_cfg.get("id", "default"))
                        sym = resolve_ccxt_symbol(client, SYMBOL)
                        client.cancel_order(old_oid, sym)
                    except Exception:
                        pass

            CYCLE_STATE[mode_key] = {
                "cycle_ts": cycle_ts,
                "status": "WAITING_FIRST_CANDLE",
                "ref_high": None,
                "ref_low": None,
                "midpoint": None,
                "buy_entry": None,
                "sell_entry": None,
                "direction": None,
                "limit_order_id": None,
                "limit_price": None,
                "sl_price": None,
                "tp_price": None,
                "qty": 0.0,
                "in_trade": old_in_trade,
                "entry_price": st.get("entry_price") if old_in_trade else None,
                "cycle_done": False,
                "last_checked_bar_ts": 0,
            }
            if old_in_trade:
                CYCLE_STATE[mode_key]["sl_price"] = st.get("sl_price")
                CYCLE_STATE[mode_key]["tp_price"] = st.get("tp_price")
                CYCLE_STATE[mode_key]["direction"] = st.get("direction")
                CYCLE_STATE[mode_key]["status"] = "IN_POSITION"

            cycle_str = datetime.fromtimestamp(cycle_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            log_event(f"[{mode_key}] New cycle started: {cycle_str}")

    monitor_pending_or_active_trade(data, mode_key, window_end_ts, now_ms, current_mark)

    with STATE_LOCK:
        st = dict(CYCLE_STATE[mode_key])

    if st["cycle_done"] or st["limit_order_id"] or st["in_trade"]:
        return

    if BOT_PAUSED:
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["status"] = "PAUSED"
        return

    if now_ms < first_close_ts:
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["status"] = "WAITING_FIRST_CANDLE"
        return

    if st["ref_high"] is None or st["ref_low"] is None:
        first_bar = next((c for c in candles if int(c[0]) == cycle_ts), None)
        if not first_bar:
            return
        ref_h = float(first_bar[2])
        ref_l = float(first_bar[3])
        mid = (ref_h + ref_l) / 2.0
        buy_ent = (mid + ref_h) / 2.0
        sell_ent = (mid + ref_l) / 2.0
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["ref_high"] = ref_h
            CYCLE_STATE[mode_key]["ref_low"] = ref_l
            CYCLE_STATE[mode_key]["midpoint"] = mid
            CYCLE_STATE[mode_key]["buy_entry"] = buy_ent
            CYCLE_STATE[mode_key]["sell_entry"] = sell_ent
            CYCLE_STATE[mode_key]["status"] = "SCANNING_BREAKOUT"
            st = dict(CYCLE_STATE[mode_key])
        log_event(
            f"[{mode_key}] First candle closed -> High: {ref_h:.2f} | BuyLvl: {buy_ent:.2f} | "
            f"Mid: {mid:.2f} | SellLvl: {sell_ent:.2f} | Low: {ref_l:.2f}"
        )

    if now_ms >= window_end_ts:
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["cycle_done"] = True
            CYCLE_STATE[mode_key]["status"] = "WINDOW_ENDED_NO_SIGNAL"
        log_event(f"[{mode_key}] Active breakout window ended without a close outside High/Low.")
        return

    closed_subsequent = [
        c for c in candles
        if int(c[0]) > cycle_ts
        and (int(c[0]) + exec_ms) <= now_ms
        and (int(c[0]) + exec_ms) <= window_end_ts
    ]
    closed_subsequent.sort(key=lambda x: int(x[0]))

    for bar in closed_subsequent:
        bar_ts = int(bar[0])
        if bar_ts <= st["last_checked_bar_ts"]:
            continue
        with STATE_LOCK:
            CYCLE_STATE[mode_key]["last_checked_bar_ts"] = bar_ts

        bar_close = float(bar[4])
        if bar_close > st["ref_high"]:
            entry_target = st.get("buy_entry") or ((st["midpoint"] + st["ref_high"]) / 2.0)
            log_event(
                f"[{mode_key}] Bullish breakout! Candle closed at {bar_close:.2f} > High {st['ref_high']:.2f}. "
                f"Placing BUY Limit at Mid-High {entry_target:.2f}."
            )
            place_midpoint_limit_order(data, mode_key, "BUY", entry_target, st["ref_high"], st["ref_low"])
            return
        elif bar_close < st["ref_low"]:
            entry_target = st.get("sell_entry") or ((st["midpoint"] + st["ref_low"]) / 2.0)
            log_event(
                f"[{mode_key}] Bearish breakout! Candle closed at {bar_close:.2f} < Low {st['ref_low']:.2f}. "
                f"Placing SELL Limit at Mid-Low {entry_target:.2f}."
            )
            place_midpoint_limit_order(data, mode_key, "SELL", entry_target, st["ref_high"], st["ref_low"])
            return


def scanner_worker():
    log_event(f"Starting {APP_TITLE} background engine for {SYMBOL}...")
    while True:
        try:
            data = load_data()
            now_ms = int(time.time() * 1000)

            if data["modes"]["1H"].get("enabled", True):
                candles_5m = get_xau_candles(data, timeframe="5m", limit=24)
                if candles_5m:
                    evaluate_mode_cycle(data, "1H", candles_5m, now_ms)
                else:
                    with STATE_LOCK:
                        if CYCLE_STATE["1H"]["status"] == "INITIALIZING":
                            CYCLE_STATE["1H"]["status"] = "FEED_RETRYING"
                    log_event("[1H] Waiting for 5m candle feed (check exchange IP/API or internet).", "WARN")
                time.sleep(0.2)

            if data["modes"]["4H"].get("enabled", True):
                candles_15m = get_xau_candles(data, timeframe="15m", limit=24)
                if candles_15m:
                    evaluate_mode_cycle(data, "4H", candles_15m, now_ms)
                else:
                    with STATE_LOCK:
                        if CYCLE_STATE["4H"]["status"] == "INITIALIZING":
                            CYCLE_STATE["4H"]["status"] = "FEED_RETRYING"
                    log_event("[4H] Waiting for 15m candle feed (check exchange IP/API or internet).", "WARN")
            # Only poll exchange positions when a limit order or trade is active (zero idle Binance API weight!)
            with STATE_LOCK:
                need_pos_poll = any(
                    CYCLE_STATE[m].get("limit_order_id") or CYCLE_STATE[m].get("in_trade")
                    for m in ("1H", "4H")
                )
            ex_cfg = get_enabled_exchange_cfg(data)
            if need_pos_poll and ex_cfg and not (ex_cfg.get("exchange_id") == "binanceusdm" and _is_binance_banned()):
                try:
                    client = build_ccxt_client(ex_cfg)
                    ensure_markets(client, ex_cfg.get("id", "default"))
                    sym = resolve_ccxt_symbol(client, SYMBOL)
                    fresh_pos = []
                    for p in client.fetch_positions([sym]):
                        amt = _signed_pos_amt(p)
                        if abs(amt) > 0:
                            fresh_pos.append({
                                "symbol": SYMBOL,
                                "side": "LONG" if amt > 0 else "SHORT",
                                "size": abs(amt),
                                "entry": float(p.get("entryPrice") or 0),
                                "mark": float(p.get("markPrice") or TICKER_PRICE_CACHE["price"] or 0),
                                "liq": float(p.get("liquidationPrice") or 0),
                                "pnl": float(p.get("unrealizedPnl") or 0),
                            })
                    with STATE_LOCK:
                        CACHED_POSITIONS[:] = fresh_pos
                except Exception as pe:
                    _record_binance_ban_if_any(pe)
            elif not need_pos_poll:
                with STATE_LOCK:
                    CACHED_POSITIONS.clear()
        except Exception as e:
            _record_binance_ban_if_any(e)
            log_event(f"Scanner loop exception: {e}\n{traceback.format_exc()}", "ERROR")
        save_runtime_snapshot()
        with STATE_LOCK:
            has_pending_or_active = any(
                CYCLE_STATE[m].get("limit_order_id") or CYCLE_STATE[m].get("in_trade")
                for m in ("1H", "4H")
            )
        time.sleep(15 if has_pending_or_active else 25)


# ---------------------------------------------------------------------------
# Flask Web Portal & REST API
# ---------------------------------------------------------------------------
app = Flask(__name__)


def _extract_request_token() -> str:
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "").strip()
    if token:
        return token
    return (request.cookies.get("xau_orb_session") or "").strip()


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = _extract_request_token()
        if not verify_session_token(token):
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


@app.route("/health")
def health():
    return jsonify({"status": "ok", "symbol": SYMBOL, "ts": int(time.time())})


@app.route("/api/login", methods=["POST"])
def api_login():
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    data = load_data()
    admin = data.get("admin", {})
    expected_user = (admin.get("username") or ADMIN_USER).strip()

    user_ok = username.lower() == expected_user.lower()
    pass_ok = _hash_password(password) == admin.get("password_hash")
    if user_ok and pass_ok:
        token = create_session_token(expected_user)
        log_event(f"Admin '{expected_user}' logged in successfully.")
        resp = make_response(jsonify({"ok": True, "token": token, "username": expected_user}))
        resp.set_cookie("xau_orb_session", token, max_age=SESSION_TTL, path="/", samesite="Lax")
        return resp

    has_env_pass = bool(os.environ.get("ADMIN_PASSWORD", "").strip())
    log_event(
        f"Failed login attempt for user='{username}' (expected='{expected_user}', ADMIN_PASSWORD_env_set={has_env_pass})",
        "WARN",
    )
    return jsonify({"ok": False, "error": "Invalid username or password"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie("xau_orb_session", path="/")
    return resp


@app.route("/api/state", methods=["GET"])
@require_auth
def api_state():
    data = load_data()
    safe_exchanges = []
    for ex in data.get("exchanges", []):
        raw_key = decrypt_secret(ex.get("api_key_enc", ""))
        masked = (raw_key[:4] + "..." + raw_key[-4:]) if len(raw_key) >= 8 else "****"
        safe_exchanges.append(
            {
                "id": ex.get("id"),
                "name": ex.get("name"),
                "exchange_id": ex.get("exchange_id"),
                "masked_key": masked,
                "enabled": bool(ex.get("enabled")),
                "testnet": bool(ex.get("testnet")),
            }
        )

    snap = load_runtime_snapshot()
    with STATE_LOCK:
        live_positions = snap.get("live_positions", list(CACHED_POSITIONS))
        cycles_copy = snap.get("cycles", json.loads(json.dumps(CYCLE_STATE)))
        logs_copy = snap.get("logs", list(SCANNER_LOGS[-100:]))
        mark_px = snap.get("mark_price") or TICKER_PRICE_CACHE["price"]

    return jsonify(
        {
            "ok": True,
            "symbol": SYMBOL,
            "mark_price": mark_px,
            "public_ip": get_public_ip(),
            "paused": BOT_PAUSED,
            "modes": data.get("modes", {}),
            "cycles": cycles_copy,
            "exchanges": safe_exchanges,
            "live_positions": live_positions,
            "telegram": {
                "enabled": bool(data.get("telegram", {}).get("enabled")),
                "bot_token": data.get("telegram", {}).get("bot_token", ""),
                "chat_id": data.get("telegram", {}).get("chat_id", ""),
            },
            "logs": logs_copy,
        }
    )


@app.route("/api/modes", methods=["POST"])
@require_auth
def api_save_modes():
    body = request.get_json(silent=True) or {}
    data = load_data()
    for mode_key in ("1H", "4H"):
        if mode_key in body:
            inc = body[mode_key]
            m = data["modes"][mode_key]
            m["enabled"] = bool(inc.get("enabled", m["enabled"]))
            m["tp_dollars"] = max(float(inc.get("tp_dollars", m["tp_dollars"])), 0.1)
            m["sl_offset_dollars"] = float(inc.get("sl_offset_dollars", m["sl_offset_dollars"]))
            m["leverage"] = max(int(inc.get("leverage", m["leverage"])), 1)
            m["margin_usdt"] = max(float(inc.get("margin_usdt", m["margin_usdt"])), 0.5)
            m["liq_guard"] = bool(inc.get("liq_guard", m["liq_guard"]))
    save_data(data)
    log_event("Updated XAUUSDT 1H & 4H strategy parameters.")
    return jsonify({"ok": True})


@app.route("/api/exchanges", methods=["POST"])
@require_auth
def api_add_exchange():
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "Binance Futures").strip()
    exchange_id = (body.get("exchange_id") or "binanceusdm").strip()
    api_key = (body.get("api_key") or "").strip()
    api_secret = (body.get("api_secret") or "").strip()
    passphrase = (body.get("passphrase") or "").strip()
    testnet = bool(body.get("testnet", False))

    if not api_key or not api_secret:
        return jsonify({"ok": False, "error": "API Key and Secret are required."}), 400

    data = load_data()
    for ex in data["exchanges"]:
        ex["enabled"] = False

    new_ex = {
        "id": "ex_" + secrets.token_hex(4),
        "name": name,
        "exchange_id": exchange_id,
        "api_key_enc": encrypt_secret(api_key),
        "api_secret_enc": encrypt_secret(api_secret),
        "passphrase_enc": encrypt_secret(passphrase),
        "enabled": True,
        "testnet": testnet,
    }
    data["exchanges"].append(new_ex)
    save_data(data)
    EXCHANGE_CLIENTS.clear()
    log_event(f"Added and enabled exchange '{name}' ({exchange_id}).")
    return jsonify({"ok": True})


@app.route("/api/exchanges/toggle", methods=["POST"])
@require_auth
def api_toggle_exchange():
    body = request.get_json(silent=True) or {}
    ex_id = body.get("id")
    data = load_data()
    target = next((x for x in data["exchanges"] if x["id"] == ex_id), None)
    if not target:
        return jsonify({"ok": False, "error": "Exchange not found"}), 404
    new_state = not bool(target.get("enabled"))
    if new_state:
        for x in data["exchanges"]:
            x["enabled"] = False
    target["enabled"] = new_state
    save_data(data)
    EXCHANGE_CLIENTS.clear()
    log_event(f"Exchange '{target['name']}' {'ENABLED' if new_state else 'DISABLED'}.")
    return jsonify({"ok": True, "enabled": new_state})


@app.route("/api/exchanges/delete", methods=["POST"])
@require_auth
def api_delete_exchange():
    body = request.get_json(silent=True) or {}
    ex_id = body.get("id")
    data = load_data()
    data["exchanges"] = [x for x in data["exchanges"] if x["id"] != ex_id]
    save_data(data)
    EXCHANGE_CLIENTS.clear()
    log_event(f"Deleted exchange {ex_id}.")
    return jsonify({"ok": True})


@app.route("/api/exchanges/test", methods=["POST"])
@require_auth
def api_test_exchange():
    body = request.get_json(silent=True) or {}
    ex_id = body.get("id")
    data = load_data()
    target = next((x for x in data["exchanges"] if x["id"] == ex_id), None)
    if not target:
        return jsonify({"ok": False, "error": "Exchange not found"}), 404
    try:
        client = build_ccxt_client(target)
        bal = client.fetch_balance()
        usdt = bal.get("USDT", {})
        free = float(usdt.get("free") or 0)
        total = float(usdt.get("total") or 0)
        return jsonify({"ok": True, "message": f"Connected! Total: {total:.2f} USDT | Available: {free:.2f} USDT"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/telegram", methods=["POST"])
@require_auth
def api_save_telegram():
    body = request.get_json(silent=True) or {}
    data = load_data()
    data["telegram"] = {
        "enabled": bool(body.get("enabled", True)),
        "bot_token": (body.get("bot_token") or "").strip(),
        "chat_id": str(body.get("chat_id") or "").strip(),
    }
    save_data(data)
    log_event("Saved Telegram configuration.")
    return jsonify({"ok": True})


@app.route("/api/telegram/test", methods=["POST"])
@require_auth
def api_test_telegram():
    data = load_data()
    ok = send_telegram(
        data,
        f"✅ <b>{APP_TITLE} — Test Alert</b>\nTelegram connection is active and ready for XAUUSDT 1H & 4H signals.",
    )
    if ok:
        return jsonify({"ok": True, "message": "Test message sent to Telegram!"})
    return jsonify({"ok": False, "error": "Failed to send message. Verify Bot Token and Chat ID."}), 400


@app.route("/api/telegram/autodetect", methods=["POST"])
@require_auth
def api_autodetect_telegram():
    body = request.get_json(silent=True) or {}
    token = (body.get("bot_token") or load_data().get("telegram", {}).get("bot_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Enter your Bot Token first."}), 400
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=8)
        res = r.json().get("result", [])
        for item in reversed(res):
            chat = (item.get("message") or item.get("channel_post") or {}).get("chat", {})
            if chat.get("id"):
                return jsonify({"ok": True, "chat_id": str(chat["id"])})
        return jsonify({"ok": False, "error": "No recent messages found. Send /start to your bot in Telegram first."}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/action/pause", methods=["POST"])
@require_auth
def api_toggle_pause():
    global BOT_PAUSED
    BOT_PAUSED = not BOT_PAUSED
    log_event(f"Scanner {'PAUSED' if BOT_PAUSED else 'RESUMED'} via dashboard.")
    return jsonify({"ok": True, "paused": BOT_PAUSED})


@app.route("/api/action/cancel_limits", methods=["POST"])
@require_auth
def api_cancel_limits():
    body = request.get_json(silent=True) or {}
    msg = cancel_pending_limits(load_data(), body.get("mode"))
    return jsonify({"ok": True, "message": msg})


@app.route("/api/action/protect_open", methods=["POST"])
@require_auth
def api_protect_open():
    data = load_data()
    ex_cfg = get_enabled_exchange_cfg(data)
    if not ex_cfg:
        return jsonify({"ok": False, "error": "No exchange enabled."}), 400
    try:
        client = build_ccxt_client(ex_cfg)
        ensure_markets(client, ex_cfg.get("id", "default"))
        sym = resolve_ccxt_symbol(client, SYMBOL)
        positions = client.fetch_positions([sym])
        active = [p for p in positions if abs(_signed_pos_amt(p)) > 0]
        if not active:
            return jsonify({"ok": False, "error": "No open XAUUSDT position found on exchange."}), 404
        p = active[0]
        amt = _signed_pos_amt(p)
        direction = "BUY" if amt > 0 else "SELL"
        qty = abs(amt)
        entry = float(p.get("entryPrice") or TICKER_PRICE_CACHE["price"] or 0)
        liq = float(p.get("liquidationPrice") or 0)
        tp_dollars = float(data["modes"]["1H"].get("tp_dollars", 5.0))
        tp_px = (entry + tp_dollars) if direction == "BUY" else (entry - tp_dollars)
        # Use 1H/4H reference Low/High if available, otherwise safe buffer ahead of liquidation
        snap_cycles = load_runtime_snapshot().get("cycles", CYCLE_STATE)
        ref_sl = snap_cycles.get("1H", {}).get("ref_low") if direction == "BUY" else snap_cycles.get("1H", {}).get("ref_high")
        if not ref_sl:
            ref_sl = (liq + 2.0) if (direction == "BUY" and liq > 0) else ((liq - 2.0) if (direction == "SELL" and liq > 0) else (entry - 5.0 if direction == "BUY" else entry + 5.0))
        guarded_sl, sl_msg, tp_msg = attach_sl_and_tp(client, sym, direction, qty, entry, ref_sl, tp_px, True, pre_staged=False)
        with STATE_LOCK:
            CYCLE_STATE["1H"]["in_trade"] = True
            CYCLE_STATE["1H"]["direction"] = direction
            CYCLE_STATE["1H"]["entry_price"] = entry
            CYCLE_STATE["1H"]["sl_price"] = guarded_sl
            CYCLE_STATE["1H"]["tp_price"] = tp_px
            CYCLE_STATE["1H"]["qty"] = qty
            CYCLE_STATE["1H"]["status"] = "IN_POSITION"
        save_runtime_snapshot()
        return jsonify({"ok": True, "message": f"Protected {direction} {qty} @ {entry:.2f} | SL: {sl_msg} | TP: {tp_msg}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/action/close_all", methods=["POST"])
@require_auth
def api_close_all():
    msg = emergency_close_all(load_data())
    return jsonify({"ok": True, "message": msg})


@app.route("/api/backup/download", methods=["GET"])
@require_auth
def api_backup_download():
    data = load_data()
    return jsonify({"ok": True, "backup": data})


@app.route("/api/backup/restore", methods=["POST"])
@require_auth
def api_backup_restore():
    body = request.get_json(silent=True) or {}
    backup = body.get("backup")
    if not isinstance(backup, dict) or "modes" not in backup:
        return jsonify({"ok": False, "error": "Invalid backup JSON format."}), 400
    save_data(backup)
    EXCHANGE_CLIENTS.clear()
    log_event("Restored configuration from backup JSON.")
    return jsonify({"ok": True})


@app.route("/api/password", methods=["POST"])
@require_auth
def api_change_password():
    body = request.get_json(silent=True) or {}
    new_pass = (body.get("new_password") or "").strip()
    if len(new_pass) < 4:
        return jsonify({"ok": False, "error": "Password must be at least 4 characters."}), 400
    data = load_data()
    data["admin"]["password_hash"] = _hash_password(new_pass)
    save_data(data)
    log_event("Admin password updated.")
    return jsonify({"ok": True})


@app.route("/api/logs/clear", methods=["POST"])
@require_auth
def api_clear_logs():
    with STATE_LOCK:
        SCANNER_LOGS.clear()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Mobile-Friendly Tabbed Web Portal HTML
# ---------------------------------------------------------------------------
HTML_PORTAL = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>XAUUSDT ORB Midpoint Bot</title>
<style>
  :root {
    --bg: #0b0f17;
    --card: #131a26;
    --border: #232e42;
    --text: #e6edf7;
    --muted: #8b9bb4;
    --gold: #f5b942;
    --green: #10b981;
    --red: #ef4444;
    --blue: #3b82f6;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.45;
  }
  .container { max-width: 1120px; margin: 0 auto; padding: 16px; }
  .topbar {
    display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
    gap: 10px; background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 18px; margin-bottom: 16px;
  }
  .brand { font-size: 1.15rem; font-weight: 700; color: var(--gold); display: flex; align-items: center; gap: 8px; }
  .badges { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
  .badge {
    background: #1b2436; border: 1px solid var(--border); border-radius: 8px;
    padding: 5px 10px; font-size: 0.82rem; color: var(--text);
  }
  .badge.live { border-color: var(--green); color: var(--green); }
  .badge.paused { border-color: var(--red); color: var(--red); }
  button {
    cursor: pointer; border: none; border-radius: 8px; padding: 9px 14px;
    font-size: 0.9rem; font-weight: 600; transition: opacity 0.15s;
  }
  button:hover { opacity: 0.9; }
  .btn-gold { background: var(--gold); color: #111; }
  .btn-blue { background: var(--blue); color: #fff; }
  .btn-green { background: var(--green); color: #fff; }
  .btn-red { background: var(--red); color: #fff; }
  .btn-ghost { background: #1e283a; color: var(--text); border: 1px solid var(--border); }
  .tabs {
    display: flex; gap: 8px; overflow-x: auto; padding-bottom: 6px; margin-bottom: 16px;
    -webkit-overflow-scrolling: touch;
  }
  .tab-btn {
    white-space: nowrap; background: var(--card); color: var(--muted);
    border: 1px solid var(--border); padding: 10px 16px; border-radius: 10px;
  }
  .tab-btn.active { background: var(--gold); color: #111; border-color: var(--gold); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 16px; }
  .card {
    background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px;
  }
  .card h3 { margin: 0 0 12px; font-size: 1.05rem; color: var(--gold); }
  .kv { display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid #1b2333; font-size: 0.92rem; }
  .kv:last-child { border-bottom: none; }
  .kv span:first-child { color: var(--muted); }
  label { display: block; font-size: 0.84rem; color: var(--muted); margin: 10px 0 4px; }
  input, select {
    width: 100%; background: #0e1420; color: var(--text); border: 1px solid var(--border);
    border-radius: 8px; padding: 10px; font-size: 16px;
  }
  .row-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
  .terminal {
    background: #070a10; border: 1px solid var(--border); border-radius: 10px;
    padding: 12px; height: 380px; overflow-y: auto; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.8rem; color: #b7c7e2; white-space: pre-wrap;
  }
  .login-wrap {
    max-width: 400px; margin: 80px auto; background: var(--card); border: 1px solid var(--border);
    border-radius: 14px; padding: 26px;
  }
  .hidden { display: none !important; }
  @media (max-width: 768px) {
    .topbar { flex-direction: column; align-items: stretch; }
    .row-actions button { width: 100%; }
  }
</style>
</head>
<body>
<div class="container">
  <!-- Login Gate -->
  <div id="loginBox" class="login-wrap">
    <h2 style="margin-top:0;color:var(--gold);">🥇 XAUUSDT ORB Bot</h2>
    <p style="color:var(--muted);font-size:0.9rem;">Sign in with your administrator credentials.</p>
    <form onsubmit="doLogin(event)">
      <label>Username</label>
      <input id="loginUser" value="admin" autocomplete="username" autocapitalize="none" autocorrect="off" spellcheck="false" />
      <label>Password</label>
      <input id="loginPass" type="password" placeholder="••••••••" autocomplete="current-password" />
      <div class="row-actions">
        <button id="loginBtn" type="submit" class="btn-gold" style="width:100%;">Sign In</button>
      </div>
    </form>
    <p id="loginErr" style="color:var(--red);font-size:0.85rem;margin-top:10px;"></p>
  </div>

  <!-- Main Dashboard -->
  <div id="appBox" class="hidden">
    <div class="topbar">
      <div class="brand">🥇 XAUUSDT ORB Midpoint Bot <span id="markBadge" class="badge">XAU: --</span></div>
      <div class="badges">
        <span id="statusBadge" class="badge live">RUNNING</span>
        <span class="badge">IP: <strong id="ipText">--</strong> <button class="btn-ghost" style="padding:2px 7px;font-size:0.75rem;" onclick="copyIP()">Copy</button></span>
        <button id="pauseBtn" class="btn-ghost" onclick="togglePause()">⏸️ Pause</button>
        <button class="btn-ghost" onclick="doLogout()">Logout</button>
      </div>
    </div>

    <div class="tabs">
      <button class="tab-btn active" onclick="showTab('overview', this)">📊 Overview & Cycles</button>
      <button class="tab-btn" onclick="showTab('strategy', this)">⚙️ Strategy (1H & 4H)</button>
      <button class="tab-btn" onclick="showTab('exchanges', this)">🏦 Exchanges</button>
      <button class="tab-btn" onclick="showTab('telegram', this)">📱 Telegram</button>
      <button class="tab-btn" onclick="showTab('backup', this)">💾 Backup & System</button>
      <button class="tab-btn" onclick="showTab('terminal', this)">📋 Terminal</button>
    </div>

    <!-- Tab 1: Overview -->
    <div id="tab-overview" class="tab-panel">
      <div class="grid">
        <div class="card">
          <h3>⏱️ 1H / 5m Cycle (Full 1H Cycle)</h3>
          <div id="cycle1H"></div>
          <div class="row-actions">
            <button class="btn-ghost" onclick="cancelLimits('1H')">🚫 Cancel 1H Limit</button>
          </div>
        </div>
        <div class="card">
          <h3>🕓 4H / 15m Cycle (Full 4H Cycle)</h3>
          <div id="cycle4H"></div>
          <div class="row-actions">
            <button class="btn-ghost" onclick="cancelLimits('4H')">🚫 Cancel 4H Limit</button>
          </div>
        </div>
      </div>
      <div class="card" style="margin-top:16px;">
        <h3>📈 Live XAUUSDT Exchange Position</h3>
        <div id="livePosBox"><p style="color:var(--muted);">No active position.</p></div>
        <div class="row-actions">
          <button class="btn-green" onclick="protectOpen()">🛡️ Attach / Sync TP & SL Now</button>
          <button class="btn-red" onclick="closeAll()">❌ Emergency Close All & Cancel Orders</button>
        </div>
      </div>
    </div>

    <!-- Tab 2: Strategy -->
    <div id="tab-strategy" class="tab-panel hidden">
      <div class="grid">
        <div class="card">
          <h3>⏱️ 1-Hour / 5-Minute Mode</h3>
          <label>Mode Status</label>
          <select id="m1_enabled"><option value="true">Enabled</option><option value="false">Disabled</option></select>
          <label>Take Profit Offset ($ USD above/below Midpoint)</label>
          <input id="m1_tp" type="number" step="0.1" />
          <label>SL Buffer ($ USD outside First 5m High/Low)</label>
          <input id="m1_sl_off" type="number" step="0.1" />
          <label>Leverage (Isolated)</label>
          <input id="m1_lev" type="number" step="1" />
          <label>Margin per Order (USDT)</label>
          <input id="m1_margin" type="number" step="0.5" />
          <label>Liquidation Guard</label>
          <select id="m1_liq"><option value="true">Enabled (Recommended)</option><option value="false">Disabled</option></select>
        </div>
        <div class="card">
          <h3>🕓 4-Hour / 15-Minute Mode</h3>
          <label>Mode Status</label>
          <select id="m4_enabled"><option value="true">Enabled</option><option value="false">Disabled</option></select>
          <label>Take Profit Offset ($ USD above/below Midpoint)</label>
          <input id="m4_tp" type="number" step="0.1" />
          <label>SL Buffer ($ USD outside First 15m High/Low)</label>
          <input id="m4_sl_off" type="number" step="0.1" />
          <label>Leverage (Isolated)</label>
          <input id="m4_lev" type="number" step="1" />
          <label>Margin per Order (USDT)</label>
          <input id="m4_margin" type="number" step="0.5" />
          <label>Liquidation Guard</label>
          <select id="m4_liq"><option value="true">Enabled (Recommended)</option><option value="false">Disabled</option></select>
        </div>
      </div>
      <div class="row-actions">
        <button class="btn-gold" onclick="saveModes()">💾 Save Strategy Settings</button>
      </div>
    </div>

    <!-- Tab 3: Exchanges -->
    <div id="tab-exchanges" class="tab-panel hidden">
      <div class="grid">
        <div class="card">
          <h3>➕ Add Exchange Account</h3>
          <label>Account Label</label>
          <input id="exName" placeholder="Binance Main" />
          <label>Exchange</label>
          <select id="exType">
            <option value="binanceusdm">Binance Futures (USDT-M)</option>
            <option value="bybit">Bybit Perpetual</option>
            <option value="bitget">Bitget Futures</option>
            <option value="okx">OKX Perpetual</option>
            <option value="kucoinfutures">KuCoin Futures</option>
            <option value="gate">Gate.io Futures</option>
          </select>
          <label>API Key</label>
          <input id="exKey" placeholder="Paste API Key" />
          <label>API Secret</label>
          <input id="exSecret" type="password" placeholder="Paste API Secret" />
          <label>Passphrase (OKX / Bitget / KuCoin only)</label>
          <input id="exPass" type="password" placeholder="Optional" />
          <div class="row-actions">
            <button class="btn-gold" onclick="addExchange()">Save & Enable Exchange</button>
          </div>
        </div>
        <div class="card">
          <h3>🏦 Configured Exchanges</h3>
          <div id="exList"></div>
        </div>
      </div>
    </div>

    <!-- Tab 4: Telegram -->
    <div id="tab-telegram" class="tab-panel hidden">
      <div class="card" style="max-width:560px;">
        <h3>📱 Two-Way Telegram Notifications & Controls</h3>
        <label>Alerts Enabled</label>
        <select id="tgEnabled"><option value="true">Enabled</option><option value="false">Disabled</option></select>
        <label>Bot Token</label>
        <input id="tgToken" placeholder="123456789:ABC..." />
        <label>Chat ID</label>
        <input id="tgChat" placeholder="Your numeric Chat ID" />
        <div class="row-actions">
          <button class="btn-gold" onclick="saveTelegram()">💾 Save Telegram</button>
          <button class="btn-blue" onclick="autoDetectTg()">🔍 Auto-Detect Chat ID</button>
          <button class="btn-green" onclick="testTelegram()">✉️ Send Test Message</button>
        </div>
      </div>
    </div>

    <!-- Tab 5: Backup & System -->
    <div id="tab-backup" class="tab-panel hidden">
      <div class="grid">
        <div class="card">
          <h3>💾 Configuration Backup & Restore</h3>
          <p style="color:var(--muted);font-size:0.88rem;">Download your settings JSON or restore after a server restart.</p>
          <div class="row-actions">
            <button class="btn-blue" onclick="downloadBackup()">📥 Download Backup (JSON)</button>
          </div>
          <label>Restore from Backup JSON File</label>
          <input id="restoreFile" type="file" accept=".json" />
          <div class="row-actions">
            <button class="btn-gold" onclick="restoreBackup()">📤 Upload & Restore</button>
          </div>
        </div>
        <div class="card">
          <h3>🔐 Change Admin Password</h3>
          <label>New Password</label>
          <input id="newPass" type="password" placeholder="Enter new password" />
          <div class="row-actions">
            <button class="btn-gold" onclick="changePassword()">Update Password</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Tab 6: Terminal -->
    <div id="tab-terminal" class="tab-panel hidden">
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
          <h3 style="margin:0;">📋 Live Scanner Terminal</h3>
          <button class="btn-ghost" onclick="clearLogs()">Clear</button>
        </div>
        <div id="termBox" class="terminal"></div>
      </div>
    </div>
  </div>
</div>

<script>
let TOKEN = "";
try { TOKEN = sessionStorage.getItem("xau_orb_token") || ""; } catch (e) {}
let pollTimer = null;

function authHeaders() {
  const h = { "Content-Type": "application/json" };
  if (TOKEN) h["Authorization"] = "Bearer " + TOKEN;
  return h;
}

async function doLogin(ev) {
  if (ev) ev.preventDefault();
  const btn = document.getElementById("loginBtn");
  const errEl = document.getElementById("loginErr");
  errEl.innerText = "";
  btn.disabled = true;
  btn.innerText = "Signing in...";
  try {
    const username = document.getElementById("loginUser").value.trim();
    const password = document.getElementById("loginPass").value;
    const res = await fetch("/api/login", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    });
    const data = await res.json();
    if (data.ok) {
      TOKEN = data.token;
      try { sessionStorage.setItem("xau_orb_token", TOKEN); } catch (e) {}
      initApp();
    } else {
      errEl.innerText = data.error || "Invalid username or password";
    }
  } catch (e) {
    errEl.innerText = "Error: " + e.message;
  } finally {
    btn.disabled = false;
    btn.innerText = "Sign In";
  }
}

function doLogout() {
  fetch("/api/logout", { method: "POST", credentials: "same-origin", headers: authHeaders() }).catch(() => {});
  TOKEN = "";
  try { sessionStorage.removeItem("xau_orb_token"); } catch (e) {}
  if (pollTimer) clearInterval(pollTimer);
  document.getElementById("appBox").classList.add("hidden");
  document.getElementById("loginBox").classList.remove("hidden");
}

function showTab(name, btn) {
  document.querySelectorAll(".tab-panel").forEach(p => p.classList.add("hidden"));
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.getElementById("tab-" + name).classList.remove("hidden");
  btn.classList.add("active");
}

function renderCycle(elId, c) {
  const fmt = v => (v !== null && v !== undefined) ? Number(v).toFixed(2) : "--";
  document.getElementById(elId).innerHTML = `
    <div class="kv"><span>Status</span><strong>${c.status}</strong></div>
    <div class="kv"><span>Ref High / Mid / Low</span><strong>${fmt(c.ref_high)} / ${fmt(c.midpoint)} / ${fmt(c.ref_low)}</strong></div>
    <div class="kv"><span>Buy (Mid-High) / Sell (Mid-Low)</span><strong style="color:var(--gold);">${fmt(c.buy_entry)} / ${fmt(c.sell_entry)}</strong></div>
    <div class="kv"><span>Direction</span><strong>${c.direction || "--"}</strong></div>
    <div class="kv"><span>Take Profit / Stop Loss</span><strong>${fmt(c.tp_price)} / ${fmt(c.sl_price)}</strong></div>
  `;
}

let firstLoad = true;
async function refreshState() {
  const res = await fetch("/api/state", { credentials: "same-origin", headers: authHeaders() });
  if (res.status === 401) { doLogout(); return; }
  const d = await res.json();
  if (!d.ok) return;

  document.getElementById("markBadge").innerText = "XAU: " + (d.mark_price ? Number(d.mark_price).toFixed(2) : "--");
  document.getElementById("ipText").innerText = d.public_ip || "--";
  const sb = document.getElementById("statusBadge");
  sb.innerText = d.paused ? "PAUSED" : "RUNNING";
  sb.className = "badge " + (d.paused ? "paused" : "live");
  document.getElementById("pauseBtn").innerText = d.paused ? "▶️ Resume" : "⏸️ Pause";

  renderCycle("cycle1H", d.cycles["1H"]);
  renderCycle("cycle4H", d.cycles["4H"]);

  const lpBox = document.getElementById("livePosBox");
  if (d.live_positions && d.live_positions.length > 0) {
    lpBox.innerHTML = d.live_positions.map(p => `
      <div class="kv"><span>Side / Size</span><strong>${p.side} (${p.size})</strong></div>
      <div class="kv"><span>Entry / Mark</span><strong>${p.entry.toFixed(2)} / ${p.mark.toFixed(2)}</strong></div>
      <div class="kv"><span>Liquidation</span><strong>${p.liq.toFixed(2)}</strong></div>
      <div class="kv"><span>Unrealized PnL</span><strong style="color:${p.pnl >= 0 ? 'var(--green)' : 'var(--red)'};">${p.pnl >= 0 ? '+' : ''}${p.pnl.toFixed(2)} USDT</strong></div>
    `).join("");
  } else {
    lpBox.innerHTML = `<p style="color:var(--muted);margin:0;">No active position on exchange.</p>`;
  }

  if (firstLoad) {
    firstLoad = false;
    const m1 = d.modes["1H"], m4 = d.modes["4H"];
    document.getElementById("m1_enabled").value = String(m1.enabled);
    document.getElementById("m1_tp").value = m1.tp_dollars;
    document.getElementById("m1_sl_off").value = m1.sl_offset_dollars;
    document.getElementById("m1_lev").value = m1.leverage;
    document.getElementById("m1_margin").value = m1.margin_usdt;
    document.getElementById("m1_liq").value = String(m1.liq_guard);

    document.getElementById("m4_enabled").value = String(m4.enabled);
    document.getElementById("m4_tp").value = m4.tp_dollars;
    document.getElementById("m4_sl_off").value = m4.sl_offset_dollars;
    document.getElementById("m4_lev").value = m4.leverage;
    document.getElementById("m4_margin").value = m4.margin_usdt;
    document.getElementById("m4_liq").value = String(m4.liq_guard);

    document.getElementById("tgEnabled").value = String(d.telegram.enabled);
    document.getElementById("tgToken").value = d.telegram.bot_token || "";
    document.getElementById("tgChat").value = d.telegram.chat_id || "";
  }

  const exBox = document.getElementById("exList");
  if (!d.exchanges.length) {
    exBox.innerHTML = `<p style="color:var(--muted);">No exchanges added yet.</p>`;
  } else {
    exBox.innerHTML = d.exchanges.map(e => `
      <div style="border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:10px;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <strong>${e.name} <small style="color:var(--muted);">(${e.exchange_id})</small></strong>
          <span class="badge ${e.enabled ? 'live' : 'paused'}">${e.enabled ? 'ACTIVE' : 'DISABLED'}</span>
        </div>
        <div style="font-size:0.82rem;color:var(--muted);margin:4px 0 8px;">Key: ${e.masked_key}</div>
        <div class="row-actions" style="margin-top:6px;">
          <button class="btn-ghost" onclick="toggleEx('${e.id}')">${e.enabled ? '⏸️ Disable' : '▶️ Enable'}</button>
          <button class="btn-blue" onclick="testEx('${e.id}')">Test & Balance</button>
          <button class="btn-red" onclick="delEx('${e.id}')">Delete</button>
        </div>
      </div>
    `).join("");
  }

  const tb = document.getElementById("termBox");
  tb.textContent = (d.logs || []).join("\n");
  tb.scrollTop = tb.scrollHeight;
}

function copyIP() {
  navigator.clipboard.writeText(document.getElementById("ipText").innerText);
  alert("Server IP copied!");
}

async function togglePause() {
  await fetch("/api/action/pause", { method: "POST", credentials: "same-origin", headers: authHeaders() });
  refreshState();
}

async function cancelLimits(mode) {
  const r = await fetch("/api/action/cancel_limits", {
    method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ mode })
  });
  const d = await r.json();
  alert(d.message);
  refreshState();
}

async function protectOpen() {
  const r = await fetch("/api/action/protect_open", { method: "POST", credentials: "same-origin", headers: authHeaders() });
  const d = await r.json();
  alert(d.ok ? d.message : ("Error: " + d.error));
  refreshState();
}

async function closeAll() {
  if (!confirm("Close open XAUUSDT position at market and cancel all pending orders?")) return;
  const r = await fetch("/api/action/close_all", { method: "POST", credentials: "same-origin", headers: authHeaders() });
  const d = await r.json();
  alert(d.message);
  refreshState();
}

async function saveModes() {
  const payload = {
    "1H": {
      enabled: document.getElementById("m1_enabled").value === "true",
      tp_dollars: parseFloat(document.getElementById("m1_tp").value),
      sl_offset_dollars: parseFloat(document.getElementById("m1_sl_off").value),
      leverage: parseInt(document.getElementById("m1_lev").value, 10),
      margin_usdt: parseFloat(document.getElementById("m1_margin").value),
      liq_guard: document.getElementById("m1_liq").value === "true"
    },
    "4H": {
      enabled: document.getElementById("m4_enabled").value === "true",
      tp_dollars: parseFloat(document.getElementById("m4_tp").value),
      sl_offset_dollars: parseFloat(document.getElementById("m4_sl_off").value),
      leverage: parseInt(document.getElementById("m4_lev").value, 10),
      margin_usdt: parseFloat(document.getElementById("m4_margin").value),
      liq_guard: document.getElementById("m4_liq").value === "true"
    }
  };
  await fetch("/api/modes", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify(payload) });
  alert("Strategy settings saved!");
  refreshState();
}

async function addExchange() {
  const payload = {
    name: document.getElementById("exName").value,
    exchange_id: document.getElementById("exType").value,
    api_key: document.getElementById("exKey").value,
    api_secret: document.getElementById("exSecret").value,
    passphrase: document.getElementById("exPass").value
  };
  const r = await fetch("/api/exchanges", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify(payload) });
  const d = await r.json();
  if (!d.ok) return alert(d.error);
  document.getElementById("exKey").value = "";
  document.getElementById("exSecret").value = "";
  document.getElementById("exPass").value = "";
  alert("Exchange saved and enabled!");
  refreshState();
}

async function toggleEx(id) {
  await fetch("/api/exchanges/toggle", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ id }) });
  refreshState();
}

async function delEx(id) {
  if (!confirm("Delete this exchange account?")) return;
  await fetch("/api/exchanges/delete", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ id }) });
  refreshState();
}

async function testEx(id) {
  const r = await fetch("/api/exchanges/test", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ id }) });
  const d = await r.json();
  alert(d.ok ? d.message : ("Error: " + d.error));
}

async function saveTelegram() {
  const payload = {
    enabled: document.getElementById("tgEnabled").value === "true",
    bot_token: document.getElementById("tgToken").value,
    chat_id: document.getElementById("tgChat").value
  };
  await fetch("/api/telegram", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify(payload) });
  alert("Telegram settings saved!");
}

async function autoDetectTg() {
  const bot_token = document.getElementById("tgToken").value;
  const r = await fetch("/api/telegram/autodetect", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ bot_token }) });
  const d = await r.json();
  if (d.ok) {
    document.getElementById("tgChat").value = d.chat_id;
    alert("Detected Chat ID: " + d.chat_id);
  } else {
    alert(d.error);
  }
}

async function testTelegram() {
  const r = await fetch("/api/telegram/test", { method: "POST", credentials: "same-origin", headers: authHeaders() });
  const d = await r.json();
  alert(d.ok ? d.message : d.error);
}

async function downloadBackup() {
  const r = await fetch("/api/backup/download", { credentials: "same-origin", headers: authHeaders() });
  const d = await r.json();
  const blob = new Blob([JSON.stringify(d.backup, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "xau_orb_bot_backup.json";
  a.click();
}

async function restoreBackup() {
  const f = document.getElementById("restoreFile").files[0];
  if (!f) return alert("Select a JSON backup file first.");
  const txt = await f.text();
  const r = await fetch("/api/backup/restore", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ backup: JSON.parse(txt) }) });
  const d = await r.json();
  alert(d.ok ? "Backup restored!" : d.error);
  firstLoad = true;
  refreshState();
}

async function changePassword() {
  const new_password = document.getElementById("newPass").value;
  const r = await fetch("/api/password", { method: "POST", credentials: "same-origin", headers: authHeaders(), body: JSON.stringify({ new_password }) });
  const d = await r.json();
  alert(d.ok ? "Password updated!" : d.error);
}

async function clearLogs() {
  await fetch("/api/logs/clear", { method: "POST", credentials: "same-origin", headers: authHeaders() });
  refreshState();
}

function initApp() {
  document.getElementById("loginBox").classList.add("hidden");
  document.getElementById("appBox").classList.remove("hidden");
  firstLoad = true;
  refreshState();
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(refreshState, 6000);
}

// Auto-resume if cookie or sessionStorage token is already valid
(async function boot() {
  const res = await fetch("/api/state", { credentials: "same-origin", headers: authHeaders() });
  if (res.status === 200) {
    initApp();
  }
})();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_PORTAL)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
_STARTED_PIDS = set()
_PID_LOCK = threading.Lock()


def start_background_threads(force: bool = False):
    global _SCANNER_LOCK_FD
    pid = os.getpid()
    with _PID_LOCK:
        if pid in _STARTED_PIDS:
            return
        if not force:
            try:
                fd = open(os.path.join(tempfile.gettempdir(), "xau_orb_scanner.lock"), "w")
                fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                _SCANNER_LOCK_FD = fd
            except Exception:
                # Another Gunicorn worker already holds the scanner lock
                return
        _STARTED_PIDS.add(pid)
    t_scan = threading.Thread(target=scanner_worker, daemon=True, name=f"ORBScanner-{pid}")
    t_scan.start()
    t_tg = threading.Thread(target=telegram_polling_worker, daemon=True, name=f"TelegramPoller-{pid}")
    t_tg.start()


@app.before_request
def _ensure_worker_threads():
    start_background_threads(force=(__name__ == "__main__"))


if __name__ == "__main__":
    start_background_threads(force=True)
    app.run(host="0.0.0.0", port=PORT, debug=False)
else:
    start_background_threads(force=False)