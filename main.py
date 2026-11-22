#!/usr/bin/env python3
"""
Polymarket 5m Signals
Open + Now = Chainlink via Polymarket RTDS (wss://ws-live-data.polymarket.com)
Fallback = Binance
"""

import os
import re
import json
import base64
import secrets
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List
from collections import deque

import httpx
import websockets
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Route

SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",")]
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.78"))
WINDOW_SECONDS = 300
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "").strip()
CONFIRM_AT = 125          # entry only after ~2 min
PRICE_HISTORY_LEN = 60
MIN_LEAD_BPS = 10.0        # stronger lead filter (was 8)
MIN_MOM_SHORT = 4.0        # short momentum confirmation
MIN_MOM_MED = 2.5          # medium momentum confirmation

# Chainlink symbol map for RTDS
CHAINLINK_SYM = {
    "BTCUSDT": "btc/usd",
    "ETHUSDT": "eth/usd",
    "SOLUSDT": "sol/usd",
    "XRPUSDT": "xrp/usd",
}
SYM_FROM_CL = {v: k for k, v in CHAINLINK_SYM.items()}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


class WindowState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_to_beat: Optional[float] = None
        self.window_start: Optional[datetime] = None
        self.prices: deque = deque(maxlen=PRICE_HISTORY_LEN)
        self.last_price: Optional[float] = None
        self.signal_sent: bool = False
        self.last_signal: Optional[dict] = None
        self.last_update: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.source: str = ""
        self.open_confirmed: bool = False  # True when open taken from Polymarket
        self.series: deque = deque(maxlen=400)  # (seconds_in_window, price) for chart

    def reset(self, price: float, start: datetime, source: str = ""):
        self.price_to_beat = price
        self.window_start = start
        self.prices.clear()
        self.prices.append(price)
        self.series.clear()
        self.series.append((0.0, price))
        self.last_price = price
        self.signal_sent = False
        self.last_signal = None
        self.last_update = datetime.now(timezone.utc)
        self.last_error = None
        self.source = source
        self.open_confirmed = False


state: Dict[str, WindowState] = {s: WindowState(s) for s in SYMBOLS}
signal_history: List[dict] = []
global_error: Optional[str] = None
live_chainlink: Dict[str, float] = {}  # symbol -> last chainlink price


def momentum_bps(prices: deque, lookback: int = 12) -> float:
    if len(prices) < lookback + 1:
        return 0.0
    return (prices[-1] - prices[-lookback]) / prices[-lookback] * 10000


def generate_signal(price_to_beat: float, current_price: float, seconds_passed: int, prices: deque) -> dict:
    """
    Multi-factor signal inspired by jury-style bots:
    - Judge 1: Lead vs priceToBeat (Chainlink open)
    - Judge 2: Short momentum (last ~12 ticks)
    - Judge 3: Medium momentum (last ~25 ticks)
    Only fire when majority agree + strong enough edge.
    """
    if seconds_passed < CONFIRM_AT:
        return {"side": None, "confidence": 0.0, "reason": "too_early", "score": 0}

    lead_bps = ((current_price - price_to_beat) / price_to_beat) * 10000
    time_left = WINDOW_SECONDS - seconds_passed
    abs_lead = abs(lead_bps)

    if abs_lead < MIN_LEAD_BPS:
        return {"side": None, "confidence": 0.0, "reason": "too_small_lead", "score": 0, "lead_bps": round(lead_bps, 2)}

    mom_short = momentum_bps(prices, lookback=12)
    mom_med = momentum_bps(prices, lookback=25)

    side = "UP" if lead_bps > 0 else "DOWN"

    # Jury votes
    votes = 0
    # Judge 1: lead
    if abs_lead >= MIN_LEAD_BPS:
        votes += 1
    # Judge 2: short momentum same direction
    if (side == "UP" and mom_short >= MIN_MOM_SHORT) or (side == "DOWN" and mom_short <= -MIN_MOM_SHORT):
        votes += 1
    # Judge 3: medium momentum same direction
    if (side == "UP" and mom_med >= MIN_MOM_MED) or (side == "DOWN" and mom_med <= -MIN_MOM_MED):
        votes += 1

    if votes < 2:
        return {
            "side": None,
            "confidence": 0.0,
            "reason": f"jury_{votes}/3",
            "score": votes,
            "lead_bps": round(lead_bps, 2),
            "mom_short": round(mom_short, 2),
            "mom_med": round(mom_med, 2),
        }

    # Confidence curve: later + stronger lead = higher conf
    base = 0.72 + (votes - 2) * 0.06          # 2 votes → 0.72, 3 votes → 0.78
    lead_boost = min(abs_lead * 0.011, 0.14)
    time_boost = 0.0
    if time_left <= 45:
        time_boost = 0.08
    elif time_left <= 70:
        time_boost = 0.04

    confidence = min(base + lead_boost + time_boost, 0.96)

    return {
        "side": side,
        "confidence": round(confidence, 3),
        "lead_bps": round(lead_bps, 2),
        "time_left": time_left,
        "score": votes,
        "mom_short": round(mom_short, 2),
        "mom_med": round(mom_med, 2),
        "reason": f"jury_{votes}/3",
    }


def is_strong_signal(signal: dict) -> bool:
    if signal.get("side") is None:
        return False
    return signal.get("confidence", 0) >= MIN_CONFIDENCE and signal.get("score", 0) >= 2


def get_window_start_unix() -> int:
    now = int(datetime.now(timezone.utc).timestamp())
    return now - (now % WINDOW_SECONDS)


def get_current_window_start() -> datetime:
    return datetime.fromtimestamp(get_window_start_unix(), tz=timezone.utc)


async def fetch_binance(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    for url in [
        f"https://data-api.binance.vision/api/v3/ticker/price?symbol={symbol}",
        f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
    ]:
        try:
            r = await client.get(url, timeout=4.0)
            if r.status_code == 200:
                return float(r.json()["price"])
        except Exception:
            continue
    return None


def twap_last_seconds(series, seconds: float = 60.0) -> Optional[float]:
    """Average of the ticks in the last `seconds` of the series (Polymarket settles on a 60s Chainlink TWAP)."""
    if not series:
        return None
    t_end = series[-1][0]
    pts = [p for t, p in series if t >= t_end - seconds]
    return sum(pts) / len(pts) if pts else None


def twap_final_minute(series) -> Optional[float]:
    """Average of the ticks in the final minute of the window (t in [240, 300])."""
    pts = [p for t, p in series if t >= WINDOW_SECONDS - 60]
    return sum(pts) / len(pts) if len(pts) >= 5 else None


async def fetch_polymarket_open(client: httpx.AsyncClient, symbol: str, start_unix: int) -> Optional[float]:
    """Price to Beat straight from Polymarket (same endpoint the website uses)."""
    base = symbol.replace("USDT", "")
    start_iso = datetime.fromtimestamp(start_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = datetime.fromtimestamp(start_unix + WINDOW_SECONDS, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        r = await client.get(
            "https://polymarket.com/api/crypto/crypto-price",
            params={
                "symbol": base,
                "eventStartTime": start_iso,
                "variant": "fiveminute",
                "endDate": end_iso,
            },
            timeout=3.0,
        )
        if r.status_code == 200:
            val = r.json().get("openPrice")
            if val is not None:
                return float(val)
    except Exception as e:
        logger.debug(f"polymarket open fetch {symbol}: {e}")
    return None


async def fetch_polymarket_close(client: httpx.AsyncClient, symbol: str, start_unix: int) -> Optional[float]:
    """Final price of a finished window straight from Polymarket."""
    base = symbol.replace("USDT", "")
    start_iso = datetime.fromtimestamp(start_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = datetime.fromtimestamp(start_unix + WINDOW_SECONDS, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        r = await client.get(
            "https://polymarket.com/api/crypto/crypto-price",
            params={"symbol": base, "eventStartTime": start_iso, "variant": "fiveminute", "endDate": end_iso},
            timeout=3.0,
        )
        if r.status_code == 200:
            val = r.json().get("closePrice")
            if val is not None:
                return float(val)
    except Exception as e:
        logger.debug(f"polymarket close fetch {symbol}: {e}")
    return None


# ====================== Virtual (paper) trading ======================
PAPER_FILE = os.getenv("PAPER_FILE", "paper_state.json")
PAPER_FEE_RATE = 0.25      # approx. Polymarket crypto taker fee: fee = shares * 0.25 * (p*(1-p))**2
PAPER_COINS = ["BTC", "ETH", "SOL", "XRP"]


def _paper_default() -> dict:
    return {
        "enabled": False,
        "deposit": 1000.0,
        "balance": 1000.0,
        "stake_mode": "pct",      # "pct" = % of balance, "usd" = fixed $
        "stake_value": 5.0,
        "coins": {c: True for c in PAPER_COINS},
        "trades": [],
        "next_id": 1,
    }


paper: dict = _paper_default()


def paper_save():
    try:
        tmp = PAPER_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(paper, f)
        os.replace(tmp, PAPER_FILE)
    except Exception as e:
        logger.warning(f"paper save failed: {e}")


def paper_load():
    try:
        with open(PAPER_FILE) as f:
            data = json.load(f)
        base = _paper_default()
        base.update({k: v for k, v in data.items() if k in base})
        base["coins"] = {c: bool((data.get("coins") or {}).get(c, True)) for c in PAPER_COINS}
        paper.clear()
        paper.update(base)
        logger.info(f"paper state loaded: balance={paper['balance']:.2f}, trades={len(paper['trades'])}")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"paper load failed: {e}")


def _json_list(v):
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            return []
    return v if isinstance(v, list) else []


async def paper_market_meta(client: httpx.AsyncClient, slug: str):
    """(conditionId, {token_id: 'Up'/'Down'}) for a 5m market."""
    meta = _cid_cache.get(slug)
    if meta:
        return meta
    r = await client.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=5.0)
    data = r.json()
    ev = data[0] if isinstance(data, list) and data else None
    mk = (ev or {}).get("markets") or []
    if not mk:
        return None
    m0 = mk[0]
    tokens = _json_list(m0.get("clobTokenIds"))
    outs = _json_list(m0.get("outcomes"))
    meta = (m0.get("conditionId"), {str(t): str(o) for t, o in zip(tokens, outs)})
    if meta[0]:
        _cid_cache[slug] = meta
    return meta


_bg_tasks: set = set()


def spawn(coro):
    """create_task that keeps a reference (otherwise the task can be garbage-collected mid-flight)."""
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


def paper_skip(coin: str, side: str, start_unix: int, reason: str):
    """Record why a signal was not traded (shown in the UI, not counted in stats)."""
    logger.info(f"paper: skipped {coin} {side}: {reason}")
    paper["trades"].insert(0, {
        "id": paper["next_id"], "ts": int(datetime.now(timezone.utc).timestamp()),
        "symbol": coin, "side": side, "window_start": start_unix,
        "entry": 0.0, "shares": 0.0, "cost": 0.0, "fee": 0.0,
        "status": "skip", "reason": reason, "pnl": 0.0, "settled_at": None, "balance_after": None,
    })
    paper["next_id"] += 1
    paper_save()


async def _paper_get_asks(client: httpx.AsyncClient, coin: str, side: str, start_unix: int):
    """Order-book asks for the side we want. Retries a few times. Returns (asks, reason)."""
    slug = f"{coin.lower()}-updown-5m-{start_unix}"
    reason = "невідома помилка"
    for attempt in range(3):
        try:
            meta = await paper_market_meta(client, slug)
            if not meta or not meta[0]:
                reason = "ринок не знайдено на Polymarket"
            else:
                want = "up" if side == "UP" else "down"
                token = next((tok for tok, o in meta[1].items() if o.lower() == want), None)
                if not token:
                    reason = "не знайдено токен ринку"
                else:
                    r = await client.get("https://clob.polymarket.com/book", params={"token_id": token}, timeout=5.0)
                    book = r.json()
                    asks = []
                    for x in (book.get("asks") or []):
                        try:
                            p, sz = float(x["price"]), float(x["size"])
                        except Exception:
                            continue
                        if 0 < p <= 0.99 and sz > 0:
                            asks.append((p, sz))
                    asks.sort()
                    if asks:
                        return asks, None
                    reason = "немає пропозицій дешевше 99¢"
        except Exception as e:
            reason = "помилка мережі Polymarket"
            logger.warning(f"paper book attempt {attempt + 1} for {coin}: {e}")
        await asyncio.sleep(0.7)
    return None, reason


async def paper_open(client: httpx.AsyncClient, symbol: str, side: str, start_unix: int):
    """Open a virtual position at the real Polymarket order book (walks the asks)."""
    coin = symbol.replace("USDT", "")
    try:
        if not paper["enabled"] or not paper["coins"].get(coin):
            return
        if any(t["status"] == "open" and t["symbol"] == coin and t["window_start"] == start_unix
               for t in paper["trades"]):
            return
        asks, reason = await _paper_get_asks(client, coin, side, start_unix)

        # state may have changed while we were waiting for the network
        if not paper["enabled"] or not paper["coins"].get(coin):
            return
        if not asks:
            paper_skip(coin, side, start_unix, reason or "немає даних")
            return
        bal = paper["balance"]
        stake = bal * paper["stake_value"] / 100.0 if paper["stake_mode"] == "pct" else paper["stake_value"]
        stake = min(stake, bal)
        if stake < 1:
            paper_skip(coin, side, start_unix, "баланс замалий для ставки")
            return

        remaining, shares, cost = stake, 0.0, 0.0
        for p, sz in asks:
            take = min(sz, remaining / p)
            shares += take
            cost += take * p
            remaining -= take * p
            if remaining < 0.01:
                break
        if shares <= 0 or cost < 1:
            paper_skip(coin, side, start_unix, "замало ліквідності")
            return
        avg = cost / shares
        fee = shares * PAPER_FEE_RATE * (avg * (1 - avg)) ** 2
        total = cost + fee
        if total > paper["balance"]:
            k = paper["balance"] / total
            shares, cost, fee, total = shares * k, cost * k, fee * k, total * k
        paper["balance"] -= total
        trade = {
            "id": paper["next_id"],
            "ts": int(datetime.now(timezone.utc).timestamp()),
            "symbol": coin,
            "side": side,
            "window_start": start_unix,
            "entry": round(avg, 4),
            "shares": round(shares, 4),
            "cost": round(cost, 2),
            "fee": round(fee, 4),
            "status": "open",
            "pnl": 0.0,
            "settled_at": None,
            "balance_after": None,
        }
        paper["next_id"] += 1
        paper["trades"].insert(0, trade)
        if len(paper["trades"]) > 400:
            keep_open = [t for t in paper["trades"] if t["status"] == "open"]
            closed = [t for t in paper["trades"] if t["status"] != "open"][:300]
            paper["trades"] = sorted(keep_open + closed, key=lambda t: t["id"], reverse=True)
        paper_save()
        logger.info(f"PAPER BUY {coin} {side} {shares:.2f} sh @ {avg:.3f} cost ${cost:.2f} fee ${fee:.3f}")
    except Exception as e:
        logger.warning(f"paper_open error: {e}")
        try:
            paper_skip(coin, side, start_unix, "внутрішня помилка")
        except Exception:
            pass


def paper_settle(coin: str, start_unix: int, actual: str) -> bool:
    for t in paper["trades"]:
        if t["status"] == "open" and t["symbol"] == coin and t["window_start"] == start_unix:
            won = t["side"] == actual
            if won:
                paper["balance"] += t["shares"]
                t["pnl"] = round(t["shares"] - t["cost"] - t["fee"], 2)
                t["status"] = "win"
            else:
                t["pnl"] = round(-(t["cost"] + t["fee"]), 2)
                t["status"] = "loss"
            t["settled_at"] = int(datetime.now(timezone.utc).timestamp())
            t["balance_after"] = round(paper["balance"], 2)
            paper_save()
            logger.info(f"PAPER {t['status'].upper()} {coin} {t['side']} pnl ${t['pnl']:.2f}")
            return True
    return False


def paper_void(t: dict):
    paper["balance"] += t["cost"] + t["fee"]
    t["status"] = "void"
    t["pnl"] = 0.0
    t["settled_at"] = int(datetime.now(timezone.utc).timestamp())
    t["balance_after"] = round(paper["balance"], 2)
    paper_save()


async def paper_settle_orphans(client: httpx.AsyncClient, symbol: str, start_unix: int):
    """Settle positions whose window ended but the normal close step missed them (e.g. after a restart)."""
    coin = symbol.replace("USDT", "")
    for t in list(paper["trades"]):
        if t["status"] != "open" or t["symbol"] != coin or t["window_start"] >= start_unix:
            continue
        o = await fetch_polymarket_open(client, symbol, t["window_start"])
        c = await fetch_polymarket_close(client, symbol, t["window_start"])
        if o is not None and c is not None:
            paper_settle(coin, t["window_start"], "UP" if c >= o else "DOWN")
        elif t["window_start"] < start_unix - 1200:
            paper_void(t)


paper_load()


# ====================== RTDS Chainlink ======================
async def rtds_loop():
    """Subscribe to Polymarket RTDS Chainlink prices."""
    url = "wss://ws-live-data.polymarket.com"
    while True:
        try:
            async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
                # one subscription for ALL chainlink symbols (no filters) - most reliable
                subs = [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "",
                }]
                await ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}))
                logger.info("RTDS subscribed to Chainlink prices")

                async def ping():
                    while True:
                        await asyncio.sleep(5)
                        try:
                            await ws.send("PING")
                        except Exception:
                            break

                ping_task = asyncio.create_task(ping())
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        topic = msg.get("topic") or ""
                        if "chainlink" not in topic and topic != "crypto_prices_chainlink":
                            continue
                        payload = msg.get("payload") or {}
                        sym = (payload.get("symbol") or "").lower()
                        val = payload.get("value")
                        if val is None and isinstance(payload.get("data"), list) and payload["data"]:
                            val = payload["data"][-1].get("value")  # initial snapshot
                        if val is None:
                            continue
                        usdt = SYM_FROM_CL.get(sym)
                        if not usdt:
                            continue
                        price = float(val)
                        if usdt not in live_chainlink:
                            logger.info(f"RTDS first chainlink price {usdt}={price}")
                        live_chainlink[usdt] = price
                finally:
                    ping_task.cancel()
        except Exception as e:
            logger.warning(f"RTDS reconnect in 3s: {e}")
            await asyncio.sleep(3)


async def price_loop():
    global global_error
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        while True:
            try:
                start_unix = get_window_start_unix()
                window_start = get_current_window_start()
                seconds_passed = int((datetime.now(timezone.utc) - window_start).total_seconds())
                any_ok = False

                for symbol in SYMBOLS:
                    ws = state[symbol]

                    # prefer Chainlink live price
                    price = live_chainlink.get(symbol)
                    source_now = "chainlink"
                    if price is None:
                        price = await fetch_binance(client, symbol)
                        source_now = "binance"

                    if price is None:
                        ws.last_error = "no price"
                        continue

                    any_ok = True
                    ws.last_error = None

                    # new window
                    
                    # close previous window: mark history outcomes ONLY when the window has ended
                    if (ws.window_start is not None and ws.window_start != window_start
                            and ws.last_signal and ws.price_to_beat and ws.last_price):
                        prev_start_unix = int(ws.window_start.timestamp())
                        final = await fetch_polymarket_close(client, symbol, prev_start_unix)
                        if final is None:
                            final = twap_final_minute(ws.series) or ws.last_price
                        open_p = ws.price_to_beat
                        actual = "UP" if final >= open_p else "DOWN"
                        predicted = ws.last_signal.get("side")
                        paper_settle(symbol.replace("USDT", ""), prev_start_unix, actual)
                        for h in signal_history:
                            if h.get("window_start") == (ws.window_start.isoformat() if hasattr(ws.window_start, "isoformat") else str(ws.window_start)) and h.get("symbol") == symbol.replace("USDT",""):
                                h["result"] = "win" if predicted == actual else "loss"
                                h["final"] = final
                                h["actual"] = actual
                                h["move"] = round(final - open_p, 6)
                                break

                    if ws.window_start is None or ws.window_start != window_start:
                        await paper_settle_orphans(client, symbol, start_unix)
                        # capture open from Chainlink if available, else binance
                        pm_open = await fetch_polymarket_open(client, symbol, start_unix)
                        if pm_open is not None:
                            open_px, open_src = pm_open, "polymarket"
                        else:
                            open_px = live_chainlink.get(symbol) or price
                            open_src = "chainlink" if symbol in live_chainlink else "binance-fallback"
                        ws.reset(open_px, window_start, source=open_src)
                        ws.open_confirmed = pm_open is not None
                        logger.info(f"[{symbol}] New window open={open_px:.4f} ({open_src})")
                        continue

                    # Polymarket open not available at window start yet -> keep retrying
                    if not ws.open_confirmed:
                        pm_open = await fetch_polymarket_open(client, symbol, start_unix)
                        if pm_open is not None:
                            ws.price_to_beat = pm_open
                            ws.open_confirmed = True
                            ws.source = "polymarket"
                            logger.info(f"[{symbol}] Open set from Polymarket: {pm_open:.4f}")

                    # update live
                    ws.prices.append(price)
                    ws.last_price = price
                    ws.series.append((round((datetime.now(timezone.utc) - ws.window_start).total_seconds(), 1), price))
                    ws.last_update = datetime.now(timezone.utc)
                    if source_now == "chainlink" and ws.source != "chainlink" and not ws.open_confirmed:
                        # upgrade open if we only had binance at start and still early
                        if seconds_passed < 15 and ws.source.startswith("binance"):
                            ws.price_to_beat = price  # rare: only first seconds
                        ws.source = "chainlink" if ws.source.startswith("chainlink") or source_now == "chainlink" else ws.source

                    if seconds_passed < CONFIRM_AT or ws.signal_sent:
                        continue

                    signal = generate_signal(ws.price_to_beat, price, seconds_passed, ws.prices)
                    if is_strong_signal(signal):
                        ws.signal_sent = True
                        entry = {
                            **signal,
                            "symbol": symbol.replace("USDT", ""),
                            "momentum_bps": signal.get("mom_short", 0),
                            "price": price,
                            "open": ws.price_to_beat,
                            "time": datetime.now(timezone.utc).isoformat(),
                            "window_start": window_start.isoformat(),
                        }
                        ws.last_signal = entry
                        signal_history.insert(0, entry)
                        spawn(paper_open(client, symbol, signal["side"], start_unix))
                        if len(signal_history) > 50:
                            signal_history.pop()
                        logger.info(
                            f"SIGNAL {symbol} {signal['side']} conf={signal['confidence']} "
                            f"score={signal.get('score')} lead={signal.get('lead_bps')} "
                            f"mom={signal.get('mom_short')}"
                        )

                global_error = None if any_ok else "Немає цін"
                await asyncio.sleep(1.5)
            except Exception as e:
                global_error = str(e)
                logger.error(f"Loop: {e}")
                await asyncio.sleep(5)


async def api_status(request):
    window_start = get_current_window_start()
    seconds_passed = int((datetime.now(timezone.utc) - window_start).total_seconds())
    time_left = max(0, WINDOW_SECONDS - seconds_passed)

    markets = {}
    for symbol, ws in state.items():
        name = symbol.replace("USDT", "")
        if ws.price_to_beat is None:
            markets[name] = {"status": "waiting", "error": ws.last_error}
            continue
        current = ws.last_price or ws.price_to_beat
        lead = ((current - ws.price_to_beat) / ws.price_to_beat) * 10000
        live = generate_signal(ws.price_to_beat, current, max(seconds_passed, CONFIRM_AT), ws.prices)
        markets[name] = {
            "open": round(ws.price_to_beat, 4),
            "current": round(current, 4),
            "lead_bps": round(lead, 2),
            "signal_sent": ws.signal_sent,
            "signal": ws.last_signal,
            "live_side": live.get("side"),
            "live_confidence": live.get("confidence", 0),
            "live_score": live.get("score", 0),
            "source": ws.source,
            "series": [[t, round(p, 4)] for t, p in ws.series],
            "twap60": (round(twap_last_seconds(ws.series), 4) if ws.series else None),
            "updated": ws.last_update.isoformat() if ws.last_update else None,
        }

    # simple win-rate from resolved history
    resolved = [h for h in signal_history if h.get("result") in ("win", "loss")]
    wins = sum(1 for h in resolved if h["result"] == "win")
    losses = sum(1 for h in resolved if h["result"] == "loss")
    total_r = wins + losses
    win_rate = round(wins / total_r * 100, 1) if total_r else None

    return JSONResponse({
        "server_time": datetime.now(timezone.utc).isoformat(),
        "window_start": window_start.isoformat(),
        "seconds_passed": seconds_passed,
        "time_left": time_left,
        "confirm_at": CONFIRM_AT,
        "error": global_error,
        "chainlink_live": {k.replace("USDT",""): v for k,v in live_chainlink.items()},
        "markets": markets,
        "history": signal_history[:30],
        "stats": {
            "signals": len(signal_history),
            "resolved": total_r,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
        },
    })


async def health(request):
    return JSONResponse({"ok": True, "chainlink": list(live_chainlink.keys())})


# HTML — same dashboard (compact)
HTML_PAGE = """<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover"/>
<meta name="apple-mobile-web-app-capable" content="yes"/>
<title>5m Signals</title>
<style>
:root{--bg:#0b0b0c;--card:#141415;--border:#232326;--text:#ececee;--muted:#8a8a93;--up:#22c55e;--down:#ef4444;--accent:#8b8bff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding:14px;padding-bottom:40px}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}
h1{font-size:1.15rem;font-weight:700}
.badge{font-size:.72rem;background:#1c1c1f;color:var(--muted);padding:4px 10px;border-radius:999px;border:1px solid var(--border)}
.badge.ok{color:var(--up);border-color:rgba(34,197,94,.3)}
.top{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}
.ac{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px 14px}
.an{font-size:.72rem;color:var(--muted)}.ap{font-size:1.05rem;font-weight:700;font-variant-numeric:tabular-nums}
.ach{font-size:.78rem;font-weight:600}.pos{color:var(--up)}.neg{color:var(--down)}
.panel{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:14px;margin-bottom:12px}
.pt{font-size:.8rem;color:var(--muted);margin-bottom:10px;display:flex;justify-content:space-between}
.mrow{display:grid;grid-template-columns:50px 1fr 1fr 90px;gap:6px;padding:9px 0;border-bottom:1px solid var(--border);font-size:.88rem;align-items:center}
.mrow.h{color:var(--muted);font-size:.7rem;text-transform:uppercase;border:none}
.bs{padding:3px 8px;border-radius:6px;font-size:.72rem;font-weight:700}
.bu{background:rgba(34,197,94,.15);color:var(--up)}.bd{background:rgba(239,68,68,.15);color:var(--down)}.bw{background:#1c1c1f;color:var(--muted)}
.timer{font-size:2rem;font-weight:700;font-variant-numeric:tabular-nums;margin:6px 0 12px}
.btns{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}
.bup,.bdn{padding:13px;border-radius:12px;border:none;font-weight:700;font-size:.9rem;text-align:center;opacity:.45;transition:.2s}
.bup{background:rgba(34,197,94,.18);color:var(--up)}.bdn{background:rgba(239,68,68,.18);color:var(--down)}
.bup.on,.bdn.on{opacity:1;box-shadow:0 0 0 2px currentColor}
.conf{height:6px;background:#1c1c1f;border-radius:4px;overflow:hidden;margin-top:6px}
.conf i{display:block;height:100%;background:var(--accent);width:0;transition:width .4s}
.ct{font-size:.85rem;color:var(--muted);margin-top:4px}
.lr{display:flex;justify-content:space-between;padding:5px 0;font-size:.85rem;border-bottom:1px solid var(--border)}
.hrow{display:grid;grid-template-columns:60px 40px 30px 40px 1fr;gap:4px;padding:7px 0;border-bottom:1px solid var(--border);font-size:.78rem;align-items:center}
.stats{display:flex;gap:12px;font-size:.78rem;color:var(--muted);margin-bottom:8px;flex-wrap:wrap}
.stats b{color:var(--text)}
.ctabs{display:flex;gap:6px;margin-bottom:10px}
.ctabs button{flex:1;background:#1c1c1f;color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:7px 0;font-size:.78rem;font-weight:600}
.ctabs button.on{color:var(--text);border-color:var(--accent)}
.cnums{display:flex;gap:18px;margin-bottom:8px}
.cnums small{display:block;font-size:.68rem;color:var(--muted)}
.cnums b{font-size:1.05rem;font-variant-numeric:tabular-nums}
.bnav{position:fixed;left:0;right:0;bottom:0;display:flex;background:#101012;border-top:1px solid var(--border);padding-bottom:env(safe-area-inset-bottom);z-index:40}
.bnav button{flex:1;background:none;border:0;color:var(--muted);padding:9px 0 8px;font-size:1.25rem;display:flex;flex-direction:column;align-items:center;gap:2px}
.bnav button span{font-size:.68rem;font-weight:600;letter-spacing:.04em}
.bnav button.on{color:var(--text)}
.sheet{position:fixed;left:0;right:0;top:0;bottom:calc(58px + env(safe-area-inset-bottom));background:var(--bg);display:none;z-index:30;flex-direction:column;padding:14px 16px 0}
.sheet.open{display:flex}
.sheet h3{margin:0 0 10px;font-size:1rem}
.sheet .scroll{flex:1;overflow-y:auto;-webkit-overflow-scrolling:touch;padding-bottom:12px}
.subtabs{display:flex;gap:6px;margin:10px 0 4px;overflow-x:auto}
.subtabs button{flex:none;padding-left:14px!important;padding-right:14px!important;white-space:nowrap;background:#1c1c1f;color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:8px 0;font-size:.74rem;font-weight:600}
.subtabs button.on{color:var(--text);border-color:var(--accent)}
.vbar{display:flex;height:10px;border-radius:6px;overflow:hidden;margin:8px 0 4px;background:#232326}
.wrow{padding:9px 0;border-bottom:1px solid var(--border);font-size:.82rem}
.wrow a{color:var(--text);text-decoration:none;font-family:ui-monospace,Menlo,monospace}
.wrow .sub{color:var(--muted);font-size:.74rem;margin-top:2px}
.sec{font-size:.7rem;letter-spacing:.12em;color:var(--muted);margin:14px 0 4px;text-transform:uppercase}
.pbtn{background:#1c1c1f;color:var(--muted);border:1px solid var(--border);border-radius:999px;padding:7px 16px;font-size:.8rem;font-weight:700}
.pbtn.on{color:#0b0b0c;background:var(--up);border-color:var(--up)}
.inp{background:#1c1c1f;border:1px solid var(--border);color:var(--text);border-radius:10px;padding:10px;font-size:1rem;width:100%;box-sizing:border-box}
.prow{display:flex;gap:8px;align-items:center;margin:10px 0}
.prow label{font-size:.78rem;color:var(--muted);min-width:78px}
.pgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.pgrid small{display:block;font-size:.68rem;color:var(--muted)}
.pgrid b{font-size:1.05rem;font-variant-numeric:tabular-nums}
.pgo{background:var(--accent);color:#0b0b0c;border:0;border-radius:10px;padding:10px 16px;font-weight:700;font-size:.85rem;white-space:nowrap}
.pgh{background:#1c1c1f;color:var(--text);border:1px solid var(--border);border-radius:10px;padding:10px 14px;font-size:.8rem;font-weight:600;white-space:nowrap}
</style>
</head>
<body>
<header>
  <h1>⚡ 5m Signals</h1>
  <div id="badge" class="badge">…</div>
</header>
<div class="top" id="top"></div>
<div class="panel">
  <div class="pt"><span>Ринки</span><span id="tmr"></span></div>
  <div class="mrow h"><div>Актив</div><div>Open</div><div>Now</div><div>Сигнал</div></div>
  <div id="mkt"></div>
</div>
<div class="panel">
  <div class="pt"><span>Графік 5m (Chainlink / Polymarket)</span><span id="cwin"></span></div>
  <div class="ctabs" id="ctabs"><button data-s="BTC">BTC</button><button data-s="ETH">ETH</button><button data-s="SOL">SOL</button><button data-s="XRP">XRP</button></div>
  <div class="ctabs" id="cmode"><button data-m="c">Свічки 1хв</button><button data-m="l">Лінія</button></div>
  <div class="cnums"><div><small>Price to beat</small><b id="cpb">—</b></div><div><small>Поточна ціна</small><b id="cpn">—</b></div><div><small>TWAP 60с</small><b id="ctw">—</b></div></div>
  <div id="chart"></div>
</div>
<div class="panel">
  <div class="pt"><span>Історія</span><span id="st"></span></div>
  <div class="stats" id="stats"></div>
  <div class="hrow" style="color:var(--muted);font-size:.65rem"><div>Час</div><div>Акт</div><div>Стор</div><div>Conf</div><div>Lead</div></div>
  <div id="hist"></div>
</div>
<div style="height:90px"></div>
<div class="sheet" id="sheet">
  <h3>Угоди на Polymarket (поточне вікно)</h3>
  <div class="ctabs" id="wtabs"><button data-s="BTC">BTC</button><button data-s="ETH">ETH</button><button data-s="SOL">SOL</button><button data-s="XRP">XRP</button></div>
  <div id="wsum"></div>
  <div class="subtabs" id="wsub"><button data-t="feed">Стрічка</button><button data-t="buy">Купівлі</button><button data-t="sell">Продажі</button><button data-t="up">Up</button><button data-t="down">Down</button><button data-t="top">Гаманці</button></div>
  <div class="scroll" id="wbody" style="color:var(--muted);font-size:.85rem">Завантаження…</div>
</div>
<div class="sheet" id="vtop">
  <h3>Найуспішніші трейдери · крипто</h3>
  <div class="ctabs" id="tmode"><button data-m="top">Топ прибуток</button><button data-m="clean">Без програшів</button></div>
  <div class="ctabs" id="lchips" style="display:none;margin-top:8px"><button data-l="0">0 програшів</button><button data-l="1">≤ 1</button><button data-l="2">≤ 2</button><button data-l="3">≤ 3</button></div>
  <div class="ctabs" id="ttabs" style="margin-top:8px"><button data-p="day">Сьогодні</button><button data-p="week">Тиждень</button><button data-p="month">Місяць</button></div>
  <div class="scroll" id="tbody" style="color:var(--muted);font-size:.85rem;margin-top:10px">Завантаження…</div>
</div>
<div class="sheet" id="vpap">
  <h3>Віртуальна торгівля (без реальних грошей)</h3>
  <div class="scroll">
    <div class="panel">
      <div class="prow" style="justify-content:space-between"><span>Торгувати віртуально</span><button class="pbtn" id="pen">ВИМК</button></div>
      <div style="font-size:.72rem;color:var(--muted);margin-bottom:6px">Монети, якими торгувати:</div>
      <div class="ctabs" id="pcoins"><button data-c="BTC">BTC</button><button data-c="ETH">ETH</button><button data-c="SOL">SOL</button><button data-c="XRP">XRP</button></div>
      <div class="prow"><label>Ставка</label><select class="inp" id="pmode" style="flex:1.2"><option value="pct">% від балансу</option><option value="usd">$ фіксована</option></select><input class="inp" id="pval" type="number" inputmode="decimal" style="flex:1"></div>
      <div class="prow"><button class="pgo" id="psave">Зберегти</button><span id="pmsg" style="font-size:.78rem;color:var(--up)"></span></div>
      <div class="prow" style="margin-top:14px"><label>Депозит $</label><input class="inp" id="pdep" type="number" inputmode="decimal" style="flex:1"><button class="pgh" id="preset">Скинути рахунок</button></div>
      <div style="font-size:.7rem;color:var(--muted)">Зміни депозит і натисни «Зберегти» (або «Скинути рахунок»): баланс стане рівним депозиту, історія віртуальних угод буде стерта.</div>
    </div>
    <div class="panel"><div id="pacc"></div><div id="pspark" style="margin-top:10px"></div></div>
    <div class="panel"><div class="pt"><span>Віртуальні угоди</span></div><div id="plist"></div></div>
  </div>
</div>
<nav class="bnav"><button id="nv1" class="on">📈<span>Сигнали</span></button><button id="nv2">🐋<span>Гаманці</span></button><button id="nv3">🏆<span>Трейдери</span></button><button id="nv4">💰<span>Віртуал</span></button></nav>
<script>
const S=["BTC","ETH","SOL","XRP"];
function f(n,d=2){return n==null?"—":Number(n).toFixed(d)}
function drawChart(d){
  window._d=d;
  const n=window._cs||"BTC",mode=window._cm||"c";
  document.querySelectorAll("#ctabs button").forEach(b=>{b.className=b.dataset.s===n?"on":""});
  document.querySelectorAll("#cmode button").forEach(b=>{b.className=b.dataset.m===mode?"on":""});
  const box=document.getElementById("chart");
  const m=d.markets[n];
  if(!m||!m.series||m.series.length<2){box.innerHTML="<div style='color:var(--muted);font-size:.85rem'>Збираю дані…</div>";return}
  const dec=n==="XRP"?4:2,ser=m.series,open=m.open,last=ser[ser.length-1][1];
  document.getElementById("cpb").textContent="$"+f(open,dec);
  const pn=document.getElementById("cpn");pn.textContent="$"+f(last,dec);pn.style.color=last>=open?"var(--up)":"var(--down)";
  const tw=document.getElementById("ctw");if(m.twap60!=null){tw.textContent="$"+f(m.twap60,dec);tw.style.color=m.twap60>=open?"var(--up)":"var(--down)"}else{tw.textContent="—"}
  document.getElementById("cwin").textContent=(d.time_left||0)+"s left";
  // 1-minute candles
  const cd=[];
  for(const p of ser){
    const k=Math.min(4,Math.floor(p[0]/60));
    if(!cd[k])cd[k]={o:p[1],h:p[1],l:p[1],c:p[1]};
    else{const c=cd[k];c.h=Math.max(c.h,p[1]);c.l=Math.min(c.l,p[1]);c.c=p[1]}
  }
  const vals=mode==="c"?cd.filter(Boolean).flatMap(c=>[c.h,c.l]):ser.map(p=>p[1]);
  let lo=Math.min(open,...vals),hi=Math.max(open,...vals);
  let pad=(hi-lo)*0.2;if(!pad)pad=hi*0.0005;lo-=pad;hi+=pad;
  const W=340,H=200,L=4,R=60,T=10,B=20,PW=W-L-R;
  const X=t=>L+Math.min(t,300)/300*PW,Y=p=>T+(hi-p)/(hi-lo)*(H-T-B);
  let g="";
  for(let i=0;i<4;i++){
    const p=hi-(hi-lo)*i/3,y=Y(p);
    g+="<line x1='"+L+"' x2='"+(W-R)+"' y1='"+y+"' y2='"+y+"' stroke='#232326' stroke-width='1'/><text x='"+(W-R+4)+"' y='"+(y+3)+"' fill='#8a8a93' font-size='9'>"+f(p,dec)+"</text>";
  }
  for(let i=1;i<5;i++){const x=L+PW*i/5;g+="<line x1='"+x+"' x2='"+x+"' y1='"+T+"' y2='"+(H-B)+"' stroke='#1a1a1d' stroke-width='1'/>"}
  const yo=Y(open),xc=X(d.confirm_at||0);
  g+="<line x1='"+L+"' x2='"+(W-R)+"' y1='"+yo+"' y2='"+yo+"' stroke='#8a8a93' stroke-width='1' stroke-dasharray='4 3'/>";
  g+="<text x='"+(W-R+4)+"' y='"+(yo-4)+"' fill='#e8e8ea' font-size='9'>open</text>";
  g+="<line x1='"+xc+"' x2='"+xc+"' y1='"+T+"' y2='"+(H-B)+"' stroke='#8b8bff' stroke-width='1' stroke-dasharray='2 3'/>";
  g+="<text x='"+(xc+3)+"' y='"+(H-B-4)+"' fill='#8b8bff' font-size='9'>conf</text>";
  if(mode==="c"){
    const cw=PW/5*0.6;
    cd.forEach((c,i)=>{
      if(!c)return;
      const col=c.c>=c.o?"#22c55e":"#ef4444",x=L+PW*(i+0.5)/5;
      const yt=Math.min(Y(c.o),Y(c.c)),hh=Math.max(1.5,Math.abs(Y(c.o)-Y(c.c)));
      g+="<line x1='"+x+"' x2='"+x+"' y1='"+Y(c.h)+"' y2='"+Y(c.l)+"' stroke='"+col+"' stroke-width='1.5'/>";
      g+="<rect x='"+(x-cw/2)+"' y='"+yt+"' width='"+cw+"' height='"+hh+"' fill='"+col+"' rx='1'/>";
    });
  }else{
    const col=last>=open?"#22c55e":"#ef4444";
    let path="";
    ser.forEach((p,i)=>{path+=(i?"L":"M")+X(p[0]).toFixed(1)+" "+Y(p[1]).toFixed(1)+" "});
    g+="<path d='"+path+"' fill='none' stroke='"+col+"' stroke-width='2' stroke-linejoin='round'/>";
    g+="<circle cx='"+X(ser[ser.length-1][0])+"' cy='"+Y(last)+"' r='3.5' fill='"+col+"'/>";
  }
  for(let i=0;i<=5;i++){const x=L+PW*i/5;g+="<text x='"+(x-(i?8:0))+"' y='"+(H-4)+"' fill='#8a8a93' font-size='9'>"+i+":00</text>"}
  box.innerHTML="<svg viewBox='0 0 "+W+" "+H+"' width='100%' style='display:block'>"+g+"</svg>";
}
document.getElementById("ctabs").onclick=function(e){const s=e.target.dataset&&e.target.dataset.s;if(s){window._cs=s;if(window._d)drawChart(window._d)}};
document.getElementById("cmode").onclick=function(e){const s=e.target.dataset&&e.target.dataset.m;if(s){window._cm=s;if(window._d)drawChart(window._d)}};
function render(d){
  drawChart(d);
  const badge=document.getElementById("badge");
  if(d.error){badge.textContent=d.error;badge.className="badge"}
  else{badge.textContent="live · "+(d.seconds_passed||0)+"s";badge.className="badge ok"}
  document.getElementById("tmr").textContent=(d.time_left||0)+"s left · conf@"+d.confirm_at+"s";
  let top="";
  for(const n of S){
    const m=d.markets[n];
    if(!m||m.status==="waiting"){top+=`<div class="ac"><div class="an">${n}</div><div class="ap">—</div></div>`;continue}
    const c=m.lead_bps>=0?"pos":"neg";
    top+=`<div class="ac"><div class="an">${n} · ${m.source||""}</div><div class="ap">$${f(m.current,n==="XRP"?4:2)}</div><div class="ach ${c}">${m.lead_bps>=0?"+":""}${m.lead_bps} bps</div></div>`;
  }
  document.getElementById("top").innerHTML=top;
  let body="";
  for(const n of S){
    const m=d.markets[n];
    if(!m||m.status==="waiting"){body+=`<div class="mrow"><div>${n}</div><div>—</div><div>—</div><div class="bs bw">…</div></div>`;continue}
    let sh=`<span class="bs bw">очікування</span>`;
    if(m.signal_sent&&m.signal){
      const s=m.signal.side;
      const sc=m.signal.score?` ${m.signal.score}/3`:"";
      sh=`<span class="bs ${s==="UP"?"bu":"bd"}">${s==="UP"?"ВГОРУ":"ВНИЗ"} ${(m.signal.confidence*100).toFixed(0)}%${sc}</span>`;
    }else if(d.seconds_passed>=d.confirm_at&&m.live_side){
      sh=`<span class="bs bw">${m.live_side} ~${(m.live_confidence*100).toFixed(0)}%</span>`;
    }
    body+=`<div class="mrow"><div style="font-weight:600">${n}</div><div>${f(m.open,n==="XRP"?4:2)}</div><div>${f(m.current,n==="XRP"?4:2)}</div><div>${sh}</div></div>`;
  }
  document.getElementById("mkt").innerHTML=body;
  const st=d.stats||{};
  if(st.win_rate!=null){
    document.getElementById("stats").innerHTML=`<span>Сигналів: <b>${st.signals||0}</b></span><span>Вирішено: <b>${st.resolved||0}</b></span><span>Win: <b class="pos">${st.wins||0}</b></span><span>Loss: <b class="neg">${st.losses||0}</b></span><span>WR: <b>${st.win_rate}%</b></span>`;
  }
  const h=d.history||[];
  if(h.length){
    let x="";
    for(const e of h){
      const t=e.time?new Date(e.time).toLocaleTimeString():"—";
      const res=e.result==="win"?"✓":e.result==="loss"?"✗":"";
      const rc=e.result==="win"?"pos":e.result==="loss"?"neg":"";
      const sc=e.score?` ${e.score}/3`:"";
      const mv=e.move==null?null:e.move;
      x+=`<div class="hrow"><div>${t}</div><div>${e.symbol||"—"}</div><div style="color:${e.side==="UP"?"var(--up)":"var(--down)"}">${e.side==="UP"?"↑":"↓"}</div><div>${(e.confidence*100).toFixed(0)}%${sc}</div><div class="${mv==null?"":mv>=0?"pos":"neg"}">${mv==null?"…":(mv>=0?"+":"-")+Math.abs(mv).toFixed(e.symbol==='XRP'?4:2)} <span class="${rc}">${res}</span></div></div>`;
    }
    document.getElementById("hist").innerHTML=x;
  }
}
async function refresh(){try{render(await(await fetch("/api/status")).json())}catch(e){document.getElementById("badge").textContent="offline"}}
(function(){
  let sym='BTC',sub='feed',tper='day',timer=null,data=null;
  const esc=x=>String(x).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
  const short=a=>a?a.slice(0,6)+'…'+a.slice(-4):'—';
  const usd=x=>'$'+Math.abs(x).toLocaleString('en-US',{maximumFractionDigits:0});
  const tm=ts=>ts?new Date(ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'}):'';
  function who(w){
    const link='https://polymarket.com/profile/'+esc(w.wallet);
    return "<a href='"+link+"' target='_blank' rel='noopener'>"+(w.name?esc(w.name)+' · ':'')+short(w.wallet)+"</a>";
  }
  function tradeRow(t){
    const c=t.outcome==='Up'?'var(--up)':'var(--down)';
    return "<div class='wrow'>"+who(t)+"<div class='sub'>"+(t.side==='BUY'?'купив':'продав')+" <span style='color:"+c+"'>"+esc(t.outcome)+"</span> · "+usd(t.usd)+" @ "+Math.round(t.price*100)+"¢ · "+tm(t.ts)+"</div></div>";
  }
  function draw(){
    document.querySelectorAll('#wtabs button').forEach(x=>{x.className=x.dataset.s===sym?'on':''});
    document.querySelectorAll('#wsub button').forEach(x=>{x.className=x.dataset.t===sub?'on':''});
    const b=document.getElementById('wbody'),sm=document.getElementById('wsum');
    if(!data){b.innerHTML='Завантаження…';return}
    if(!data.ok){sm.innerHTML='';b.innerHTML=esc(data.error||'Помилка');return}
    const up=data.totals.Up,dn=data.totals.Down,tot=up+dn||1;
    sm.innerHTML="<div style='display:flex;justify-content:space-between;font-size:.85rem'><span style='color:var(--up)'>Up "+usd(up)+"</span><span style='color:var(--down)'>Down "+usd(dn)+"</span></div><div class='vbar'><div style='width:"+(up/tot*100)+"%;background:#22c55e'></div><div style='flex:1;background:#ef4444'></div></div><div style='font-size:.72rem;color:var(--muted)'>Завантажено угод: "+(data.count||0)+" (куплено, у $)</div>";
    let x='';
    if(sub==='top'){
      for(const w of data.wallets){
        x+="<div class='wrow'>"+who(w)+"<div class='sub'>Купив: <span style='color:var(--up)'>Up "+usd(w.bUp)+"</span> · <span style='color:var(--down)'>Down "+usd(w.bDown)+"</span></div><div class='sub'>Продав: <span style='color:var(--up)'>Up "+usd(w.sUp)+"</span> · <span style='color:var(--down)'>Down "+usd(w.sDown)+"</span> · угод: "+w.n+" · "+tm(w.last)+"</div></div>";
      }
    }else{
      const list=data.trades.filter(t=>sub==='feed'||(sub==='buy'&&t.side==='BUY')||(sub==='sell'&&t.side==='SELL')||(sub==='up'&&t.outcome==='Up')||(sub==='down'&&t.outcome==='Down'));
      for(const t of list)x+=tradeRow(t);
    }
    b.innerHTML=x||"<div style='color:var(--muted)'>Поки порожньо</div>";
  }
  async function load(){
    try{
      const r=await fetch('/api/trades?symbol='+sym);
      data=await r.json();
    }catch(e){data={ok:false,error:'Не вдалося завантажити'}}
    draw();
  }
  let tmode='top',tloss=0,ttimer=null;
  async function loadT(){
    clearTimeout(ttimer);
    document.querySelectorAll('#tmode button').forEach(x=>{x.className=x.dataset.m===tmode?'on':''});
    document.querySelectorAll('#lchips button').forEach(x=>{x.className=(+x.dataset.l===tloss)?'on':''});
    document.getElementById('lchips').style.display=tmode==='clean'?'flex':'none';
    document.querySelectorAll('#ttabs button').forEach(x=>{
      x.className=x.dataset.p===tper?'on':'';
      x.style.display=(tmode==='clean'&&x.dataset.p==='month')?'none':'';
    });
    const b=document.getElementById('tbody');
    try{
      if(tmode==='clean'){
        const per=tper==='month'?'week':tper;
        const d=await (await fetch('/api/clean?period='+per)).json();
        const rows=(d.rows||[]).filter(w=>w.losses<=tloss);
        let x='';
        if(d.status==='loading'){x+="<div style='color:var(--muted);margin-bottom:8px'>Рахую… перевірено "+(d.progress||0)+" з "+(d.total||'?')+" гаманців (перший раз до хвилини)</div>"}
        if(d.status==='error'){x+="<div style='color:var(--down);margin-bottom:8px'>"+esc(d.error||'Помилка')+"</div>"}
        for(const w of rows){
          const c=w.pnl>=0?'var(--up)':'var(--down)';
          x+="<div class='wrow'><div style='display:flex;justify-content:space-between;align-items:center'><span>"+who(w)+"</span><b style='color:"+c+"'>"+(w.pnl>=0?'+':'-')+usd(w.pnl)+"</b></div><div class='sub'>Виграшів "+w.wins+" · програшів "+w.losses+" · позицій "+w.n+"</div></div>";
        }
        if(!rows.length&&d.status!=='loading'&&d.status!=='error')x+="<div style='color:var(--muted)'>Немає гаманців з такою кількістю програшів за цей період</div>";
        if(d.status==='ready'||(d.rows&&d.rows.length)){
          x+="<div style='font-size:.7rem;color:var(--muted);margin-top:12px;line-height:1.4'>Період: "+(per==='day'?'останні 24 год':'останні 7 днів')+". Перевіряються топ-"+(d.total||40)+" крипто-гаманців за прибутком; враховані закриті позиції"+(d.scope_all?' (всі крипто-ринки: Polymarket не віддав тип ринку)':' на 5-хв ринках BTC/ETH/SOL/XRP')+". Гаманці з надто великою кількістю угод пропущені, бо їх неможливо перевірити повністю"+(d.incomplete?': '+d.incomplete:'')+". Минулі результати не гарантують майбутніх.</div>";
        }
        b.innerHTML=x;
        if(d.status==='loading')ttimer=setTimeout(loadT,3000);
        return;
      }
      const r=await fetch('/api/top?period='+tper);
      const d=await r.json();
      if(!d.ok){b.innerHTML=esc(d.error||'Помилка');return}
      let x='';
      for(const w of d.rows){
        const c=w.pnl>=0?'var(--up)':'var(--down)';
        x+="<div class='wrow'><div style='display:flex;justify-content:space-between;align-items:center'><span><span style='color:var(--muted)'>#"+w.rank+"</span> "+who(w)+"</span><b style='color:"+c+"'>"+(w.pnl>=0?'+':'-')+usd(w.pnl)+"</b></div></div>";
      }
      b.innerHTML=x||'Поки порожньо';
    }catch(e){b.innerHTML='Не вдалося завантажити'}
  }
  let pinit=false,pen=false,pcoins={},pdepSrv=0;
  const money=x=>(x<0?'-':'')+'$'+Math.abs(x).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  function drawCfg(){
    const e=document.getElementById('pen');e.textContent=pen?'УВІМК':'ВИМК';e.className='pbtn'+(pen?' on':'');
    document.querySelectorAll('#pcoins button').forEach(b=>{b.className=pcoins[b.dataset.c]?'on':''});
  }
  function fillCfg(c){
    pen=c.enabled;pcoins=Object.assign({},c.coins);pdepSrv=c.deposit;
    document.getElementById('pmode').value=c.stake_mode;
    document.getElementById('pval').value=c.stake_value;
    document.getElementById('pdep').value=c.deposit;
    drawCfg();
  }
  function drawAcc(d){
    const a=d.account,c=a.pnl>=0?'var(--up)':'var(--down)';
    document.getElementById('pacc').innerHTML="<div class='pgrid'><div><small>Баланс (вільний)</small><b>"+money(a.balance)+"</b></div><div><small>Разом з відкритими</small><b>"+money(a.equity)+"</b></div><div><small>Прибуток</small><b style='color:"+c+"'>"+(a.pnl>=0?'+':'')+money(a.pnl)+" ("+a.pnl_pct.toFixed(1)+"%)</b></div><div><small>Win rate</small><b>"+a.winrate.toFixed(0)+"% ("+a.wins+"/"+(a.wins+a.losses)+")</b></div></div>";
    const cv=d.curve;
    if(cv.length>1){
      const lo=Math.min(...cv),hi=Math.max(...cv),W=320,H=64,sp=(hi-lo)||1;
      const pts=cv.map((v,i)=>(i/(cv.length-1)*W).toFixed(1)+','+(H-4-(v-lo)/sp*(H-8)).toFixed(1)).join(' ');
      const col=cv[cv.length-1]>=cv[0]?'#22c55e':'#ef4444';
      document.getElementById('pspark').innerHTML="<svg viewBox='0 0 "+W+" "+H+"' width='100%' style='display:block'><polyline points='"+pts+"' fill='none' stroke='"+col+"' stroke-width='2'/></svg>";
    }else document.getElementById('pspark').innerHTML="<div style='font-size:.78rem;color:var(--muted)'>Графік з'явиться після перших закритих угод</div>";
    let x='';
    for(const t of d.trades){
      const sc=t.side==='UP'?'var(--up)':'var(--down)';
      let st;
      if(t.status==='open')st="<span style='color:var(--muted)'>очікує…</span>";
      else if(t.status==='void')st="<span style='color:var(--muted)'>скасовано</span>";
      else if(t.status==='skip')st="<span style='color:var(--muted)'>пропущено</span>";
      else st="<b style='color:"+(t.pnl>=0?'var(--up)':'var(--down)')+"'>"+(t.status==='win'?'✓ ':'✕ ')+(t.pnl>=0?'+':'')+money(t.pnl)+"</b>";
      const tmm=new Date(t.ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
      if(t.status==='skip'){
        x+="<div class='wrow'><div style='display:flex;justify-content:space-between'><span>"+t.symbol+" <span style='color:"+sc+"'>"+t.side+"</span></span>"+st+"</div><div class='sub'>"+new Date(t.ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})+" · "+(t.reason||'')+"</div></div>";
        continue;
      }
      x+="<div class='wrow'><div style='display:flex;justify-content:space-between'><span>"+t.symbol+" <span style='color:"+sc+"'>"+t.side+"</span></span>"+st+"</div><div class='sub'>"+tmm+" · вхід "+Math.round(t.entry*100)+"¢ · ставка "+money(t.cost)+" · комісія "+money(t.fee)+"</div></div>";
    }
    document.getElementById('plist').innerHTML=x||"<div style='color:var(--muted);font-size:.85rem'>Ще немає угод. Бот відкриє віртуальну угоду на наступному сигналі.</div>";
  }
  async function loadP(){
    try{
      const d=await (await fetch('/api/paper')).json();
      if(!pinit){fillCfg(d.config);pinit=true}
      drawAcc(d);
    }catch(e){}
  }
  async function postP(url,body){
    const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    return r.json();
  }
  async function saveCfg(explicit){
    const body={enabled:pen,coins:pcoins};
    if(explicit){
      body.stake_mode=document.getElementById('pmode').value;
      body.stake_value=parseFloat(document.getElementById('pval').value);
      const dep=parseFloat(document.getElementById('pdep').value);
      if(dep>0&&Math.abs(dep-pdepSrv)>1e-9){
        if(!confirm('Депозит змінено на '+money(dep)+'. Скинути рахунок і стерти історію віртуальних угод?')){document.getElementById('pdep').value=pdepSrv;return}
        body.deposit=dep;
      }
    }
    const d=await postP('/api/paper/config',body);
    if(explicit)fillCfg(d.config);else{pdepSrv=d.config.deposit;drawCfg()}
    drawAcc(d);
    if(explicit){const m=document.getElementById('pmsg');m.textContent='Збережено ✓';setTimeout(()=>{m.textContent=''},1800)}
  }
  document.getElementById('pen').onclick=()=>{pen=!pen;drawCfg();saveCfg(false)};
  document.getElementById('pcoins').onclick=e=>{const c=e.target.dataset&&e.target.dataset.c;if(c){pcoins[c]=!pcoins[c];drawCfg();saveCfg(false)}};
  document.getElementById('psave').onclick=()=>saveCfg(true);
  document.getElementById('preset').onclick=async()=>{
    if(!confirm('Скинути віртуальний рахунок і стерти історію угод?'))return;
    const d=await postP('/api/paper/reset',{deposit:parseFloat(document.getElementById('pdep').value)});
    fillCfg(d.config);drawAcc(d);
  };
  function show(v){
    document.getElementById('sheet').classList.toggle('open',v==='wal');
    document.getElementById('vtop').classList.toggle('open',v==='top');
    document.getElementById('vpap').classList.toggle('open',v==='pap');
    document.getElementById('nv1').className=v==='main'?'on':'';
    document.getElementById('nv2').className=v==='wal'?'on':'';
    document.getElementById('nv3').className=v==='top'?'on':'';
    document.getElementById('nv4').className=v==='pap'?'on':'';
    clearInterval(timer);clearTimeout(ttimer);
    if(v==='wal'){data=null;draw();load();timer=setInterval(load,5000)}
    else if(v==='top'){loadT();timer=setInterval(loadT,60000)}
    else if(v==='pap'){loadP();timer=setInterval(loadP,4000)}
    else window.scrollTo(0,0);
  }
  document.getElementById('nv1').onclick=()=>show('main');
  document.getElementById('nv2').onclick=()=>show('wal');
  document.getElementById('nv3').onclick=()=>show('top');
  document.getElementById('nv4').onclick=()=>show('pap');
  document.getElementById('wtabs').onclick=e=>{const s=e.target.dataset&&e.target.dataset.s;if(s){sym=s;data=null;draw();load()}};
  document.getElementById('wsub').onclick=e=>{const t=e.target.dataset&&e.target.dataset.t;if(t){sub=t;draw()}};
  document.getElementById('ttabs').onclick=e=>{const p=e.target.dataset&&e.target.dataset.p;if(p){tper=p;loadT()}};
  document.getElementById('tmode').onclick=e=>{const mm=e.target.dataset&&e.target.dataset.m;if(mm){tmode=mm;if(mm==='clean'&&tper==='month')tper='day';loadT()}};
  document.getElementById('lchips').onclick=e=>{const l=e.target.dataset&&e.target.dataset.l;if(l!==undefined&&l!==''){tloss=+l;loadT()}};
})();
refresh();setInterval(refresh,2000);
</script>
</body>
</html>"""


async def dashboard(request):
    return HTMLResponse(HTML_PAGE)


async def on_startup():
    asyncio.create_task(rtds_loop())
    asyncio.create_task(price_loop())
    logger.info("RTDS + price loops started")


_cid_cache: Dict[str, tuple] = {}
_trades_cache: Dict[str, tuple] = {}
MAX_TRADE_PAGES = 4


async def api_trades(request):
    """Who is trading the current 5m market on Polymarket (public data: wallets, sizes)."""
    sym = (request.query_params.get("symbol") or "BTC").upper()
    if sym not in ("BTC", "ETH", "SOL", "XRP"):
        sym = "BTC"
    now = int(datetime.now(timezone.utc).timestamp())
    start = now - now % WINDOW_SECONDS
    slug = f"{sym.lower()}-updown-5m-{start}"

    cached = _trades_cache.get(slug)
    if cached and now - cached[0] < 4:
        return JSONResponse(cached[1])

    def g(t, *keys, default=None):
        for k in keys:
            if k in t and t[k] is not None:
                return t[k]
        return default

    def to_ts(v):
        if v is None:
            return 0
        try:
            x = int(float(v))
        except Exception:
            try:
                return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
            except Exception:
                return 0
        return x // 1000 if x > 10**11 else x

    def as_list(v):
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except Exception:
                return []
        return v if isinstance(v, list) else []

    raw: list = []
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            meta = _cid_cache.get(slug)
            if not meta:
                r = await c.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
                data = r.json()
                ev = data[0] if isinstance(data, list) and data else None
                mk = (ev or {}).get("markets") or []
                if mk:
                    m0 = mk[0]
                    tokens = as_list(m0.get("clobTokenIds"))
                    outs = as_list(m0.get("outcomes"))
                    tmap = {str(t): str(o) for t, o in zip(tokens, outs)}
                    meta = (m0.get("conditionId"), tmap)
                    if meta[0]:
                        _cid_cache[slug] = meta
            if not meta or not meta[0]:
                return JSONResponse({"ok": False, "error": "Ринок ще не знайдено, спробуй за кілька секунд", "slug": slug})
            cid, tmap = meta

            # first page (try a few param variants), then follow the cursor
            body = None
            last_err = ""
            for params in (
                {"condition": cid, "limit": 500, "taker_only": "false"},
                {"condition": cid, "limit": 200, "taker_only": "false"},
                {"condition": cid, "taker_only": "false"},
                {"condition": cid},
            ):
                r = await c.get("https://data-api.polymarket.com/v2/trades", params=params)
                try:
                    b = r.json()
                except Exception:
                    b = None
                if r.status_code == 200 and b is not None:
                    body = b
                    break
                last_err = f"{r.status_code} {str(b)[:150]}"
                logger.warning(f"data-api v2/trades {params}: {last_err}")
            if body is None:
                return JSONResponse({"ok": False, "error": f"Polymarket: {last_err}", "slug": slug})

            pages = 0
            while True:
                rows = body.get("data") if isinstance(body, dict) else body
                if isinstance(rows, list):
                    raw.extend(rows)
                pages += 1
                pg = (body.get("pagination") or {}) if isinstance(body, dict) else {}
                cur = pg.get("next_cursor")
                if not cur or pages >= MAX_TRADE_PAGES or pg.get("has_more") is False:
                    break
                r = await c.get("https://data-api.polymarket.com/v2/trades", params={"condition": cid, "cursor": cur})
                if r.status_code != 200:
                    break
                try:
                    body = r.json()
                except Exception:
                    break
    except Exception as e:
        logger.warning(f"api_trades error: {e}")
        if not raw:
            return JSONResponse({"ok": False, "error": "Не вдалося отримати дані з Polymarket", "slug": slug})

    trades = []
    wallets: Dict[str, dict] = {}
    totals = {"Up": 0.0, "Down": 0.0}
    seen = set()
    for t in raw:
        if not isinstance(t, dict):
            continue
        try:
            addr = g(t, "proxy_wallet", "proxyWallet", "wallet", default="")
            side = str(g(t, "side", default="")).upper()
            size = float(g(t, "size", default=0))
            price = float(g(t, "price", default=0))
            ts = to_ts(g(t, "timestamp", "created_at", "createdAt"))
        except Exception:
            continue
        outcome = g(t, "outcome", default="")
        if outcome not in ("Up", "Down"):
            tok = str(g(t, "token_id", "asset", "assetId", "asset_id", default=""))
            outcome = tmap.get(tok, outcome)
        if outcome not in ("Up", "Down"):
            idx = g(t, "outcome_index", "outcomeIndex")
            if idx in (0, 1, "0", "1"):
                outcome = "Up" if int(idx) == 0 else "Down"
        txh = g(t, "transaction_hash", "transactionHash", default="")
        key = (txh, addr, side, outcome, size, price, ts) if txh else None
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        usd = size * price
        name = g(t, "name", "pseudonym", default="")
        trades.append({"wallet": addr, "name": name, "side": side, "outcome": outcome,
                       "usd": round(usd, 2), "price": round(price, 3), "ts": ts})
        if outcome in totals and side == "BUY":
            totals[outcome] += usd
        w = wallets.setdefault(addr, {"wallet": addr, "name": name, "bUp": 0.0, "bDown": 0.0,
                                      "sUp": 0.0, "sDown": 0.0, "n": 0, "last": 0})
        if outcome in ("Up", "Down"):
            w[("b" if side == "BUY" else "s") + outcome] += usd
        w["n"] += 1
        w["last"] = max(w["last"], ts)

    trades.sort(key=lambda x: x["ts"], reverse=True)
    top = sorted(wallets.values(), key=lambda w: w["bUp"] + w["bDown"] + w["sUp"] + w["sDown"], reverse=True)[:40]
    for w in top:
        for k in ("bUp", "bDown", "sUp", "sDown"):
            w[k] = round(w[k], 2)

    payload = {
        "ok": True,
        "symbol": sym,
        "slug": slug,
        "window_start": start,
        "count": len(trades),
        "totals": {"Up": round(totals["Up"], 2), "Down": round(totals["Down"], 2)},
        "wallets": top,
        "trades": trades[:500],
    }
    _trades_cache[slug] = (now, payload)
    if len(_trades_cache) > 40:
        for k in sorted(_trades_cache, key=lambda k: _trades_cache[k][0])[:20]:
            _trades_cache.pop(k, None)
    return JSONResponse(payload)


_top_cache: Dict[str, tuple] = {}


async def api_top(request):
    """Best traders by profit (Polymarket leaderboard, crypto category)."""
    period = (request.query_params.get("period") or "day").lower()
    if period not in ("day", "week", "month"):
        period = "day"
    now = int(datetime.now(timezone.utc).timestamp())
    cached = _top_cache.get(period)
    if cached and now - cached[0] < 60:
        return JSONResponse(cached[1])

    def g(t, *keys, default=None):
        for k in keys:
            if k in t and t[k] is not None:
                return t[k]
        return default

    attempts = [
        ("https://data-api.polymarket.com/v2/leaderboard",
         {"time_period": period, "category": "crypto", "sort_by": "PNL", "limit": 25}),
        ("https://data-api.polymarket.com/v1/leaderboard",
         {"timePeriod": period.upper(), "category": "CRYPTO", "orderBy": "PNL", "limit": 25}),
    ]
    rows = None
    last_err = ""
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            for url, params in attempts:
                r = await c.get(url, params=params)
                try:
                    body = r.json()
                except Exception:
                    body = None
                if r.status_code == 200 and body is not None:
                    data = body.get("data") if isinstance(body, dict) else body
                    if isinstance(data, dict):
                        data = next((v for v in data.values() if isinstance(v, list)), [])
                    if isinstance(data, list):
                        rows = data
                        break
                last_err = f"{r.status_code} {str(body)[:150]}"
                logger.warning(f"leaderboard {url}: {last_err}")
    except Exception as e:
        logger.warning(f"api_top error: {e}")
        return JSONResponse({"ok": False, "error": "Не вдалося отримати дані з Polymarket"})
    if rows is None:
        return JSONResponse({"ok": False, "error": f"Polymarket: {last_err}"})

    out = []
    for i, t in enumerate(rows):
        if not isinstance(t, dict):
            continue
        try:
            pnl = float(g(t, "pnl", "profit", default=0))
        except Exception:
            pnl = 0.0
        out.append({
            "rank": g(t, "rank", default=i + 1),
            "wallet": g(t, "proxy_wallet", "proxyWallet", "address", "wallet", default=""),
            "name": g(t, "user_name", "userName", "name", "pseudonym", default=""),
            "pnl": round(pnl, 2),
        })
    payload = {"ok": True, "period": period, "rows": out}
    _top_cache[period] = (now, payload)
    return JSONResponse(payload)


async def _paper_payload():
    trades = paper["trades"]
    closed = [t for t in trades if t["status"] in ("win", "loss")]
    wins = sum(1 for t in closed if t["status"] == "win")
    losses = len(closed) - wins
    open_trades = [t for t in trades if t["status"] == "open"]
    open_cost = sum(t["cost"] + t["fee"] for t in open_trades)
    pnl = sum(t["pnl"] for t in closed)
    curve = [round(paper["deposit"], 2)]
    run = paper["deposit"]
    for t in sorted([t for t in trades if t["status"] in ("win", "loss", "void") and t["settled_at"]],
                    key=lambda t: (t["settled_at"], t["id"])):
        run += t["pnl"]
        curve.append(round(run, 2))
    return {
        "config": {
            "enabled": paper["enabled"],
            "coins": paper["coins"],
            "stake_mode": paper["stake_mode"],
            "stake_value": paper["stake_value"],
            "deposit": paper["deposit"],
        },
        "account": {
            "balance": round(paper["balance"], 2),
            "equity": round(paper["balance"] + open_cost, 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl / paper["deposit"] * 100, 2) if paper["deposit"] else 0.0,
            "wins": wins,
            "losses": losses,
            "open": len(open_trades),
            "winrate": round(wins / len(closed) * 100, 1) if closed else 0.0,
        },
        "curve": curve[-200:],
        "trades": trades[:100],
    }


async def api_paper(request):
    return JSONResponse(await _paper_payload())


async def api_paper_config(request):
    try:
        b = await request.json()
    except Exception:
        b = {}
    if "enabled" in b:
        paper["enabled"] = bool(b["enabled"])
    coins = b.get("coins")
    if isinstance(coins, dict):
        for c in PAPER_COINS:
            if c in coins:
                paper["coins"][c] = bool(coins[c])
    mode = b.get("stake_mode")
    if mode in ("pct", "usd"):
        paper["stake_mode"] = mode
    try:
        v = float(b.get("stake_value"))
        if paper["stake_mode"] == "pct":
            v = max(0.1, min(100.0, v))
        else:
            v = max(1.0, min(1e7, v))
        paper["stake_value"] = round(v, 2)
    except (TypeError, ValueError):
        pass
    if "deposit" in b:
        try:
            dep = float(b["deposit"])
            if abs(dep - paper["deposit"]) > 1e-9:
                _paper_reset(dep)
        except (TypeError, ValueError):
            pass
    paper_save()
    return JSONResponse(await _paper_payload())


def _paper_reset(dep: float):
    dep = max(1.0, min(1e9, dep))
    paper["deposit"] = round(dep, 2)
    paper["balance"] = round(dep, 2)
    paper["trades"] = []
    paper_save()


async def api_paper_reset(request):
    try:
        b = await request.json()
        dep = float(b.get("deposit"))
    except Exception:
        return JSONResponse(await _paper_payload())
    _paper_reset(dep)
    return JSONResponse(await _paper_payload())


# ====================== Wallets without losses ======================
_clean: Dict[str, dict] = {}
CLEAN_CANDIDATES = 40       # how many top-profit crypto wallets we check
CLEAN_MIN_POSITIONS = 5     # ignore wallets with fewer closed positions in the period
CLEAN_MAX_PAGES = 3         # pages per wallet and status (500 rows each); beyond that the data is "incomplete"
CLEAN_TTL = 600
_UPDOWN_RE = re.compile(r"(btc|eth|sol|xrp)-updown-5m-")


def _g(t, *keys, default=None):
    for k in keys:
        if isinstance(t, dict) and k in t and t[k] is not None:
            return t[k]
    return default


def _to_ts(v) -> int:
    if v is None:
        return 0
    try:
        x = int(float(v))
    except Exception:
        try:
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0
    return x // 1000 if x > 10**11 else x


async def _fetch_leaderboard(c: httpx.AsyncClient, period: str, limit: int):
    attempts = [
        ("https://data-api.polymarket.com/v2/leaderboard",
         {"time_period": period, "category": "crypto", "sort_by": "PNL", "limit": limit}),
        ("https://data-api.polymarket.com/v1/leaderboard",
         {"timePeriod": period.upper(), "category": "CRYPTO", "orderBy": "PNL", "limit": limit}),
    ]
    err = ""
    for url, params in attempts:
        try:
            r = await c.get(url, params=params)
            body = r.json()
        except Exception as e:
            err = str(e)[:100]
            continue
        if r.status_code == 200:
            data = body.get("data") if isinstance(body, dict) else body
            if isinstance(data, dict):
                data = next((v for v in data.values() if isinstance(v, list)), [])
            if isinstance(data, list):
                out = []
                for t in data:
                    if not isinstance(t, dict):
                        continue
                    w = _g(t, "proxy_wallet", "proxyWallet", "address", "wallet", default="")
                    if w:
                        out.append({"wallet": w, "name": _g(t, "user_name", "userName", "name", "pseudonym", default="")})
                return out, ""
        err = f"{r.status_code} {str(body)[:120]}"
    return [], err


async def _wallet_window_stats(c: httpx.AsyncClient, wallet: str, since_ts: int):
    """Wins/losses of a wallet on closed 5m crypto positions since since_ts.
    Returns None if data could not be read; 'complete': False if there were too many rows to verify."""
    wins = losses = n = 0
    pnl = 0.0
    complete = True
    scope_all = False
    for status in ("CLOSED", "REDEEMABLE_LOST"):
        cursor = None
        pages = 0
        while True:
            if cursor:
                params = {"user": wallet, "cursor": cursor}
            else:
                params = {"user": wallet, "status": status, "sort_by": "TIMESTAMP",
                          "sort_direction": "DESC", "limit": 500}
            r = await c.get("https://data-api.polymarket.com/v2/positions", params=params)
            if r.status_code != 200:
                return None
            body = r.json()
            rows = body.get("data") if isinstance(body, dict) else body
            if not isinstance(rows, list):
                return None
            oldest = None
            for row in rows:
                ts = _to_ts(_g(row, "last_event_at", "timestamp", "updated_at"))
                if ts:
                    oldest = ts if oldest is None else min(oldest, ts)
                if ts and ts < since_ts:
                    continue
                slug = _g(row, "slug", "market_slug", "event_slug", "eventSlug")
                if slug is not None:
                    if not _UPDOWN_RE.search(str(slug).lower()):
                        continue
                else:
                    scope_all = True
                try:
                    rp = float(_g(row, "realized_pnl", "realizedPnl", default=0) or 0)
                except Exception:
                    rp = 0.0
                n += 1
                pnl += rp
                if status == "REDEEMABLE_LOST" or rp < -0.01:
                    losses += 1
                elif rp > 0.01:
                    wins += 1
            pg = (body.get("pagination") or {}) if isinstance(body, dict) else {}
            nxt = pg.get("next_cursor")
            if not rows or not nxt or (oldest is not None and oldest < since_ts):
                break
            pages += 1
            if pages >= CLEAN_MAX_PAGES:
                complete = False
                break
            cursor = nxt
    return {"wins": wins, "losses": losses, "n": n, "pnl": round(pnl, 2),
            "complete": complete, "scope_all": scope_all}


async def _clean_compute(period: str):
    st = _clean[period]
    st["status"] = "loading"
    st["progress"] = 0
    st["error"] = ""
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - (86400 if period == "day" else 7 * 86400)
    try:
        async with httpx.AsyncClient(timeout=12.0) as c:
            cands, err = await _fetch_leaderboard(c, period, CLEAN_CANDIDATES)
            if not cands:
                st["status"] = "error"
                st["error"] = f"Не вдалося отримати кандидатів: {err}"
                return
            st["total"] = len(cands)
            sem = asyncio.Semaphore(6)
            results = []
            incomplete = 0
            failed = 0
            scope_all = False

            async def one(cd):
                nonlocal incomplete, failed, scope_all
                async with sem:
                    try:
                        res = await _wallet_window_stats(c, cd["wallet"], since)
                    except Exception as e:
                        logger.warning(f"clean stats {cd['wallet'][:8]}: {e}")
                        res = None
                st["progress"] += 1
                if res is None:
                    failed += 1
                    return
                if res["scope_all"]:
                    scope_all = True
                if not res["complete"]:
                    incomplete += 1
                    return
                if res["n"] >= CLEAN_MIN_POSITIONS and res["losses"] <= 3:
                    results.append({"wallet": cd["wallet"], "name": cd["name"], **res})

            await asyncio.gather(*[one(cd) for cd in cands])
            results.sort(key=lambda r: r["pnl"], reverse=True)
            st["rows"] = results
            st["incomplete"] = incomplete
            st["failed"] = failed
            st["scope_all"] = scope_all
            st["ts"] = int(datetime.now(timezone.utc).timestamp())
            if failed == len(cands):
                st["status"] = "error"
                st["error"] = "Polymarket не віддав історію позицій гаманців"
            else:
                st["status"] = "ready"
    except Exception as e:
        logger.warning(f"_clean_compute error: {e}")
        st["status"] = "error"
        st["error"] = "Помилка під час підрахунку"


async def api_clean(request):
    period = (request.query_params.get("period") or "day").lower()
    if period not in ("day", "week"):
        period = "day"
    now = int(datetime.now(timezone.utc).timestamp())
    st = _clean.get(period)
    if st is None:
        st = _clean[period] = {"status": "loading", "ts": 0, "rows": [], "progress": 0, "total": CLEAN_CANDIDATES,
                               "incomplete": 0, "failed": 0, "scope_all": False, "error": ""}
        spawn(_clean_compute(period))
    elif st["status"] != "loading" and now - st["ts"] > CLEAN_TTL:
        spawn(_clean_compute(period))
        st["status"] = "loading"
    return JSONResponse({"ok": True, "period": period, **{k: st.get(k) for k in
                         ("status", "rows", "progress", "total", "incomplete", "failed", "scope_all", "error")}})


class PasswordMiddleware(BaseHTTPMiddleware):
    """Password for the dashboard (browser login window). Off if DASHBOARD_PASSWORD is not set."""
    async def dispatch(self, request, call_next):
        if not DASHBOARD_PASSWORD or request.url.path == "/health":
            return await call_next(request)
        auth = request.headers.get("authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                pwd = decoded.split(":", 1)[1] if ":" in decoded else ""
                if secrets.compare_digest(pwd.encode(), DASHBOARD_PASSWORD.encode()):
                    return await call_next(request)
            except Exception:
                pass
        return Response("Unauthorized", status_code=401,
                        headers={"WWW-Authenticate": 'Basic realm="Signals"'})


app = Starlette(
    routes=[
        Route("/", dashboard),
        Route("/api/status", api_status),
        Route("/api/trades", api_trades),
        Route("/api/top", api_top),
        Route("/api/clean", api_clean),
        Route("/api/paper", api_paper),
        Route("/api/paper/config", api_paper_config, methods=["POST"]),
        Route("/api/paper/reset", api_paper_reset, methods=["POST"]),
        Route("/health", health),
    ],
    on_startup=[on_startup],
    middleware=[Middleware(PasswordMiddleware)],
)
