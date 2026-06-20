import base64
import csv
import io
import json
import math
import os
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

from .storage_service import atomic_write_json, get_data_path
from .futures_kline_provider import (
    KlineProviderError,
    fetch_futures_rest_klines,
    fetch_futures_24hr_tickers,
    futures_kline_provider_health,
)
from .binance_symbol_provider import list_binance_futures_symbols


DEFAULT_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "TON/USDT",
    "DOT/USDT", "LTC/USDT", "BCH/USDT", "TRX/USDT", "NEAR/USDT",
    "APT/USDT", "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT",
    "TIA/USDT", "INJ/USDT", "WLD/USDT", "PEPE/USDT", "SHIB/USDT",
    "FLOKI/USDT", "BONK/USDT", "WIF/USDT", "BOME/USDT", "POPCAT/USDT",
    "ORDI/USDT", "SATS/USDT", "ONDO/USDT", "JUP/USDT", "STRK/USDT",
    "NOT/USDT", "DOGS/USDT", "NEIRO/USDT", "TURBO/USDT", "PNUT/USDT",
    "ACT/USDT", "GOAT/USDT", "MOODENG/USDT", "PENGU/USDT", "VIRTUAL/USDT",
    "AIXBT/USDT", "FARTCOIN/USDT", "TRUMP/USDT", "MELANIA/USDT", "BERA/USDT",
    "KAITO/USDT", "HYPE/USDT", "PUMP/USDT", "WAL/USDT", "LAYER/USDT",
    "PARTI/USDT", "INIT/USDT", "SIGN/USDT", "SOPH/USDT", "HUMA/USDT",
]

BINANCE_STOCK_EXCHANGE_IDS = {
    "binance_stock",
    "binance_stocks",
    "binance_us_stock",
    "binance_us_stocks",
    "binance_equity",
    "binance_equities",
    "binance_rwa",
    "binance_web3_stock",
}

BINANCE_STOCK_SEARCH_SYMBOLS = [
    "AAPL/USDT", "NVDA/USDT", "TSLA/USDT", "MSFT/USDT", "AMZN/USDT",
    "META/USDT", "GOOGL/USDT", "GOOG/USDT", "AVGO/USDT", "AMD/USDT",
    "NFLX/USDT", "PLTR/USDT", "COIN/USDT", "MSTR/USDT", "SMCI/USDT",
    "SPY/USDT", "QQQ/USDT", "DIA/USDT", "IWM/USDT", "EEM/USDT",
]

BINANCE_STOCK_LIST_URL = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/"
    "buw/wallet/market/token/rwa/stock/detail/list/ai"
)
BINANCE_STOCK_DYNAMIC_URL = (
    "https://www.binance.com/bapi/defi/v2/public/wallet-direct/"
    "buw/wallet/market/token/rwa/dynamic/ai"
)
BINANCE_STOCK_KLINE_URL = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/"
    "buw/wallet/dex/market/token/kline/ai"
)
BINANCE_STOCK_SNAPSHOT_FILE = get_data_path("binance_stock_symbols_snapshot.json")

_exchange_cache = {}
_ohlcv_cache = {}
_public_data_cache = {}
_public_data_lock = threading.Lock()
_binance_stock_cache = {"ts": 0.0, "symbols": [], "markets": {}, "lookup": {}}
_binance_stock_lock = threading.Lock()
_live_fetch_lock = threading.Lock()
_last_live_fetch_at = 0.0
_live_error_until = {}
_live_error_count = {}
_ws_kline_cache = {}
_ws_live_kline_cache = {}
_ws_kline_lock = threading.Lock()
_ws_stream_state = {}
_ws_stream_lock = threading.Lock()


class LiveMarketBackoff(RuntimeError):
    pass


class LiveMarketUnavailable(RuntimeError):
    pass


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip() == "1"


def _env_float(name: str, default: float, minimum: float = 0.0, maximum: Optional[float] = None) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    if value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _live_market_enabled() -> bool:
    return os.environ.get("IKUNANCE_LIVE_MARKET", "0").strip() == "1"


def _allow_synthetic_fallback() -> bool:
    return (not _live_market_enabled()) or _env_flag("IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK")


def _allow_binance_spot_kline_fallback() -> bool:
    return os.environ.get("IKUNANCE_ALLOW_BINANCE_SPOT_KLINE_FALLBACK", "1").strip() != "0"


def _allow_binance_stock_seed_fallback() -> bool:
    return os.environ.get("IKUNANCE_ALLOW_BINANCE_STOCK_SEED_FALLBACK", "0").strip() == "1"


def _symbol_sync_network_allowed() -> bool:
    return (
        _live_market_enabled()
        or os.environ.get("IKUNANCE_SYMBOL_SYNC_NETWORK", "0").strip() == "1"
    )


def _ohlcv_cache_ttl_seconds() -> float:
    return _env_float("IKUNANCE_OHLCV_CACHE_TTL", 5.0, minimum=0.0, maximum=60.0)


def _public_data_cache_ttl_seconds(has_rows: bool) -> float:
    name = "IKUNANCE_PUBLIC_DATA_CACHE_TTL" if has_rows else "IKUNANCE_PUBLIC_DATA_EMPTY_CACHE_TTL"
    default = 21600.0 if has_rows else 60.0
    return _env_float(name, default, minimum=0.0, maximum=86400.0)


def _normalize_proxy(proxy: str = "") -> str:
    proxy = str(proxy or "").strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


def _env_market_proxy() -> str:
    return _normalize_proxy(
        os.environ.get("IKUNANCE_MARKET_PROXY")
        or os.environ.get("IKUNANCE_PROXY")
        or ""
    )


def _before_live_fetch(exchange_id: str) -> None:
    global _last_live_fetch_at
    now = time.time()
    backoff_until = _live_error_until.get(exchange_id, 0)
    if now < backoff_until:
        raise LiveMarketBackoff(f"{exchange_id} live market backoff active")

    min_interval = _env_float("IKUNANCE_LIVE_FETCH_MIN_INTERVAL", 0.35, minimum=0.0, maximum=5.0)
    elapsed = now - _last_live_fetch_at
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    _last_live_fetch_at = time.time()


def _record_live_success(exchange_id: str) -> None:
    _live_error_count[exchange_id] = 0
    _live_error_until.pop(exchange_id, None)


def _record_live_error(exchange_id: str) -> None:
    count = _live_error_count.get(exchange_id, 0) + 1
    _live_error_count[exchange_id] = count
    base = _env_float("IKUNANCE_LIVE_FETCH_BACKOFF_BASE", 5.0, minimum=1.0, maximum=60.0)
    max_delay = _env_float("IKUNANCE_LIVE_FETCH_BACKOFF_MAX", 60.0, minimum=5.0, maximum=600.0)
    _live_error_until[exchange_id] = time.time() + min(max_delay, base * (2 ** min(count - 1, 5)))


class FallbackExchange:
    """Deterministic local market adapter for startup checks and tests."""

    def __init__(self, exchange_id: str = "binance"):
        self.id = _canonical_exchange_id(exchange_id)
        if self.id == "binance_stock":
            source_symbols = binance_stock_search_symbols()
            if not source_symbols and _allow_binance_stock_seed_fallback():
                source_symbols = BINANCE_STOCK_SEARCH_SYMBOLS
        else:
            source_symbols = DEFAULT_SYMBOLS
        self.symbols = [f"{symbol}:USDT" for symbol in source_symbols]
        self.markets = {symbol: {"linear": True, "active": True} for symbol in self.symbols}

    def load_markets(self):
        return self.markets

    def fetch_tickers(self, symbols: Optional[Iterable[str]] = None):
        selected = list(symbols) if symbols else self.symbols
        tickers = {}
        for index, symbol in enumerate(selected):
            base = symbol.split("/")[0]
            price = 100 + index * 7 + (sum(ord(ch) for ch in base) % 41)
            tickers[symbol] = {
                "last": float(price),
                "percentage": ((index % 5) - 2) * 2.35,
            }
        return tickers

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 300):
        return _synthetic_ohlcv(symbol, timeframe, limit)


class BinancePublicFuturesExchange:
    """Official Binance public-data bridge used when REST is geo-blocked."""

    id = "binance"

    def __init__(self):
        self.symbols = [f"{symbol}:USDT" for symbol in DEFAULT_SYMBOLS]
        self.markets = {symbol: {"linear": True, "active": True} for symbol in self.symbols}

    def load_markets(self):
        try:
            rows = list_binance_futures_symbols(force_refresh=_live_market_enabled(), structured=True)
        except Exception:
            rows = []
        if rows:
            self.symbols = [f"{row['displayName']}:USDT" for row in rows if row.get("displayName")]
            self.markets = {
                symbol: {"linear": True, "active": True, "source": "fapi_exchangeInfo"}
                for symbol in self.symbols
            }
        return self.markets

    def fetch_tickers(self, symbols: Optional[Iterable[str]] = None):
        self.load_markets()
        selected = list(symbols) if symbols else None
        try:
            tickers = fetch_futures_24hr_tickers(selected)
            if tickers:
                self.symbols = sorted(set(self.symbols) | set(tickers))
                self.markets.update({symbol: {"linear": True, "active": True} for symbol in self.symbols})
                return tickers
        except KlineProviderError:
            pass

        wanted = {_to_binance_symbol(symbol) for symbol in selected or []}
        payload = _binance_ws_json("/market/ws/!ticker@arr", timeout=15)
        if not isinstance(payload, list):
            raise LiveMarketUnavailable("Binance WebSocket ticker payload was not a list")
        tickers = {}
        for row in payload:
            raw = str(row.get("s", ""))
            if not raw.endswith("USDT") or (wanted and raw not in wanted):
                continue
            symbol = _from_binance_symbol(raw)
            try:
                last = float(row.get("c"))
                pct = float(row.get("P"))
            except (TypeError, ValueError):
                continue
            tickers[f"{symbol}:USDT"] = {"last": last, "close": last, "percentage": pct}
        if not tickers:
            raise LiveMarketUnavailable("Binance WebSocket returned no USDT futures tickers")
        self.symbols = sorted(set(self.symbols) | set(tickers))
        self.markets.update({symbol: {"linear": True, "active": True} for symbol in self.symbols})
        return tickers

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 300):
        futures_rest_rows = []
        if _live_market_enabled() or _env_flag("IKUNANCE_FUTURES_REST_BOOTSTRAP"):
            try:
                futures_rest_rows = fetch_futures_rest_klines(symbol, timeframe, limit)
            except KlineProviderError:
                futures_rest_rows = []
        rows = _merge_ohlcv_rows(
            futures_rest_rows
            + _load_binance_public_klines(symbol, timeframe, limit)
            + _get_ws_closed_klines(symbol, timeframe),
            limit,
        )
        if not _has_recent_ohlcv(rows, timeframe):
            try:
                spot_rows = _fetch_binance_spot_data_klines(symbol, timeframe, limit)
            except Exception:
                spot_rows = []
            if spot_rows:
                rows = _merge_ohlcv_rows(rows + spot_rows, limit)
        live = _get_ws_live_kline(symbol, timeframe)
        if live:
            rows = _merge_ohlcv_rows(rows + [live], limit)
        else:
            fetched_live = _fetch_binance_ws_kline(symbol, timeframe)
            if fetched_live:
                rows = _merge_ohlcv_rows(rows + [fetched_live], limit)
                if len(fetched_live) > 6 and fetched_live[6] == "binance_ws_live":
                    _cache_ws_live_kline(symbol, timeframe, fetched_live)
                elif len(fetched_live) > 6 and fetched_live[6] == "binance_ws_closed":
                    _cache_ws_closed_kline(symbol, timeframe, fetched_live)
        rows = _append_live_placeholder_if_needed(rows, timeframe, limit)
        if len(rows) < min(limit, 50):
            raise LiveMarketUnavailable(f"Binance public data returned only {len(rows)} candles for {symbol}")
        if not _has_recent_ohlcv(rows, timeframe):
            raise LiveMarketUnavailable(f"Binance public data has no recent candles for {symbol}")
        return rows[-limit:]


class BinanceStockRwaExchange:
    """Official Binance Web3 tokenized US-stock market adapter."""

    id = "binance_stock"

    def __init__(self):
        self.symbols = binance_stock_search_symbols()
        self.markets = {symbol: {"linear": True, "active": True, "stock": True} for symbol in self.symbols}

    def load_markets(self):
        symbols, markets, _lookup = _load_binance_stock_markets()
        self.symbols = list(symbols)
        self.markets = dict(markets)
        return self.markets

    def fetch_tickers(self, symbols: Optional[Iterable[str]] = None):
        self.load_markets()
        selected = list(symbols) if symbols else BINANCE_STOCK_SEARCH_SYMBOLS
        tickers = {}
        for symbol in selected:
            market = _binance_stock_market_for_symbol(symbol)
            if not market:
                continue
            payload = _fetch_binance_stock_dynamic(market)
            last = _binance_stock_price(payload)
            if last is None:
                continue
            pct = _float_or_default(
                _nested_value(payload, "tokenInfo", "priceChangePct24h"),
                _float_or_default(_nested_value(payload, "stockInfo", "priceChangePct24h"), 0.0),
            )
            normalized = f"{market['ticker']}/USDT"
            tickers[normalized] = {
                "last": last,
                "close": last,
                "percentage": pct,
                "exchange": self.id,
                "tokenSymbol": market.get("tokenSymbol"),
            }
        if not tickers:
            raise LiveMarketUnavailable("Binance stock adapter returned no ticker data")
        return tickers

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 300):
        self.load_markets()
        market = _binance_stock_market_for_symbol(symbol)
        if not market:
            raise LiveMarketUnavailable(f"Binance stock token not found for {symbol}")
        rows = _fetch_binance_stock_klines(market, timeframe, limit)
        if len(rows) < min(limit, 50):
            raise LiveMarketUnavailable(f"Binance stock kline returned only {len(rows)} candles for {symbol}")
        return rows[-limit:]


def _to_binance_symbol(symbol: str) -> str:
    return str(symbol or "").upper().split(":")[0].replace("/", "")


def _from_binance_symbol(symbol: str) -> str:
    raw = str(symbol or "").upper()
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


def _normalize_market_symbol(symbol: str) -> str:
    raw = str(symbol or "").upper().strip().split(":")[0]
    if "/" in raw:
        return raw
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


def _merge_ohlcv_rows(rows, limit: int):
    deduped = {}
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            ts = int(float(row[0]))
            normalized = [
                ts,
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            ]
        except (TypeError, ValueError):
            continue
        if len(row) > 6:
            normalized.append(row[6])
        existing = deduped.get(ts)
        if (
            existing
            and len(existing) > 6
            and existing[6] == "binance_ws_closed"
            and (len(normalized) <= 6 or normalized[6] != "binance_ws_closed")
        ):
            continue
        deduped[ts] = normalized
    return [deduped[ts] for ts in sorted(deduped)][-limit:]


def _cache_ws_closed_kline(symbol: str, timeframe: str, row: List[float]) -> None:
    key = (_normalize_market_symbol(symbol), (timeframe or "15m").lower())
    with _ws_kline_lock:
        rows = _ws_kline_cache.setdefault(key, [])
        rows.append(row)
        _ws_kline_cache[key] = _merge_ohlcv_rows(rows, 500)


def _get_ws_closed_klines(symbol: str, timeframe: str) -> List[List[float]]:
    key = (_normalize_market_symbol(symbol), (timeframe or "15m").lower())
    with _ws_kline_lock:
        return list(_ws_kline_cache.get(key, []))


def _cache_ws_live_kline(symbol: str, timeframe: str, row: List[float]) -> None:
    key = (_normalize_market_symbol(symbol), (timeframe or "15m").lower())
    with _ws_kline_lock:
        _ws_live_kline_cache[key] = {"ts": time.time(), "row": row}


def _get_ws_live_kline(symbol: str, timeframe: str) -> Optional[List[float]]:
    key = (_normalize_market_symbol(symbol), (timeframe or "15m").lower())
    with _ws_kline_lock:
        cached = _ws_live_kline_cache.get(key)
        if not cached:
            return None
        if time.time() - cached.get("ts", 0) > 120:
            return None
        return list(cached.get("row") or [])


def _timeframe_ms(timeframe: str) -> int:
    return int(_timeframe_to_delta(timeframe).total_seconds() * 1000)


def _append_live_placeholder_if_needed(rows: List[List[float]], timeframe: str, limit: int) -> List[List[float]]:
    if not rows:
        return rows
    latest = rows[-1]
    marker = str(latest[6]) if len(latest) > 6 else ""
    if marker in {"binance_ws_live", "binance_ws_live_placeholder", "binance_spot_data_api_live"}:
        return rows[-limit:]
    if not _has_recent_ohlcv(rows, timeframe):
        return rows[-limit:]
    next_ts = int(latest[0]) + _timeframe_ms(timeframe)
    placeholder = [
        next_ts,
        float(latest[4]),
        float(latest[4]),
        float(latest[4]),
        float(latest[4]),
        0.0,
        "binance_ws_live_placeholder",
    ]
    return _merge_ohlcv_rows(rows + [placeholder], limit)


def _has_recent_ohlcv(rows: List[List[float]], timeframe: str) -> bool:
    if not rows:
        return False
    try:
        latest_ts = max(int(float(row[0])) for row in rows if isinstance(row, (list, tuple)) and len(row) >= 6)
    except ValueError:
        return False
    tf_ms = _timeframe_ms(timeframe)
    latest_close_ms = latest_ts + tf_ms
    return (time.time() * 1000) - latest_close_ms <= max(tf_ms * 3, 30 * 60 * 1000)


def _canonical_exchange_id(exchange_id: str) -> str:
    raw = str(exchange_id or "binance").strip().lower()
    return "binance_stock" if raw in BINANCE_STOCK_EXCHANGE_IDS else (raw or "binance")


def is_binance_stock_exchange(exchange_id: str) -> bool:
    return _canonical_exchange_id(exchange_id) == "binance_stock"


def binance_stock_search_symbols() -> List[str]:
    try:
        live_symbols, _markets, _lookup = _load_binance_stock_markets()
        return sorted(set(live_symbols))
    except Exception as exc:
        if _allow_binance_stock_seed_fallback():
            print(f"[WARN] Binance stock symbol sync failed; using explicit seed fallback: {exc}")
            return sorted(set(BINANCE_STOCK_SEARCH_SYMBOLS))
        print(f"[WARN] Binance stock symbol sync failed; stock symbols unavailable: {exc}")
        return []


def _http_json(url: str, timeout: int = 20):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json_loads(response.read().decode("utf-8", "replace"))


def _api_data(payload, context: str):
    if not isinstance(payload, dict):
        raise LiveMarketUnavailable(f"{context} returned non-object payload")
    code = str(payload.get("code") or "")
    if code and code != "000000":
        raise LiveMarketUnavailable(f"{context} returned code={code}")
    return payload.get("data")


def _float_or_none(value) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _float_or_default(value, default: float = 0.0) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _nested_value(data, *keys):
    cursor = data
    for key in keys:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def _binance_stock_maps_from_api_data(data):
    if not isinstance(data, list):
        raise LiveMarketUnavailable("Binance stock list returned no data")

    symbols = []
    markets = {}
    lookup = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        chain_id = str(item.get("chainId") or "").strip()
        contract = str(item.get("contractAddress") or "").strip()
        if not ticker or not chain_id or not contract:
            continue
        symbol = f"{ticker}/USDT"
        if symbol in markets:
            continue
        market = {
            "linear": True,
            "active": True,
            "stock": True,
            "ticker": ticker,
            "symbol": symbol,
            "tokenSymbol": item.get("symbol") or ticker,
            "chainId": chain_id,
            "contractAddress": contract,
            "multiplier": item.get("multiplier"),
            "source": item.get("source") or "binance_stock_rwa",
        }
        symbols.append(symbol)
        markets[symbol] = market
        lookup[ticker] = market

    if not symbols:
        raise LiveMarketUnavailable("Binance stock list contained no symbols")
    return symbols, markets, lookup


def _read_binance_stock_snapshot():
    if not os.path.exists(BINANCE_STOCK_SNAPSHOT_FILE):
        return None
    try:
        with open(BINANCE_STOCK_SNAPSHOT_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    rows = payload.get("markets") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    try:
        symbols, markets, lookup = _binance_stock_maps_from_api_data(rows)
    except LiveMarketUnavailable:
        return None
    return {
        "symbols": symbols,
        "markets": markets,
        "lookup": lookup,
        "updatedAt": float(payload.get("updatedAt") or 0),
        "source": str(payload.get("source") or "snapshot"),
    }


def _write_binance_stock_snapshot(symbols, markets):
    payload = {
        "updatedAt": time.time(),
        "source": "binance_stock_rwa",
        "symbols": list(symbols),
        "markets": list(markets.values()),
    }
    try:
        atomic_write_json(BINANCE_STOCK_SNAPSHOT_FILE, payload, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"[WARN] Binance stock snapshot write failed: {exc}")


def _stock_snapshot_is_fresh(snapshot):
    if not snapshot:
        return False
    stale_seconds = int(os.environ.get("IKUNANCE_STOCK_SYMBOL_STALE_SECONDS", "86400") or "86400")
    return time.time() - float(snapshot.get("updatedAt") or 0) < stale_seconds


def _cache_binance_stock(symbols, markets, lookup, ts=None):
    now = time.time() if ts is None else float(ts or time.time())
    with _binance_stock_lock:
        _binance_stock_cache["symbols"] = list(symbols)
        _binance_stock_cache["markets"] = dict(markets)
        _binance_stock_cache["lookup"] = dict(lookup)
        _binance_stock_cache["ts"] = now


def _load_binance_stock_markets():
    now = time.time()
    with _binance_stock_lock:
        if _binance_stock_cache["symbols"] and now - _binance_stock_cache["ts"] < 1800:
            return (
                list(_binance_stock_cache["symbols"]),
                dict(_binance_stock_cache["markets"]),
                dict(_binance_stock_cache["lookup"]),
            )

    snapshot = _read_binance_stock_snapshot()
    if snapshot and not _symbol_sync_network_allowed() and _stock_snapshot_is_fresh(snapshot):
        _cache_binance_stock(
            snapshot["symbols"],
            snapshot["markets"],
            snapshot["lookup"],
            snapshot.get("updatedAt") or now,
        )
        return list(snapshot["symbols"]), dict(snapshot["markets"]), dict(snapshot["lookup"])

    if not _symbol_sync_network_allowed():
        if snapshot:
            _cache_binance_stock(
                snapshot["symbols"],
                snapshot["markets"],
                snapshot["lookup"],
                snapshot.get("updatedAt") or now,
            )
            return list(snapshot["symbols"]), dict(snapshot["markets"]), dict(snapshot["lookup"])
        if _allow_binance_stock_seed_fallback():
            symbols = list(BINANCE_STOCK_SEARCH_SYMBOLS)
            markets = {
                symbol: {"linear": True, "active": True, "stock": True, "ticker": symbol.split("/")[0], "symbol": symbol}
                for symbol in symbols
            }
            lookup = {symbol.split("/")[0]: market for symbol, market in markets.items()}
            _cache_binance_stock(symbols, markets, lookup, now)
            return symbols, markets, lookup
        raise LiveMarketUnavailable("Binance stock snapshot unavailable and network sync disabled")

    try:
        data = _api_data(_http_json(BINANCE_STOCK_LIST_URL), "Binance stock list")
        symbols, markets, lookup = _binance_stock_maps_from_api_data(data)
        _write_binance_stock_snapshot(symbols, markets)
        _cache_binance_stock(symbols, markets, lookup, now)
        return list(symbols), dict(markets), dict(lookup)
    except Exception as exc:
        if snapshot:
            print(f"[WARN] Binance stock live sync failed; using snapshot: {exc}")
            _cache_binance_stock(
                snapshot["symbols"],
                snapshot["markets"],
                snapshot["lookup"],
                snapshot.get("updatedAt") or now,
            )
            return list(snapshot["symbols"]), dict(snapshot["markets"]), dict(snapshot["lookup"])
        raise


def _binance_stock_market_for_symbol(symbol: str):
    _symbols, _markets, lookup = _load_binance_stock_markets()
    raw = str(symbol or "").upper().split(":")[0].strip()
    ticker = raw.split("/")[0] if "/" in raw else raw
    if "/" not in raw and ticker.endswith("USDT"):
        ticker = ticker[:-4]
    return lookup.get(ticker)


def _binance_stock_query_url(base_url: str, market: Dict, **params) -> str:
    query = {
        "chainId": market["chainId"],
        "contractAddress": market["contractAddress"],
        **params,
    }
    return base_url + "?" + "&".join(f"{key}={value}" for key, value in query.items())


def _fetch_binance_stock_dynamic(market: Dict):
    url = _binance_stock_query_url(BINANCE_STOCK_DYNAMIC_URL, market)
    data = _api_data(_http_json(url), "Binance stock dynamic")
    if not isinstance(data, dict):
        raise LiveMarketUnavailable("Binance stock dynamic returned no data")
    return data


def _binance_stock_price(dynamic_payload: Dict) -> Optional[float]:
    stock_price = _float_or_none(_nested_value(dynamic_payload, "stockInfo", "price"))
    if stock_price is not None:
        return stock_price
    token_price = _float_or_none(_nested_value(dynamic_payload, "tokenInfo", "price"))
    multiplier = _float_or_none(_nested_value(dynamic_payload, "tokenInfo", "sharesMultiplier"))
    if token_price is None:
        return None
    if multiplier and multiplier > 0:
        return token_price / multiplier
    return token_price


def _fetch_binance_stock_klines(market: Dict, timeframe: str, limit: int):
    requested_limit = max(50, min(int(limit or 300), 500))
    url = _binance_stock_query_url(
        BINANCE_STOCK_KLINE_URL,
        market,
        interval=timeframe,
        limit=requested_limit,
    )
    data = _api_data(_http_json(url), "Binance stock kline")
    rows = data.get("klineInfos") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise LiveMarketUnavailable("Binance stock kline returned no rows")
    parsed = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        try:
            parsed.append([
                int(float(row[0])),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]) if len(row) > 5 else 0.0,
            ])
        except (TypeError, ValueError):
            continue
    parsed.sort(key=lambda item: item[0])
    return parsed[-requested_limit:]


def _timeframe_to_delta(timeframe: str) -> timedelta:
    return {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }.get(timeframe, timedelta(minutes=15))


def _binance_public_data_url(symbol: str, timeframe: str, day: datetime) -> str:
    raw = _to_binance_symbol(symbol)
    date_text = day.strftime("%Y-%m-%d")
    return (
        "https://data.binance.vision/data/futures/um/daily/klines/"
        f"{raw}/{timeframe}/{raw}-{timeframe}-{date_text}.zip"
    )


def _load_binance_public_day(symbol: str, timeframe: str, day: datetime) -> List[List[float]]:
    raw = _to_binance_symbol(symbol)
    cache_key = ("binance_public_day", raw, timeframe, day.strftime("%Y-%m-%d"))
    now = time.time()
    with _public_data_lock:
        cached = _public_data_cache.get(cache_key)
        if isinstance(cached, dict):
            rows = cached.get("rows", [])
            ttl = _public_data_cache_ttl_seconds(bool(rows))
            if now - float(cached.get("ts", 0)) < ttl:
                return list(rows)
        elif cached is not None:
            return list(cached)

    url = _binance_public_data_url(symbol, timeframe, day)
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            blob = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            rows = []
        else:
            raise
    else:
        rows = []
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            name = archive.namelist()[0]
            text = io.TextIOWrapper(archive.open(name), encoding="utf-8")
            for item in csv.reader(text):
                if not item or item[0] == "open_time":
                    continue
                try:
                    rows.append([
                        int(float(item[0])),
                        float(item[1]),
                        float(item[2]),
                        float(item[3]),
                        float(item[4]),
                        float(item[5]),
                    ])
                except (IndexError, TypeError, ValueError):
                    continue

    with _public_data_lock:
        _public_data_cache[cache_key] = {"ts": now, "rows": list(rows)}
    return rows


def _load_binance_public_klines(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    step = _timeframe_to_delta(timeframe)
    days_needed = max(2, min(14, int((limit * step.total_seconds()) // 86400) + 3))
    today = datetime.now(timezone.utc).date()
    rows = []
    for offset in range(days_needed):
        day = datetime.combine(today - timedelta(days=offset), datetime.min.time(), tzinfo=timezone.utc)
        rows.extend(_load_binance_public_day(symbol, timeframe, day))
        if len(rows) >= limit:
            break
    rows.sort(key=lambda row: row[0])
    deduped = []
    seen = set()
    for row in rows:
        if row[0] in seen:
            continue
        seen.add(row[0])
        deduped.append(row)
    return deduped[-limit:]


def _fetch_binance_spot_data_klines(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    if not _allow_binance_spot_kline_fallback():
        return []
    raw = _to_binance_symbol(symbol)
    if not raw.endswith("USDT"):
        return []
    capped_limit = max(2, min(int(limit or 80), 1000))
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={raw}&interval={timeframe}&limit={capped_limit}"
    payload = _http_json(url, timeout=10)
    if not isinstance(payload, list):
        return []
    now_ms = int(time.time() * 1000)
    rows = []
    for item in payload:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        try:
            open_time = int(item[0])
            close_time = int(item[6]) if len(item) > 6 else open_time + _timeframe_ms(timeframe)
            marker = "binance_spot_data_api_live" if close_time > now_ms else "binance_spot_data_api"
            rows.append([
                open_time,
                float(item[1]),
                float(item[2]),
                float(item[3]),
                float(item[4]),
                float(item[5]),
                marker,
            ])
        except (TypeError, ValueError):
            continue
    return _merge_ohlcv_rows(rows, capped_limit)


def _read_ws_frame(sock):
    header = sock.recv(2)
    if len(header) < 2:
        raise LiveMarketUnavailable("Binance WebSocket frame header was incomplete")
    b1, b2 = header
    opcode = b1 & 0x0F
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", sock.recv(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", sock.recv(8))[0]
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk:
            break
        payload += chunk
    return opcode, payload


def _masked_ws_frame(opcode: int, payload: bytes = b"") -> bytes:
    mask = os.urandom(4)
    first = 0x80 | (opcode & 0x0F)
    length = len(payload)
    if length < 126:
        header = bytes([first, 0x80 | length])
    elif length < 65536:
        header = bytes([first, 0x80 | 126]) + struct.pack("!H", length)
    else:
        header = bytes([first, 0x80 | 127]) + struct.pack("!Q", length)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return header + mask + masked


def _send_ws_pong(sock, payload: bytes = b"") -> None:
    sock.sendall(_masked_ws_frame(0xA, payload))


def _binance_ws_connect(path: str, timeout: int = 12):
    host = "fstream.binance.com"
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    raw = socket.create_connection((host, 443), timeout=timeout)
    sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
    sock.settimeout(timeout)
    sock.sendall(request.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(1)
        if len(response) > 4096:
            sock.close()
            raise LiveMarketUnavailable("Binance WebSocket handshake was too large")
    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        sock.close()
        raise LiveMarketUnavailable(response[:300].decode("utf-8", "replace"))
    return sock


def _binance_ws_json(path: str, timeout: int = 12):
    sock = _binance_ws_connect(path, timeout=timeout)
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            opcode, payload = _read_ws_frame(sock)
            if opcode == 1:
                return json_loads(payload.decode("utf-8", "replace"))
            if opcode == 9:
                _send_ws_pong(sock, payload)
            if opcode == 8:
                raise LiveMarketUnavailable("Binance WebSocket closed before data frame")
        raise LiveMarketUnavailable("Binance WebSocket timed out waiting for data")
    finally:
        try:
            sock.close()
        except OSError:
            pass


def json_loads(text: str):
    import json
    return json.loads(text)


def _fetch_binance_ws_kline(symbol: str, timeframe: str) -> Optional[List[float]]:
    raw = _to_binance_symbol(symbol).lower()
    payload = _binance_ws_json(f"/market/ws/{raw}@kline_{timeframe}", timeout=15)
    candle = payload.get("k") if isinstance(payload, dict) else None
    if not isinstance(candle, dict):
        return None
    marker = "binance_ws_closed" if candle.get("x") else "binance_ws_live"
    return [
        int(candle["t"]),
        float(candle["o"]),
        float(candle["h"]),
        float(candle["l"]),
        float(candle["c"]),
        float(candle["v"]),
        marker,
    ]


def _stream_generation_active(stream_key: str, generation: int) -> bool:
    with _ws_stream_lock:
        state = _ws_stream_state.get(stream_key)
        return bool(state and state.get("generation") == generation)


def _update_stream_state(stream_key: str, generation: int, **updates) -> None:
    with _ws_stream_lock:
        state = _ws_stream_state.get(stream_key)
        if not state or state.get("generation") != generation:
            return
        state.update(updates)


def _row_from_ws_kline(candle: Dict, marker: str) -> Optional[List[float]]:
    try:
        return [
            int(candle["t"]),
            float(candle["o"]),
            float(candle["h"]),
            float(candle["l"]),
            float(candle["c"]),
            float(candle["v"]),
            marker,
        ]
    except (KeyError, TypeError, ValueError):
        return None


def _handle_stream_kline(payload: Dict, timeframe: str) -> bool:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        data = payload
    candle = data.get("k") if isinstance(data, dict) else None
    if not isinstance(candle, dict):
        return False
    symbol = _from_binance_symbol(candle.get("s") or data.get("s"))
    marker = "binance_ws_closed" if candle.get("x") else "binance_ws_live"
    row = _row_from_ws_kline(candle, marker)
    if not row:
        return False
    if marker == "binance_ws_closed":
        _cache_ws_closed_kline(symbol, timeframe, row)
    else:
        _cache_ws_live_kline(symbol, timeframe, row)
    return True


def _stream_state_key(symbol: str, timeframe: str) -> str:
    return f"{(timeframe or '15m').lower()}|{_normalize_market_symbol(symbol)}"


def _run_binance_kline_stream(timeframe: str, symbols: List[str], generation: int, stream_key: str) -> None:
    symbol = sorted(set(symbols))[0] if symbols else ""
    if not symbol:
        return
    path = f"/market/ws/{_to_binance_symbol(symbol).lower()}@kline_{timeframe}"
    backoff = 1.0
    while _stream_generation_active(stream_key, generation):
        sock = None
        try:
            sock = _binance_ws_connect(path, timeout=20)
            sock.settimeout(30)
            backoff = 1.0
            _update_stream_state(
                stream_key,
                generation,
                connected=True,
                lastError=None,
                lastConnectedAt=time.time(),
            )
            while _stream_generation_active(stream_key, generation):
                opcode, payload = _read_ws_frame(sock)
                if opcode == 1:
                    message = json_loads(payload.decode("utf-8", "replace"))
                    if _handle_stream_kline(message, timeframe):
                        _update_stream_state(stream_key, generation, lastMessageAt=time.time())
                elif opcode == 9:
                    _send_ws_pong(sock, payload)
                elif opcode == 8:
                    break
        except Exception as exc:
            _update_stream_state(
                stream_key,
                generation,
                connected=False,
                lastError=f"{type(exc).__name__}: {exc}",
            )
            if _stream_generation_active(stream_key, generation):
                time.sleep(backoff)
                backoff = min(30.0, backoff * 2)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def ensure_binance_kline_streams(symbol_timeframes: Iterable) -> None:
    grouped = {}
    for item in symbol_timeframes or []:
        try:
            symbol, timeframe = item
        except (TypeError, ValueError):
            continue
        normalized = _normalize_market_symbol(symbol)
        if not normalized.endswith("/USDT"):
            continue
        tf = str(timeframe or "15m").lower()
        grouped.setdefault(tf, set()).add(normalized)

    with _ws_stream_lock:
        for timeframe, new_symbols in grouped.items():
            for symbol in sorted(new_symbols):
                stream_key = _stream_state_key(symbol, timeframe)
                current = _ws_stream_state.get(stream_key) or {}
                thread = current.get("thread")
                if thread and thread.is_alive():
                    continue
                generation = int(current.get("generation") or 0) + 1
                worker = threading.Thread(
                    target=_run_binance_kline_stream,
                    args=(timeframe, [symbol], generation, stream_key),
                    name=f"binance_kline_{timeframe}_{_to_binance_symbol(symbol).lower()}",
                    daemon=True,
                )
                _ws_stream_state[stream_key] = {
                    "symbols": [symbol],
                    "timeframe": timeframe,
                    "generation": generation,
                    "thread": worker,
                    "connected": False,
                    "lastError": None,
                    "lastConnectedAt": None,
                    "lastMessageAt": None,
                    "startedAt": time.time(),
                }
                worker.start()


def binance_kline_stream_status() -> Dict:
    with _ws_stream_lock:
        streams = {
            timeframe: {
                "symbols": list(state.get("symbols") or []),
                "generation": state.get("generation"),
                "connected": bool(state.get("connected")),
                "lastError": state.get("lastError"),
                "lastConnectedAt": state.get("lastConnectedAt"),
                "lastMessageAt": state.get("lastMessageAt"),
                "alive": bool(state.get("thread") and state["thread"].is_alive()),
            }
            for timeframe, state in _ws_stream_state.items()
        }
    with _ws_kline_lock:
        closed = {f"{symbol}|{timeframe}": len(rows) for (symbol, timeframe), rows in _ws_kline_cache.items()}
        live = {f"{symbol}|{timeframe}": int(bool(data.get("row"))) for (symbol, timeframe), data in _ws_live_kline_cache.items()}
    return {"streams": streams, "closedCache": closed, "liveCache": live}


def _binance_public_bridge(exchange_id: str):
    return BinancePublicFuturesExchange() if _canonical_exchange_id(exchange_id) == "binance" else None


def _binance_provider_mode() -> str:
    return os.environ.get("IKUNANCE_BINANCE_PROVIDER", "official").strip().lower() or "official"


def _make_ccxt_exchange(exchange_id: str, proxy: str = ""):
    exchange_id = _canonical_exchange_id(exchange_id)
    if exchange_id == "binance_stock":
        return BinanceStockRwaExchange()
    try:
        import ccxt
    except Exception:
        return None

    exchange_cls = getattr(ccxt, exchange_id, None)
    if exchange_cls is None:
        return None

    config = {"enableRateLimit": True, "options": {"defaultType": "swap"}}
    proxy = _normalize_proxy(proxy)
    if proxy:
        config["proxies"] = {"http": proxy, "https": proxy}
    return exchange_cls(config)


def get_exchange(ud: Optional[Dict] = None, manual_proxy: str = ""):
    ud = ud or {}
    exchange_id = _canonical_exchange_id(ud.get("exchange_id", "binance"))
    proxy = _normalize_proxy(manual_proxy or ud.get("proxy", "") or _env_market_proxy())
    live_market = _live_market_enabled()
    provider_mode = _binance_provider_mode() if exchange_id == "binance" else "native"
    cache_key = (exchange_id, proxy, live_market, provider_mode)
    if cache_key not in _exchange_cache:
        if live_market and exchange_id == "binance" and provider_mode != "ccxt":
            live_exchange = BinancePublicFuturesExchange()
        else:
            live_exchange = _make_ccxt_exchange(exchange_id, proxy) if live_market else None
        if live_exchange is None:
            if _allow_synthetic_fallback():
                live_exchange = FallbackExchange(exchange_id)
            else:
                raise LiveMarketUnavailable(f"{exchange_id} live market adapter unavailable")
        _exchange_cache[cache_key] = live_exchange
    return _exchange_cache[cache_key]


def fetch_ohlcv_cached(symbol: str, timeframe: str, ud: Optional[Dict] = None, manual_proxy: str = "", limit: int = 300):
    exchange_id = _canonical_exchange_id((ud or {}).get("exchange_id", "binance"))
    cache_key = (exchange_id, symbol, timeframe, limit)
    now = time.time()
    cached = _ohlcv_cache.get(cache_key)
    if cached and now - cached["ts"] < _ohlcv_cache_ttl_seconds():
        return cached["data"]

    exchange = get_exchange(ud=ud, manual_proxy=manual_proxy)
    try:
        if _live_market_enabled():
            with _live_fetch_lock:
                _before_live_fetch(exchange_id)
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                _record_live_success(exchange_id)
        else:
            data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as exc:
        if _live_market_enabled() and not isinstance(exc, LiveMarketBackoff):
            _record_live_error(exchange_id)
        bridge = _binance_public_bridge(exchange_id)
        if _live_market_enabled() and bridge is not None:
            data = bridge.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            _ohlcv_cache[cache_key] = {"ts": now, "data": data}
            return data
        if not _allow_synthetic_fallback():
            raise LiveMarketUnavailable(f"{exchange_id} live OHLCV fetch failed: {type(exc).__name__}: {exc}") from exc
        data = _synthetic_ohlcv(symbol, timeframe, limit)
    _ohlcv_cache[cache_key] = {"ts": now, "data": data}
    return data


def load_linear_usdt_symbols(exchange_id: str = "binance", ud: Optional[Dict] = None) -> List[str]:
    user_data = dict(ud or {})
    user_data["exchange_id"] = _canonical_exchange_id(exchange_id or user_data.get("exchange_id", "binance"))
    exchange = get_exchange(ud=user_data)
    try:
        if _live_market_enabled():
            with _live_fetch_lock:
                _before_live_fetch(user_data["exchange_id"])
                exchange.load_markets()
                _record_live_success(user_data["exchange_id"])
        else:
            exchange.load_markets()
        raw = [
            symbol for symbol in getattr(exchange, "symbols", [])
            if "/USDT" in symbol and getattr(exchange, "markets", {}).get(symbol, {}).get("linear")
        ]
    except Exception as exc:
        if _live_market_enabled() and not isinstance(exc, LiveMarketBackoff):
            _record_live_error(user_data["exchange_id"])
        bridge = _binance_public_bridge(user_data["exchange_id"])
        if _live_market_enabled() and bridge is not None:
            bridge.load_markets()
            return [symbol.split(":")[0] for symbol in bridge.symbols]
        if not _allow_synthetic_fallback():
            raise LiveMarketUnavailable(f"{user_data['exchange_id']} live symbol load failed: {type(exc).__name__}: {exc}") from exc
        raw = FallbackExchange(user_data["exchange_id"]).symbols
    return [symbol.split(":")[0] for symbol in raw]


def fetch_tickers_safe(exchange_id: str = "binance", ud: Optional[Dict] = None, symbols: Optional[Iterable[str]] = None):
    user_data = dict(ud or {})
    user_data["exchange_id"] = _canonical_exchange_id(exchange_id or user_data.get("exchange_id", "binance"))
    exchange = get_exchange(ud=user_data)
    try:
        if _live_market_enabled():
            with _live_fetch_lock:
                _before_live_fetch(user_data["exchange_id"])
                data = exchange.fetch_tickers(symbols)
                _record_live_success(user_data["exchange_id"])
                return data
        return exchange.fetch_tickers(symbols)
    except Exception as exc:
        if _live_market_enabled() and not isinstance(exc, LiveMarketBackoff):
            _record_live_error(user_data["exchange_id"])
        bridge = _binance_public_bridge(user_data["exchange_id"])
        if _live_market_enabled() and bridge is not None:
            return bridge.fetch_tickers(symbols)
        if not _allow_synthetic_fallback():
            raise LiveMarketUnavailable(f"{user_data['exchange_id']} live ticker fetch failed: {type(exc).__name__}: {exc}") from exc
        return FallbackExchange(user_data["exchange_id"]).fetch_tickers(symbols)


def clear_ohlcv_cache():
    _ohlcv_cache.clear()


def _synthetic_ohlcv(symbol: str, timeframe: str, limit: int) -> List[List[float]]:
    step_ms = {
        "1m": 60_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }.get(timeframe, 900_000)
    seed = sum(ord(ch) for ch in symbol)
    base = 80 + seed % 120
    start = int(time.time() * 1000) - limit * step_ms
    rows = []
    for index in range(limit):
        drift = index * 0.035
        wave = math.sin((index + seed % 17) / 8) * 2.4
        close = base + drift + wave
        open_ = close - math.sin((index + seed % 11) / 5) * 0.9
        high = max(open_, close) + 0.8
        low = min(open_, close) - 0.8
        volume = 1000 + (index % 21) * 31
        rows.append([start + index * step_ms, open_, high, low, close, volume])
    return rows
