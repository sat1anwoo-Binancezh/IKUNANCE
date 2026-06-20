import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request

from .storage_service import atomic_write_json, get_data_path


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
SNAPSHOT_FILE = get_data_path("binance_futures_symbols_snapshot.json")

SEED_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "TON/USDT",
    "DOT/USDT", "LTC/USDT", "BCH/USDT", "TRX/USDT", "NEAR/USDT",
    "APT/USDT", "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT",
    "TIA/USDT", "INJ/USDT", "WLD/USDT", "PEPE/USDT", "SHIB/USDT",
    "FLOKI/USDT", "BONK/USDT", "WIF/USDT", "BOME/USDT", "POPCAT/USDT",
    "ORDI/USDT", "SATS/USDT", "ONDO/USDT", "JUP/USDT", "STRK/USDT",
    "NOT/USDT", "DOGS/USDT", "NEIRO/USDT", "TURBO/USDT", "PNUT/USDT",
    "ACT/USDT", "GOAT/USDT", "MOODENG/USDT", "PENGU/USDT", "VIRTUAL/USDT",
    "AIXBT/USDT", "FARTCOIN/USDT", "TRUMP/USDT", "MELANIA/USDT",
    "BERA/USDT", "KAITO/USDT", "HYPE/USDT", "PUMP/USDT", "WAL/USDT",
    "LAYER/USDT", "PARTI/USDT", "INIT/USDT", "SIGN/USDT", "SOPH/USDT",
    "HUMA/USDT",
]


@dataclass(frozen=True)
class SymbolSnapshot:
    symbols: List[Dict]
    updated_at: float
    source: str
    error_code: Optional[str] = None
    error: Optional[str] = None


class SymbolProviderError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


_lock = threading.Lock()
_cache: Optional[SymbolSnapshot] = None


def _normalize_proxy(proxy: str = "") -> str:
    proxy = str(proxy or "").strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


def _market_proxy() -> str:
    return _normalize_proxy(
        os.environ.get("IKUNANCE_MARKET_PROXY")
        or os.environ.get("IKUNANCE_PROXY")
        or ""
    )


def _network_refresh_allowed(force: bool) -> bool:
    return (
        force
        or os.environ.get("IKUNANCE_LIVE_MARKET", "0").strip() == "1"
        or os.environ.get("IKUNANCE_SYMBOL_SYNC_NETWORK", "0").strip() == "1"
    )


def _display_symbol(raw: str) -> str:
    text = str(raw or "").upper().strip()
    if "/" in text:
        return text
    if text.endswith("USDT"):
        return f"{text[:-4]}/USDT"
    return f"{text}/USDT" if text else ""


def _compact_symbol(display: str) -> str:
    return str(display or "").upper().replace("/", "")


def _seed_records() -> List[Dict]:
    rows = []
    for display in SEED_SYMBOLS:
        raw = _compact_symbol(display)
        rows.append({
            "symbol": raw,
            "baseAsset": display.split("/")[0],
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "displayName": display,
            "source": "seed",
        })
    return rows


def _dedupe(records: List[Dict]) -> List[Dict]:
    seen = set()
    result = []
    for item in records or []:
        display = _display_symbol(item.get("displayName") or item.get("symbol"))
        raw = _compact_symbol(display)
        if not raw or raw in seen or not raw.endswith("USDT"):
            continue
        seen.add(raw)
        base = item.get("baseAsset") or display.split("/")[0]
        result.append({
            "symbol": raw,
            "baseAsset": str(base).upper(),
            "quoteAsset": str(item.get("quoteAsset") or "USDT").upper(),
            "contractType": str(item.get("contractType") or "PERPETUAL").upper(),
            "status": str(item.get("status") or "TRADING").upper(),
            "displayName": display,
            "source": item.get("source") or "snapshot",
        })
    return sorted(result, key=lambda row: row["symbol"])


def _read_snapshot_file() -> Optional[SymbolSnapshot]:
    if not os.path.exists(SNAPSHOT_FILE):
        return None
    try:
        with open(SNAPSHOT_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    rows = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    return SymbolSnapshot(
        symbols=_dedupe(rows + _seed_records()),
        updated_at=float(payload.get("updatedAt") or 0),
        source=str(payload.get("source") or "snapshot"),
        error_code=payload.get("errorCode"),
        error=payload.get("error"),
    )


def _write_snapshot(snapshot: SymbolSnapshot) -> None:
    payload = {
        "updatedAt": snapshot.updated_at,
        "source": snapshot.source,
        "symbols": snapshot.symbols,
        "errorCode": snapshot.error_code,
        "error": snapshot.error,
    }
    atomic_write_json(SNAPSHOT_FILE, payload, ensure_ascii=False, indent=2)


def _parse_exchange_info(payload: Dict) -> List[Dict]:
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list):
        raise SymbolProviderError("BINANCE_SYMBOL_PAYLOAD", "exchangeInfo payload has no symbols list")

    rows = []
    for item in symbols:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        raw = str(item.get("symbol") or "").upper()
        display = _display_symbol(raw)
        rows.append({
            "symbol": raw,
            "baseAsset": item.get("baseAsset") or display.split("/")[0],
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "displayName": display,
            "source": "fapi_exchangeInfo",
        })
    if not rows:
        raise SymbolProviderError("BINANCE_SYMBOL_EMPTY", "exchangeInfo contained no trading USDT perpetuals")
    return _dedupe(rows)


def _fetch_exchange_info(timeout: int = 10) -> List[Dict]:
    try:
        status_code, payload, text = _http_get_json(EXCHANGE_INFO_URL, timeout=timeout)
    except (urllib.error.URLError, OSError) as exc:
        raise SymbolProviderError("BINANCE_SYMBOL_NETWORK", str(exc)) from exc
    if status_code == 451:
        raise SymbolProviderError(
            "BINANCE_SYMBOL_HTTP_451",
            "Binance Futures exchangeInfo is unavailable from this server exit IP",
            status_code=451,
        )
    if status_code >= 400:
        raise SymbolProviderError(
            f"BINANCE_SYMBOL_HTTP_{status_code}",
            text[:300],
            status_code=status_code,
        )
    return _parse_exchange_info(payload)


def _http_get_json(url: str, *, timeout: int = 10):
    proxy = _market_proxy()
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy}) if proxy else urllib.request.ProxyHandler({})
    )
    request = urllib.request.Request(url, headers={"User-Agent": "IKUNANCE/alpha"})
    try:
        with opener.open(request, timeout=timeout) as response:
            text = response.read().decode("utf-8", "replace")
            return response.getcode(), json.loads(text), text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError:
            payload = {}
        return exc.code, payload, text
    except json.JSONDecodeError as exc:
        raise SymbolProviderError("BINANCE_SYMBOL_JSON", "exchangeInfo returned invalid JSON") from exc


def refresh_binance_futures_symbols(*, force: bool = False) -> SymbolSnapshot:
    global _cache
    now = time.time()
    ttl = max(60, int(os.environ.get("IKUNANCE_SYMBOL_SNAPSHOT_TTL", "3600") or "3600"))
    with _lock:
        if _cache and not force and now - _cache.updated_at < ttl:
            return _cache

    if _network_refresh_allowed(force):
        try:
            rows = _fetch_exchange_info()
            snapshot = SymbolSnapshot(symbols=_dedupe(rows + _seed_records()), updated_at=now, source="fapi_exchangeInfo")
            _write_snapshot(snapshot)
            with _lock:
                _cache = snapshot
            return snapshot
        except SymbolProviderError as exc:
            fallback = _read_snapshot_file()
            rows = fallback.symbols if fallback else _seed_records()
            snapshot = SymbolSnapshot(
                symbols=_dedupe(rows),
                updated_at=fallback.updated_at if fallback else now,
                source="snapshot_fallback" if fallback else "seed_fallback",
                error_code=exc.code,
                error=str(exc),
            )
            with _lock:
                _cache = snapshot
            return snapshot

    fallback = _read_snapshot_file()
    snapshot = fallback or SymbolSnapshot(symbols=_dedupe(_seed_records()), updated_at=now, source="seed")
    with _lock:
        _cache = snapshot
    return snapshot


def list_binance_futures_symbols(*, force_refresh: bool = False, structured: bool = False):
    snapshot = refresh_binance_futures_symbols(force=force_refresh)
    if structured:
        return list(snapshot.symbols)
    return [row["displayName"] for row in snapshot.symbols]


def search_binance_futures_symbols(query: str, *, limit: int = 30, force_refresh: bool = False) -> Dict:
    base_query = str(query or "").upper().replace("/USDT", "").replace("USDT", "").replace(".P", "").strip()
    if len(base_query) < 2:
        return {"data": [], "records": [], "query": base_query, "source": "empty", "liveChecked": False}

    snapshot = refresh_binance_futures_symbols(force=force_refresh)
    exact, starts, contains = [], [], []
    for item in snapshot.symbols:
        base = item["baseAsset"]
        raw = item["symbol"]
        display = item["displayName"]
        haystack = {base, raw, display.replace("/", "")}
        if base == base_query or raw == base_query:
            exact.append(item)
        elif any(value.startswith(base_query) for value in haystack):
            starts.append(item)
        elif any(base_query in value for value in haystack):
            contains.append(item)

    records = (exact + starts + contains)[:limit]
    return {
        "data": [row["displayName"] for row in records],
        "records": records,
        "query": base_query,
        "source": snapshot.source,
        "liveChecked": snapshot.source == "fapi_exchangeInfo",
        "updatedAt": snapshot.updated_at,
        "fresh": time.time() - snapshot.updated_at < int(os.environ.get("IKUNANCE_SYMBOL_STALE_SECONDS", "86400") or "86400"),
        "errorCode": snapshot.error_code,
        "error": snapshot.error,
    }


def binance_futures_symbol_health() -> Dict:
    snapshot = refresh_binance_futures_symbols(force=False)
    age = max(0.0, time.time() - snapshot.updated_at) if snapshot.updated_at else None
    return {
        "source": snapshot.source,
        "count": len(snapshot.symbols),
        "updatedAt": snapshot.updated_at,
        "ageSeconds": age,
        "fresh": age is not None and age < int(os.environ.get("IKUNANCE_SYMBOL_STALE_SECONDS", "86400") or "86400"),
        "errorCode": snapshot.error_code,
        "error": snapshot.error,
    }
