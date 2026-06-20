import os
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


FAPI_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
FAPI_24HR_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"

_last_error: Optional[Dict] = None
_last_success: Optional[Dict] = None


class KlineProviderError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _normalize_proxy(proxy: str = "") -> str:
    proxy = str(proxy or "").strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


def _market_proxy(manual_proxy: str = "") -> str:
    return _normalize_proxy(
        manual_proxy
        or os.environ.get("IKUNANCE_MARKET_PROXY")
        or os.environ.get("IKUNANCE_PROXY")
        or ""
    )


def _to_binance_symbol(symbol: str) -> str:
    return str(symbol or "").upper().split(":")[0].replace("/", "")


def _record_error(exc: KlineProviderError) -> None:
    global _last_error
    _last_error = {
        "ts": time.time(),
        "code": exc.code,
        "message": str(exc),
        "statusCode": exc.status_code,
    }


def _record_success(kind: str, source: str, count: int) -> None:
    global _last_success
    _last_success = {
        "ts": time.time(),
        "kind": kind,
        "source": source,
        "count": int(count or 0),
    }


def fetch_futures_rest_klines(
    symbol: str,
    timeframe: str = "15m",
    limit: int = 80,
    *,
    manual_proxy: str = "",
    timeout: int = 10,
) -> List[List[float]]:
    raw = _to_binance_symbol(symbol)
    if not raw.endswith("USDT"):
        return []
    capped_limit = max(3, min(int(limit or 80), 1500))
    proxy = _market_proxy(manual_proxy)
    params = {"symbol": raw, "interval": timeframe, "limit": capped_limit}
    try:
        status_code, payload, text = _http_get_json(FAPI_KLINES_URL, params=params, timeout=timeout, proxy=proxy)
    except (urllib.error.URLError, OSError) as exc:
        error = KlineProviderError("BINANCE_FUTURES_REST_NETWORK", str(exc))
        _record_error(error)
        raise error from exc

    if status_code == 451:
        error = KlineProviderError(
            "BINANCE_FUTURES_REST_HTTP_451",
            "Binance Futures REST is unavailable from this server exit IP",
            status_code=451,
        )
        _record_error(error)
        raise error
    if status_code >= 400:
        error = KlineProviderError(
            f"BINANCE_FUTURES_REST_HTTP_{status_code}",
            text[:300],
            status_code=status_code,
        )
        _record_error(error)
        raise error

    if not isinstance(payload, list):
        error = KlineProviderError("BINANCE_FUTURES_REST_PAYLOAD", "Futures REST payload was not a list")
        _record_error(error)
        raise error

    rows = []
    for item in payload:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        try:
            rows.append([
                int(item[0]),
                float(item[1]),
                float(item[2]),
                float(item[3]),
                float(item[4]),
                float(item[5]),
                "binance_futures_rest",
            ])
        except (TypeError, ValueError):
            continue
    _record_success("klines", "binance_futures_rest", len(rows))
    return rows[-capped_limit:]


def fetch_futures_24hr_tickers(
    symbols: Optional[List[str]] = None,
    *,
    manual_proxy: str = "",
    timeout: int = 10,
) -> Dict[str, Dict]:
    """Fetch official Binance USD-M Futures 24h tickers and map them to ccxt-like keys."""
    wanted = {_to_binance_symbol(symbol) for symbol in symbols or [] if _to_binance_symbol(symbol)}
    proxy = _market_proxy(manual_proxy)
    params = {}
    if len(wanted) == 1:
        params["symbol"] = next(iter(wanted))

    try:
        status_code, payload, text = _http_get_json(FAPI_24HR_TICKER_URL, params=params, timeout=timeout, proxy=proxy)
    except (urllib.error.URLError, OSError) as exc:
        error = KlineProviderError("BINANCE_FUTURES_TICKER_NETWORK", str(exc))
        _record_error(error)
        raise error from exc

    if status_code == 451:
        error = KlineProviderError(
            "BINANCE_FUTURES_TICKER_HTTP_451",
            "Binance Futures 24h ticker is unavailable from this server exit IP",
            status_code=451,
        )
        _record_error(error)
        raise error
    if status_code >= 400:
        error = KlineProviderError(
            f"BINANCE_FUTURES_TICKER_HTTP_{status_code}",
            text[:300],
            status_code=status_code,
        )
        _record_error(error)
        raise error

    rows = payload if isinstance(payload, list) else [payload]
    tickers: Dict[str, Dict] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("symbol") or "").upper()
        if not raw.endswith("USDT") or (wanted and raw not in wanted):
            continue
        try:
            last = float(item.get("lastPrice"))
            pct = float(item.get("priceChangePercent") or 0.0)
        except (TypeError, ValueError):
            continue
        display = f"{raw[:-4]}/USDT"
        tickers[f"{display}:USDT"] = {
            "last": last,
            "close": last,
            "percentage": pct,
            "source": "binance_futures_rest_24hr",
        }
    if not tickers:
        error = KlineProviderError("BINANCE_FUTURES_TICKER_EMPTY", "Binance Futures 24h ticker returned no usable USDT rows")
        _record_error(error)
        raise error
    _record_success("tickers", "binance_futures_rest_24hr", len(tickers))
    return tickers


def futures_kline_provider_health() -> Dict:
    return {
        "provider": "BinanceFuturesRestProvider",
        "endpoints": {
            "klines": FAPI_KLINES_URL,
            "ticker24hr": FAPI_24HR_TICKER_URL,
        },
        "proxyConfigured": bool(_market_proxy()),
        "lastSuccess": dict(_last_success) if _last_success else None,
        "lastError": dict(_last_error) if _last_error else None,
    }


def _http_get_json(url: str, *, params: Dict, timeout: int, proxy: str = ""):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
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
        raise KlineProviderError("BINANCE_FUTURES_REST_JSON", "Futures REST returned invalid JSON") from exc
