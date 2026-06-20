# -*- coding: utf-8 -*-
"""
信号分析引擎 V2 - 优化版
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import numpy as np

from .storage_service import atomic_write_json, get_data_path
from .ws_engine import publish_signal

logger = logging.getLogger(__name__)
BEIJING_TZ = timezone(timedelta(hours=8))

# 全局线程池
_analyze_pool = ThreadPoolExecutor(max_workers=20, thread_name_prefix="analyze_")

# 时间周期映射
TIMEFRAME_MINUTES = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440,
}


@dataclass(frozen=True)
class SignalResult:
    """单个标的的分析结果"""
    symbol: str
    price: float
    trend: str
    signal: str
    detail: str
    action: str
    candle_time: float


def _compute_ema(values: np.ndarray, span: int) -> np.ndarray:
    """计算 EMA"""
    if len(values) < 2:
        return values
    alpha = 2.0 / (span + 1)
    result = np.empty_like(values)
    result[0] = values[0]
    for i in range(1, len(values)):
        result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]
    return result


def analyze_symbol(ohlcv: list, symbol: str, trigger_mode: str = "close") -> Optional[SignalResult]:
    """对单个标的执行 MACD 策略分析"""
    if len(ohlcv) < 50:
        return None

    close = np.array([row[4] for row in ohlcv], dtype=np.float64)
    timestamps = [row[0] for row in ohlcv]

    ema12 = _compute_ema(close, 12)
    ema26 = _compute_ema(close, 26)
    macd_line = ema12 - ema26
    signal_line = _compute_ema(macd_line, 9)
    hist = macd_line - signal_line
    trend_span = min(200, len(close))
    ema_trend = _compute_ema(close, trend_span)

    idx = -2 if trigger_mode == "close" else -1
    curr_close = close[idx]
    curr_hist = hist[idx]
    prev1_hist = hist[idx - 1]
    prev2_hist = hist[idx - 2]
    trend = "BULL" if curr_close > ema_trend[idx] else "BEAR"

    sig_name, sig_detail, action = _classify_signal(curr_hist, prev1_hist, prev2_hist)

    return SignalResult(
        symbol=symbol, price=float(curr_close), trend=trend,
        signal=sig_name, detail=sig_detail, action=action,
        candle_time=float(timestamps[idx]),
    )


def _classify_signal(curr: float, prev1: float, prev2: float) -> tuple:
    """根据柱状图判定信号"""
    curr_green = curr > 0

    if curr > prev1 and prev1 < prev2 and prev1 < 0:
        return "趋势确认", "红柱缩短 (收脚)", "LONG"
    if curr_green and curr > prev1 and prev1 < prev2:
        return "趋势演进", "绿柱首次增强", "LONG"
    if curr < prev1 and prev1 > prev2 and prev1 > 0:
        return "趋势确认", "绿柱缩短 (缩头)", "SHORT"
    if not curr_green and curr < prev1 and prev1 > prev2:
        return "趋势演进", "红柱首次增强", "SHORT"
    return "-", "-", "-"


# 信号去重系统
_alerted_cache: Optional[Dict[str, float]] = None
_alerted_lock = threading.Lock()
_alerted_dirty = False
_ALERTED_FILE = get_data_path("alerted_signals.json")


def _ensure_alerted_loaded() -> None:
    global _alerted_cache
    if _alerted_cache is not None:
        return
    if os.path.exists(_ALERTED_FILE):
        try:
            with open(_ALERTED_FILE, "r", encoding="utf-8") as f:
                _alerted_cache = json.load(f)
        except (json.JSONDecodeError, OSError):
            _alerted_cache = {}
    else:
        _alerted_cache = {}
    if not isinstance(_alerted_cache, dict):
        _alerted_cache = {}
    cutoff = time.time() - 86400
    for k in list(_alerted_cache.keys()):
        if not isinstance(_alerted_cache[k], (int, float)) or _alerted_cache[k] < cutoff:
            del _alerted_cache[k]


def _alert_key(symbol: str, timeframe: str, candle_time: float, action: str, scope: str = "global") -> str:
    scope = str(scope or "global")
    return f"{scope}|{symbol}|{timeframe}|{candle_time}|{action}"


def is_signal_alerted(symbol: str, timeframe: str, candle_time: float, action: str, scope: str = "global") -> bool:
    key = _alert_key(symbol, timeframe, candle_time, action, scope)
    with _alerted_lock:
        _ensure_alerted_loaded()
        return key in _alerted_cache


def mark_signal_alerted(symbol: str, timeframe: str, candle_time: float, action: str, scope: str = "global") -> None:
    global _alerted_dirty
    key = _alert_key(symbol, timeframe, candle_time, action, scope)
    with _alerted_lock:
        _ensure_alerted_loaded()
        _alerted_cache[key] = time.time()
        _alerted_dirty = True


def flush_alerted_cache() -> None:
    global _alerted_dirty
    with _alerted_lock:
        if not _alerted_dirty or _alerted_cache is None:
            return
        try:
            atomic_write_json(_ALERTED_FILE, _alerted_cache, ensure_ascii=True)
            _alerted_dirty = False
        except OSError as e:
            logger.warning("写入去重缓存失败: %s", e)


# Web 信号缓存
_web_signals: List[Dict] = []
_web_dedup: set = set()
_web_lock = threading.Lock()


def append_web_signal(sig: Dict) -> None:
    key = f"{sig.get('symbol')}|{sig.get('timeframe')}|{sig.get('candle_time')}|{sig.get('action')}"
    with _web_lock:
        if key in _web_dedup:
            return
        sig["_ts"] = time.time()
        sig["_id"] = key
        _web_signals.append(sig)
        _web_dedup.add(key)
        if len(_web_signals) > 500:
            cutoff = time.time() - 86400
            _web_signals[:] = [s for s in _web_signals if s.get("_ts", 0) > cutoff]
            _web_dedup.clear()
            _web_dedup.update(s["_id"] for s in _web_signals)


def get_web_signals(since_ts: float = 0) -> List[Dict]:
    with _web_lock:
        return [s for s in _web_signals if s.get("_ts", 0) > since_ts]


# 信号历史
_SIGNALS_FILE = get_data_path("signal_history.json")
_history_lock = threading.Lock()
_history_dedup: Optional[set] = None


def _load_history() -> Dict:
    if not os.path.exists(_SIGNALS_FILE):
        return {"signals": []}
    try:
        with open(_SIGNALS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"signals": []}
        signals = data.get("signals", [])
        if not isinstance(signals, list):
            signals = []
        cutoff = (datetime.now(BEIJING_TZ) - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
        data["signals"] = [s for s in signals if isinstance(s, dict) and s.get("time", "") >= cutoff]
        return data
    except (json.JSONDecodeError, OSError):
        return {"signals": []}


def _save_history(data: Dict) -> None:
    try:
        atomic_write_json(_SIGNALS_FILE, data, ensure_ascii=False)
    except OSError as e:
        logger.error("保存信号历史失败: %s", e)


def load_signal_history() -> Dict:
    return _load_history()


def append_signal_history(signal_obj: Dict) -> None:
    global _history_dedup
    key = f"{signal_obj.get('symbol')}|{signal_obj.get('timeframe')}|{signal_obj.get('candle_time')}|{signal_obj.get('action')}"

    with _history_lock:
        history = _load_history()
        if _history_dedup is None:
            _history_dedup = set()
            for s in history["signals"]:
                ek = f"{s.get('symbol')}|{s.get('timeframe')}|{s.get('candle_time')}|{s.get('action')}"
                _history_dedup.add(ek)
        if key in _history_dedup:
            return
        history["signals"].append(signal_obj)
        _history_dedup.add(key)
        _save_history(history)


# 信号触发
def trigger_alert(result: SignalResult, ud: Dict, timeframe: str) -> Optional[Dict]:
    if result.action == "-":
        return None

    tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 60)
    open_dt = datetime.fromtimestamp(result.candle_time / 1000, tz=BEIJING_TZ)
    close_ms = result.candle_time + tf_minutes * 60 * 1000
    close_dt = datetime.fromtimestamp(close_ms / 1000, tz=BEIJING_TZ)

    staleness_limit_ms = tf_minutes * 60 * 4 * 1000
    if (time.time() * 1000) - close_ms > staleness_limit_ms:
        return None

    dedupe_scope = (ud or {}).get("_dedupe_scope") or "global"
    origin = (ud or {}).get("_notification_origin") or "manual_scan"
    market_source = (ud or {}).get("_market_source") or "unknown"
    strategy_source = (
        (ud or {}).get("_strategy_source")
        or "MACD histogram three-bar capture"
    )

    if is_signal_alerted(result.symbol, timeframe, result.candle_time, result.action, dedupe_scope):
        return None

    mark_signal_alerted(result.symbol, timeframe, result.candle_time, result.action, dedupe_scope)

    trigger_time_str = close_dt.strftime("%H:%M")
    trigger_time_full = close_dt.strftime("%Y-%m-%d %H:%M:%S")
    open_time_full = open_dt.strftime("%Y-%m-%d %H:%M:%S")

    signal_data = {
        "symbol": result.symbol, "type": result.signal, "detail": result.detail,
        "action": result.action, "timeframe": timeframe, "price": result.price,
        "trend": result.trend,
        "market_source": market_source,
        "strategy_source": strategy_source,
        "exchange": (ud or {}).get("exchange_id", "binance"),
        "origin": origin,
        "time": trigger_time_full, "trigger_time": trigger_time_str,
        "trigger_time_full": trigger_time_full, "open_time_full": open_time_full,
        "candle_time": result.candle_time,
    }

    append_web_signal(signal_data.copy())
    append_signal_history(signal_data.copy())
    publish_signal(signal_data.copy())

    return {
        "symbol": result.symbol, "signal": result.signal, "detail": result.detail,
        "action": result.action, "price": result.price, "open_time": open_time_full,
        "close_time": trigger_time_full, "timeframe": timeframe,
        "exchange": signal_data["exchange"], "type": result.signal,
        "trend": result.trend,
        "market_source": market_source,
        "strategy_source": strategy_source,
        "candle_time": result.candle_time, "trigger_time": trigger_time_str,
        "trigger_time_full": trigger_time_full,
    }
