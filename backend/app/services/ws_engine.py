import os
import queue
import threading
import time
import uuid


_subscriptions = set()
_listeners = set()
_callbacks = []
_lock = threading.Lock()
_proxy = ""


def _positive_int_env(name, default, minimum=1):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else default


_listener_queue_size = _positive_int_env("IKUNANCE_SSE_QUEUE_SIZE", 200, minimum=10)


def start_engine():
    return True


def stop_engine():
    with _lock:
        _subscriptions.clear()
        _listeners.clear()


def normalize_subscription(exchange: str, symbol: str, timeframe: str):
    exchange = (exchange or "binance").strip().lower()
    symbol = (symbol or "").strip().upper()
    timeframe = (timeframe or "15m").strip().lower()
    return exchange, symbol, timeframe


def subscribe(exchange: str, symbol: str, timeframe: str):
    exchange, symbol, timeframe = normalize_subscription(exchange, symbol, timeframe)
    if not symbol:
        return None
    with _lock:
        _subscriptions.add((exchange, symbol, timeframe))
    return {"exchange": exchange, "symbol": symbol, "timeframe": timeframe}


def unsubscribe(exchange: str, symbol: str, timeframe: str):
    exchange, symbol, timeframe = normalize_subscription(exchange, symbol, timeframe)
    with _lock:
        _subscriptions.discard((exchange, symbol, timeframe))


def get_active_subscriptions():
    with _lock:
        return sorted(_subscriptions)


def register_sse_listener():
    listener = queue.Queue(maxsize=_listener_queue_size)
    with _lock:
        _listeners.add(listener)
    return listener


def unregister_sse_listener(listener):
    with _lock:
        _listeners.discard(listener)


def configure_proxy(proxy: str):
    global _proxy
    _proxy = proxy or ""


def register_signal_callback(callback):
    if callable(callback):
        _callbacks.append(callback)


def normalize_signal(signal):
    payload = dict(signal or {})
    exchange, symbol, timeframe = normalize_subscription(
        payload.get("exchange", "binance"),
        payload.get("symbol", ""),
        payload.get("timeframe", "15m"),
    )
    payload["exchange"] = exchange
    payload["symbol"] = symbol
    payload["timeframe"] = timeframe
    payload.setdefault("type", "signal")
    payload.setdefault("_ts", time.time())
    payload.setdefault("id", payload.get("_id") or uuid.uuid4().hex)
    return payload


def publish_signal(signal):
    signal = normalize_signal(signal)
    with _lock:
        listeners = list(_listeners)
    for listener in listeners:
        try:
            listener.put_nowait(signal)
        except queue.Full:
            try:
                listener.get_nowait()
            except queue.Empty:
                pass
            try:
                listener.put_nowait(signal)
            except queue.Full:
                pass
    for callback in list(_callbacks):
        try:
            callback(signal)
        except Exception:
            pass
