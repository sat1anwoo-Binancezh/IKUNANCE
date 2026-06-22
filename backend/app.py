from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context, g
from flask_cors import CORS
import atexit
import time
import json
import os
import queue
import re
import sys
import threading
import uuid
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

SERVICE_ROOT = os.path.join(os.path.dirname(__file__), "app")
if SERVICE_ROOT not in sys.path:
    sys.path.insert(0, SERVICE_ROOT)

from services.exchange_service import (
    get_exchange as _get_singleton_exchange,
    fetch_ohlcv_cached as _fetch_ohlcv_cached,
    clear_ohlcv_cache as _clear_ohlcv_cache,
    load_linear_usdt_symbols as _load_linear_usdt_symbols,
    fetch_tickers_safe as _fetch_tickers_safe,
    binance_stock_search_symbols as _binance_stock_search_symbols,
    is_binance_stock_exchange as _is_binance_stock_exchange,
    ensure_binance_kline_streams as _ensure_binance_kline_streams,
    binance_kline_stream_status as _binance_kline_stream_status,
)
from services.binance_symbol_provider import (
    binance_futures_symbol_health as _binance_futures_symbol_health,
    list_binance_futures_symbols as _list_binance_futures_symbols,
    refresh_binance_futures_symbols as _refresh_binance_futures_symbols,
    search_binance_futures_symbols as _search_binance_futures_symbols,
)
from services.futures_kline_provider import futures_kline_provider_health as _futures_kline_provider_health
from services.macd_state import MacdStateManager
from services.notification_service import send_all_notifications, send_test_notification, format_signal_notification
from services import auth_service
from services.email_service import send_email_sync
from services.storage_service import atomic_write_json, ensure_storage_dirs, get_data_path
from services.signal_engine import (
    analyze_symbol as _analyze_symbol,
    trigger_alert as _trigger_alert,
    get_web_signals,
    load_signal_history,
    flush_alerted_cache,
)
from services.ws_engine import (
    start_engine as _start_ws_engine,
    stop_engine as _stop_ws_engine,
    subscribe as _ws_subscribe,
    unsubscribe as _ws_unsubscribe,
    get_active_subscriptions as _ws_get_subs,
    register_sse_listener as _sse_register,
    unregister_sse_listener as _sse_unregister,
    configure_proxy as _ws_configure_proxy,
    register_signal_callback as _ws_register_signal_cb,
    normalize_subscription as _ws_normalize_subscription,
)


BACKEND_ROOT = Path(__file__).resolve().parent


def _resolve_frontend_dist():
    configured = os.environ.get("IKUNANCE_FRONTEND_DIST", "").strip()
    candidates = []
    if configured:
        candidates.append(Path(configured))
    candidates.extend([
        BACKEND_ROOT.parent / "dist",
        BACKEND_ROOT.parent / "frontend" / "dist",
    ])
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if (resolved / "index.html").exists():
            return str(resolved)
    return str((BACKEND_ROOT.parent / "dist").resolve())


FRONTEND_DIST = _resolve_frontend_dist()


app = Flask(__name__, static_folder=FRONTEND_DIST, static_url_path='')
_cors_origins = os.environ.get("IKUNANCE_CORS_ORIGINS", "*").strip()
if _cors_origins and _cors_origins != "*":
    CORS(app, origins=[item.strip() for item in _cors_origins.split(",") if item.strip()], supports_credentials=True)
else:
    CORS(app)


@app.after_request
def add_frontend_cache_headers(response):
    path = request.path or "/"
    content_type = response.headers.get("Content-Type", "")
    if response.status_code == 200 and content_type.startswith("text/html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
    elif response.status_code == 200 and path.startswith("/assets/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return response


def _positive_int(value, default, minimum=1):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= minimum else default


def _normalize_proxy_setting(value):
    proxy = str(value or "").strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


app.config['MAX_CONTENT_LENGTH'] = _positive_int(os.environ.get('IKUNANCE_MAX_UPLOAD_MB'), 8) * 1024 * 1024
ensure_storage_dirs()
APP_VERSION = os.environ.get("IKUNANCE_APP_VERSION", "local").strip() or "local"
STARTED_AT = time.time()
ALLOWED_TIMEFRAMES = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
ALLOWED_TRIGGER_MODES = {"close", "realtime"}
WATCHLIST_LIMIT = 20
MAX_SCAN_SYMBOLS = WATCHLIST_LIMIT

MANUAL_PROXY = _normalize_proxy_setting(
    os.environ.get("IKUNANCE_MARKET_PROXY")
    or os.environ.get("IKUNANCE_PROXY")
    or ""
)

_instance_lock_file = None


def _acquire_single_instance_lock():
    global _instance_lock_file
    if os.environ.get("IKUNANCE_SINGLE_INSTANCE_LOCK", "1").strip() == "0":
        return

    lock_path = Path(get_data_path("runtime", "backend.lock"))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(f"Another IKUNANCE backend instance is already running; lock={lock_path}") from exc

    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    _instance_lock_file = handle


def _release_single_instance_lock():
    global _instance_lock_file
    handle = _instance_lock_file
    _instance_lock_file = None
    if not handle:
        return
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    except OSError:
        pass
    try:
        handle.close()
    except OSError:
        pass


_acquire_single_instance_lock()
atexit.register(_release_single_instance_lock)

# 闁冲厜鍋撻柍鍏夊亾 闁告凹鍨版慨?WebSocket 閻庡湱鍋炲鍌氼嚕閺囩喐鎯?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?
if MANUAL_PROXY:
    _ws_configure_proxy(MANUAL_PROXY)
_start_ws_engine()

_scan_pool = ThreadPoolExecutor(
    max_workers=_positive_int(os.environ.get("IKUNANCE_SCAN_WORKERS"), 4, minimum=1),
    thread_name_prefix="scan_",
)
_scan_gate = threading.BoundedSemaphore(_positive_int(os.environ.get("IKUNANCE_SCAN_CONCURRENCY"), 1, minimum=1))
_macd_state_manager = MacdStateManager()
_rate_limit_buckets = {}
_rate_limit_lock = threading.Lock()


def _log_event(level, message, **context):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        **context,
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


def _secure_cookie_enabled():
    return os.environ.get("IKUNANCE_FORCE_HTTPS", "0").strip() == "1"


def _set_app_cookie(response, name, value, max_age):
    response.set_cookie(
        name,
        value,
        max_age=max_age,
        httponly=True,
        samesite="Lax",
        secure=_secure_cookie_enabled(),
    )


def _delete_app_cookie(response, name):
    response.delete_cookie(name, samesite="Lax", secure=_secure_cookie_enabled())


@app.after_request
def _apply_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "no-referrer-when-downgrade")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if os.environ.get("IKUNANCE_FORCE_HTTPS", "0").strip() == "1":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.errorhandler(404)
def _api_not_found(error):
    if request.path.startswith("/api/"):
        return jsonify({"status": "error", "msg": "api route not found"}), 404
    return error


@app.errorhandler(405)
def _api_method_not_allowed(error):
    if request.path.startswith("/api/"):
        return jsonify({"status": "error", "msg": "method not allowed"}), 405
    return error


@app.errorhandler(413)
def _api_payload_too_large(error):
    if request.path.startswith("/api/"):
        return jsonify({"status": "error", "msg": "payload too large"}), 413
    return error


@app.errorhandler(Exception)
def _api_unhandled_error(error):
    from werkzeug.exceptions import HTTPException

    if isinstance(error, HTTPException):
        return error
    if request.path.startswith("/api/"):
        _log_event("error", "unhandled_api_error", path=request.path, error=error.__class__.__name__)
        return jsonify({"status": "error", "msg": "internal server error"}), 500
    raise error


@app.before_request
def _request_started():
    request._started_at = time.time()


@app.after_request
def _log_request(response):
    duration_ms = round((time.time() - getattr(request, "_started_at", time.time())) * 1000, 2)
    if os.environ.get("IKUNANCE_ACCESS_LOG", "1").strip() != "0" and request.path.startswith("/api/"):
        _log_event(
            "info",
            "http_request",
            method=request.method,
            path=request.path,
            status=response.status_code,
            duration_ms=duration_ms,
        )
    return response


def _json_body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def _text_setting(value, max_length=500):
    if value is None:
        return ""
    if not isinstance(value, str):
        return ""
    return value.strip()[:max_length]


def _client_key():
    forwarded = request.headers.get("X-Forwarded-For", "")
    ip = forwarded.split(",", 1)[0].strip() if forwarded else request.remote_addr
    return ip or "unknown"


def _rate_limit(scope, limit=None, window_seconds=None):
    limit = _positive_int(limit if limit is not None else os.environ.get("IKUNANCE_AUTH_RATE_LIMIT"), 20, minimum=0)
    window_seconds = _positive_int(window_seconds if window_seconds is not None else os.environ.get("IKUNANCE_AUTH_RATE_WINDOW"), 60)
    if limit <= 0:
        return None
    now = time.time()
    key = (scope, _client_key())
    with _rate_limit_lock:
        recent = [
            ts for ts in _rate_limit_buckets.get(key, [])
            if now - ts < window_seconds
        ]
        if len(recent) >= limit:
            reset_after = max(1, int(window_seconds - (now - recent[0])))
            _rate_limit_buckets[key] = recent
            return reset_after
        recent.append(now)
        _rate_limit_buckets[key] = recent
    return None


def _rate_limited_response(reset_after):
    response = jsonify({"status": "error", "msg": "Too many requests, please try again later."})
    response.status_code = 429
    response.headers["Retry-After"] = str(reset_after)
    return response


def _safe_sound_filename(value):
    raw = Path(str(value or '')).name
    raw = raw.rsplit('.', 1)[0]
    raw = re.sub(r'[^A-Za-z0-9_.-]+', '_', raw).strip('._-')
    if not raw:
        raw = f"sound_{uuid.uuid4().hex[:12]}"
    return f"{raw[:80]}.mp3"


def _sound_path(filename):
    safe_name = _safe_sound_filename(filename)
    if safe_name != filename:
        return None
    return os.path.join(UPLOAD_FOLDER, safe_name)


def _path_status(path):
    target = Path(path)
    return {
        "path": str(target),
        "exists": target.exists(),
        "writable": os.access(target, os.W_OK) if target.exists() else False,
    }


def _env_flag(name, default="0"):
    return os.environ.get(name, default).strip() in {"1", "true", "TRUE", "yes", "on"}


def _runtime_diagnostics(data_dir):
    return {
        "dataWritable": _path_status(data_dir)["writable"],
        "frontendReady": (Path(FRONTEND_DIST) / "index.html").exists(),
        "outboundDisabled": _env_flag("IKUNANCE_DISABLE_OUTBOUND"),
        "forceHttps": _env_flag("IKUNANCE_FORCE_HTTPS"),
        "secretsConfigured": {
            "doubaoApiKey": bool(os.environ.get("DOUBAO_API_KEY", "").strip()),
        },
    }


@app.route('/api/health')
def api_health():
    data_dir = get_data_path("")
    subscriptions = _ws_get_subs()
    return jsonify({
        "status": "ok",
        "service": "ikunance-backend",
        "version": APP_VERSION,
        "uptime_seconds": int(time.time() - STARTED_AT),
        "liveMarket": os.environ.get("IKUNANCE_LIVE_MARKET", "0").strip() == "1",
        "marketProxyConfigured": bool(MANUAL_PROXY or os.environ.get("IKUNANCE_MARKET_PROXY") or os.environ.get("IKUNANCE_PROXY")),
        "dataDir": _path_status(data_dir),
        "frontendDist": _path_status(FRONTEND_DIST),
        "userDataDir": _path_status(USER_DATA_DIR) if "USER_DATA_DIR" in globals() else None,
        "customSoundsDir": _path_status(UPLOAD_FOLDER) if "UPLOAD_FOLDER" in globals() else None,
        "diagnostics": _runtime_diagnostics(data_dir),
        "wsSubscriptions": len(subscriptions),
        "binanceKlineStreams": _binance_kline_stream_status(),
        "symbolProvider": _binance_futures_symbol_health(),
        "futuresRestProvider": _futures_kline_provider_health(),
        "macdState": _macd_state_manager.health(),
        "runtimeLimits": {
            "scanWorkers": getattr(_scan_pool, "_max_workers", None),
            "scanConcurrency": _positive_int(os.environ.get("IKUNANCE_SCAN_CONCURRENCY"), 1, minimum=1),
            "scanRateLimit": _positive_int(os.environ.get("IKUNANCE_SCAN_RATE_LIMIT"), 12, minimum=0),
            "pushMonitorInterval": _push_monitor_interval_seconds(),
            "pushMonitorCadence": _push_monitor_cadence_seconds(),
            "pushMonitorWindow": _push_monitor_window_seconds(),
            "singleInstanceLock": os.environ.get("IKUNANCE_SINGLE_INSTANCE_LOCK", "1").strip() != "0",
        },
        "pushMonitor": _push_monitor_status_snapshot(),
        "time": int(time.time()),
    })


# 闁冲厜鍋撻柍鍏夊亾 濞ｅ洠鈧啿濞囬柛銉у仩閻ㄧ喖鏁嶅绔engine 濞存籂鍛櫢濞ｅ洠鈧啿濞囬柡鍐╁劶閸ゆ粓宕濋妸銉ョ岛闂侇収鍠曞▎?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾

def _on_ws_signal(signal: dict) -> None:
    """Dispatch a websocket signal notification for users with matching settings."""
    if signal.get("origin") in {"manual_scan", "push_monitor"}:
        return

    def _send_for_user(uid: str) -> None:
        try:
            ud = load_user_config(uid)

            # 闁?濞存嚎鍊栧Σ妤呭箥閳ь剝绠涢崨娣偓蹇涘礌瑜版帒甯?
            user_exchange = ud.get('exchange_id') or 'binance'
            if signal.get('exchange') and user_exchange != signal.get('exchange'):
                return

            # 闁?timeframe 闊洤鎳橀妴蹇涘椽瀹€鈧弫銈夊箣閻ゎ垼鍟庣紓鍐惧枤濞堟垶绋夐埀顒勬嚊鏉堝墽绀勫ǎ鍥ｂ偓鍐插▏闁哄嫷鍨伴幗銏＄▔椤忓嫭鍣柡鍫㈠枔濞堟垹浜搁崡鐐叉锭闁规亽鍔戦崑鍛▔椤忓嫭鍣柡鍫㈠櫐缁?
            user_tf = ud.get('timeframe') or '15m'
            if signal.get('timeframe') and signal.get('timeframe') != user_tf:
                return

            # 闁?闁烩晜鍨剁敮鍫曞礆濡ゅ嫨鈧啫螣閳ュ磭纭€閺夆晛娲﹂幎?
            watchlist_mode = ud.get('watchlist_mode') or 'favorites'  # favorites / all
            if watchlist_mode != 'all':
                wl = _normalize_watchlist(ud.get('watchlist', []))
                sig_exchange = signal.get('exchange', '')
                sig_symbol   = signal.get('symbol', '')
                match = any(
                    it['symbol'] == sig_symbol and it['exchange'] == sig_exchange
                    for it in wl
                )
                if not match:
                    return
            # all 婵☆垪鈧磭纭€闁挎稒纰嶆晶宥夊嫉婢跺鍨奸柣銊ュ閸忔﹢骞掗…鎺旂濞戞挸绉风换鍐煥?
            alert_settings = ud.get('alert_settings', {})
            if not alert_settings.get('email', False):
                return
            sender   = ud.get('email', '')
            password = ud.get('email_pass', '')
            if not sender or not password:
                return
            # 闁哄瀚紓鎾绘焽椤旂粯顐?
            from services.email_service import send_email
            import datetime as _dt
            action       = signal.get('action', '')
            sym          = signal.get('symbol', '').replace('/USDT', '')
            tf           = signal.get('timeframe', '')
            detail       = signal.get('detail', '')
            price        = signal.get('price', 0)
            trend        = signal.get('trend', '')
            exchange_lbl = signal.get('exchange', '').upper()
            now_str      = _dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            candle_ts    = signal.get('candle_time', 0)
            action_label = '📈 上涨 LONG' if action == 'LONG' else '📉 下跌 SHORT'

            # K缂佹儳鐏濈槐鎴︽儎濡粯顦ч梻鍌濇彧缁辨獑andle_time 闁哄嫷鍨扮槐鎴︽儎濡粯顦ч梻鍌氱摠閸?ms闁?            candle_ts    = signal.get('candle_time', 0)
            tf_ms_map    = {
                '1m':60000,'3m':180000,'5m':300000,'15m':900000,
                '30m':1800000,'1h':3600000,'2h':7200000,
                '4h':14400000,'6h':21600000,'12h':43200000,'1d':86400000
            }
            tf_ms        = tf_ms_map.get(tf, 0)
            if candle_ts:
                open_str  = _dt.datetime.fromtimestamp(candle_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                close_str = _dt.datetime.fromtimestamp((candle_ts + tf_ms) / 1000).strftime('%Y-%m-%d %H:%M:%S') if tf_ms else '-'
            else:
                open_str  = '-'
                close_str = signal.get('time', '-')

            subject    = f"[{tf}] {sym} {action_label} - I-KUNANCE Signal"
            body_lines = [
                '-' * 44,
                '  I-KUNANCE realtime signal notification',
                '-' * 44,
                f"  Symbol:     {signal.get('symbol', '')}  ({exchange_lbl})",
                f"  Direction:  {action_label}",
                f"  Price:      {price}",
                f"  Signal:     {signal.get('type', '')} - {detail}",
                f"  Trend:      {trend}",
                f"  Timeframe:  {tf}",
                '-' * 44,
                f"  Candle open:  {open_str}",
                f"  Candle close: {close_str}",
                f"  Trigger time: {now_str}",
                '-' * 44,
                '  This email was sent automatically by I-KUNANCE. Please do not reply.',
                '-' * 44,
            ]
            notify_ud = dict(ud)
            notify_ud['alert_settings'] = {'email': True}
            result = send_all_notifications([signal], notify_ud, signal.get('timeframe') or ud.get('timeframe') or '15m')
            print(f"[INFO] [email_push] uid={uid} symbol={signal.get('symbol', '')} status={result.get('status')} channels={result.get('channels')}")
        except Exception as e:
            print(f"[WARN] [闂侇収鍠曞▎銏ゅ箳閵娾斁鍋撴稊?{uid}: {type(e).__name__}: {e}")

    for uid in _iter_registered_user_config_uids():
        _scan_pool.submit(_send_for_user, uid)


_ws_register_signal_cb(_on_ws_signal)

USER_DATA_DIR = get_data_path('user_data')
os.makedirs(USER_DATA_DIR, exist_ok=True)

# 闁冲厜鍋撻柍鍏夊亾 闁活潿鍔嶉崺娑㈡煀瀹ュ洨鏋傜紒鐙呯磿閹?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

_user_cache = {}
_user_cache_lock = threading.Lock()

def _default_config():
    return {
        "api_key": "", "secret_key": "", "email": "", "email_pass": "",
        "proxy": "", "watchlist": [],
        "timeframe": "15m", "trigger_mode": "close",
        "watchlist_mode": "favorites", "exchange_id": "binance",
        "alert_settings": {
            "app_push": False, "toast": True, "email": False,
            "webhook": False, "sound": False, "sound_type": "beep"
        },
        "email_template": {
            "include_price": True, "include_trend": True,
            "include_signal": True, "include_detail": True, "include_action": True
        }
    }

def _merge_alert_settings(existing=None, patch=None):
    settings = dict(_default_config()["alert_settings"])
    if isinstance(existing, dict):
        settings.update(existing)
    if isinstance(patch, dict):
        settings.update(patch)
    return settings

def _merge_email_template(existing=None, patch=None):
    template = dict(_default_config()["email_template"])
    if isinstance(existing, dict):
        template.update(existing)
    if isinstance(patch, dict):
        template.update(patch)
    return template

def _user_file(uid):
    safe = uid.replace('/', '_').replace('\\', '_').replace('..', '_')
    return os.path.join(USER_DATA_DIR, f'{safe}.json')

def load_user_config(uid):
    with _user_cache_lock:
        if uid in _user_cache:
            return _user_cache[uid]
    conf = _default_config()
    path = _user_file(uid)
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            for k, v in saved.items():
                if k == "alert_settings" and isinstance(v, dict):
                    conf[k] = _merge_alert_settings(conf.get(k), v)
                elif k == "email_template" and isinstance(v, dict):
                    conf[k] = _merge_email_template(conf.get(k), v)
                else:
                    conf[k] = v
        except:
            pass
    conf['watchlist'] = _normalize_watchlist(conf.get('watchlist', []))
    conf['alert_settings'] = _merge_alert_settings(conf.get('alert_settings'))
    conf['email_template'] = _merge_email_template(conf.get('email_template'))
    with _user_cache_lock:
        _user_cache[uid] = conf
    return conf

def save_user_config(uid, config):
    config = dict(config)
    config['watchlist'] = _normalize_watchlist(config.get('watchlist', []))
    config['alert_settings'] = _merge_alert_settings(config.get('alert_settings'))
    config['email_template'] = _merge_email_template(config.get('email_template'))
    with _user_cache_lock:
        _user_cache[uid] = config
    os.makedirs(USER_DATA_DIR, exist_ok=True)
    atomic_write_json(_user_file(uid), config, ensure_ascii=False, indent=2)

def _registered_account_uids():
    try:
        accounts = auth_service.load_accounts()
        users = accounts.get('users', {})
        if isinstance(users, dict):
            return set(users.keys())
    except Exception:
        pass
    return set()

def _is_registered_account_uid(uid):
    return uid in _registered_account_uids()

def _iter_registered_user_config_uids():
    if not os.path.isdir(USER_DATA_DIR):
        return []
    registered = _registered_account_uids()
    result = []
    for name in os.listdir(USER_DATA_DIR):
        if not name.endswith('.json'):
            continue
        uid = name[:-5]
        if uid in registered:
            result.append(uid)
    return result

def _request_token(allow_query=False):
    auth_header = request.headers.get('Authorization', '')
    bearer = auth_header[7:].strip() if auth_header.startswith('Bearer ') else ''
    return (
        request.headers.get('X-Token', '')
        or bearer
        or (request.args.get('token', '') if allow_query else '')
        or request.cookies.get('ikun_token', '')
    )

def _anonymous_uid_response(payload):
    uid = get_uid()
    resp = jsonify(payload)
    if uid.startswith('anon_'):
        _set_app_cookie(resp, 'ikun_sid', uid.replace('anon_', ''), 86400*365)
    return resp

def _user_json_response(payload):
    """Return JSON and persist the anonymous session id when there is no login token."""
    return _anonymous_uid_response(payload)

def _int_query(name, default, min_value=None, max_value=None):
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value

def get_uid(allow_query_token=False):
    cached_uid = getattr(g, 'ikun_uid', None)
    if cached_uid:
        return cached_uid
    token = _request_token(allow_query=allow_query_token)
    if token:
        accounts = auth_service.load_accounts()
        email = accounts.get('sessions', {}).get(token)
        if email:
            g.ikun_uid = email
            return email
    sid = request.cookies.get('ikun_sid', '')
    if not sid:
        sid = str(uuid.uuid4())
    g.ikun_uid = 'anon_' + sid
    return g.ikun_uid

def get_user_data():
    return load_user_config(get_uid())

USER_DATA = _default_config()

def get_exchange(ud=None):
    if ud is None:
        try:
            ud = get_user_data()
        except:
            ud = _default_config()
    return _get_singleton_exchange(ud=ud, manual_proxy=MANUAL_PROXY)


_SIGNAL_STRATEGY_SOURCE = "MACD histogram three-bar capture"
_DEFAULT_SCAN_KLINE_LIMIT = 80


def _scan_kline_limit():
    return min(120, _positive_int(os.environ.get("IKUNANCE_SCAN_KLINE_LIMIT"), _DEFAULT_SCAN_KLINE_LIMIT, minimum=50))


def _ohlcv_market_source(ohlcv):
    if not ohlcv:
        return "empty"
    markers = []
    for row in ohlcv:
        if isinstance(row, (list, tuple)) and len(row) > 6:
            markers.append(str(row[6]))
    has_futures_rest = any(marker.startswith("binance_futures_rest") for marker in markers)
    has_spot_data_api = any(marker.startswith("binance_spot_data_api") for marker in markers)
    has_ws = any(marker.startswith("binance_ws") for marker in markers)
    if has_futures_rest and has_ws:
        return "binance_futures_rest+binance_ws"
    if has_futures_rest:
        return "binance_futures_rest"
    if has_spot_data_api and has_ws:
        return "binance_spot_data_api+binance_ws"
    if has_spot_data_api:
        return "binance_spot_data_api"
    if "binance_ws_closed" in markers:
        return "binance_ws_closed"
    if "binance_ws_live" in markers:
        return "binance_ws_live"
    if os.environ.get("IKUNANCE_LIVE_MARKET", "0").strip() == "1":
        return "live_market"
    return "synthetic_or_test"

# 闁冲厜鍋撻柍鍏夊亾 闁规鍋呭鎸庢綇閸涱厼袠 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

def _scan_one_symbol(symbol, timeframe, trigger_mode, ud):
    """Scan one symbol and return the analysis result plus optional alert signal."""
    try:
        ohlcv = _fetch_ohlcv_cached(symbol, timeframe, ud=ud, manual_proxy=MANUAL_PROXY, limit=_scan_kline_limit())
        market_source = _ohlcv_market_source(ohlcv)
        if timeframe == "15m" and _normalize_exchange((ud or {}).get("exchange_id") or "binance") == "binance":
            closed_rows = [
                row for row in ohlcv
                if not (isinstance(row, (list, tuple)) and len(row) > 6 and "live" in str(row[6]))
            ]
            if closed_rows:
                _macd_state_manager.bootstrap(symbol, closed_rows)
        effective_trigger = trigger_mode
        result = _analyze_symbol(ohlcv, symbol, effective_trigger)
        if result and result.action != "-":
            alert_ud = dict(ud or {})
            alert_ud["_market_source"] = market_source
            alert_ud["_strategy_source"] = _SIGNAL_STRATEGY_SOURCE
            sig = _trigger_alert(result, alert_ud, timeframe)
            return result, sig, None
        return result, None, None
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)}"
        print(f"[ERROR] {symbol}: {msg}")
        return None, None, {"symbol": symbol, "msg": msg}

# 闁冲厜鍋撻柍鍏夊亾 Flask 閻犱警鍨抽弫?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

# 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?SSE 閻庡湱鍋炲鍌涚┍閳ュ啿濞囬柟鎭掑姂閳?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

@app.route('/api/stream')
def api_stream():
    """Stream realtime signals with Server-Sent Events."""
    get_uid(allow_query_token=True)
    timeframe = request.args.get('timeframe', '1h')
    exchange = request.args.get('exchange', 'binance')
    symbols_raw = request.args.get('symbols', '')
    exchange, _, timeframe = _ws_normalize_subscription(exchange, '', timeframe)
    symbols = [
        _ws_normalize_subscription(exchange, s, timeframe)[1]
        for s in symbols_raw.split(',')
        if s.strip()
    ]

    for sym in symbols:
        _ws_subscribe(exchange, sym, timeframe)

    def event_stream():
        q = _sse_register()
        yield ": heartbeat\n\n"
        try:
            while True:
                try:
                    signal = q.get(timeout=25)
                    sig_exchange = signal.get('exchange', '')
                    sig_symbol = signal.get('symbol', '')
                    if (not exchange or sig_exchange == exchange) and (not symbols or sig_symbol in symbols):
                        yield f"data: {json.dumps(signal, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            _sse_unregister(q)

    return Response(
        stream_with_context(event_stream()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        }
    )

# 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?閻犱降鍨藉Σ鍕不閿涘嫭鍊?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

def _mobile_signal_payload(signal, timeframe=None):
    payload = dict(signal or {})
    tf = payload.get("timeframe") or timeframe or "15m"
    payload.update(format_signal_notification(payload, tf))
    return payload


@app.route('/api/mobile/stream')
def api_mobile_stream():
    """Stream only the authenticated user's visible signals to the Android app."""
    uid = get_uid(allow_query_token=True)

    def event_stream():
        q = _sse_register()
        yield ": heartbeat\n\n"
        try:
            while True:
                try:
                    signal = q.get(timeout=25)
                    ud = load_user_config(uid)
                    if _signal_visible_to_user(signal, ud):
                        payload = _mobile_signal_payload(signal, ud.get("timeframe") or "15m")
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            _sse_unregister(q)

    return Response(
        stream_with_context(event_stream()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        }
    )


@app.route('/api/ws/subscribe', methods=['POST'])
def api_ws_subscribe():
    """Add or refresh a stream subscription."""
    data      = request.get_json(force=True, silent=True) or {}
    exchange  = data.get('exchange', 'binance')
    symbol    = data.get('symbol', '')
    timeframe = data.get('timeframe', '1h')
    if not symbol:
        return jsonify({'error': 'symbol required'}), 400
    subscription = _ws_subscribe(exchange, symbol, timeframe)
    return jsonify({'ok': True, 'subscribed': subscription})


@app.route('/api/ws/unsubscribe', methods=['POST'])
def api_ws_unsubscribe():
    """Remove a stream subscription."""
    data      = request.get_json(force=True, silent=True) or {}
    exchange  = data.get('exchange', 'binance')
    symbol    = data.get('symbol', '')
    timeframe = data.get('timeframe', '1h')
    if not symbol:
        return jsonify({'error': 'symbol required'}), 400
    _ws_unsubscribe(exchange, symbol, timeframe)
    return jsonify({'ok': True})


@app.route('/api/ws/subscriptions')
def api_ws_subscriptions():
    """Return active stream subscriptions."""
    subs = _ws_get_subs()
    return jsonify({'subscriptions': [
        {'exchange': e, 'symbol': s, 'timeframe': t} for e, s, t in subs
    ]})


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/scan')
def api_scan():
    limited = _rate_limit(
        "api_scan",
        limit=os.environ.get("IKUNANCE_SCAN_RATE_LIMIT", 12),
        window_seconds=os.environ.get("IKUNANCE_SCAN_RATE_WINDOW", 60),
    )
    if limited:
        return _rate_limited_response(limited)

    wait_seconds = _positive_int(os.environ.get("IKUNANCE_SCAN_WAIT_SECONDS"), 2, minimum=0)
    acquired = _scan_gate.acquire(timeout=wait_seconds)
    if not acquired:
        response = jsonify({"status": "error", "msg": "scan busy, please retry shortly", "data": [], "alerts": [], "errors": []})
        response.status_code = 429
        response.headers["Retry-After"] = "3"
        return response

    try:
        uid = get_uid()
        ud = dict(load_user_config(uid))
        ud['_notification_origin'] = 'manual_scan'
        ud['_dedupe_scope'] = f'user:{uid}'
        timeframe = request.args.get('timeframe', '1h')
        trigger_mode = request.args.get('trigger', 'close')
        if timeframe not in ALLOWED_TIMEFRAMES:
            return jsonify({"status": "error", "msg": "invalid timeframe", "data": [], "alerts": [], "errors": []}), 400
        if trigger_mode not in ALLOWED_TRIGGER_MODES:
            return jsonify({"status": "error", "msg": "invalid trigger", "data": [], "alerts": [], "errors": []}), 400
        req_exchange = request.args.get('exchange', '').strip()
        # 闁告挸绉堕顒佸閻樿櫕闄嶉柣?exchange 闁告瑥鍊归弳鐔稿濡搫甯ラ柍銉︽煛閳ь剚鏌ㄦ晶鐘电博椤栨繂娈伴梺顐㈩槸閸亞鎮伴妸锕€鐦诲ù婧垮€栧Σ妤呭箥閳ь剟姊鹃弮鍌ょ€查柨娑樿嫰閹绮╅婵堫€€闁活偀鍋撻悹?        req_exchange = request.args.get('exchange', '').strip()
        if req_exchange:
            ud = dict(ud)           # 婵炴潙鎳忕€氬湱鎷瑰┑鎾剁濞戞挸绉甸挅鍕蓟閹惧啿鏂у┑?ud
            ud['exchange_id'] = req_exchange
        # 闁告挸绉堕顒佸閻樿櫕闄嶉柣?symbols 闁告瑥鍊归弳鐔兼晬閸儮鍋撳Δ鈧ぐ鍧楀礆閸℃稒顓鹃柨娑橆槷缁鳖參宕楅崼顒傜闁告熬绠戦崹顖滄嫚鐠囧弶鍊电紒鏃戝灡鐎垫梹绋婇崨顓烆嚙闁告帗顨夐妴?
        req_symbols = request.args.get('symbols', '').strip()
        legacy_wl = request.args.get('wl', '').strip()
        symbol_source = req_symbols or legacy_wl
        if symbol_source:
            requested = [_normalize_symbol(s) for s in symbol_source.split(',') if _normalize_symbol(s)]
            if len(requested) > MAX_SCAN_SYMBOLS:
                return jsonify({"status": "error", "msg": "too many symbols", "data": [], "alerts": [], "errors": []}), 400
            requested_entries = [
                {"symbol": symbol, "exchange": ud.get('exchange_id', 'binance')}
                for symbol in requested
            ]
        else:
            requested_entries = _normalize_watchlist(ud.get('watchlist', []), ud.get('exchange_id', 'binance'))[:WATCHLIST_LIMIT]
        scan_entries, validation_errors = _verified_watchlist_entries(requested_entries, ud.get('exchange_id', 'binance'))
        if not scan_entries:
            return jsonify({"data": [], "alerts": [], "alert_config": ud['alert_settings'], "errors": validation_errors})
        def run_task(entry):
            s = entry['symbol']
            entry_ud = dict(ud)
            entry_ud['exchange_id'] = entry.get('exchange') or ud.get('exchange_id', 'binance')
            scan_result = _scan_one_symbol(s, timeframe, trigger_mode, entry_ud)
            if len(scan_result) == 2:
                result, sig = scan_result
                scan_error = None
            else:
                result, sig, scan_error = scan_result
            if scan_error:
                return None, None, scan_error
            if result:
                return {
                    "symbol": result.symbol, "price": result.price, "trend": result.trend,
                    "signal": result.signal,
                    "detail": result.detail if result.detail != "-" else "MACD 未满足策略触发条件",
                    "action": result.action,
                    "candle_time": result.candle_time
                }, sig, None
            return None, None, {"symbol": s, "msg": "scan returned no data"}
        scan_futures = [_scan_pool.submit(run_task, entry) for entry in scan_entries]
        results = []
        triggered_alerts = []
        errors = list(validation_errors)
        for f in scan_futures:
            try:
                data, sig, err = f.result(timeout=20)
                if data: results.append(data)
                if sig: triggered_alerts.append(sig)
                if err: errors.append(err)
            except Exception as e:
                print(f"[WARNING] [api_scan] {e}")
                errors.append({"symbol": "", "msg": str(e)})

        # 闁告瑦鍨块埀顑跨窔閳ь剚姘ㄩ悡?(鐎殿喖鍊归?
        if triggered_alerts:
            send_all_notifications(triggered_alerts, ud, timeframe)

        wl_order = {entry['symbol']: i for i, entry in enumerate(scan_entries)}
        results.sort(key=lambda x: wl_order.get(x['symbol'], 999))
        return _user_json_response({"data": results, "alerts": triggered_alerts, "alert_config": ud['alert_settings'], "errors": errors})
    except Exception as e:
        print(f"[ERROR] [api_scan] {e}")
        return jsonify({"data": [], "alerts": [], "alert_config": _default_config()['alert_settings'], "errors": [{"symbol": "", "msg": str(e)}]}), 200
    finally:
        _scan_gate.release()

def _http_get_json(url, *, proxy="", timeout=10):
    proxy = _normalize_proxy_setting(proxy)
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy}) if proxy else urllib.request.ProxyHandler({})
    )
    req = urllib.request.Request(url, headers={"User-Agent": "IKUNANCE/alpha"})
    with opener.open(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", "replace"))

@app.route('/api/scan_io')
def api_scan_io():
    """Return Binance open-interest ranking."""
    ud = get_user_data()
    exchange_id = ud.get('exchange_id', 'binance')
    if exchange_id != 'binance':
        return jsonify({"error": f"open interest ranking currently supports Binance only: {exchange_id}", "data": []})
    try:
        proxy_raw = MANUAL_PROXY or ud.get('proxy', '')
        exchange = get_exchange(ud=ud)
        exchange.load_markets()
        fut_symbols = [s for s in exchange.symbols if s.endswith('/USDT') and exchange.markets[s].get('linear')][:80]
        if not fut_symbols:
            return jsonify({"status": "success", "data": [], "errors": [{"msg": "no linear USDT symbols available"}]})
        results = []
        errors = []
        results_lock = threading.Lock()
        tickers = exchange.fetch_tickers([f for f in fut_symbols[:80]]) or {}
        def fetch_one(sym):
            try:
                bn_sym = sym.replace('/','').replace(':USDT','')
                payload = _http_get_json(
                    f"https://fapi.binance.com/fapi/v1/openInterest?symbol={bn_sym}",
                    proxy=proxy_raw,
                    timeout=10,
                )
                oi_now = float(payload['openInterest'])
                price = tickers.get(sym, {}).get('last', 0)
                if not price: return
                oi_val = round(oi_now * price / 1e6, 2)
                with results_lock:
                    results.append({"symbol": sym, "price": price, "oi_amount": oi_now, "io_value": oi_val})
            except Exception as exc:
                with results_lock:
                    if len(errors) < 5:
                        errors.append({"symbol": sym, "msg": exc.__class__.__name__})
        io_futures = [_scan_pool.submit(fetch_one, s) for s in fut_symbols[:80]]
        for f in io_futures:
            try: f.result(timeout=15)
            except Exception as exc:
                if len(errors) < 5:
                    errors.append({"symbol": "", "msg": exc.__class__.__name__})
        results.sort(key=lambda x: x['io_value'], reverse=True)
        return jsonify({"status": "success", "data": results[:20], "errors": errors})
    except Exception as e:
        print(f"IO fetch error: {e}")
        return jsonify({"status": "error", "data": [], "errors": [{"msg": e.__class__.__name__}]})

# 闁圭顦锕傚及閹炬潙顣?ID 闁绘瑯鍓涢悵娑氱磽閹惧磭鎽犻柛姘墢鐎规娊宕氬Δ鍕┾偓?
_all_symbols_cache: dict = {}   # {exchange_id: {"data": [...], "ts": float}}
_all_symbols_lock = threading.Lock()
_market_movers_cache = {"data": [], "symbols": [], "ts": 0, "key": ""}
_market_movers_lock = threading.Lock()
_orion_market_cache = {"data": [], "ts": 0, "error": ""}
_orion_market_lock = threading.Lock()
_orion_rankings_cache = {"data": {}, "ts": 0, "error": ""}
_orion_rankings_lock = threading.Lock()

CORE_SEARCH_SYMBOLS = [
    'BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT','DOGE/USDT','ADA/USDT','AVAX/USDT','LINK/USDT','TON/USDT',
    'DOT/USDT','LTC/USDT','BCH/USDT','TRX/USDT','NEAR/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT','SEI/USDT',
    'TIA/USDT','INJ/USDT','WLD/USDT','PEPE/USDT','SHIB/USDT','FLOKI/USDT','BONK/USDT','WIF/USDT','BOME/USDT','POPCAT/USDT',
]

HOT_SEARCH_SYMBOLS = [
    'ORDI/USDT','SATS/USDT','ONDO/USDT','JUP/USDT','STRK/USDT','NOT/USDT','DOGS/USDT','NEIRO/USDT','TURBO/USDT',
    'PNUT/USDT','ACT/USDT','GOAT/USDT','MOODENG/USDT','PENGU/USDT','VIRTUAL/USDT','AIXBT/USDT','FARTCOIN/USDT',
    'TRUMP/USDT','MELANIA/USDT','BERA/USDT','KAITO/USDT','HYPE/USDT','PUMP/USDT','WAL/USDT','LAYER/USDT',
    'PARTI/USDT','INIT/USDT','SIGN/USDT','SOPH/USDT','HUMA/USDT',
]

def _normalize_search_symbol(symbol):
    symbol = _normalize_symbol(symbol)
    return symbol if symbol.endswith('/USDT') else ''

def _merge_symbol_lists(*lists):
    merged = []
    seen = set()
    for symbols in lists:
        for symbol in symbols or []:
            normalized = _normalize_search_symbol(symbol)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
    return merged

def _filter_symbols_for_query(symbols, query, limit=30, allow_synthetic=False):
    base_query = str(query or '').upper().replace('/USDT', '').replace('USDT', '').replace('.P', '').strip()
    if not base_query:
        return []
    exact = []
    starts = []
    contains = []
    for symbol in _merge_symbol_lists(symbols):
        base = symbol.split('/')[0]
        if base == base_query:
            exact.append(symbol)
        elif base.startswith(base_query):
            starts.append(symbol)
        elif base_query in base:
            contains.append(symbol)
    results = (exact + starts + contains)[:limit]
    if allow_synthetic and not results and re.fullmatch(r'[A-Z0-9]{2,20}', base_query):
        results.append(f'{base_query}/USDT')
    return results


def _decorate_futures_symbol_records(records):
    rows = []
    for item in records or []:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        display = _normalize_search_symbol(row.get("displayName") or row.get("symbol"))
        if not display:
            continue
        base = display.split("/")[0]
        row.update({
            "symbol": display.replace("/", ""),
            "baseAsset": row.get("baseAsset") or base,
            "quoteAsset": row.get("quoteAsset") or "USDT",
            "contractType": row.get("contractType") or "PERPETUAL",
            "status": row.get("status") or "TRADING",
            "displayName": display,
            "exchange": "binance",
            "exchangeId": "binance",
            "exchangeLabel": "Binance Futures",
            "marketType": "futures",
        })
        rows.append(row)
    return rows


def _binance_stock_symbol_records(query="", limit=1000):
    symbols = _binance_stock_search_symbols()
    matches = _filter_symbols_for_query(symbols, query, limit=limit) if query else _merge_symbol_lists(symbols)
    rows = []
    for display in matches:
        display = _normalize_search_symbol(display)
        if not display:
            continue
        base = display.split("/")[0]
        rows.append({
            "symbol": display.replace("/", ""),
            "baseAsset": base,
            "quoteAsset": "USDT",
            "contractType": "STOCK",
            "status": "TRADING",
            "displayName": display,
            "exchange": "binance_stock",
            "exchangeId": "binance_stock",
            "exchangeLabel": "Binance Stock",
            "marketType": "stock",
            "source": "binance_stock_provider",
        })
    return rows


def _merge_symbol_records(*record_lists):
    merged = []
    seen = set()
    for records in record_lists:
        for item in records or []:
            if not isinstance(item, dict):
                continue
            display = _normalize_search_symbol(item.get("displayName") or item.get("symbol"))
            if not display:
                continue
            exchange_id = str(item.get("exchangeId") or item.get("exchange") or "binance").strip().lower()
            key = (exchange_id, display)
            if key in seen:
                continue
            seen.add(key)
            row = dict(item)
            row["displayName"] = display
            row["exchangeId"] = exchange_id
            row["exchange"] = exchange_id
            merged.append(row)
    return merged


def _display_names_from_records(records):
    names = []
    seen = set()
    for item in records or []:
        display = _normalize_search_symbol(item.get("displayName") if isinstance(item, dict) else item)
        if display and display not in seen:
            seen.add(display)
            names.append(display)
    return names


def _is_binance_futures_exchange_id(exchange_id):
    return str(exchange_id or "binance").strip().lower() == "binance"

def _market_cache_key(ud=None):
    ud = dict(ud or {})
    return "|".join([
        "binance",
        _normalize_proxy_setting(MANUAL_PROXY or ud.get("proxy", "")),
    ])


def _extract_mover_symbols(ud=None):
    now = time.time()
    cache_key = _market_cache_key(ud)
    with _market_movers_lock:
        if (
            _market_movers_cache["key"] == cache_key
            and _market_movers_cache["symbols"]
            and now - _market_movers_cache["ts"] < 60
        ):
            return list(_market_movers_cache["symbols"])
    try:
        user_data = dict(ud or {})
        user_data["exchange_id"] = "binance"
        tickers = _fetch_tickers_safe("binance", ud=user_data)
        ranked = []
        for symbol, ticker in tickers.items():
            if '/USDT' not in symbol:
                continue
            pct = ticker.get('percentage')
            pct = float(pct) if pct is not None else 0.0
            ranked.append((abs(pct), symbol.split(':')[0]))
        ranked.sort(key=lambda item: item[0], reverse=True)
        symbols = [symbol for _, symbol in ranked[:30]]
        with _market_movers_lock:
            _market_movers_cache["symbols"] = symbols
            _market_movers_cache["ts"] = now
            _market_movers_cache["key"] = cache_key
        return symbols
    except Exception as e:
        print(f"[WARN] 闁绘埈鍘介—渚€寮介崶鈺傜暠闁圭粯鍔曡ぐ鍥ㄥ緞鏉堫偉袝: {e}")
        return []


def _base_search_symbols(exchange_id, ud=None):
    if _is_binance_stock_exchange(exchange_id):
        return _binance_stock_search_symbols()
    if _is_binance_futures_exchange_id(exchange_id):
        return _merge_symbol_lists(
            CORE_SEARCH_SYMBOLS,
            HOT_SEARCH_SYMBOLS,
            _list_binance_futures_symbols(force_refresh=False),
            _extract_mover_symbols(ud),
        )
    return _merge_symbol_lists(CORE_SEARCH_SYMBOLS, HOT_SEARCH_SYMBOLS, _extract_mover_symbols(ud))


def _build_market_movers(ud=None):
    user_data = dict(ud or {})
    user_data["exchange_id"] = "binance"
    tickers = _fetch_tickers_safe('binance', ud=user_data)
    rows = []
    for symbol, ticker in tickers.items():
        if '/USDT' not in symbol:
            continue
        pct = ticker.get('percentage') or ticker.get('change') or 0
        try:
            pct = float(pct)
        except (TypeError, ValueError):
            pct = 0
        rows.append({'symbol': _normalize_symbol(symbol), 'price': ticker.get('last') or ticker.get('close') or 0, 'percentage': pct, 'change': pct, 'exchange': 'binance'})
    gainers = sorted(
        (item for item in rows if item.get('percentage', 0) >= 0),
        key=lambda item: item.get('percentage', 0),
        reverse=True,
    )[:5]
    losers = sorted(
        (item for item in rows if item.get('percentage', 0) < 0),
        key=lambda item: item.get('percentage', 0),
    )[:5]
    return gainers + losers


def _orion_change_1d(ticker):
    tf1d = ticker.get("tf1d") or {}
    value = tf1d.get("changePercent")
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_orion_symbol(raw):
    symbol = str(raw or "").strip().upper()
    if not symbol.endswith("USDT"):
        return ""
    return symbol


def _orion_float(value, default=0.0):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def _orion_tf_metric(ticker, timeframe, metric, default=0.0):
    frame = ticker.get(f"tf{timeframe}") or {}
    if not isinstance(frame, dict):
        return default
    return _orion_float(frame.get(metric), default)


def _orion_row(ticker, metric_value, metric_key):
    symbol = _format_orion_symbol(ticker.get("symbol"))
    if not symbol:
        return None
    price = _orion_float(ticker.get("price") or ticker.get("markPrice"))
    return {
        "symbol": symbol,
        "displayName": f"{symbol}\u6c38\u7eed\u5408\u7ea6",
        "price": price,
        "change": metric_value,
        "changePercent": metric_value,
        "metric": metric_key,
        "source": "orion",
    }


def _fetch_orion_screener_payload():
    return _http_get_json(
        "https://screener.orionterminal.com/api/screener",
        proxy=os.environ.get("IKUNANCE_ORION_PROXY", ""),
        timeout=12,
    )


def _build_orion_usdt_perpetuals(limit=10):
    payload = _fetch_orion_screener_payload()
    tickers = payload.get("tickers") if isinstance(payload, dict) else []
    rows = []
    for ticker in tickers or []:
        if not isinstance(ticker, dict):
            continue
        symbol = _format_orion_symbol(ticker.get("symbol"))
        if not symbol:
            continue
        change_1d = _orion_change_1d(ticker)
        try:
            price = float(ticker.get("price") or ticker.get("markPrice") or 0)
        except (TypeError, ValueError):
            price = 0.0
        rows.append({
            "symbol": symbol,
            "displayName": f"{symbol}永续合约",
            "price": price,
            "change": change_1d,
            "changePercent": change_1d,
            "source": "orion",
        })
    rows.sort(key=lambda item: item.get("changePercent", 0), reverse=True)
    return rows[:limit]


def _build_orion_market_rankings(limit=8):
    payload = _fetch_orion_screener_payload()
    tickers = [
        item for item in (payload.get("tickers") if isinstance(payload, dict) else []) or []
        if isinstance(item, dict) and _format_orion_symbol(item.get("symbol"))
    ]

    def build_rank(metric_key, value_fn, sort_key=None, reverse=True):
        rows = []
        for ticker in tickers:
            value = value_fn(ticker)
            row = _orion_row(ticker, value, metric_key)
            if row:
                rows.append(row)
        rows.sort(key=sort_key or (lambda item: item.get("changePercent", 0)), reverse=reverse)
        return rows[:limit]

    return {
        "gainers": build_rank(
            "1d_change",
            lambda ticker: _orion_tf_metric(ticker, "1d", "changePercent"),
            reverse=True,
        ),
        "losers": build_rank(
            "1d_change",
            lambda ticker: _orion_tf_metric(ticker, "1d", "changePercent"),
            reverse=False,
        ),
        "oiMovers": build_rank(
            "1h_oi_change",
            lambda ticker: _orion_tf_metric(ticker, "1h", "oiChange"),
            sort_key=lambda item: abs(item.get("changePercent", 0)),
            reverse=True,
        ),
        "volumeSurges": build_rank(
            "1h_volume_change",
            lambda ticker: _orion_tf_metric(ticker, "1h", "volumeChange"),
            reverse=True,
        ),
    }


@app.route('/api/orion/market-rankings')
def api_orion_market_rankings():
    """Return ORION Binance USDT perpetual rankings for the market plaza."""
    try:
        limit = _positive_int(request.args.get("limit"), 8, minimum=1)
        limit = min(limit, 20)
        ttl = _positive_int(os.environ.get("IKUNANCE_ORION_CACHE_SECONDS"), 15, minimum=5)
        now = time.time()
        with _orion_rankings_lock:
            if _orion_rankings_cache["data"] and now - _orion_rankings_cache["ts"] < ttl:
                return jsonify({
                    "status": "success",
                    "source": "orion",
                    "cached": True,
                    "rankings": _orion_rankings_cache["data"],
                    "updatedAt": _orion_rankings_cache["ts"],
                })
        rankings = _build_orion_market_rankings(limit=limit)
        with _orion_rankings_lock:
            _orion_rankings_cache["data"] = rankings
            _orion_rankings_cache["ts"] = now
            _orion_rankings_cache["error"] = ""
        return jsonify({
            "status": "success",
            "source": "orion",
            "cached": False,
            "rankings": rankings,
            "updatedAt": now,
        })
    except Exception as e:
        with _orion_rankings_lock:
            cached = dict(_orion_rankings_cache["data"] or {})
            cached_ts = _orion_rankings_cache["ts"]
            _orion_rankings_cache["error"] = str(e)
        return jsonify({
            "status": "success" if cached else "error",
            "source": "orion",
            "cached": bool(cached),
            "rankings": cached,
            "updatedAt": cached_ts,
            "error": str(e),
        })


@app.route('/api/orion/usdt-perpetuals')
def api_orion_usdt_perpetuals():
    """Return ORION Binance USDT perpetual daily gainers for the market plaza."""
    try:
        limit = _positive_int(request.args.get("limit"), 10, minimum=1)
        limit = min(limit, 30)
        ttl = _positive_int(os.environ.get("IKUNANCE_ORION_CACHE_SECONDS"), 15, minimum=5)
        now = time.time()
        with _orion_market_lock:
            if _orion_market_cache["data"] and now - _orion_market_cache["ts"] < ttl:
                return jsonify({
                    "status": "success",
                    "source": "orion",
                    "cached": True,
                    "data": list(_orion_market_cache["data"])[:limit],
                    "updatedAt": _orion_market_cache["ts"],
                })
        data = _build_orion_usdt_perpetuals(limit=limit)
        with _orion_market_lock:
            _orion_market_cache["data"] = data
            _orion_market_cache["ts"] = now
            _orion_market_cache["error"] = ""
        return jsonify({
            "status": "success",
            "source": "orion",
            "cached": False,
            "data": data,
            "updatedAt": now,
        })
    except Exception as e:
        with _orion_market_lock:
            cached = list(_orion_market_cache["data"])
            cached_ts = _orion_market_cache["ts"]
            _orion_market_cache["error"] = str(e)
        return jsonify({
            "status": "success" if cached else "error",
            "source": "orion",
            "cached": bool(cached),
            "data": cached[:10],
            "updatedAt": cached_ts,
            "error": str(e),
        })


@app.route('/api/market_movers')
def api_market_movers():
    """Return Binance USDT perpetual top 5 gainers and top 5 losers."""
    now = time.time()
    ud = get_user_data()
    cache_key = _market_cache_key(ud)
    with _market_movers_lock:
        if (
            _market_movers_cache["key"] == cache_key
            and _market_movers_cache["data"]
            and now - _market_movers_cache["ts"] < 60
        ):
            return jsonify({'status': 'success', 'data': list(_market_movers_cache["data"]), 'cached': True})
    try:
        data = _build_market_movers(ud)
        with _market_movers_lock:
            _market_movers_cache["data"] = data
            _market_movers_cache["symbols"] = [item["symbol"] for item in data]
            _market_movers_cache["ts"] = now
            _market_movers_cache["key"] = cache_key
        return jsonify({'status': 'success', 'data': data, 'cached': False})
    except Exception as e:
        with _market_movers_lock:
            cached = list(_market_movers_cache["data"])
        return jsonify({'status': 'error' if not cached else 'success', 'data': cached, 'cached': bool(cached), 'error': str(e)})

    try:
        tickers = _fetch_tickers_safe('binance')
        rows = []
        for symbol, ticker in tickers.items():
            if '/USDT' not in symbol:
                continue
            pct = ticker.get('percentage') or ticker.get('change') or 0
            try:
                pct = float(pct)
            except (TypeError, ValueError):
                pct = 0
            rows.append({'symbol': _normalize_symbol(symbol), 'price': ticker.get('last') or ticker.get('close') or 0, 'percentage': pct, 'change': pct, 'exchange': 'binance'})
        gainers = sorted(
            (item for item in rows if item.get('percentage', 0) >= 0),
            key=lambda item: item.get('percentage', 0),
            reverse=True,
        )[:5]
        losers = sorted(
            (item for item in rows if item.get('percentage', 0) < 0),
            key=lambda item: item.get('percentage', 0),
        )[:5]
        return jsonify({'status': 'success', 'data': gainers + losers})
    except Exception as e:
        return jsonify({'status': 'error', 'data': [], 'error': str(e)})

@app.route('/api/all_symbols')
def api_all_symbols():
    ud = get_user_data()
    exchange_id = request.args.get('exchange', '').strip() or ud.get('exchange_id', 'binance')
    ud = dict(ud)
    ud['exchange_id'] = exchange_id
    now = time.time()
    if _is_binance_futures_exchange_id(exchange_id):
        force = request.args.get("refresh", "").strip() in {"1", "true", "yes"}
        structured = request.args.get("structured", "").strip() in {"1", "true", "yes"}
        futures_rows = _decorate_futures_symbol_records(
            _list_binance_futures_symbols(force_refresh=force, structured=True)
        )
        rows = _merge_symbol_records(futures_rows, _binance_stock_symbol_records())
        if structured:
            return jsonify({
                "status": "success",
                "data": rows,
                "meta": _binance_futures_symbol_health(),
            })
        return jsonify(sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud), _display_names_from_records(rows))))

    with _all_symbols_lock:
        entry = _all_symbols_cache.get(exchange_id, {"data": [], "ts": 0})
        if now - entry["ts"] < 3600 and entry["data"]:
            return jsonify(entry["data"])

    import concurrent.futures
    def _load():
        return _load_linear_usdt_symbols(exchange_id, ud)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_load)
            exchange_symbols = fut.result(timeout=10)
        symbols = sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud), exchange_symbols))
        with _all_symbols_lock:
            _all_symbols_cache[exchange_id] = {"data": symbols, "ts": now}
        return jsonify(symbols)
    except concurrent.futures.TimeoutError:
        print(f"[WARN] Loading all symbols timed out for {exchange_id}; using fallback list")
        return jsonify(sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud))))
    except Exception as e:
        print(f"[WARN] Loading all symbols timed out for {exchange_id}; using fallback list")
        with _all_symbols_lock:
            cached = _all_symbols_cache.get(exchange_id, {}).get("data", [])
        return jsonify(cached or sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud))))

@app.route('/api/search_symbols')
def api_search_symbols():
    query = request.args.get('q', '')
    ud = get_user_data()
    exchange_id = request.args.get('exchange', '').strip() or ud.get('exchange_id', 'binance')
    base_query = str(query or '').upper().replace('/USDT', '').replace('USDT', '').replace('.P', '').strip()
    if len(base_query) < 2:
        return jsonify({"status": "success", "data": [], "query": base_query, "source": "empty", "liveChecked": False})

    if _is_binance_futures_exchange_id(exchange_id):
        force = request.args.get("refresh", "").strip() in {"1", "true", "yes"}
        result = _search_binance_futures_symbols(base_query, force_refresh=force)
        futures_records = _decorate_futures_symbol_records(result.get("records", []))
        stock_records = _binance_stock_symbol_records(base_query, limit=30)
        records = _merge_symbol_records(futures_records, stock_records)
        return jsonify({
            "status": "success",
            "data": _display_names_from_records(records),
            "records": records,
            "query": base_query,
            "exchange": exchange_id,
            "source": f"{result.get('source', 'binance-futures-provider')}+binance-stock-provider",
            "liveChecked": True,
            "fresh": result.get("fresh"),
            "updatedAt": result.get("updatedAt"),
            "errorCode": result.get("errorCode"),
            "error": result.get("error"),
        })

    def _response(data, source, live_checked=False, error=None):
        payload = {
            "status": "error" if error else "success",
            "data": data,
            "query": base_query,
            "exchange": exchange_id,
            "source": source,
            "liveChecked": bool(live_checked),
        }
        if error:
            payload["error"] = str(error)
        return jsonify(payload)

    now = time.time()
    with _all_symbols_lock:
        entry = _all_symbols_cache.get(exchange_id, {"data": [], "ts": 0})
        cached = entry["data"] if now - entry["ts"] < 3600 else []

    base_symbols = sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud), cached))
    cached_results = _filter_symbols_for_query(base_symbols, base_query)
    if cached_results:
        return _response(cached_results, "cache", live_checked=False)

    try:
        import concurrent.futures

        def _load_live_symbols():
            return _load_linear_usdt_symbols(exchange_id, {"exchange_id": exchange_id})

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            raw = pool.submit(_load_live_symbols).result(timeout=12)
        symbols = sorted(_merge_symbol_lists(_base_search_symbols(exchange_id, ud), raw))
        with _all_symbols_lock:
            _all_symbols_cache[exchange_id] = {"data": symbols, "ts": now}
        live_results = _filter_symbols_for_query(symbols, base_query)
        return _response(live_results, "live", live_checked=True)
    except Exception as e:
        print(f"[WARN] Live symbol search failed for {exchange_id} query={base_query}: {e}")
        return _response([], "live-error", live_checked=False, error=e)


@app.route('/api/symbols/binance-futures')
def api_binance_futures_symbols():
    query = request.args.get("q", "")
    force = request.args.get("refresh", "").strip() in {"1", "true", "yes"}
    if query.strip():
        result = _search_binance_futures_symbols(query, force_refresh=force)
        return jsonify({
            "status": "success",
            "data": result.get("records", []),
            "symbols": result.get("data", []),
            "query": result.get("query", ""),
            "meta": {
                "source": result.get("source"),
                "fresh": result.get("fresh"),
                "updatedAt": result.get("updatedAt"),
                "liveChecked": True,
                "errorCode": result.get("errorCode"),
                "error": result.get("error"),
            },
        })
    rows = _list_binance_futures_symbols(force_refresh=force, structured=True)
    return jsonify({
        "status": "success",
        "data": rows,
        "symbols": [row["displayName"] for row in rows],
        "meta": _binance_futures_symbol_health(),
    })


@app.route('/api/symbols/binance-futures/refresh', methods=['POST'])
def api_refresh_binance_futures_symbols():
    snapshot = _refresh_binance_futures_symbols(force=True)
    return jsonify({
        "status": "success",
        "data": snapshot.symbols,
        "symbols": [row["displayName"] for row in snapshot.symbols],
        "meta": _binance_futures_symbol_health(),
    })


@app.route('/api/market/state')
def api_market_state():
    ud = get_user_data()
    exchange_id = request.args.get('exchange', '').strip() or ud.get('exchange_id', 'binance')
    timeframe = request.args.get('timeframe', '15m')
    if timeframe not in ALLOWED_TIMEFRAMES:
        return jsonify({"status": "error", "msg": "invalid timeframe", "data": []}), 400
    raw_symbols = request.args.get('symbols') or request.args.get('symbol') or ''
    symbols = [_normalize_symbol(s) for s in raw_symbols.split(',') if _normalize_symbol(s)]
    if len(symbols) > WATCHLIST_LIMIT:
        return jsonify({"status": "error", "msg": "too many symbols", "data": []}), 400
    ud = dict(ud)
    ud['exchange_id'] = exchange_id
    rows = []
    errors = []
    for symbol in symbols:
        try:
            scan_ud = _scan_ud_for_symbol(ud, symbol)
            ohlcv = _fetch_ohlcv_cached(symbol, timeframe, ud=scan_ud, manual_proxy=MANUAL_PROXY, limit=_scan_kline_limit())
            state = _macd_state_manager.bootstrap(symbol, ohlcv)
            signal = state.classify_current()
            item = state.snapshot()
            item["marketSource"] = _ohlcv_market_source(ohlcv)
            item["currentSignal"] = signal.__dict__ if signal else None
            rows.append(item)
        except Exception as exc:
            errors.append({"symbol": symbol, "msg": f"{type(exc).__name__}: {exc}"})
    return jsonify({"status": "success", "data": rows, "errors": errors, "health": _macd_state_manager.health()})

def _normalize_symbol(raw):
    symbol = str(raw or '').strip().upper()
    if ':' in symbol:
        symbol = symbol.split(':')[0]
    symbol = re.sub(r'(\.P|PERP)$', '', symbol)
    symbol = symbol.replace('/USDTUSDT', '/USDT')
    symbol = symbol.replace('USDTUSDT', 'USDT')
    if not symbol:
        return ''
    if '/' not in symbol and symbol.endswith('USDT'):
        symbol = symbol[:-4]
    if not symbol.endswith('/USDT'):
        symbol = symbol + '/USDT' if '/' not in symbol else symbol
    return symbol

def _normalize_exchange(raw, fallback='binance'):
    exchange = str(raw or fallback or 'binance').strip().lower()
    if _is_binance_stock_exchange(exchange):
        return 'binance_stock'
    return exchange or 'binance'

def _binance_futures_symbol_set():
    try:
        rows = _list_binance_futures_symbols(force_refresh=False, structured=True)
    except Exception:
        rows = []
    symbols = set()
    for row in rows or []:
        if isinstance(row, dict):
            display = _normalize_symbol(row.get("displayName") or row.get("symbol"))
        else:
            display = _normalize_symbol(row)
        if display:
            symbols.add(display)
    return symbols

def _binance_stock_symbol_set():
    try:
        return set(_normalize_symbol(symbol) for symbol in _binance_stock_search_symbols())
    except Exception:
        return set()

def _symbol_verification(symbol, exchange):
    symbol = _normalize_symbol(symbol)
    exchange = _normalize_exchange(exchange)
    if not symbol:
        return {"ok": False, "exchange": exchange, "msg": "symbol required"}
    if exchange == 'binance':
        if symbol in _binance_futures_symbol_set():
            return {"ok": True, "exchange": "binance", "msg": "verified futures symbol"}
        if symbol in _binance_stock_symbol_set():
            return {"ok": True, "exchange": "binance_stock", "msg": "verified Binance Stock/RWA symbol"}
        return {
            "ok": False,
            "exchange": "binance",
            "msg": f"{symbol} is not in verified Binance Futures or Binance Stock/RWA symbols",
        }
    if exchange == 'binance_stock':
        if symbol in _binance_stock_symbol_set():
            return {"ok": True, "exchange": "binance_stock", "msg": "verified Binance Stock/RWA symbol"}
        return {
            "ok": False,
            "exchange": "binance_stock",
            "msg": f"{symbol} is not in verified Binance Stock/RWA symbols",
        }
    return {"ok": True, "exchange": exchange, "msg": "non-Binance exchange validation deferred"}

def _is_binance_stock_symbol(symbol):
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return False
    return symbol in _binance_stock_symbol_set()

def _effective_exchange_for_symbol(exchange, symbol):
    exchange = _normalize_exchange(exchange)
    if exchange == 'binance':
        symbol = _normalize_symbol(symbol)
        if symbol in _binance_futures_symbol_set():
            return 'binance'
        if _is_binance_stock_symbol(symbol):
            return 'binance_stock'
    return exchange

def _scan_ud_for_symbol(ud, symbol):
    base = dict(ud or {})
    effective_exchange = _effective_exchange_for_symbol(base.get('exchange_id') or 'binance', symbol)
    base['exchange_id'] = effective_exchange
    return base

def _watchlist_entry(symbol, exchange):
    symbol = _normalize_symbol(symbol)
    exchange = _effective_exchange_for_symbol(exchange, symbol)
    if not symbol:
        return None
    return {"symbol": symbol, "exchange": exchange}

def _normalize_watchlist(wl, fallback_exchange='binance'):
    """Normalize persisted watchlist entries from old and new schemas."""
    result = []
    seen = set()
    for item in wl:
        if isinstance(item, dict):
            symbol = item.get('symbol') or item.get('display') or item.get('base')
            exchange = item.get('exchange') or item.get('exchangeId') or fallback_exchange
        else:
            symbol = item
            exchange = fallback_exchange
        entry = _watchlist_entry(symbol, exchange)
        if not entry:
            continue
        key = (entry['symbol'], entry['exchange'])
        if key in seen:
            continue
        seen.add(key)
        result.append(entry)
    return result

def _merge_watchlists(existing, incoming, fallback_exchange='binance', replace=False):
    base = [] if replace else _normalize_watchlist(existing or [], fallback_exchange)
    candidates = base + _normalize_watchlist(incoming or [], fallback_exchange)
    result = []
    seen = set()
    for item in candidates:
        key = (item['symbol'], item['exchange'])
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= WATCHLIST_LIMIT:
            break
    return result

def _verified_watchlist_entry(item, fallback_exchange='binance'):
    normalized = _normalize_watchlist([item], fallback_exchange)
    if not normalized:
        return None, "symbol required"
    entry = normalized[0]
    verified = _symbol_verification(entry['symbol'], entry['exchange'])
    if not verified.get("ok"):
        return None, verified.get("msg") or "symbol not verified"
    return {"symbol": entry['symbol'], "exchange": verified.get("exchange") or entry['exchange']}, None

def _verified_watchlist_entries(watchlist, fallback_exchange='binance'):
    entries = []
    errors = []
    seen = set()
    for item in _normalize_watchlist(watchlist or [], fallback_exchange):
        entry, err = _verified_watchlist_entry(item, fallback_exchange)
        if not entry:
            errors.append({"symbol": item.get("symbol", ""), "msg": err})
            continue
        key = (entry['symbol'], entry['exchange'])
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)
    return entries, errors


def _signal_visible_to_user(signal, ud):
    """Return True when a persisted/global signal belongs to the user's monitor scope."""
    if not isinstance(signal, dict):
        return False
    watchlist_mode = ud.get('watchlist_mode') or 'favorites'
    if watchlist_mode == 'all':
        return True
    fallback_exchange = _normalize_exchange(ud.get('exchange_id') or 'binance')
    watchlist, _validation_errors = _verified_watchlist_entries(ud.get('watchlist', []), fallback_exchange)
    sig_symbol = _normalize_symbol(signal.get('symbol', ''))
    sig_exchange = _effective_exchange_for_symbol(signal.get('exchange') or fallback_exchange, sig_symbol)
    if not sig_symbol:
        return False
    return any(
        item['symbol'] == sig_symbol
        and _effective_exchange_for_symbol(item['exchange'], item['symbol']) == sig_exchange
        for item in watchlist
    )


_push_monitor_started = False
_push_monitor_lock = threading.Lock()
_push_monitor_status_lock = threading.Lock()
_push_monitor_status = {
    "started": False,
    "lastRunAt": None,
    "lastFinishedAt": None,
    "nextRunAt": None,
    "activeWindow": False,
    "windowStartedAt": None,
    "windowEndsAt": None,
    "scanPasses": 0,
    "targetCount": 0,
    "signalCount": 0,
    "emailAttempts": 0,
    "emailSent": 0,
    "errors": [],
}


def _push_monitor_status_snapshot():
    with _push_monitor_status_lock:
        snapshot = dict(_push_monitor_status)
        snapshot["errors"] = list(_push_monitor_status.get("errors", []))[-10:]
        return snapshot


def _push_monitor_update(**updates):
    with _push_monitor_status_lock:
        for key, value in updates.items():
            if key == "errors":
                current = list(_push_monitor_status.get("errors", []))
                current.extend(value or [])
                _push_monitor_status["errors"] = current[-10:]
            else:
                _push_monitor_status[key] = value


def _push_monitor_cadence_seconds():
    return 15 * 60


def _push_monitor_window_seconds():
    return min(60, _positive_int(os.environ.get('IKUNANCE_PUSH_MONITOR_WINDOW'), 60, minimum=1))


def _push_monitor_interval_seconds():
    return _push_monitor_cadence_seconds()


def _iso_from_ts(ts):
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()


def _push_monitor_window_state(now_ts=None):
    now_ts = time.time() if now_ts is None else float(now_ts)
    cadence = _push_monitor_cadence_seconds()
    window = _push_monitor_window_seconds()
    boundary = int(now_ts // cadence) * cadence
    window_end = boundary + window
    if now_ts < window_end:
        return {
            "active": True,
            "start_ts": boundary,
            "end_ts": window_end,
            "next_start_ts": boundary + cadence,
            "sleep_seconds": 0,
        }
    next_start = boundary + cadence
    return {
        "active": False,
        "start_ts": next_start,
        "end_ts": next_start + window,
        "next_start_ts": next_start,
        "sleep_seconds": max(1, next_start - now_ts),
    }


def _push_monitor_symbol_pause_seconds():
    try:
        value = float(os.environ.get('IKUNANCE_PUSH_MONITOR_SYMBOL_PAUSE', '0'))
    except (TypeError, ValueError):
        value = 0
    return min(1.0, max(0.0, value))


def _iter_push_monitor_targets():
    """Return unique scan targets, each with the users that should receive email."""
    targets = {}
    for uid in _iter_registered_user_config_uids():
        ud = load_user_config(uid)
        settings = ud.get('alert_settings', {})
        if not settings.get('email') or not ud.get('email') or not ud.get('email_pass'):
            continue
        timeframe = ud.get('timeframe') or '15m'
        if timeframe not in ALLOWED_TIMEFRAMES:
            timeframe = '15m'
        exchange_id = _normalize_exchange(ud.get('exchange_id') or 'binance')
        watchlist, validation_errors = _verified_watchlist_entries(ud.get('watchlist', []), exchange_id)
        if validation_errors:
            print(f"[WARN] [push_monitor] uid={uid} skipped invalid symbols: {validation_errors}", flush=True)
        for item in watchlist:
            key = (item['exchange'], item['symbol'], timeframe)
            target = targets.setdefault(key, {
                'exchange': item['exchange'],
                'symbol': item['symbol'],
                'timeframe': timeframe,
                'scan_ud': {
                    'exchange_id': item['exchange'],
                    '_notification_origin': 'push_monitor',
                    '_dedupe_scope': f"push:{item['exchange']}:{item['symbol']}:{timeframe}",
                },
                'recipients': [],
            })
            target['recipients'].append({'uid': uid, 'ud': ud})
    return list(targets.values())


def _ensure_push_monitor_kline_streams(targets):
    specs = [
        (target.get('symbol'), target.get('timeframe') or '15m')
        for target in targets or []
        if _normalize_exchange(target.get('exchange') or 'binance') == 'binance'
    ]
    if specs:
        _ensure_binance_kline_streams(specs)


def _send_push_monitor_email(uid, signal, ud):
    try:
        if not _signal_visible_to_user(signal, ud):
            return {"status": "skipped", "reason": "outside user watchlist"}
        user_tf = ud.get('timeframe') or '15m'
        if signal.get('timeframe') and signal.get('timeframe') != user_tf:
            return {"status": "skipped", "reason": "timeframe mismatch"}
        settings = ud.get('alert_settings', {})
        if not settings.get('email') or not ud.get('email') or not ud.get('email_pass'):
            return {"status": "skipped", "reason": "email disabled or missing credentials"}

        notify_ud = dict(ud)
        notify_ud['alert_settings'] = {'email': True}
        result = send_all_notifications([signal], notify_ud, signal.get('timeframe') or user_tf)
        print(
            f"[INFO] [push_monitor_email] uid={uid} symbol={signal.get('symbol', '')} "
            f"status={result.get('status')} channels={result.get('channels')}",
            flush=True,
        )
        return result
    except Exception as exc:
        print(f"[WARN] [push_monitor_email] uid={uid}: {type(exc).__name__}: {exc}", flush=True)
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}


def _push_monitor_scan_targets_once(targets, window_end_ts):
    run_errors = []
    signal_count = 0
    email_attempts = 0
    email_sent = 0
    scanned = 0
    symbol_pause = _push_monitor_symbol_pause_seconds()

    for index, target in enumerate(targets):
        if time.time() >= window_end_ts:
            break
        scanned += 1
        try:
            result, signal, scan_error = _scan_one_symbol(
                target['symbol'],
                target['timeframe'],
                'close',
                target['scan_ud'],
            )
            if scan_error:
                run_errors.append(f"{target['symbol']}: {scan_error.get('msg', '')}")
            if signal:
                signal_count += 1
                for recipient in target.get('recipients', []):
                    email_attempts += 1
                    send_result = _send_push_monitor_email(
                        recipient['uid'],
                        signal,
                        recipient['ud'],
                    )
                    channels = send_result.get('channels') or []
                    sent = 0
                    for channel in channels:
                        if channel.get('channel') == 'email' and channel.get('ok'):
                            sent += int(channel.get('sent') or 0)
                    email_sent += sent
                    if send_result.get('status') not in {'success', 'skipped'}:
                        run_errors.append(
                            f"{recipient['uid']} {target['symbol']}: {send_result.get('msg') or send_result.get('error') or send_result.get('status')}"
                        )
        except Exception as exc:
            msg = f"{target['symbol']} {target['timeframe']}: {type(exc).__name__}: {exc}"
            run_errors.append(msg)
            print(f"[WARN] [push_monitor] {msg}", flush=True)
        if symbol_pause and index < len(targets) - 1:
            remaining = window_end_ts - time.time()
            if remaining <= 0:
                break
            time.sleep(min(symbol_pause, remaining))

    return {
        "scanned": scanned,
        "signals": signal_count,
        "email_attempts": email_attempts,
        "email_sent": email_sent,
        "errors": run_errors,
    }


def _push_monitor_loop():
    while True:
        try:
            live_market = os.environ.get("IKUNANCE_LIVE_MARKET", "0").strip() == "1"
            if not live_market or os.environ.get('IKUNANCE_PUSH_MONITOR', '1') == '0':
                window = _push_monitor_window_state()
                _push_monitor_update(
                    started=True,
                    lastRunAt=datetime.now(timezone.utc).isoformat(),
                    lastFinishedAt=datetime.now(timezone.utc).isoformat(),
                    activeWindow=False,
                    nextRunAt=_iso_from_ts(window["next_start_ts"]),
                    windowStartedAt=None,
                    windowEndsAt=None,
                    scanPasses=0,
                    targetCount=0,
                    signalCount=0,
                    emailAttempts=0,
                    emailSent=0,
                    errors=["live market disabled or push monitor disabled"],
                )
                time.sleep(min(60, window["sleep_seconds"]))
                continue

            window = _push_monitor_window_state()
            if not window["active"]:
                targets = _iter_push_monitor_targets()
                _ensure_push_monitor_kline_streams(targets)
                _push_monitor_update(
                    started=True,
                    activeWindow=False,
                    nextRunAt=_iso_from_ts(window["next_start_ts"]),
                    windowStartedAt=None,
                    windowEndsAt=None,
                    scanPasses=0,
                    targetCount=len(targets),
                    signalCount=0,
                    emailAttempts=0,
                    emailSent=0,
                )
                time.sleep(min(60, window["sleep_seconds"]))
                continue

            window_end_ts = window["end_ts"]
            totals = {
                "scanned": 0,
                "signals": 0,
                "email_attempts": 0,
                "email_sent": 0,
                "errors": [],
            }
            scan_passes = 0
            _push_monitor_update(
                started=True,
                lastRunAt=datetime.now(timezone.utc).isoformat(),
                activeWindow=True,
                nextRunAt=_iso_from_ts(window["next_start_ts"]),
                windowStartedAt=_iso_from_ts(window["start_ts"]),
                windowEndsAt=_iso_from_ts(window_end_ts),
                scanPasses=0,
                targetCount=0,
                signalCount=0,
                emailAttempts=0,
                emailSent=0,
            )

            while time.time() < window_end_ts:
                targets = _iter_push_monitor_targets()
                _ensure_push_monitor_kline_streams(targets)
                if not targets:
                    time.sleep(min(5, max(0.1, window_end_ts - time.time())))
                    continue
                scan_passes += 1
                result = _push_monitor_scan_targets_once(targets, window_end_ts)
                totals["scanned"] += result["scanned"]
                totals["signals"] += result["signals"]
                totals["email_attempts"] += result["email_attempts"]
                totals["email_sent"] += result["email_sent"]
                totals["errors"].extend(result["errors"])
                _push_monitor_update(
                    scanPasses=scan_passes,
                    targetCount=len(targets),
                    signalCount=totals["signals"],
                    emailAttempts=totals["email_attempts"],
                    emailSent=totals["email_sent"],
                    errors=result["errors"],
                )

            _push_monitor_update(
                lastFinishedAt=datetime.now(timezone.utc).isoformat(),
                activeWindow=False,
                targetCount=totals["scanned"],
                signalCount=totals["signals"],
                emailAttempts=totals["email_attempts"],
                emailSent=totals["email_sent"],
                errors=totals["errors"],
            )
            print(
                f"[INFO] [push_monitor] window_scanned={totals['scanned']} passes={scan_passes} "
                f"signals={totals['signals']} email_attempts={totals['email_attempts']} "
                f"email_sent={totals['email_sent']} next={_iso_from_ts(window['next_start_ts'])} "
                f"errors={len(totals['errors'])}",
                flush=True,
            )
        except Exception as exc:
            msg = f"loop error: {type(exc).__name__}: {exc}"
            _push_monitor_update(
                lastFinishedAt=datetime.now(timezone.utc).isoformat(),
                errors=[msg],
            )
            print(f"[WARN] [push_monitor] {msg}", flush=True)


def _start_push_monitor_once():
    global _push_monitor_started
    with _push_monitor_lock:
        if _push_monitor_started:
            return
        _push_monitor_started = True
    thread = threading.Thread(target=_push_monitor_loop, name='push_monitor', daemon=True)
    thread.start()


@app.route('/api/add_symbol', methods=['POST'])
def api_add():
    uid = get_uid()
    ud = load_user_config(uid)
    data = request.get_json(silent=True) or {}
    symbol = _normalize_symbol(data.get('symbol', ''))
    requested_exchange = _normalize_exchange(
        data.get('exchange') or data.get('exchangeId') or ud.get('exchange_id', 'binance'),
    )
    timeframe = data.get('timeframe', ud.get('timeframe', '15m')).strip()
    if not symbol:
        return _user_json_response({'status': 'error', 'msg': 'symbol required', 'watchlist': ud.get('watchlist', [])}), 400
    verified = _symbol_verification(symbol, requested_exchange)
    if not verified.get("ok"):
        return _user_json_response({
            'status': 'error',
            'msg': verified.get("msg") or 'symbol not verified',
            'watchlist': ud.get('watchlist', []),
        }), 400
    exchange_id = verified.get("exchange") or requested_exchange
    wl = _normalize_watchlist(ud.get('watchlist', []))
    exists = any(it['symbol'] == symbol and it['exchange'] == exchange_id for it in wl)
    if not exists:
        if len(wl) >= WATCHLIST_LIMIT:
            return jsonify({'status': 'error', 'msg': 'watchlist limit reached'}), 400
        wl.append(_watchlist_entry(symbol, exchange_id))
        ud['watchlist'] = wl
        save_user_config(uid, ud)
    else:
        ud['watchlist'] = wl
    if exchange_id == 'binance':
        _ws_subscribe(exchange_id, symbol, timeframe)
    return _user_json_response({'status': 'success', 'watchlist': ud['watchlist']})

@app.route('/api/remove_symbol', methods=['POST'])
def api_remove():
    uid = get_uid()
    ud = load_user_config(uid)
    data = request.get_json(silent=True) or {}
    symbol = _normalize_symbol(data.get('symbol', ''))
    exchange_id = _effective_exchange_for_symbol(
        data.get('exchange') or data.get('exchangeId') or ud.get('exchange_id', 'binance'),
        symbol,
    )
    timeframe = data.get('timeframe', ud.get('timeframe', '15m')).strip()
    if not symbol:
        return _user_json_response({'status': 'error', 'msg': 'symbol required', 'watchlist': ud.get('watchlist', [])}), 400
    wl = _normalize_watchlist(ud.get('watchlist', []))
    wl = [it for it in wl if not (it['symbol'] == symbol and it['exchange'] == exchange_id)]
    ud['watchlist'] = wl
    save_user_config(uid, ud)
    if exchange_id == 'binance':
        _ws_unsubscribe(exchange_id, symbol, timeframe)
    return _user_json_response({'status': 'success', 'watchlist': ud['watchlist']})

@app.route('/api/save_settings', methods=['POST'])
def api_save():
    uid = get_uid()
    ud = load_user_config(uid)
    data = request.get_json(silent=True) or {}
    if 'apiKey' in data: ud['api_key'] = _text_setting(data.get('apiKey'), 500)
    if 'secretKey' in data: ud['secret_key'] = _text_setting(data.get('secretKey'), 500)
    if 'doubaoApiKey' in data: ud['doubao_api_key'] = _text_setting(data.get('doubaoApiKey'), 500)
    if 'email' in data: ud['email'] = _text_setting(data.get('email'), 320)
    if 'emailPass' in data: ud['email_pass'] = _text_setting(data.get('emailPass'), 500)
    if 'proxy' in data:
        global MANUAL_PROXY
        p = _normalize_proxy_setting(_text_setting(data.get('proxy'), 500))
        ud['proxy'] = p
        if p and "127.0.0.1" in p:
            MANUAL_PROXY = p
            _ws_configure_proxy(MANUAL_PROXY)
    if 'alertSettings' in data:
        if isinstance(data['alertSettings'], dict):
            ud['alert_settings'] = _merge_alert_settings(ud.get('alert_settings'), data['alertSettings'])
    if 'emailTemplate' in data and isinstance(data['emailTemplate'], dict):
        ud['email_template'] = _merge_email_template(ud.get('email_template'), data['emailTemplate'])
    if 'timeframe' in data and data['timeframe'] in ALLOWED_TIMEFRAMES: ud['timeframe'] = data['timeframe']
    if 'triggerMode' in data and data['triggerMode'] in ALLOWED_TRIGGER_MODES: ud['trigger_mode'] = data['triggerMode']
    if 'watchlistMode' in data: ud['watchlist_mode'] = _text_setting(data['watchlistMode'], 40)
    if 'exchangeId' in data: ud['exchange_id'] = _normalize_exchange(data['exchangeId'], ud.get('exchange_id', 'binance'))
    if 'watchlist' in data and isinstance(data['watchlist'], list):
        replace_watchlist = bool(data.get('replaceWatchlist') or data.get('watchlistReplace'))
        ud['watchlist'] = _merge_watchlists(
            ud.get('watchlist', []),
            data['watchlist'],
            ud.get('exchange_id', 'binance'),
            replace=replace_watchlist,
        )
    if 'nickname' in data: ud['nickname'] = _text_setting(data['nickname'], 80)
    if 'webhookUrl'  in data: ud['webhook_url']  = _text_setting(data['webhookUrl'], 1000)
    # 闁规亽鍔戦埀顑跨劍缁楊參鏌嗛幘璇插赋缂?    if 'webhookUrl'  in data: ud['webhook_url']  = data['webhookUrl']
    if 'tgToken'     in data: ud['tg_token']     = _text_setting(data['tgToken'], 500)
    if 'tgChatId'    in data: ud['tg_chat_id']   = _text_setting(data['tgChatId'], 200)
    if 'discordUrl'  in data: ud['discord_url']  = _text_setting(data['discordUrl'], 1000)
    save_user_config(uid, ud)
    return _user_json_response({"status": "success", "msg": "ok", "watchlist": ud.get('watchlist', [])})

UPLOAD_FOLDER = get_data_path('custom_sounds')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/api/upload_sound', methods=['POST'])
def upload_sound():
    uid = get_uid()
    ud = load_user_config(uid)
    if 'file' not in request.files:
        return jsonify({"status": "error", "msg": "婵炲备鍓濆﹢渚€寮崶锔筋偨"})
    f = request.files['file']
    name = request.form.get('name', '').strip()
    source_name = name or f.filename.rsplit('.', 1)[0]
    if not (f.filename or '').lower().endswith('.mp3'):
        return jsonify({"status": "error", "msg": "闁告瑯浜濋弫顕€骞愭稊绁?"})
    safe_name = _safe_sound_filename(source_name)
    f.save(os.path.join(UPLOAD_FOLDER, safe_name))
    custom = ud.get('custom_sounds', [])
    entry = {"name": source_name[:80], "file": safe_name}
    if entry not in custom: custom.append(entry)
    ud['custom_sounds'] = custom
    save_user_config(uid, ud)
    return jsonify({"status": "success", "sounds": custom})

@app.route('/api/list_sounds')
def list_sounds():
    return jsonify(get_user_data().get('custom_sounds', []))

@app.route('/api/sound/<filename>')
def serve_sound(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

@app.route('/api/delete_sound', methods=['POST'])
def delete_sound():
    uid = get_uid()
    ud = load_user_config(uid)
    data = _json_body()
    name = data.get('file', '')
    path = _sound_path(name)
    if path and os.path.exists(path):
        os.remove(path)
    custom = [s for s in ud.get('custom_sounds', []) if s['file'] != name]
    ud['custom_sounds'] = custom
    save_user_config(uid, ud)
    return jsonify({"status": "success", "sounds": custom})

@app.route('/api/get_settings')
def api_get_settings():
    ud = get_user_data()
    return _user_json_response({
        "apiKey": ud.get('api_key', ''), "secretKey": ud.get('secret_key', ''),
        "email": ud.get('email', ''), "emailPass": ud.get('email_pass', ''),
        "proxy": ud.get('proxy', ''), "emailTemplate": _merge_email_template(ud.get('email_template')),
        "alertSettings": _merge_alert_settings(ud.get('alert_settings')),
        "timeframe": ud.get('timeframe', '15m'),
        "triggerMode": ud.get('trigger_mode', 'close'),
        "watchlistMode": ud.get('watchlist_mode', 'favorites'),
        "watchlist": ud.get('watchlist', []),
        "exchangeId": ud.get('exchange_id', 'binance'),
        "customSounds": ud.get('custom_sounds', []),
        "nickname": ud.get('nickname', ''),
        "webhookUrl": ud.get('webhook_url', ''),
        "tgToken":    ud.get('tg_token', ''),
        "tgChatId":   ud.get('tg_chat_id', ''),
        "discordUrl": ud.get('discord_url', ''),
        "doubaoApiKey": ud.get('doubao_api_key', ''),
    })

# 闁冲厜鍋撻柍鍏夊亾 閻犱降鍊涢惁澶屾崉椤栨粍鏆?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

@app.route('/api/auth/register', methods=['POST'])
def auth_register():
    limited = _rate_limit("auth_register")
    if limited:
        return _rate_limited_response(limited)
    data = _json_body()
    # 闁稿繒鍘ч鎰板礈瀹ュ浂浼傞悗娑欘殕椤斿矂鏁嶅鐩縮s / password闁挎稑鐦坰ername / nickname
    password = data.get('pass') or data.get('password', '')
    nickname  = data.get('username') or data.get('nickname', '')
    ok, result = auth_service.register(
        email=data.get('email', ''),
        password=password,
        nickname=nickname,
    )
    if ok:
        # 婵炲鍔岄崬浠嬪箣閹邦剙顫犻柣鈺佺摠鐢瓨娼婚弬鎸庣 token闁挎稑鐭侀鈧柛鎾崇Ф椤忣剟鎯勭€涙ê澶嶉弶鈺傜☉閸?
        email = data.get('email', '').strip().lower()
        result['user'] = {
            'email': result.get('email', email),
            'nickname': result.get('nickname', nickname or email.split('@')[0]),
            'role': result.get('role', 'user'),
        }
    resp = jsonify(result)
    if ok:
        _set_app_cookie(resp, 'ikun_token', result.get('token', ''), 86400*30)
    return resp

@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    limited = _rate_limit("auth_login")
    if limited:
        return _rate_limited_response(limited)
    data = _json_body()
    # 闁稿繒鍘ч鎰板礈瀹ュ浂浼傞悗娑欘殕椤斿矂鏁嶅鐩縮s / password
    password = data.get('pass') or data.get('password', '')
    ok, result = auth_service.login(
        email=data.get('email', ''),
        password=password,
    )
    if ok:
        email = data.get('email', '').strip().lower()
        result['user'] = {
            'email': result.get('email', email),
            'nickname': result.get('nickname', email.split('@')[0]),
            'role': result.get('role', 'user'),
        }
    resp = jsonify(result)
    if ok:
        _set_app_cookie(resp, 'ikun_token', result.get('token', ''), 86400*30)
    return resp

@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    data = _json_body()
    _, result = auth_service.google_login(
        email=data.get('email', ''),
        name=data.get('name', ''),
        gid=data.get('gid', ''),
        picture=data.get('picture', ''),
    )
    resp = jsonify(result)
    if result.get('status') == 'success':
        _set_app_cookie(resp, 'ikun_token', result.get('token', ''), 86400*30)
    return resp

@app.route('/api/auth/check', methods=['POST'])
def auth_check():
    # 闁衡偓椤栨稑鐦☉鎾愁槺椤帡寮悷鎵濞?token闁挎稒鐡琽dy.token / X-Token header / Authorization: Bearer
    data = _json_body()
    token = (
        data.get('token')
        or _request_token()
    )
    ok, result = auth_service.check_session(token)
    if ok:
        result['ok'] = True
        result['status'] = 'ok'
        result['user'] = {
            'email': result.get('email', ''),
            'nickname': result.get('nickname', ''),
            'role': result.get('role', 'user'),
        }
    return jsonify(result)

@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    token = _json_body().get('token', '') or _request_token()
    auth_service.logout(token)
    resp = jsonify({"status": "success"})
    _delete_app_cookie(resp, 'ikun_token')
    return resp

@app.route('/api/auth/forgot', methods=['POST'])
def auth_forgot():
    """Handle password reset requests without leaking whether an account exists."""
    limited = _rate_limit('auth_forgot')
    if limited:
        return _rate_limited_response(limited)
    email = _json_body().get('email', '').strip().lower()
    if not email:
        return jsonify({'status': 'error', 'msg': 'email required'})
    accounts = auth_service.load_accounts()
    if email in accounts.get('users', {}):
        content = f'Password reset was requested for {email}.\nIf this was not you, ignore this message.\n- I-KUNANCE'
        try:
            ud = load_user_config(email)
            sender = ud.get('email', '')
            pwd = ud.get('email_pass', '')
            if sender and pwd:
                from services.email_service import send_email_sync
                send_email_sync('I-KUNANCE password reset', content, sender, pwd)
        except Exception as e:
            print(f'[forgot] email send skipped: {e}')
    return jsonify({'status': 'success', 'msg': 'if the account exists, reset instructions were sent'})

@app.route('/api/auth/change_password', methods=['POST'])
def auth_change_password():
    data = _json_body()
    old_pwd = data.get('oldPassword', '')
    new_pwd = data.get('newPassword', '')
    if not old_pwd or not new_pwd:
        return jsonify({'status': 'error', 'msg': 'oldPassword and newPassword required'})
    if len(new_pwd) < 8:
        return jsonify({'status': 'error', 'msg': 'new password must be at least 8 characters'})
    token = data.get('token') or request.headers.get('X-Token', '') or (request.headers.get('Authorization', '')[7:].strip() if request.headers.get('Authorization', '').startswith('Bearer ') else '')
    ok, result = auth_service.check_session(token)
    if not ok:
        return jsonify({'status': 'error', 'msg': 'invalid session'})
    email = result.get('email', '')
    _, change_result = auth_service.change_password(email, old_pwd, new_pwd)
    return jsonify(change_result)

@app.route('/api/test_push', methods=['POST'])
def api_test_push():
    """Send a test notification to the configured channel."""
    data = _json_body()
    channel = data.get('channel', '')
    return jsonify(send_test_notification(channel, data))

@app.route('/api/signal_history')
def api_signal_history():
    since = request.args.get('since', '')
    ud = get_user_data()
    history = load_signal_history()
    signals = history.get('signals', [])
    if since: signals = [s for s in signals if s.get('time', '') > since]
    signals = [s for s in signals if _signal_visible_to_user(s, ud)]
    return jsonify({"signals": signals})

@app.route('/api/web_signals')
def api_web_signals():
    try:
        since = float(request.args.get('since', '0') or 0)
    except (TypeError, ValueError):
        since = 0
    ud = get_user_data()
    sigs = get_web_signals(since)
    sigs = [s for s in sigs if _signal_visible_to_user(s, ud)]
    clean = [{
        "symbol": s.get("symbol"), "type": s.get("type"), "detail": s.get("detail"),
        "action": s.get("action"), "timeframe": s.get("timeframe"),
        "price": s.get("price"), "trigger_time": s.get("trigger_time"),
        "trigger_time_full": s.get("trigger_time_full"),
        "time": s.get("time"), "_ts": s.get("_ts", 0)
    } for s in sigs]
    return jsonify({"signals": clean})


@app.route('/api/mobile/recent-signals')
def api_mobile_recent_signals():
    try:
        limit = min(50, max(1, int(request.args.get('limit', '20') or 20)))
    except (TypeError, ValueError):
        limit = 20
    try:
        since_ts = float(request.args.get('since', '0') or 0)
    except (TypeError, ValueError):
        since_ts = 0

    ud = get_user_data()
    history = load_signal_history()
    signals = history.get('signals', [])
    if since_ts:
        signals = [s for s in signals if float(s.get('_ts') or 0) > since_ts or float(s.get('candle_time') or 0) / 1000 > since_ts]
    visible = [_mobile_signal_payload(s, ud.get('timeframe') or '15m') for s in signals if _signal_visible_to_user(s, ud)]
    visible.sort(key=lambda s: (float(s.get('_ts') or 0), float(s.get('candle_time') or 0)), reverse=True)
    return jsonify({"status": "success", "signals": visible[:limit]})

# 闁冲厜鍋撻柍鍏夊亾 闁活潿鍔嶉崺娑㈠箰閸ャ劎鍨奸幖?闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?

@app.route('/api/indicators', methods=['GET'])
def api_get_indicators():
    """Return saved user indicators."""
    ud = get_user_data()
    return jsonify({'indicators': ud.get('indicators', [])})

@app.route('/api/indicators/save', methods=['POST'])
def api_save_indicator():
    """Save or replace one user indicator."""
    uid = get_uid()
    ud = load_user_config(uid)
    data = _json_body()
    name = data.get('name', '').strip()
    code = data.get('code', '').strip()
    if not name or not code:
        return jsonify({'status': 'error', 'msg': 'name and code required'})
    if len(name) > 80:
        return jsonify({'status': 'error', 'msg': 'name cannot exceed 80 characters'}), 400
    if len(code) > 20000:
        return jsonify({'status': 'error', 'msg': 'code cannot exceed 20000 characters'}), 400
    indicators = ud.get('indicators', [])
    if not isinstance(indicators, list):
        indicators = []
    existing = next((i for i, x in enumerate(indicators) if x.get('name') == name), None)
    entry = {'id': data.get('id') or uuid.uuid4().hex, 'name': name, 'code': code}
    if existing is not None:
        entry['id'] = indicators[existing].get('id', entry['id'])
        indicators[existing] = entry
    else:
        if len(indicators) >= 50:
            return jsonify({'status': 'error', 'msg': 'indicator limit reached'}), 400
        indicators.append(entry)
    ud['indicators'] = indicators
    save_user_config(uid, ud)
    return jsonify({'status': 'success', 'indicators': indicators})

@app.route('/api/indicators/<indicator_id>', methods=['DELETE'])
def api_delete_indicator(indicator_id):
    """Delete a saved indicator by id."""
    uid = get_uid()
    ud = load_user_config(uid)
    indicators = ud.get('indicators', [])
    if not any(x.get('id') == indicator_id for x in indicators):
        return jsonify({'status': 'error', 'msg': 'indicator not found'}), 404
    indicators = [x for x in indicators if x.get('id') != indicator_id]
    ud['indicators'] = indicators
    save_user_config(uid, ud)
    return jsonify({'status': 'success', 'indicators': indicators})

@app.route('/api/ai/chat', methods=['POST'])
def api_ai_chat():
    """Proxy AI chat requests to Doubao, optionally as an SSE stream."""
    import json as _json, urllib.request as _req, urllib.error
    api_key = os.environ.get('DOUBAO_API_KEY', '')
    if not api_key:
        ud = get_user_data()
        api_key = ud.get('doubao_api_key', '')
    if not api_key:
        return jsonify({'status': 'error', 'msg': 'missing Doubao API key'}), 400
    body = _json_body()
    messages = body.get('messages', [])
    want_stream = body.get('stream', True)
    system_prompt = 'You are an I-KUNANCE quant strategy assistant. Return concise, valid TradingView PineScript v5 when asked for code.'
    payload = {'model': 'doubao-pro-32k-241215', 'messages': [{'role': 'system', 'content': system_prompt}] + messages, 'stream': want_stream, 'max_tokens': 2048, 'temperature': 0.3}
    ark_url = 'https://ark.cn-beijing.volces.com/api/v3/chat/completions'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'}
    if want_stream:
        def generate():
            req = _req.Request(ark_url, data=_json.dumps(payload).encode(), headers=headers, method='POST')
            try:
                with _req.urlopen(req, timeout=60) as resp:
                    for raw in resp:
                        line = raw.decode('utf-8').rstrip('\n\r')
                        if line.startswith('data: '):
                            yield line + '\n\n'
            except urllib.error.HTTPError as e:
                err = e.read().decode('utf-8', errors='replace')
                yield f'data: {_json.dumps({"error": err})}\n\n'
            except Exception as e:
                yield f'data: {_json.dumps({"error": str(e)})}\n\n'
        return Response(generate(), mimetype='text/event-stream', headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'})
    req = _req.Request(ark_url, data=_json.dumps(payload).encode(), headers=headers, method='POST')
    try:
        with _req.urlopen(req, timeout=60) as resp:
            result = _json.loads(resp.read())
        content = result['choices'][0]['message']['content']
        return jsonify({'status': 'success', 'content': content})
    except urllib.error.HTTPError as e:
        return jsonify({
            'status': 'error',
            'msg': 'Doubao upstream request failed',
            'upstreamStatus': e.code,
        }), 502
    except (_json.JSONDecodeError, KeyError, IndexError, TypeError):
        return jsonify({'status': 'error', 'msg': 'invalid Doubao response'}), 502
    except Exception:
        return jsonify({'status': 'error', 'msg': 'Doubao upstream unavailable'}), 502

@app.route('/api/test_email', methods=['POST'])
def api_test_email():
    ud = get_user_data()
    sender = ud.get('email', '')
    password = ud.get('email_pass', '')
    if not sender or not password:
        return jsonify({'status': 'error', 'msg': 'email credentials required'})
    content = 'This is a test email from I-KUNANCE.\n\nYour email notification channel is working.'
    success, msg = send_email_sync('I-KUNANCE test email', content, sender, password)
    if success:
        return jsonify({'status': 'success', 'msg': msg})
    return jsonify({'status': 'error', 'msg': msg})

from services.community_service import (
    get_news, get_posts, create_post, toggle_like, delete_post
)

@app.route('/api/community/news')
def api_community_news():
    force = request.args.get('force', '') == '1'
    return jsonify(get_news(force=force))

@app.route('/api/community/posts')
def api_community_posts():
    page      = _int_query('page', 1, min_value=1)
    page_size = _int_query('page_size', 20, min_value=1, max_value=100)
    tag       = request.args.get('tag', '')
    return jsonify(get_posts(page=page, page_size=page_size, tag=tag))

@app.route('/api/community/posts', methods=['POST'])
def api_community_create_post():
    ud = get_user_data()
    owner_id = get_uid()
    data = _json_body()
    nickname = ud.get('nickname') or ud.get('email', 'anonymous').split('@')[0]
    initials = nickname[:2].upper()
    # 缂佺姭鍋撻柛妤佹礈濞堟垶锛愬鍡楊棌闁告繂鐗嗙粭?
    colors = ['#4db8ff','#0ecb81','#a855f7','#f59e0b','#ef4444','#06b6d4','#84cc16']
    color  = colors[sum(ord(c) for c in nickname) % len(colors)]
    return _anonymous_uid_response(create_post(
        author=nickname, avatar=initials, avatar_color=color,
        content=data.get('content', ''), tag=data.get('tag', ''),
        owner_id=owner_id
    ))

@app.route('/api/community/posts/<post_id>/like', methods=['POST'])
def api_community_like(post_id):
    user_id = get_uid()
    return _anonymous_uid_response(toggle_like(post_id, user_id))

@app.route('/api/community/posts/<post_id>', methods=['DELETE'])
def api_community_delete_post(post_id):
    user_id = get_uid()
    return jsonify(delete_post(post_id, user_id))


_start_push_monitor_once()


if __name__ == '__main__':
    host = os.environ.get('IKUNANCE_HOST', '0.0.0.0')
    port = _positive_int(os.environ.get('IKUNANCE_PORT'), 5000)
    app.run(host=host, port=port, debug=False, threaded=True)
