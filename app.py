from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import ccxt
import pandas as pd
import time
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import threading
from datetime import datetime, timedelta
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__, static_folder='../frontend/dist', static_url_path='')
CORS(app)

MANUAL_PROXY = ""

_exchange_instance = None
_exchange_lock = threading.Lock()
_exchange_proxy_hash = ""
_scan_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="scan_")
_ohlcv_cache = {}
_ohlcv_cache_lock = threading.Lock()
_OHLCV_CACHE_TTL = 15

def _get_singleton_exchange(ud=None):
    global _exchange_instance, _exchange_proxy_hash
    if ud is None:
        ud = _default_config()
    proxy_url = ""
    if MANUAL_PROXY and "127.0.0.1" in MANUAL_PROXY:
        proxy_url = MANUAL_PROXY
    elif ud.get("proxy"):
        proxy_url = ud["proxy"]
    if proxy_url and not proxy_url.startswith("http"):
        proxy_url = f"http://{proxy_url}"
    current_hash = proxy_url
    with _exchange_lock:
        if _exchange_instance is not None and _exchange_proxy_hash == current_hash:
            return _exchange_instance
        config = {'enableRateLimit': True, 'options': {'defaultType': 'future'}, 'timeout': 15000}
        if proxy_url:
            config['proxies'] = {'http': proxy_url, 'https': proxy_url}
            config['verify'] = False
        api_key = ud.get("api_key", "")
        secret_key = ud.get("secret_key", "")
        if api_key and secret_key:
            config['apiKey'] = api_key
            config['secret'] = secret_key
        _exchange_instance = ccxt.binance(config)
        _exchange_proxy_hash = current_hash
        return _exchange_instance

def _fetch_ohlcv_cached(symbol, timeframe, ud=None, limit=300):
    cache_key = (symbol, timeframe)
    now = time.time()
    with _ohlcv_cache_lock:
        if cache_key in _ohlcv_cache:
            fetch_time, data = _ohlcv_cache[cache_key]
            if now - fetch_time < _OHLCV_CACHE_TTL:
                return data
    exchange = _get_singleton_exchange(ud=ud)
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    with _ohlcv_cache_lock:
        _ohlcv_cache[cache_key] = (now, ohlcv)
        expired = [k for k, (t, _) in _ohlcv_cache.items() if now - t > 300]
        for k in expired:
            del _ohlcv_cache[k]
    return ohlcv

USER_DATA_DIR = 'user_data'
os.makedirs(USER_DATA_DIR, exist_ok=True)
ALERTED_FILE = 'alerted_signals.json'
_alerted_lock = threading.Lock()
_alerted_cache = None

def _ensure_alerted_loaded():
    global _alerted_cache
    if _alerted_cache is not None:
        return
    if os.path.exists(ALERTED_FILE):
        try:
            with open(ALERTED_FILE, 'r') as f:
                _alerted_cache = json.load(f)
        except:
            _alerted_cache = {}
    else:
        _alerted_cache = {}
    cutoff = time.time() - 24 * 3600
    to_delete = []
    for key in _alerted_cache:
        parts = key.split('|')
        if len(parts) != 4:
            to_delete.append(key)
            continue
        try:
            candle_ts = int(parts[2]) / 1000
            if candle_ts < cutoff:
                to_delete.append(key)
        except:
            to_delete.append(key)
    for k in to_delete:
        del _alerted_cache[k]

def is_signal_alerted(symbol, timeframe, candle_time, action):
    key = f"{symbol}|{timeframe}|{candle_time}|{action}"
    with _alerted_lock:
        _ensure_alerted_loaded()
        return key in _alerted_cache

def mark_signal_alerted(symbol, timeframe, candle_time, action):
    key = f"{symbol}|{timeframe}|{candle_time}|{action}"
    with _alerted_lock:
        _ensure_alerted_loaded()
        _alerted_cache[key] = time.time()
        def save_alerted():
            try:
                with open(ALERTED_FILE, 'w') as f:
                    json.dump(_alerted_cache, f)
            except Exception as e:
                print(f"⚠️ 保存去重记录失败: {e}")
        threading.Thread(target=save_alerted, daemon=True).start()

_web_signal_cache = []
_web_signal_cache_lock = threading.Lock()
_web_signal_dedup = {}

def _append_web_signal(sig):
    key = f"{sig.get('symbol')}|{sig.get('timeframe')}|{sig.get('candle_time')}|{sig.get('action')}"
    with _web_signal_cache_lock:
        if key in _web_signal_dedup:
            return
        sig['_ts'] = time.time()
        sig['_id'] = key
        _web_signal_cache.append(sig)
        _web_signal_dedup[key] = time.time()
        cutoff = time.time() - 24 * 3600
        _web_signal_cache[:] = [s for s in _web_signal_cache if s.get('_ts', 0) > cutoff]
        _web_signal_dedup.clear()
        for s in _web_signal_cache:
            _web_signal_dedup[s['_id']] = s['_ts']

def _pop_web_signals(since_ts=0):
    with _web_signal_cache_lock:
        return [s for s in _web_signal_cache if s.get('_ts', 0) > since_ts]

_signal_history_lock = threading.Lock()

def _append_signal_safe(signal_obj):
    key = f"{signal_obj.get('symbol')}|{signal_obj.get('timeframe')}|{signal_obj.get('candle_time')}|{signal_obj.get('action')}"
    with _signal_history_lock:
        history = load_signal_history()
        existing = set()
        for s in history['signals']:
            ek = f"{s.get('symbol')}|{s.get('timeframe')}|{s.get('candle_time')}|{s.get('action')}"
            existing.add(ek)
        if key not in existing:
            history['signals'].append(signal_obj)
            save_signal_history(history)

LAST_ALERT_CANDLE = {}
_user_cache = {}
_user_cache_lock = threading.Lock()

def _default_config():
    return {
        "api_key": "", "secret_key": "", "email": "", "email_pass": "",
        "proxy": "", "watchlist": [],
        "alert_settings": {
            "app_push": False, "toast": True, "email": True,
            "webhook": False, "sound": True, "sound_type": "beep"
        },
        "email_template": {
            "include_price": True, "include_trend": True,
            "include_signal": True, "include_detail": True, "include_action": True
        }
    }

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
            with open(path, 'r') as f:
                saved = json.load(f)
            for k, v in saved.items():
                if k == "alert_settings" and isinstance(v, dict):
                    conf[k].update(v)
                else:
                    conf[k] = v
        except:
            pass
    with _user_cache_lock:
        _user_cache[uid] = conf
    return conf

def save_user_config(uid, config):
    with _user_cache_lock:
        _user_cache[uid] = config
    with open(_user_file(uid), 'w') as f:
        json.dump(config, f, ensure_ascii=False)

def get_uid():
    token = request.headers.get('X-Token', '') or request.cookies.get('ikun_token', '')
    if token:
        accounts = load_accounts()
        email = accounts.get('sessions', {}).get(token)
        if email:
            return email
    sid = request.cookies.get('ikun_sid', '')
    if not sid:
        sid = str(uuid.uuid4())
    return 'anon_' + sid

def get_user_data():
    return load_user_config(get_uid())

USER_DATA = _default_config()

def send_email(subject, content, sender=None, password=None):
    if not sender or not password:
        return
    receiver = sender
    try:
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = Header('I-KUNANCE', 'utf-8').encode() + f" <{sender}>"
        msg['To'] = receiver
        msg['Subject'] = Header(subject, 'utf-8')
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        use_ssl = False
        if "qq.com" in sender:
            smtp_server = "smtp.qq.com"; smtp_port = 465; use_ssl = True
        elif "163.com" in sender:
            smtp_server = "smtp.163.com"; smtp_port = 465; use_ssl = True
        if use_ssl:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        server.login(sender, password)
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()
        print(f"📧 [邮件发送成功] {subject} → {receiver}")
    except Exception as e:
        print(f"❌ [邮件发送失败] {subject} | {e}")

def trigger_alert(symbol, signal, detail, action, price, candle_time, ud=None, timeframe='1h'):
    tf_minutes = {'1m':1,'5m':5,'15m':15,'30m':30,'1h':60,'4h':240,'1d':1440}.get(timeframe, 60)
    if candle_time:
        open_dt = datetime.fromtimestamp(candle_time / 1000)
        close_ts = candle_time + tf_minutes * 60 * 1000
        close_dt = datetime.fromtimestamp(close_ts / 1000)
        trigger_time_str = close_dt.strftime('%H:%M')
        trigger_time_full = close_dt.strftime('%Y-%m-%d %H:%M:%S')
        open_time_full = open_dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        now = datetime.now()
        trigger_time_str = now.strftime('%H:%M')
        trigger_time_full = now.strftime('%Y-%m-%d %H:%M:%S')
        open_time_full = trigger_time_full
    if action == "-":
        return
    if candle_time:
        _ct_close_ms = candle_time + tf_minutes * 60 * 1000
        if (time.time() * 1000) - _ct_close_ms > tf_minutes * 60 * 4 * 1000:
            return False
    if is_signal_alerted(symbol, timeframe, candle_time, action):
        return False
    mark_signal_alerted(symbol, timeframe, candle_time, action)
    if not ud:
        ud = _default_config()
    _append_web_signal({
        "symbol": symbol, "type": signal, "detail": detail,
        "action": action, "timeframe": timeframe, "price": price,
        "time": trigger_time_full, "trigger_time": trigger_time_str,
        "trigger_time_full": trigger_time_full, "open_time_full": open_time_full,
        "candle_time": candle_time
    })
    _append_signal_safe({
        "symbol": symbol, "type": signal, "detail": detail,
        "action": action, "timeframe": timeframe,
        "time": trigger_time_full, "trigger_time": trigger_time_str,
        "trigger_time_full": trigger_time_full, "open_time_full": open_time_full,
        "candle_time": candle_time
    })
    return {
        "symbol": symbol, "signal": signal, "detail": detail,
        "action": action, "price": price,
        "open_time": open_time_full, "close_time": trigger_time_full,
        "timeframe": timeframe
    }

def send_batch_signals_email(signals, ud, timeframe):
    if not signals:
        return
    sender = ud.get('email', '')
    password = ud.get('email_pass', '')
    if not sender or not password:
        return
    alert_settings = ud.get('alert_settings', {})
    if not alert_settings.get('email', True):
        return
    title_parts = [f"{s['symbol'].replace('/USDT','')} {s['action']}" for s in signals]
    close_time_str = signals[0]['close_time'][11:16] if signals else ''
    subject = f"[{timeframe}] {'  |  '.join(title_parts)} @ {close_time_str}"
    tpl = ud.get('email_template', {})
    lines = [f"I-KUNANCE 信号汇总 | {timeframe} | 收盘时间: {close_time_str}", "=" * 50]
    for i, s in enumerate(signals, 1):
        lines.append(f"\n【{i}】{s['symbol']}  →  {s['action']}")
        if tpl.get('include_price', True): lines.append(f"  价格: {s['price']}")
        if tpl.get('include_signal', True): lines.append(f"  信号: {s['signal']}")
        if tpl.get('include_detail', True): lines.append(f"  详情: {s['detail']}")
        if tpl.get('include_action', True): lines.append(f"  建议: {'做多 LONG' if s['action']=='LONG' else '做空 SHORT'}")
        lines.append(f"  开盘时间: {s['open_time']}")
        lines.append(f"  收盘时间: {s['close_time']}")
        lines.append(f"  周期: {s['timeframe']}")
        lines.append("-" * 40)
    lines.append("\n发送人: I-KUNANCE")
    try:
        send_email(subject, "\n".join(lines), sender, password)
    except Exception as e:
        print(f"❌ [批量邮件] {e}")

def get_exchange(ud=None):
    if ud is None:
        try:
            ud = get_user_data()
        except:
            ud = _default_config()
    return _get_singleton_exchange(ud=ud)

def analyze_symbol(symbol, timeframe, trigger_mode='close', ud=None):
    try:
        ohlcv = _fetch_ohlcv_cached(symbol, timeframe, ud=ud, limit=300)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        k = df['close'].ewm(span=12, adjust=False, min_periods=12).mean()
        d = df['close'].ewm(span=26, adjust=False, min_periods=26).mean()
        macd = k - d
        signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
        hist = macd - signal
        ema200 = df['close'].ewm(span=200, adjust=False, min_periods=200).mean()
        idx = -2 if trigger_mode == 'close' else -1
        curr = df.iloc[idx]
        prev1_hist = hist.iloc[idx - 1]
        prev2_hist = hist.iloc[idx - 2]
        curr_hist = hist.iloc[idx]
        trend = "BULL" if curr['close'] > ema200.iloc[idx] else "BEAR"
        curr_green = curr_hist > 0
        sig_name = "-"; sig_detail = "-"; action = "-"
        shrink_head = (curr_hist < prev1_hist) and (prev1_hist > prev2_hist) and (prev1_hist > 0)
        shrink_foot = (curr_hist > prev1_hist) and (prev1_hist < prev2_hist) and (prev1_hist < 0)
        red_strengthen = (not curr_green) and (curr_hist < prev1_hist) and (prev1_hist > prev2_hist)
        green_strengthen = curr_green and (curr_hist > prev1_hist) and (prev1_hist < prev2_hist)
        if shrink_foot:
            sig_name = "🟣 趋势确认"; sig_detail = "红柱缩短 (收脚)"; action = "LONG"
        elif green_strengthen:
            sig_name = "🟡 趋势演进"; sig_detail = "绿柱首次增强"; action = "LONG"
        elif shrink_head:
            sig_name = "🟣 趋势确认"; sig_detail = "绿柱缩短 (缩头)"; action = "SHORT"
        elif red_strengthen:
            sig_name = "🟡 趋势演进"; sig_detail = "红柱首次增强"; action = "SHORT"
        elif prev1_hist < 0 and curr_hist > 0:
            sig_name = "🟡 趋势演进"; sig_detail = "红柱转绿柱(零轴突破)"; action = "LONG"
        elif prev1_hist > 0 and curr_hist < 0:
            sig_name = "🟡 趋势演进"; sig_detail = "绿柱转红柱(零轴跌破)"; action = "SHORT"
        return {
            "symbol": symbol, "price": curr['close'], "trend": trend,
            "signal": sig_name, "detail": sig_detail, "action": action,
            "candle_time": curr['timestamp']
        }
    except Exception as e:
        print(f"❌ {symbol}: {type(e).__name__} - {str(e)}")
        return None

import requests as http_req

def fetch_io_top20():
    try:
        px = None
        if MANUAL_PROXY and "127.0.0.1" in MANUAL_PROXY:
            px = {"http": MANUAL_PROXY, "https": MANUAL_PROXY}
        elif USER_DATA.get("proxy"):
            p = USER_DATA["proxy"]
            if not p.startswith("http"): p = "http://" + p
            px = {"http": p, "https": p}
        exchange = get_exchange()
        exchange.load_markets()
        fut_symbols = [s for s in exchange.symbols if s.endswith('/USDT') and exchange.markets[s].get('linear')][:80]
        results = []
        results_lock = threading.Lock()
        tickers = exchange.fetch_tickers([f for f in fut_symbols[:80]])
        def fetch_one(sym):
            try:
                bn_sym = sym.replace('/','')
                r = http_req.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={bn_sym}", proxies=px, verify=False, timeout=10)
                oi_now = float(r.json()['openInterest'])
                price = tickers.get(sym, {}).get('last', 0)
                if not price: return
                oi_val = round(oi_now * price / 1e6, 2)
                with results_lock:
                    results.append({"symbol": sym, "price": price, "oi_amount": oi_now, "io_value": oi_val})
            except:
                pass
        io_futures = [_scan_pool.submit(fetch_one, s) for s in fut_symbols[:80]]
        for f in io_futures:
            try: f.result(timeout=15)
            except: pass
        results.sort(key=lambda x: x['io_value'], reverse=True)
        return results[:20]
    except Exception as e:
        print(f"IO fetch error: {e}")
        return []

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/scan')
def api_scan():
    ud = get_user_data()
    timeframe = request.args.get('timeframe', '1h')
    trigger_mode = request.args.get('trigger', 'close')
    wl = ud['watchlist'][:10]
    if not wl:
        return jsonify({"data": [], "alerts": [], "alert_config": ud['alert_settings']})
    def run_task(s):
        result = analyze_symbol(s, timeframe, trigger_mode, ud=ud)
        if result and result.get('action') != "-":
            trigger_alert(symbol=result['symbol'], signal=result['signal'], detail=result['detail'],
                action=result['action'], price=result['price'], candle_time=result['candle_time'],
                ud=ud, timeframe=timeframe)
        return result
    scan_futures = [_scan_pool.submit(run_task, s) for s in wl]
    results = []
    for f in scan_futures:
        try:
            data = f.result(timeout=20)
            if data: results.append(data)
        except Exception as e:
            print(f"⚠️ [api_scan] {e}")
    wl_order = {s: i for i, s in enumerate(wl)}
    results.sort(key=lambda x: wl_order.get(x['symbol'], 999))
    resp = jsonify({"data": results, "alerts": [], "alert_config": ud['alert_settings']})
    if get_uid().startswith('anon_'):
        resp.set_cookie('ikun_sid', get_uid().replace('anon_', ''), max_age=86400*365)
    return resp

@app.route('/api/scan_io')
def api_scan_io():
    return jsonify(fetch_io_top20())

ALL_SYMBOLS_CACHE = {"data": [], "ts": 0}

@app.route('/api/all_symbols')
def api_all_symbols():
    now = time.time()
    if now - ALL_SYMBOLS_CACHE["ts"] > 3600 or not ALL_SYMBOLS_CACHE["data"]:
        try:
            ex = get_exchange()
            ex.load_markets()
            raw = [s for s in ex.symbols if '/USDT' in s and ex.markets[s].get('linear')]
            ALL_SYMBOLS_CACHE["data"] = sorted(list(set([s.split(':')[0] for s in raw])))
            ALL_SYMBOLS_CACHE["ts"] = now
        except Exception as e:
            print(f"❌ 加载标的失败: {e}")
            if not ALL_SYMBOLS_CACHE["data"]: return jsonify([])
    return jsonify(ALL_SYMBOLS_CACHE["data"])

@app.route('/api/add_symbol', methods=['POST'])
def api_add():
    uid = get_uid()
    ud = load_user_config(uid)
    symbol = request.json.get('symbol', '').strip().upper()
    if ':' in symbol: symbol = symbol.split(':')[0]
    if not symbol.endswith('/USDT'):
        symbol = symbol + '/USDT' if '/' not in symbol else symbol
    if symbol not in ud['watchlist']:
        if len(ud['watchlist']) >= 10:
            return jsonify({"status": "error", "msg": "自选上限10个，请先移除"})
        ud['watchlist'].append(symbol)
        save_user_config(uid, ud)
    return jsonify({"status": "success", "watchlist": ud['watchlist']})

@app.route('/api/remove_symbol', methods=['POST'])
def api_remove():
    uid = get_uid()
    ud = load_user_config(uid)
    symbol = request.json.get('symbol', '').strip()
    if symbol in ud['watchlist']:
        ud['watchlist'].remove(symbol)
        save_user_config(uid, ud)
    return jsonify({"status": "success", "watchlist": ud['watchlist']})

@app.route('/api/save_settings', methods=['POST'])
def api_save():
    uid = get_uid()
    ud = load_user_config(uid)
    data = request.json
    if 'apiKey' in data: ud['api_key'] = data.get('apiKey', '')
    if 'secretKey' in data: ud['secret_key'] = data.get('secretKey', '')
    if 'email' in data: ud['email'] = data.get('email', '')
    if 'emailPass' in data: ud['email_pass'] = data.get('emailPass', '')
    if 'proxy' in data:
        global MANUAL_PROXY
        p = data.get('proxy', '').strip()
        ud['proxy'] = p
        if p:
            if not p.startswith('http'): p = 'http://' + p
            MANUAL_PROXY = p
    if 'alertSettings' in data:
        if 'alert_settings' not in ud: ud['alert_settings'] = {}
        ud['alert_settings'].update(data['alertSettings'])
    if 'emailTemplate' in data: ud['email_template'] = data['emailTemplate']
    if 'timeframe' in data: ud['timeframe'] = data['timeframe']
    if 'triggerMode' in data: ud['trigger_mode'] = data['triggerMode']
    save_user_config(uid, ud)
    return jsonify({"status": "success", "msg": "配置已保存"})

UPLOAD_FOLDER = 'custom_sounds'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/api/upload_sound', methods=['POST'])
def upload_sound():
    uid = get_uid()
    ud = load_user_config(uid)
    if 'file' not in request.files: return jsonify({"status": "error", "msg": "没有文件"})
    f = request.files['file']
    name = request.form.get('name', '').strip()
    if not name: name = f.filename.rsplit('.', 1)[0]
    if not f.filename.endswith('.mp3'): return jsonify({"status": "error", "msg": "只支持mp3"})
    safe_name = name.replace(' ', '_').replace('/', '_') + '.mp3'
    f.save(os.path.join(UPLOAD_FOLDER, safe_name))
    custom = ud.get('custom_sounds', [])
    entry = {"name": name, "file": safe_name}
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
    name = request.json.get('file', '')
    path = os.path.join(UPLOAD_FOLDER, name)
    if os.path.exists(path): os.remove(path)
    custom = [s for s in ud.get('custom_sounds', []) if s['file'] != name]
    ud['custom_sounds'] = custom
    save_user_config(uid, ud)
    return jsonify({"status": "success", "sounds": custom})

@app.route('/api/get_settings')
def api_get_settings():
    ud = get_user_data()
    return jsonify({
        "apiKey": ud.get('api_key', ''), "secretKey": ud.get('secret_key', ''),
        "email": ud.get('email', ''), "emailPass": ud.get('email_pass', ''),
        "proxy": ud.get('proxy', ''), "emailTemplate": ud.get('email_template', {}),
        "alertSettings": ud.get('alert_settings', {}),
        "timeframe": ud.get('timeframe', '15m'),
        "triggerMode": ud.get('trigger_mode', 'close'),
        "watchlist": ud.get('watchlist', [])
    })

ACCOUNTS_FILE = 'user_accounts.json'
SIGNALS_FILE = 'signal_history.json'

def load_signal_history():
    if os.path.exists(SIGNALS_FILE):
        try:
            with open(SIGNALS_FILE, 'r') as f:
                data = json.load(f)
            cutoff = (datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')
            data['signals'] = [s for s in data.get('signals', []) if s.get('time', '') >= cutoff]
            return data
        except:
            pass
    return {"signals": []}

def save_signal_history(data):
    with open(SIGNALS_FILE, 'w') as f:
        json.dump(data, f, ensure_ascii=False)

def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"users": {}, "sessions": {}}

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, 'w') as f:
        json.dump(accounts, f)

@app.route('/api/auth/register', methods=['POST'])
def auth_register():
    data = request.json
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    nickname = data.get('nickname', '').strip()
    if not email or not password: return jsonify({"status": "error", "msg": "邮箱和密码不能为空"})
    if len(password) < 6: return jsonify({"status": "error", "msg": "密码至少6位"})
    accounts = load_accounts()
    if email in accounts['users']: return jsonify({"status": "error", "msg": "该邮箱已注册"})
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    token = str(uuid.uuid4())
    accounts['users'][email] = {
        "password": pw_hash, "nickname": nickname or email.split('@')[0],
        "avatar": "", "provider": "local", "created": str(datetime.now())
    }
    accounts['sessions'][token] = email
    save_accounts(accounts)
    return jsonify({"status": "success", "token": token, "user": {
        "email": email, "nickname": accounts['users'][email]['nickname'], "provider": "local"
    }})

@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    data = request.json
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    accounts = load_accounts()
    user = accounts['users'].get(email)
    if not user: return jsonify({"status": "error", "msg": "账号不存在"})
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if user['password'] != pw_hash: return jsonify({"status": "error", "msg": "密码错误"})
    token = str(uuid.uuid4())
    accounts['sessions'][token] = email
    save_accounts(accounts)
    return jsonify({"status": "success", "token": token, "user": {
        "email": email, "nickname": user['nickname'], "provider": user.get('provider', 'local')
    }})

@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    data = request.json
    email = data.get('email', '').strip().lower()
    name = data.get('name', '')
    gid = data.get('gid', '')
    if not email: return jsonify({"status": "error", "msg": "Google 登录失败"})
    accounts = load_accounts()
    if email not in accounts['users']:
        accounts['users'][email] = {
            "password": "", "nickname": name or email.split('@')[0],
            "avatar": data.get('picture', ''), "provider": "google",
            "google_id": gid, "created": str(datetime.now())
        }
    token = str(uuid.uuid4())
    accounts['sessions'][token] = email
    save_accounts(accounts)
    user_info = accounts['users'][email]
    return jsonify({"status": "success", "token": token, "user": {
        "email": email, "nickname": user_info['nickname'],
        "provider": user_info.get('provider', 'google'), "avatar": user_info.get('avatar', '')
    }})

@app.route('/api/auth/check', methods=['POST'])
def auth_check():
    token = request.json.get('token', '')
    accounts = load_accounts()
    email = accounts['sessions'].get(token)
    if not email or email not in accounts['users']: return jsonify({"status": "error"})
    user = accounts['users'][email]
    return jsonify({"status": "success", "user": {
        "email": email, "nickname": user['nickname'],
        "provider": user.get('provider', 'local'), "avatar": user.get('avatar', '')
    }})

@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    token = request.json.get('token', '')
    accounts = load_accounts()
    accounts['sessions'].pop(token, None)
    save_accounts(accounts)
    return jsonify({"status": "success"})

@app.route('/api/signal_history')
def api_signal_history():
    since = request.args.get('since', '')
    history = load_signal_history()
    signals = history.get('signals', [])
    if since: signals = [s for s in signals if s.get('time', '') > since]
    return jsonify({"signals": signals})

@app.route('/api/web_signals')
def api_web_signals():
    since = float(request.args.get('since', '0'))
    sigs = _pop_web_signals(since)
    clean = [{
        "symbol": s.get("symbol"), "type": s.get("type"), "detail": s.get("detail"),
        "action": s.get("action"), "timeframe": s.get("timeframe"),
        "price": s.get("price"), "trigger_time": s.get("trigger_time"),
        "trigger_time_full": s.get("trigger_time_full"),
        "time": s.get("time"), "_ts": s.get("_ts", 0)
    } for s in sigs]
    return jsonify({"signals": clean})

@app.route('/api/test_email', methods=['POST'])
def api_test_email():
    ud = get_user_data()
    sender = ud.get("email", "")
    password = ud.get("email_pass", "")
    if not sender or not password:
        return jsonify({"status": "error", "msg": "请先配置邮箱和应用密码"})
    try:
        msg = MIMEText("这是 I-KUNANCE 信号监控系统的测试邮件。\n\n如果收到说明配置成功！\n\n发送人: IKUNANCE", 'plain', 'utf-8')
        msg['From'] = f"IKUNANCE <{sender}>"
        msg['To'] = sender
        msg['Subject'] = Header("🧪 I-KUNANCE 测试邮件", 'utf-8')
        smtp_server = "smtp.gmail.com"; smtp_port = 587; use_ssl = False
        if "qq.com" in sender: smtp_server = "smtp.qq.com"; smtp_port = 465; use_ssl = True
        elif "163.com" in sender: smtp_server = "smtp.163.com"; smtp_port = 465; use_ssl = True
        if use_ssl:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        server.login(sender, password)
        server.sendmail(sender, [sender], msg.as_string())
        server.quit()
        return jsonify({"status": "success", "msg": "测试邮件已发送至 " + sender})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})

@app.route('/api/bg_status')
def api_bg_status():
    return jsonify({
        "running": _bg_scanner_running,
        "active_users": len(_get_all_active_uids()),
        "alert_cache_size": len(LAST_ALERT_CANDLE)
    })

_bg_scanner_running = False
_bg_scanner_lock = threading.Lock()

def _get_all_active_uids():
    uids = []
    if not os.path.exists(USER_DATA_DIR): return uids
    for fname in os.listdir(USER_DATA_DIR):
        if fname.endswith('.json'):
            uid = fname[:-5]
            try:
                conf = load_user_config(uid)
                if conf.get('watchlist'): uids.append(uid)
            except: pass
    return uids

def _bg_scan_one_user(uid):
    ud = load_user_config(uid)
    wl = ud.get('watchlist', [])[:10]
    if not wl: return
    timeframe = ud.get('timeframe', '15m')
    trigger_mode = ud.get('trigger_mode', 'close')
    collected_signals = []
    collected_lock = threading.Lock()
    def run_task(s):
        result = analyze_symbol(s, timeframe, trigger_mode, ud=ud)
        if result and result.get('action') != '-':
            sig = trigger_alert(symbol=result['symbol'], signal=result['signal'],
                detail=result['detail'], action=result['action'], price=result['price'],
                candle_time=result['candle_time'], ud=ud, timeframe=timeframe)
            if sig and isinstance(sig, dict):
                with collected_lock: collected_signals.append(sig)
        return result
    futures = [_scan_pool.submit(run_task, s) for s in wl]
    for f in futures:
        try: f.result(timeout=20)
        except Exception as e: print(f"❌ [后台] {uid} 分析出错: {e}")
    if collected_signals:
        try: send_batch_signals_email(collected_signals, ud, timeframe)
        except Exception as e: print(f"❌ [后台] 批量邮件出错: {e}")

def _do_scan_all_users():
    uids = _get_all_active_uids()
    if not uids: return
    for uid in uids:
        try: _bg_scan_one_user(uid)
        except Exception as e: print(f"❌ [后台扫描器] {uid}: {e}")

def _background_scanner():
    global _bg_scanner_running
    _bg_scanner_running = True
    while _bg_scanner_running:
        try:
            now = datetime.now()
            minutes_past = now.minute % 15
            seconds_past = minutes_past * 60 + now.second
            remain = (15 * 60) - seconds_past
            if remain <= 0: remain += 15 * 60
            elapsed = 0
            while elapsed < remain:
                if not _bg_scanner_running: return
                time.sleep(1); elapsed += 1
            for _ in range(5):
                if not _bg_scanner_running: return
                time.sleep(1)
            with _ohlcv_cache_lock: _ohlcv_cache.clear()
            try: _do_scan_all_users()
            except Exception as e: print(f"❌ [后台] 扫描出错: {e}")
        except Exception as e:
            print(f"❌ [后台] 主循环出错: {e}")
            for _ in range(30):
                if not _bg_scanner_running: return
                time.sleep(1)

def start_background_scanner():
    global _bg_scanner_running
    with _bg_scanner_lock:
        if _bg_scanner_running: return
    t = threading.Thread(target=_background_scanner, daemon=True, name="BG-Scanner")
    t.start()

if __name__ == '__main__':
    import os as _os
    if _os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        start_background_scanner()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)