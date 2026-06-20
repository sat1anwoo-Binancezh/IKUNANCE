import json
import os
from datetime import datetime, timedelta, timezone
import urllib.error
import urllib.request

from .email_service import send_email_sync

BEIJING_TZ = timezone(timedelta(hours=8))

TIMEFRAME_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def _outbound_disabled():
    return os.environ.get("IKUNANCE_DISABLE_OUTBOUND", "0").strip() == "1"


def _post_json(url, payload, timeout=8):
    if _outbound_disabled():
        return {"ok": True, "skipped": True, "reason": "outbound disabled"}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return {"ok": 200 <= resp.status < 300, "status": resp.status}


def _telegram_url(token):
    return f"https://api.telegram.org/bot{token}/sendMessage"


def _format_alert_lines(alerts, timeframe):
    lines = [f"IKUNANCE signal alert · {timeframe}"]
    for alert in alerts or []:
        lines.append(
            "{symbol} {action} {signal} {detail} price={price}".format(
                symbol=alert.get("symbol", ""),
                action=alert.get("action", ""),
                signal=alert.get("signal") or alert.get("type", ""),
                detail=alert.get("detail", ""),
                price=alert.get("price", ""),
            ).strip()
        )
    return "\n".join(lines)


def _email_action_label(action):
    if action == "LONG":
        return "📈 上涨 LONG"
    if action == "SHORT":
        return "📉 下跌 SHORT"
    return str(action or "-")


def _format_dt_from_ms(ms):
    if not ms:
        return "-"
    try:
        return datetime.fromtimestamp(ms / 1000, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        return "-"


def _format_now_beijing():
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _format_signal_email_subject(alert, timeframe):
    tf = alert.get("timeframe") or timeframe or ""
    sym = (alert.get("symbol") or "").replace("/USDT", "")
    return f"[{tf}] {sym} {_email_action_label(alert.get('action', ''))} - I-KUNANCE Signal"


def _format_signal_email_body(alert, timeframe):
    tf = alert.get("timeframe") or timeframe or ""
    candle_ts = alert.get("candle_time", 0) or 0
    tf_ms = TIMEFRAME_MS.get(tf, 0)
    open_str = _format_dt_from_ms(candle_ts) if candle_ts else (alert.get("open_time_full") or alert.get("open_time") or "-")
    if candle_ts and tf_ms:
        close_str = _format_dt_from_ms(candle_ts + tf_ms)
    else:
        close_str = alert.get("close_time") or alert.get("trigger_time_full") or alert.get("time") or "-"

    action_label = _email_action_label(alert.get("action", ""))
    exchange_lbl = (alert.get("exchange") or "").upper()
    detail = alert.get("detail", "")
    signal_type = alert.get("type") or alert.get("signal") or ""
    market_source = alert.get("market_source") or "unknown"
    strategy_source = alert.get("strategy_source") or "unknown"
    now_str = _format_now_beijing()

    body_lines = [
        "-" * 44,
        "  I-KUNANCE realtime signal notification",
        "-" * 44,
        f"  Symbol:     {alert.get('symbol', '')}  ({exchange_lbl})",
        f"  Direction:  {action_label}",
        f"  Price:      {alert.get('price', '')}",
        f"  Signal:     {signal_type} - {detail}",
        f"  Trend:      {alert.get('trend', '')}",
        f"  Market source: {market_source}",
        f"  Strategy:   {strategy_source}",
        f"  Timeframe:  {tf}",
        "-" * 44,
        f"  Candle open:  {open_str}",
        f"  Candle close: {close_str}",
        f"  Trigger time: {now_str}",
        "-" * 44,
        "  This email was sent automatically by I-KUNANCE. Please do not reply.",
        "-" * 44,
    ]
    return "\n".join(body_lines)


def format_signal_notification(alert, timeframe):
    """Return the same title/body used by email notifications."""
    return {
        "title": _format_signal_email_subject(alert or {}, timeframe),
        "body": _format_signal_email_body(alert or {}, timeframe),
    }


def _send_channel(channel, url, payload):
    try:
        return {"channel": channel, **_post_json(url, payload)}
    except urllib.error.HTTPError as exc:
        return {"channel": channel, "ok": False, "error": "upstream http error", "status": exc.code}
    except TimeoutError:
        return {"channel": channel, "ok": False, "error": "upstream timeout"}
    except Exception as exc:
        return {"channel": channel, "ok": False, "error": exc.__class__.__name__}


def send_test_notification(channel, payload):
    channel = (channel or "").strip().lower()
    if channel == "webhook":
        url = (payload or {}).get("webhookUrl", "")
        if not url:
            return {"status": "error", "msg": "请先填写 Webhook URL"}
        result = _send_channel("webhook", url, {"symbol": "TEST", "signal": "MACD_BULL", "price": 0, "test": True})
    elif channel == "discord":
        url = (payload or {}).get("discordUrl", "")
        if not url:
            return {"status": "error", "msg": "请先填写 Webhook URL"}
        result = _send_channel("discord", url, {"content": "IKUNANCE 推送测试成功！这是一条来自监控系统的测试消息。"})
    elif channel == "tg":
        token = (payload or {}).get("tgToken", "")
        chat = (payload or {}).get("tgChatId", "")
        if not token or not chat:
            return {"status": "error", "msg": "请先填写 Bot Token 和 Chat ID"}
        result = _send_channel("tg", _telegram_url(token), {"chat_id": chat, "text": "IKUNANCE 推送测试成功！这是一条来自监控系统的测试消息。"})
    else:
        return {"status": "error", "msg": "未知渠道"}

    if result.get("ok"):
        skipped = "（已跳过真实外发）" if result.get("skipped") else ""
        return {"status": "success", "msg": f"{channel} 测试消息已发送{skipped}", "channel": result}
    return {"status": "error", "msg": result.get("error") or f"HTTP {result.get('status')}", "channel": result}


def send_all_notifications(alerts, ud, timeframe):
    alerts = alerts or []
    settings = ud.get("alert_settings", {}) if ud else {}
    results = []

    if settings.get("webhook") and ud.get("webhook_url"):
        results.append(_send_channel("webhook", ud["webhook_url"], {"alerts": alerts, "timeframe": timeframe}))
    elif settings.get("webhook"):
        results.append({"channel": "webhook", "ok": False, "error": "missing webhook url"})

    if settings.get("discord") and ud.get("discord_url"):
        results.append(_send_channel("discord", ud["discord_url"], {"content": _format_alert_lines(alerts, timeframe)}))
    elif settings.get("discord"):
        results.append({"channel": "discord", "ok": False, "error": "missing discord url"})

    if settings.get("tg") and ud.get("tg_token") and ud.get("tg_chat_id"):
        results.append(_send_channel("tg", _telegram_url(ud["tg_token"]), {"chat_id": ud["tg_chat_id"], "text": _format_alert_lines(alerts, timeframe)}))
    elif settings.get("tg"):
        results.append({"channel": "tg", "ok": False, "error": "missing telegram token or chat id"})

    if settings.get("email"):
        sender = ud.get("email", "")
        password = ud.get("email_pass", "")
        if sender and password:
            if _outbound_disabled():
                results.append({"channel": "email", "ok": True, "skipped": True, "reason": "outbound disabled"})
            else:
                email_results = []
                for alert in alerts:
                    subject = _format_signal_email_subject(alert, timeframe)
                    content = _format_signal_email_body(alert, timeframe)
                    ok, msg = send_email_sync(subject, content, sender, password)
                    email_results.append({"ok": ok, "msg": msg})
                all_ok = all(item["ok"] for item in email_results)
                errors = [item["msg"] for item in email_results if not item["ok"]]
                results.append({
                    "channel": "email",
                    "ok": all_ok,
                    "sent": sum(1 for item in email_results if item["ok"]),
                    "error": "; ".join(errors),
                    "msg": "ok" if all_ok else "; ".join(errors),
                })
        else:
            results.append({"channel": "email", "ok": False, "error": "missing sender or password"})

    return {
        "status": "success" if not results or all(item.get("ok", False) for item in results) else "partial",
        "sent": len(alerts),
        "timeframe": timeframe,
        "channels": results,
    }
