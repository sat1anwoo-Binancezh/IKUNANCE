import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
SERVICE_ROOT = BACKEND_ROOT / "app"
for path in (BACKEND_ROOT, SERVICE_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


class ServiceTest(unittest.TestCase):
    def test_atomic_json_write_writes_complete_file_and_cleans_temp(self):
        from services.storage_service import atomic_write_json

        with tempfile.TemporaryDirectory(prefix="ikunance-storage-test-") as temp_dir:
            target = Path(temp_dir) / "nested" / "settings.json"
            payload = {"status": "success", "name": "测试", "items": [1, 2, 3]}

            atomic_write_json(str(target), payload, ensure_ascii=False, indent=2)

            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), payload)
            leftovers = list(target.parent.glob(".*.tmp"))
            self.assertEqual(leftovers, [])

    def test_json_storage_loaders_recover_from_bad_shapes(self):
        from services import auth_service, community_service, signal_engine

        with tempfile.TemporaryDirectory(prefix="ikunance-json-shape-test-") as temp_dir:
            root = Path(temp_dir)

            old_accounts = auth_service.ACCOUNTS_FILE
            auth_service.ACCOUNTS_FILE = str(root / "accounts.json")
            Path(auth_service.ACCOUNTS_FILE).write_text('["bad"]', encoding="utf-8")
            try:
                accounts = auth_service.load_accounts()
                self.assertEqual(accounts["sessions"], {})
                self.assertIn("111", accounts["users"])
                self.assertEqual(accounts["users"]["111"]["role"], "admin")
                Path(auth_service.ACCOUNTS_FILE).write_text('{"users": [], "sessions": "bad"}', encoding="utf-8")
                accounts = auth_service.load_accounts()
                self.assertEqual(accounts["sessions"], {})
                self.assertIn("111", accounts["users"])
                self.assertEqual(accounts["users"]["111"]["role"], "admin")
            finally:
                auth_service.ACCOUNTS_FILE = old_accounts

            old_posts = community_service.POSTS_FILE
            community_service.POSTS_FILE = str(root / "posts.json")
            Path(community_service.POSTS_FILE).write_text('[{"id":"ok","content":"hello"}, "bad", 1]', encoding="utf-8")
            try:
                posts = community_service.get_posts()["data"]
                self.assertEqual(posts, [{"id": "ok", "content": "hello"}])
            finally:
                community_service.POSTS_FILE = old_posts

            old_history = signal_engine._SIGNALS_FILE
            old_alerted = signal_engine._ALERTED_FILE
            old_alerted_cache = signal_engine._alerted_cache
            old_history_dedup = signal_engine._history_dedup
            signal_engine._SIGNALS_FILE = str(root / "history.json")
            signal_engine._ALERTED_FILE = str(root / "alerted.json")
            signal_engine._alerted_cache = None
            signal_engine._history_dedup = None
            Path(signal_engine._SIGNALS_FILE).write_text('{"signals":["bad",{"time":"2099-01-01 00:00:00","symbol":"BTC/USDT"}]}', encoding="utf-8")
            Path(signal_engine._ALERTED_FILE).write_text('{"old":"bad","fresh":9999999999}', encoding="utf-8")
            try:
                history = signal_engine.load_signal_history()
                self.assertEqual(history["signals"], [{"time": "2099-01-01 00:00:00", "symbol": "BTC/USDT"}])
                self.assertFalse(signal_engine.is_signal_alerted("old", "15m", 1, "LONG"))
                self.assertEqual(signal_engine._alerted_cache, {"fresh": 9999999999})
            finally:
                signal_engine._SIGNALS_FILE = old_history
                signal_engine._ALERTED_FILE = old_alerted
                signal_engine._alerted_cache = old_alerted_cache
                signal_engine._history_dedup = old_history_dedup

    def test_signal_dedupe_scope_does_not_cross_users_or_push_monitor(self):
        from services import signal_engine

        old_alerted_cache = signal_engine._alerted_cache
        old_alerted_dirty = signal_engine._alerted_dirty
        try:
            signal_engine._alerted_cache = {}
            signal_engine._alerted_dirty = False

            signal_engine.mark_signal_alerted("BTC/USDT", "15m", 123, "LONG", scope="user:alice")

            self.assertTrue(signal_engine.is_signal_alerted("BTC/USDT", "15m", 123, "LONG", scope="user:alice"))
            self.assertFalse(signal_engine.is_signal_alerted("BTC/USDT", "15m", 123, "LONG", scope="user:bob"))
            self.assertFalse(signal_engine.is_signal_alerted("BTC/USDT", "15m", 123, "LONG", scope="push:binance:BTC/USDT:15m"))
        finally:
            signal_engine._alerted_cache = old_alerted_cache
            signal_engine._alerted_dirty = old_alerted_dirty

    def test_auth_service_seeds_builtin_admin_with_hashed_password(self):
        from services import auth_service

        with tempfile.TemporaryDirectory(prefix="ikunance-auth-admin-test-") as temp_dir:
            old_accounts = auth_service.ACCOUNTS_FILE
            auth_service.ACCOUNTS_FILE = str(Path(temp_dir) / "accounts.json")
            try:
                accounts = auth_service.load_accounts()
                admin = accounts["users"]["111"]
                self.assertEqual(admin["role"], "admin")
                self.assertTrue(admin["password"].startswith("pbkdf2_sha256$"))
                self.assertNotEqual(admin["password"], "123123123")

                ok, result = auth_service.login("111", "123123123")
                self.assertTrue(ok)
                self.assertEqual(result["role"], "admin")
                self.assertEqual(result["email"], "111")
            finally:
                auth_service.ACCOUNTS_FILE = old_accounts

    def test_notification_service_respects_outbound_disable(self):
        from services.notification_service import send_all_notifications

        old = os.environ.get("IKUNANCE_DISABLE_OUTBOUND")
        os.environ["IKUNANCE_DISABLE_OUTBOUND"] = "1"
        try:
            result = send_all_notifications(
                [{"symbol": "BTC/USDT", "action": "LONG"}],
                {
                    "alert_settings": {"webhook": True, "discord": True, "tg": True, "email": True},
                    "webhook_url": "https://example.invalid/hook",
                    "discord_url": "https://example.invalid/discord",
                    "tg_token": "token",
                    "tg_chat_id": "chat",
                    "email": "bot@example.com",
                    "email_pass": "secret",
                },
                "15m",
            )
        finally:
            if old is None:
                os.environ.pop("IKUNANCE_DISABLE_OUTBOUND", None)
            else:
                os.environ["IKUNANCE_DISABLE_OUTBOUND"] = old

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["sent"], 1)
        channels = {item["channel"]: item for item in result["channels"]}
        self.assertEqual(set(channels), {"webhook", "discord", "tg", "email"})
        self.assertTrue(all(item["skipped"] for item in channels.values()))

    def test_notification_service_uses_realtime_email_format(self):
        from services.notification_service import send_all_notifications, format_signal_notification

        alert = {
            "symbol": "PLAY/USDT",
            "action": "SHORT",
            "type": "趋势演进",
            "detail": "红柱首次增强",
            "price": 0.09805,
            "timeframe": "15m",
            "exchange": "binance",
            "candle_time": 1_700_000_000_000,
            "trend": "BEAR",
            "market_source": "binance_ws_live",
            "strategy_source": "MACD histogram three-bar capture",
        }

        with patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            result = send_all_notifications(
                [alert],
                {"alert_settings": {"email": True}, "email": "bot@example.com", "email_pass": "secret"},
                "15m",
            )

        self.assertEqual(result["status"], "success")
        send_email.assert_called_once()
        subject, body, sender, password = send_email.call_args.args
        self.assertEqual(subject, "[15m] PLAY 📉 下跌 SHORT - I-KUNANCE Signal")
        self.assertEqual(sender, "bot@example.com")
        self.assertEqual(password, "secret")
        self.assertIn("I-KUNANCE realtime signal notification", body)
        self.assertIn("Symbol:     PLAY/USDT  (BINANCE)", body)
        self.assertIn("Direction:  📉 下跌 SHORT", body)
        self.assertIn("Signal:     趋势演进 - 红柱首次增强", body)
        self.assertIn("Trend:      BEAR", body)
        self.assertIn("Market source: binance_ws_live", body)
        self.assertIn("Strategy:   MACD histogram three-bar capture", body)
        self.assertIn("Candle open:  2023-11-15 06:13:20", body)
        self.assertIn("Candle close: 2023-11-15 06:28:20", body)
        mobile = format_signal_notification(alert, "15m")
        self.assertEqual(mobile["title"], subject)
        self.assertEqual(mobile["body"], body)

    def test_notification_service_sends_one_standard_email_per_alert(self):
        from services.notification_service import send_all_notifications

        alerts = [
            {"symbol": "BTC/USDT", "action": "LONG", "type": "趋势演进", "detail": "绿柱增强", "timeframe": "15m"},
            {"symbol": "ETH/USDT", "action": "SHORT", "type": "趋势演进", "detail": "红柱增强", "timeframe": "15m"},
        ]

        with patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            result = send_all_notifications(
                alerts,
                {"alert_settings": {"email": True}, "email": "bot@example.com", "email_pass": "secret"},
                "15m",
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(send_email.call_count, 2)
        subjects = [call.args[0] for call in send_email.call_args_list]
        self.assertEqual(subjects, [
            "[15m] BTC 📈 上涨 LONG - I-KUNANCE Signal",
            "[15m] ETH 📉 下跌 SHORT - I-KUNANCE Signal",
        ])

    def test_signal_engine_classifies_only_requested_macd_histogram_patterns(self):
        from services.signal_engine import _classify_signal

        cases = [
            ((-0.08, -0.12, -0.10), "LONG"),
            ((0.30, 0.10, 0.20), "LONG"),
            ((0.10, 0.30, 0.20), "SHORT"),
            ((-0.30, -0.10, -0.20), "SHORT"),
            ((0.30, -0.10, -0.20), "-"),
            ((-0.30, 0.10, 0.20), "-"),
        ]

        for values, expected_action in cases:
            with self.subTest(values=values):
                _, _, action = _classify_signal(*values)
                self.assertEqual(action, expected_action)

    def test_signal_engine_does_not_filter_macd_signal_by_trend_or_body(self):
        from services import signal_engine

        start_ts = 1_700_000_000_000
        rows = []
        price = 100.0
        for index in range(60):
            open_price = price + 2.0
            close_price = price
            high = open_price + 0.1
            low = close_price - 0.1
            rows.append([start_ts + index * 60_000, open_price, high, low, close_price, 1000])
            price -= 0.5

        with patch.object(signal_engine, "_classify_signal", return_value=("macd", "forced long", "LONG")):
            result = signal_engine.analyze_symbol(rows, "BTC/USDT", "close")

        self.assertIsNotNone(result)
        self.assertEqual(result.action, "LONG")
        self.assertEqual(result.signal, "macd")

    def test_trigger_alert_carries_market_strategy_and_trend_metadata(self):
        from services import signal_engine

        candle_time = int(time.time() * 1000) - 15 * 60 * 1000
        with tempfile.TemporaryDirectory(prefix="ikunance-signal-meta-test-") as temp_dir:
            old_signals_file = signal_engine._SIGNALS_FILE
            old_alerted_file = signal_engine._ALERTED_FILE
            old_alerted_cache = signal_engine._alerted_cache
            old_alerted_dirty = signal_engine._alerted_dirty
            old_history_dedup = signal_engine._history_dedup
            signal_engine._SIGNALS_FILE = str(Path(temp_dir) / "signal_history.json")
            signal_engine._ALERTED_FILE = str(Path(temp_dir) / "alerted_signals.json")
            signal_engine._alerted_cache = {}
            signal_engine._alerted_dirty = False
            signal_engine._history_dedup = None
            try:
                result = signal_engine.SignalResult(
                    symbol="PLAY/USDT",
                    price=0.09805,
                    trend="BEAR",
                    signal="trend continuation",
                    detail="histogram expanding",
                    action="SHORT",
                    candle_time=candle_time,
                )
                signal = signal_engine.trigger_alert(
                    result,
                    {
                        "exchange_id": "binance",
                        "_notification_origin": "push_monitor",
                        "_dedupe_scope": "test:PLAY/USDT:15m",
                        "_market_source": "binance_ws_live",
                        "_strategy_source": "MACD histogram three-bar capture",
                    },
                    "15m",
                )
            finally:
                signal_engine._SIGNALS_FILE = old_signals_file
                signal_engine._ALERTED_FILE = old_alerted_file
                signal_engine._alerted_cache = old_alerted_cache
                signal_engine._alerted_dirty = old_alerted_dirty
                signal_engine._history_dedup = old_history_dedup

        self.assertIsNotNone(signal)
        self.assertEqual(signal["action"], "SHORT")
        self.assertEqual(signal["trend"], "BEAR")
        self.assertEqual(signal["market_source"], "binance_ws_live")
        self.assertEqual(signal["strategy_source"], "MACD histogram three-bar capture")

    def test_trigger_alert_formats_signal_times_in_beijing_timezone(self):
        from services import signal_engine

        candle_time = 1_700_000_000_000
        close_time = candle_time + 15 * 60 * 1000
        with tempfile.TemporaryDirectory(prefix="ikunance-signal-timezone-test-") as temp_dir:
            old_signals_file = signal_engine._SIGNALS_FILE
            old_alerted_file = signal_engine._ALERTED_FILE
            old_alerted_cache = signal_engine._alerted_cache
            old_alerted_dirty = signal_engine._alerted_dirty
            old_history_dedup = signal_engine._history_dedup
            signal_engine._SIGNALS_FILE = str(Path(temp_dir) / "signal_history.json")
            signal_engine._ALERTED_FILE = str(Path(temp_dir) / "alerted_signals.json")
            signal_engine._alerted_cache = {}
            signal_engine._alerted_dirty = False
            signal_engine._history_dedup = None
            try:
                result = signal_engine.SignalResult(
                    symbol="BTC/USDT",
                    price=100.0,
                    trend="BULL",
                    signal="trend confirmation",
                    detail="histogram",
                    action="LONG",
                    candle_time=candle_time,
                )
                with patch.object(signal_engine.time, "time", return_value=close_time / 1000 + 10):
                    signal = signal_engine.trigger_alert(
                        result,
                        {
                            "exchange_id": "binance",
                            "_notification_origin": "push_monitor",
                            "_dedupe_scope": "test:BTC/USDT:15m:beijing",
                        },
                        "15m",
                    )
            finally:
                signal_engine._SIGNALS_FILE = old_signals_file
                signal_engine._ALERTED_FILE = old_alerted_file
                signal_engine._alerted_cache = old_alerted_cache
                signal_engine._alerted_dirty = old_alerted_dirty
                signal_engine._history_dedup = old_history_dedup

        self.assertIsNotNone(signal)
        self.assertEqual(signal["open_time"], "2023-11-15 06:13:20")
        self.assertEqual(signal["close_time"], "2023-11-15 06:28:20")
        self.assertEqual(signal["trigger_time"], "06:28")
        self.assertEqual(signal["trigger_time_full"], "2023-11-15 06:28:20")

    def test_notification_service_reports_partial_for_missing_email_credentials(self):
        from services.notification_service import send_all_notifications

        result = send_all_notifications(
            [{"symbol": "BTC/USDT", "action": "LONG"}],
            {"alert_settings": {"webhook": True, "discord": True, "tg": True, "email": True}},
            "15m",
        )
        self.assertEqual(result["status"], "partial")
        channels = {item["channel"]: item for item in result["channels"]}
        self.assertEqual(set(channels), {"webhook", "discord", "tg", "email"})
        self.assertIn("missing", channels["webhook"]["error"])
        self.assertIn("missing", channels["discord"]["error"])
        self.assertIn("missing", channels["tg"]["error"])
        self.assertIn("missing", channels["email"]["error"])

    def test_test_push_respects_outbound_disable(self):
        from services.notification_service import send_test_notification

        old = os.environ.get("IKUNANCE_DISABLE_OUTBOUND")
        os.environ["IKUNANCE_DISABLE_OUTBOUND"] = "1"
        try:
            result = send_test_notification("webhook", {"webhookUrl": "https://example.invalid/hook"})
        finally:
            if old is None:
                os.environ.pop("IKUNANCE_DISABLE_OUTBOUND", None)
            else:
                os.environ["IKUNANCE_DISABLE_OUTBOUND"] = old

        self.assertEqual(result["status"], "success")
        self.assertTrue(result["channel"]["skipped"])

    def test_email_service_validates_missing_credentials(self):
        from services.email_service import send_email_sync

        ok, msg = send_email_sync("subject", "content", "", "")
        self.assertFalse(ok)
        self.assertIn("missing", msg)

    def test_signal_engine_accepts_short_new_listing_history(self):
        from services.signal_engine import analyze_symbol

        start_ts = 1_700_000_000_000
        rows = []
        price = 100.0
        for index in range(80):
            open_price = price
            close_price = price + 0.2 + (0.03 if index % 2 else -0.01)
            high = max(open_price, close_price) + 0.1
            low = min(open_price, close_price) - 0.1
            rows.append([start_ts + index * 60_000, open_price, high, low, close_price, 1000 + index])
            price = close_price

        result = analyze_symbol(rows, "NEWCOIN/USDT", "close")

        self.assertIsNotNone(result)
        self.assertEqual(result.symbol, "NEWCOIN/USDT")

    def test_exchange_service_uses_official_binance_provider_by_default(self):
        from services import exchange_service

        class FakeExchange:
            pass

        old_cache = dict(exchange_service._exchange_cache)
        exchange_service._exchange_cache.clear()
        try:
            with patch.dict(os.environ, {"IKUNANCE_LIVE_MARKET": "1", "IKUNANCE_MARKET_PROXY": "127.0.0.1:7890"}), \
                 patch.object(exchange_service, "_make_ccxt_exchange", return_value=FakeExchange()) as make_exchange:
                exchange = exchange_service.get_exchange({"exchange_id": "binance"})

            self.assertIsInstance(exchange, exchange_service.BinancePublicFuturesExchange)
            make_exchange.assert_not_called()
        finally:
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)

    def test_exchange_service_can_opt_into_ccxt_provider(self):
        from services import exchange_service

        class FakeExchange:
            pass

        old_cache = dict(exchange_service._exchange_cache)
        exchange_service._exchange_cache.clear()
        try:
            with patch.dict(os.environ, {
                "IKUNANCE_LIVE_MARKET": "1",
                "IKUNANCE_MARKET_PROXY": "127.0.0.1:7890",
                "IKUNANCE_BINANCE_PROVIDER": "ccxt",
            }), patch.object(exchange_service, "_make_ccxt_exchange", return_value=FakeExchange()) as make_exchange:
                exchange = exchange_service.get_exchange({"exchange_id": "binance"})

            self.assertIsInstance(exchange, FakeExchange)
            make_exchange.assert_called_once_with("binance", "http://127.0.0.1:7890")
        finally:
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)

    def test_official_binance_provider_uses_exchange_info_and_rest_tickers(self):
        from services import exchange_service

        rows = [
            {"displayName": "BTC/USDT"},
            {"displayName": "PLAY/USDT"},
        ]
        rest_tickers = {
            "BTC/USDT:USDT": {"last": 65000.0, "percentage": 1.5, "source": "binance_futures_rest_24hr"}
        }

        with patch("services.exchange_service.list_binance_futures_symbols", return_value=rows) as list_symbols, \
             patch("services.exchange_service.fetch_futures_24hr_tickers", return_value=rest_tickers) as fetch_tickers:
            exchange = exchange_service.BinancePublicFuturesExchange()
            markets = exchange.load_markets()
            tickers = exchange.fetch_tickers(["BTC/USDT"])

        list_symbols.assert_called()
        fetch_tickers.assert_called_once_with(["BTC/USDT"])
        self.assertIn("PLAY/USDT:USDT", markets)
        self.assertEqual(tickers["BTC/USDT:USDT"]["last"], 65000.0)
        self.assertEqual(tickers["BTC/USDT:USDT"]["source"], "binance_futures_rest_24hr")

    def test_exchange_service_ohlcv_cache_ttl_is_configurable(self):
        from services import exchange_service

        old = os.environ.get("IKUNANCE_OHLCV_CACHE_TTL")
        try:
            os.environ.pop("IKUNANCE_OHLCV_CACHE_TTL", None)
            self.assertEqual(exchange_service._ohlcv_cache_ttl_seconds(), 5.0)
            os.environ["IKUNANCE_OHLCV_CACHE_TTL"] = "9"
            self.assertEqual(exchange_service._ohlcv_cache_ttl_seconds(), 9.0)
        finally:
            if old is None:
                os.environ.pop("IKUNANCE_OHLCV_CACHE_TTL", None)
            else:
                os.environ["IKUNANCE_OHLCV_CACHE_TTL"] = old

    def test_exchange_service_empty_public_data_cache_expires(self):
        import io
        import zipfile
        from datetime import datetime, timezone
        from urllib.error import HTTPError
        from services import exchange_service

        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return self.payload

        archive_buf = io.BytesIO()
        with zipfile.ZipFile(archive_buf, "w") as archive:
            archive.writestr(
                "BTCUSDT-15m-2026-06-10.csv",
                "open_time,open,high,low,close,volume\n"
                "1700000000000,1,2,0.5,1.5,100\n",
            )
        payload = archive_buf.getvalue()
        calls = {"count": 0}

        def fake_urlopen(url, timeout=15):
            calls["count"] += 1
            if calls["count"] == 1:
                raise HTTPError(url, 404, "not found", None, None)
            return FakeResponse(payload)

        old_empty_ttl = os.environ.get("IKUNANCE_PUBLIC_DATA_EMPTY_CACHE_TTL")
        old_cache = dict(exchange_service._public_data_cache)
        old_time = exchange_service.time.time
        try:
            os.environ["IKUNANCE_PUBLIC_DATA_EMPTY_CACHE_TTL"] = "1"
            exchange_service._public_data_cache.clear()
            with patch("services.exchange_service.urllib.request.urlopen", side_effect=fake_urlopen):
                with patch("services.exchange_service.time.time", return_value=1000.0):
                    first = exchange_service._load_binance_public_day(
                        "BTC/USDT",
                        "15m",
                        datetime(2026, 6, 10, tzinfo=timezone.utc),
                    )
                with patch("services.exchange_service.time.time", return_value=1002.0):
                    second = exchange_service._load_binance_public_day(
                        "BTC/USDT",
                        "15m",
                        datetime(2026, 6, 10, tzinfo=timezone.utc),
                    )
        finally:
            if old_empty_ttl is None:
                os.environ.pop("IKUNANCE_PUBLIC_DATA_EMPTY_CACHE_TTL", None)
            else:
                os.environ["IKUNANCE_PUBLIC_DATA_EMPTY_CACHE_TTL"] = old_empty_ttl
            exchange_service._public_data_cache.clear()
            exchange_service._public_data_cache.update(old_cache)
            exchange_service.time.time = old_time

        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(calls["count"], 2)

    def test_exchange_service_does_not_fake_live_market_failures_by_default(self):
        from services import exchange_service

        class BrokenExchange:
            symbols = ["BTC/USDT:USDT"]
            markets = {"BTC/USDT:USDT": {"linear": True}}

            def fetch_tickers(self, *args, **kwargs):
                raise RuntimeError("binance blocked")

        old_cache = dict(exchange_service._exchange_cache)
        old_error_until = dict(exchange_service._live_error_until)
        old_error_count = dict(exchange_service._live_error_count)
        exchange_service._exchange_cache.clear()
        exchange_service._live_error_until.clear()
        exchange_service._live_error_count.clear()
        try:
            with patch.dict(os.environ, {"IKUNANCE_LIVE_MARKET": "1", "IKUNANCE_BINANCE_PROVIDER": "ccxt"}, clear=False), \
                 patch.object(exchange_service, "_make_ccxt_exchange", return_value=BrokenExchange()), \
                 patch.object(exchange_service, "_binance_public_bridge", return_value=None):
                os.environ.pop("IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK", None)
                with self.assertRaises(exchange_service.LiveMarketUnavailable):
                    exchange_service.fetch_tickers_safe("binance")

                exchange_service._exchange_cache.clear()
                exchange_service._live_error_until.clear()
                exchange_service._live_error_count.clear()
                os.environ["IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK"] = "1"
                tickers = exchange_service.fetch_tickers_safe("binance")
                self.assertIn("BTC/USDT:USDT", tickers)
        finally:
            os.environ.pop("IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK", None)
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)
            exchange_service._live_error_until.clear()
            exchange_service._live_error_until.update(old_error_until)
            exchange_service._live_error_count.clear()
            exchange_service._live_error_count.update(old_error_count)

    def test_exchange_service_uses_binance_public_bridge_when_rest_is_blocked(self):
        from services import exchange_service

        class BrokenExchange:
            def fetch_tickers(self, *args, **kwargs):
                raise RuntimeError("binance rest blocked")

        class BridgeExchange:
            def fetch_tickers(self, symbols=None):
                return {"BTC/USDT:USDT": {"last": 100.0, "percentage": 1.2}}

        old_cache = dict(exchange_service._exchange_cache)
        old_error_until = dict(exchange_service._live_error_until)
        old_error_count = dict(exchange_service._live_error_count)
        exchange_service._exchange_cache.clear()
        exchange_service._live_error_until.clear()
        exchange_service._live_error_count.clear()
        try:
            with patch.dict(os.environ, {"IKUNANCE_LIVE_MARKET": "1", "IKUNANCE_BINANCE_PROVIDER": "ccxt"}, clear=False), \
                 patch.object(exchange_service, "_make_ccxt_exchange", return_value=BrokenExchange()), \
                 patch.object(exchange_service, "_binance_public_bridge", return_value=BridgeExchange()):
                tickers = exchange_service.fetch_tickers_safe("binance")

            self.assertEqual(tickers["BTC/USDT:USDT"]["last"], 100.0)
        finally:
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)
            exchange_service._live_error_until.clear()
            exchange_service._live_error_until.update(old_error_until)
            exchange_service._live_error_count.clear()
            exchange_service._live_error_count.update(old_error_count)

    def test_exchange_service_supports_binance_stock_rwa_adapter(self):
        from services import exchange_service

        start_ts = 1_700_000_000_000

        def fake_http_json(url, timeout=20):
            if "stock/detail/list" in url:
                return {
                    "code": "000000",
                    "data": [
                        {
                            "ticker": "AAPL",
                            "symbol": "AAPLon",
                            "chainId": "1",
                            "contractAddress": "0xapple",
                            "multiplier": "1",
                        }
                    ],
                }
            if "rwa/dynamic" in url:
                return {
                    "code": "000000",
                    "data": {
                        "stockInfo": {"price": "201.50"},
                        "tokenInfo": {"priceChangePct24h": "1.25"},
                    },
                }
            if "token/kline" in url:
                return {
                    "code": "000000",
                    "data": {
                        "klineInfos": [
                            [start_ts + index * 900_000, "200", "202", "199", str(200 + index / 10), "10"]
                            for index in range(60)
                        ]
                    },
                }
            raise AssertionError(url)

        old_cache = dict(exchange_service._exchange_cache)
        old_stock_cache = dict(exchange_service._binance_stock_cache)
        old_stock_snapshot_file = exchange_service.BINANCE_STOCK_SNAPSHOT_FILE
        exchange_service._exchange_cache.clear()
        exchange_service._binance_stock_cache.update({"ts": 0.0, "symbols": [], "markets": {}, "lookup": {}})
        try:
            with tempfile.TemporaryDirectory(prefix="ikunance-stock-snapshot-test-") as temp_dir, \
                 patch.dict(os.environ, {"IKUNANCE_LIVE_MARKET": "1"}, clear=False), \
                 patch.object(exchange_service, "_http_json", side_effect=fake_http_json):
                exchange_service.BINANCE_STOCK_SNAPSHOT_FILE = str(Path(temp_dir) / "stock_snapshot.json")
                exchange = exchange_service.get_exchange({"exchange_id": "binance_us_stock"})
                self.assertIsInstance(exchange, exchange_service.BinanceStockRwaExchange)
                self.assertIn("AAPL/USDT", exchange_service.load_linear_usdt_symbols("binance_stock"))
                tickers = exchange_service.fetch_tickers_safe("binance_stock", symbols=["AAPL/USDT"])
                self.assertEqual(tickers["AAPL/USDT"]["last"], 201.5)
                self.assertEqual(tickers["AAPL/USDT"]["percentage"], 1.25)
                rows = exchange_service.fetch_ohlcv_cached("AAPL/USDT", "15m", {"exchange_id": "binance_stock"}, limit=5)
                self.assertEqual(len(rows), 5)
                self.assertEqual(len(rows[0]), 6)
        finally:
            exchange_service.BINANCE_STOCK_SNAPSHOT_FILE = old_stock_snapshot_file
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)
            exchange_service._binance_stock_cache.clear()
            exchange_service._binance_stock_cache.update(old_stock_cache)

    def test_exchange_service_falls_back_when_live_fetch_fails(self):
        from services import exchange_service

        class BrokenExchange:
            symbols = ["BTC/USDT:USDT"]
            markets = {"BTC/USDT:USDT": {"linear": True}}

            def load_markets(self):
                raise RuntimeError("upstream down")

            def fetch_ohlcv(self, *args, **kwargs):
                raise RuntimeError("ohlcv down")

            def fetch_tickers(self, *args, **kwargs):
                raise RuntimeError("tickers down")

        old_cache = dict(exchange_service._exchange_cache)
        exchange_service._exchange_cache.clear()
        exchange_service._exchange_cache[("binance", "", False, "official")] = BrokenExchange()
        try:
            rows = exchange_service.fetch_ohlcv_cached("BTC/USDT", "15m", {"exchange_id": "binance"}, limit=210)
            self.assertGreaterEqual(len(rows), 210)
            self.assertEqual(len(rows[0]), 6)

            symbols = exchange_service.load_linear_usdt_symbols("binance", {"exchange_id": "binance"})
            self.assertIn("BTC/USDT", symbols)

            tickers = exchange_service.fetch_tickers_safe("binance")
            self.assertIn("BTC/USDT:USDT", tickers)
        finally:
            exchange_service._exchange_cache.clear()
            exchange_service._exchange_cache.update(old_cache)

    def test_binance_public_bridge_merges_closed_ws_kline_before_live_row(self):
        from services import exchange_service

        base_ts = 1_800_000_000_000
        old_rows = [
            [base_ts + index * 900_000, 100, 101, 99, 100 + index * 0.1, 10]
            for index in range(78)
        ]
        closed_ts = base_ts + 100 * 900_000
        closed_row = [closed_ts, 120, 123, 119, 122, 15, "binance_ws_closed"]
        live_row = [closed_ts + 900_000, 122, 124, 121, 123, 3, "binance_ws_live"]

        old_closed_cache = dict(exchange_service._ws_kline_cache)
        old_live_cache = dict(exchange_service._ws_live_kline_cache)
        exchange_service._ws_kline_cache.clear()
        exchange_service._ws_live_kline_cache.clear()
        try:
            exchange_service._cache_ws_closed_kline("BTC/USDT", "15m", closed_row)
            exchange_service._cache_ws_live_kline("BTC/USDT", "15m", live_row)
            with patch.object(exchange_service, "_load_binance_public_klines", return_value=old_rows), \
                 patch.object(exchange_service, "_fetch_binance_ws_kline", return_value=None):
                rows = exchange_service.BinancePublicFuturesExchange().fetch_ohlcv("BTC/USDT", "15m", 80)

            self.assertEqual(rows[-2][0], closed_ts)
            self.assertEqual(rows[-2][6], "binance_ws_closed")
            self.assertEqual(rows[-1][0], closed_ts + 900_000)
            self.assertEqual(rows[-1][6], "binance_ws_live")
        finally:
            exchange_service._ws_kline_cache.clear()
            exchange_service._ws_kline_cache.update(old_closed_cache)
            exchange_service._ws_live_kline_cache.clear()
            exchange_service._ws_live_kline_cache.update(old_live_cache)

    def test_binance_public_bridge_adds_placeholder_after_latest_closed_kline(self):
        from services import exchange_service

        base_ts = 1_800_000_000_000
        old_rows = [
            [base_ts + index * 900_000, 100, 101, 99, 100 + index * 0.1, 10]
            for index in range(78)
        ]
        closed_ts = base_ts + 100 * 900_000
        closed_row = [closed_ts, 120, 123, 119, 122, 15, "binance_ws_closed"]

        old_closed_cache = dict(exchange_service._ws_kline_cache)
        old_live_cache = dict(exchange_service._ws_live_kline_cache)
        exchange_service._ws_kline_cache.clear()
        exchange_service._ws_live_kline_cache.clear()
        try:
            exchange_service._cache_ws_closed_kline("BTC/USDT", "15m", closed_row)
            with patch.object(exchange_service, "_load_binance_public_klines", return_value=old_rows), \
                 patch.object(exchange_service, "_fetch_binance_ws_kline", return_value=None):
                rows = exchange_service.BinancePublicFuturesExchange().fetch_ohlcv("BTC/USDT", "15m", 80)

            self.assertEqual(rows[-2][0], closed_ts)
            self.assertEqual(rows[-2][6], "binance_ws_closed")
            self.assertEqual(rows[-1][0], closed_ts + 900_000)
            self.assertEqual(rows[-1][6], "binance_ws_live_placeholder")
        finally:
            exchange_service._ws_kline_cache.clear()
            exchange_service._ws_kline_cache.update(old_closed_cache)
            exchange_service._ws_live_kline_cache.clear()
            exchange_service._ws_live_kline_cache.update(old_live_cache)

    def test_binance_public_bridge_uses_spot_data_api_when_futures_history_is_stale(self):
        from services import exchange_service

        stale_rows = [
            [1_700_000_000_000 + index * 900_000, 100, 101, 99, 100, 10]
            for index in range(80)
        ]
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // 900_000) * 900_000
        payload = []
        for index in range(80):
            open_time = current_open - (79 - index) * 900_000
            payload.append([
                open_time,
                "200.0",
                "202.0",
                "199.0",
                str(200 + index / 10),
                "10.0",
                open_time + 899_999,
            ])

        old_closed_cache = dict(exchange_service._ws_kline_cache)
        old_live_cache = dict(exchange_service._ws_live_kline_cache)
        exchange_service._ws_kline_cache.clear()
        exchange_service._ws_live_kline_cache.clear()
        try:
            with patch.object(exchange_service, "_load_binance_public_klines", return_value=stale_rows), \
                 patch.object(exchange_service, "_fetch_binance_ws_kline", return_value=None), \
                 patch.object(exchange_service, "_http_json", return_value=payload):
                rows = exchange_service.BinancePublicFuturesExchange().fetch_ohlcv("BTC/USDT", "15m", 80)

            markers = [row[6] for row in rows if len(row) > 6]
            self.assertIn("binance_spot_data_api", markers)
            self.assertIn("binance_spot_data_api_live", markers)
            self.assertGreater(rows[-2][0], stale_rows[-1][0])
        finally:
            exchange_service._ws_kline_cache.clear()
            exchange_service._ws_kline_cache.update(old_closed_cache)
            exchange_service._ws_live_kline_cache.clear()
            exchange_service._ws_live_kline_cache.update(old_live_cache)

    def test_binance_symbol_provider_filters_trading_usdt_perpetuals(self):
        from services import binance_symbol_provider

        payload = {
            "symbols": [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
                {"symbol": "XRPBUSD", "baseAsset": "XRP", "quoteAsset": "BUSD", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "OLDUSDT", "baseAsset": "OLD", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "BREAK"},
            ]
        }

        old_cache = binance_symbol_provider._cache
        old_snapshot = binance_symbol_provider.SNAPSHOT_FILE
        with tempfile.TemporaryDirectory(prefix="ikunance-symbol-provider-test-") as temp_dir:
            binance_symbol_provider._cache = None
            binance_symbol_provider.SNAPSHOT_FILE = str(Path(temp_dir) / "symbols.json")
            try:
                with patch("services.binance_symbol_provider._http_get_json", return_value=(200, payload, "ok")):
                    snapshot = binance_symbol_provider.refresh_binance_futures_symbols(force=True)
                symbols = [row["displayName"] for row in snapshot.symbols]
                self.assertIn("BTC/USDT", symbols)
                self.assertNotIn("ETH/USDT", [row["displayName"] for row in snapshot.symbols if row.get("source") == "fapi_exchangeInfo" and row["symbol"] == "ETHUSDT"])
                search = binance_symbol_provider.search_binance_futures_symbols("btc")
                self.assertEqual(search["data"][0], "BTC/USDT")
                self.assertTrue(Path(binance_symbol_provider.SNAPSHOT_FILE).exists())
            finally:
                binance_symbol_provider._cache = old_cache
                binance_symbol_provider.SNAPSHOT_FILE = old_snapshot

    def test_binance_symbol_provider_falls_back_to_snapshot_on_http_451(self):
        from services import binance_symbol_provider

        old_cache = binance_symbol_provider._cache
        old_snapshot = binance_symbol_provider.SNAPSHOT_FILE
        with tempfile.TemporaryDirectory(prefix="ikunance-symbol-451-test-") as temp_dir:
            snapshot_file = Path(temp_dir) / "symbols.json"
            snapshot_file.write_text(json.dumps({
                "updatedAt": 1234,
                "source": "test_snapshot",
                "symbols": [{
                    "symbol": "PLAYUSDT",
                    "baseAsset": "PLAY",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "displayName": "PLAY/USDT",
                }],
            }), encoding="utf-8")
            binance_symbol_provider._cache = None
            binance_symbol_provider.SNAPSHOT_FILE = str(snapshot_file)
            try:
                with patch("services.binance_symbol_provider._http_get_json", return_value=(451, {}, "blocked")):
                    snapshot = binance_symbol_provider.refresh_binance_futures_symbols(force=True)
                self.assertEqual(snapshot.source, "snapshot_fallback")
                self.assertEqual(snapshot.error_code, "BINANCE_SYMBOL_HTTP_451")
                self.assertIn("PLAY/USDT", [row["displayName"] for row in snapshot.symbols])
            finally:
                binance_symbol_provider._cache = old_cache
                binance_symbol_provider.SNAPSHOT_FILE = old_snapshot

    def test_futures_kline_provider_classifies_http_451(self):
        from services import futures_kline_provider

        with patch("services.futures_kline_provider._http_get_json", return_value=(451, {}, "blocked")):
            with self.assertRaises(futures_kline_provider.KlineProviderError) as raised:
                futures_kline_provider.fetch_futures_rest_klines("BTC/USDT", "15m", 80)
        self.assertEqual(raised.exception.code, "BINANCE_FUTURES_REST_HTTP_451")
        self.assertEqual(raised.exception.status_code, 451)

    def test_futures_kline_provider_maps_official_24hr_ticker(self):
        from services import futures_kline_provider

        payload = [
            {"symbol": "BTCUSDT", "lastPrice": "65000.5", "priceChangePercent": "2.15"},
            {"symbol": "ETHUSDT", "lastPrice": "3500", "priceChangePercent": "-0.5"},
            {"symbol": "BTCUSDC", "lastPrice": "65000", "priceChangePercent": "1.0"},
        ]
        with patch("services.futures_kline_provider._http_get_json", return_value=(200, payload, "ok")):
            tickers = futures_kline_provider.fetch_futures_24hr_tickers(["BTC/USDT"])

        self.assertEqual(set(tickers), {"BTC/USDT:USDT"})
        self.assertEqual(tickers["BTC/USDT:USDT"]["last"], 65000.5)
        self.assertEqual(tickers["BTC/USDT:USDT"]["percentage"], 2.15)
        self.assertEqual(tickers["BTC/USDT:USDT"]["source"], "binance_futures_rest_24hr")

    def test_macd_state_manager_matches_native_histogram_rules(self):
        from services.macd_state import MacdState

        cases = [
            ([-0.10, -0.12, -0.08], "LONG", "Red histogram shrinking"),
            ([0.20, 0.10, 0.30], "LONG", "Green histogram first strengthening"),
            ([0.20, 0.30, 0.10], "SHORT", "Green histogram shrinking"),
            ([-0.20, -0.10, -0.30], "SHORT", "Red histogram first strengthening"),
        ]

        for hist_tail, action, detail in cases:
            with self.subTest(hist_tail=hist_tail):
                state = MacdState("TEST/USDT")
                state.hist_tail = list(hist_tail)
                state.close_tail = [100.0]
                state.trend_ema = 99.0
                state.last_candle_time = 1_700_000_000_000
                signal = state.classify_current()
                self.assertEqual(signal.action, action)
                self.assertEqual(signal.detail, detail)


if __name__ == "__main__":
    unittest.main()
