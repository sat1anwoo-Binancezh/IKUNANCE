import importlib
import io
import hashlib
import json
import os
import shutil
import time
import uuid
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


class BackendCoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["IKUNANCE_LIVE_MARKET"] = "0"
        os.environ["IKUNANCE_ACCESS_LOG"] = "0"
        os.chdir(BACKEND_ROOT)
        cls.app_module = importlib.import_module("app")
        cls.temp_dir = Path(tempfile.mkdtemp(prefix="ikunance-backend-test-"))
        cls.app_module.USER_DATA_DIR = str(cls.temp_dir / "user_data")
        os.makedirs(cls.app_module.USER_DATA_DIR, exist_ok=True)
        cls.app_module.UPLOAD_FOLDER = str(cls.temp_dir / "custom_sounds")
        os.makedirs(cls.app_module.UPLOAD_FOLDER, exist_ok=True)
        cls.app_module._user_cache.clear()
        cls.app_module.auth_service.ACCOUNTS_FILE = str(cls.temp_dir / "accounts.json")
        cls.app_module.delete_post.__globals__["delete_post"].__globals__["POSTS_FILE"] = str(cls.temp_dir / "community_posts.json")
        cls.client = cls.app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        os.environ.pop("IKUNANCE_ACCESS_LOG", None)
        if hasattr(cls, "temp_dir"):
            shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _register_token(self):
        email = f"test-{uuid.uuid4()}@example.com"
        response = self.client.post(
            "/api/auth/register",
            json={"email": email, "password": "password123", "nickname": "tester"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        token = payload["token"]
        self._clear_client_cookies()
        return token

    def _register_email(self):
        token = self._register_token()
        return self.app_module.auth_service.load_accounts()["sessions"][token]

    def setUp(self):
        self._clear_client_cookies()
        self.app_module._user_cache.clear()
        if os.path.isdir(self.app_module.USER_DATA_DIR):
            for path in Path(self.app_module.USER_DATA_DIR).glob("*.json"):
                path.unlink()

    def _clear_client_cookies(self):
        for name in ("ikun_token", "ikun_sid"):
            try:
                self.client.delete_cookie(name)
            except TypeError:
                try:
                    self.client.delete_cookie("localhost", name)
                except TypeError:
                    pass

    def _assert_json_not_500(self, response, path):
        self.assertNotEqual(response.status_code, 500, path)
        self.assertIsNotNone(response.get_json(), path)

    def test_app_imports_and_settings_endpoint_works(self):
        response = self.client.get("/api/get_settings")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("alertSettings", payload)
        self.assertIn("watchlist", payload)

    def test_anonymous_settings_cookie_keeps_watchlist_and_email_together(self):
        response = self.client.get("/api/get_settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn("ikun_sid=", response.headers.get("Set-Cookie", ""))

        add_response = self.client.post(
            "/api/add_symbol",
            json={"symbol": "BTC/USDT", "exchange": "binance", "timeframe": "15m"},
        )
        self.assertEqual(add_response.status_code, 200)

        save_response = self.client.post(
            "/api/save_settings",
            json={
                "email": "sender@example.com",
                "emailPass": "secret",
                "alertSettings": {"email": True},
                "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            },
        )
        self.assertEqual(save_response.status_code, 200)

        settings = self.client.get("/api/get_settings").get_json()
        self.assertEqual(settings["email"], "sender@example.com")
        self.assertEqual(settings["watchlist"], [{"symbol": "BTC/USDT", "exchange": "binance"}])

    def test_email_alert_channel_survives_partial_settings_saves(self):
        token = self._register_token()
        headers = {"X-Token": token}

        first = self.client.post(
            "/api/save_settings",
            json={
                "email": "sender@example.com",
                "emailPass": "secret",
                "alertSettings": {"email": True, "toast": True},
            },
            headers=headers,
        )
        self.assertEqual(first.status_code, 200)

        partial = self.client.post(
            "/api/save_settings",
            json={"email": "sender@example.com", "emailPass": "new-secret"},
            headers=headers,
        )
        self.assertEqual(partial.status_code, 200)
        settings = self.client.get("/api/get_settings", headers=headers).get_json()
        self.assertIs(settings["alertSettings"]["email"], True)
        self.assertIs(settings["alertSettings"]["toast"], True)
        self.assertIn("sound_type", settings["alertSettings"])

        malformed = self.client.post(
            "/api/save_settings",
            json={"alertSettings": ["bad"]},
            headers=headers,
        )
        self.assertEqual(malformed.status_code, 200)
        settings = self.client.get("/api/get_settings", headers=headers).get_json()
        self.assertIs(settings["alertSettings"]["email"], True)

    def test_save_settings_does_not_accidentally_shrink_watchlist(self):
        for symbol in ("BTC/USDT", "ETH/USDT", "OP/USDT"):
            response = self.client.post(
                "/api/add_symbol",
                json={"symbol": symbol, "exchange": "binance", "timeframe": "15m"},
            )
            self.assertEqual(response.status_code, 200)

        save_response = self.client.post(
            "/api/save_settings",
            json={
                "email": "sender@example.com",
                "emailPass": "secret",
                "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            },
        )
        self.assertEqual(save_response.status_code, 200)

        settings = self.client.get("/api/get_settings").get_json()
        self.assertEqual(
            settings["watchlist"],
            [
                {"symbol": "BTC/USDT", "exchange": "binance"},
                {"symbol": "ETH/USDT", "exchange": "binance"},
                {"symbol": "OP/USDT", "exchange": "binance"},
            ],
        )

        remove_response = self.client.post(
            "/api/remove_symbol",
            json={"symbol": "OP/USDT", "exchange": "binance", "timeframe": "15m"},
        )
        self.assertEqual(remove_response.status_code, 200)
        settings = self.client.get("/api/get_settings").get_json()
        self.assertNotIn({"symbol": "OP/USDT", "exchange": "binance"}, settings["watchlist"])

    def test_add_symbol_rejects_unverified_binance_futures_symbol(self):
        with mock.patch.object(
            self.app_module,
            "_list_binance_futures_symbols",
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "displayName": "BTC/USDT",
                }
            ],
        ), mock.patch.object(self.app_module, "_binance_stock_search_symbols", return_value=[]):
            response = self.client.post(
                "/api/add_symbol",
                json={"symbol": "SQQQUSDT", "exchange": "binance", "timeframe": "15m"},
            )

        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("not in verified Binance", payload["msg"])

    def test_health_endpoint_is_deployment_friendly(self):
        old_key = os.environ.get("DOUBAO_API_KEY")
        os.environ["DOUBAO_API_KEY"] = "super-secret-doubao-key"
        response = self.client.get("/api/health")
        try:
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["service"], "ikunance-backend")
            self.assertIn("liveMarket", payload)
            self.assertIn("version", payload)
            self.assertIn("uptime_seconds", payload)
            self.assertIn("dataDir", payload)
            self.assertIn("frontendDist", payload)
            self.assertIn("wsSubscriptions", payload)
            self.assertTrue(payload["dataDir"]["exists"])
            self.assertTrue(payload["dataDir"]["writable"])
            self.assertTrue(payload["frontendDist"]["exists"])
            self.assertTrue(payload["diagnostics"]["dataWritable"])
            self.assertTrue(payload["diagnostics"]["frontendReady"])
            self.assertTrue(payload["diagnostics"]["secretsConfigured"]["doubaoApiKey"])
            self.assertNotIn("super-secret-doubao-key", json.dumps(payload))
        finally:
            if old_key is None:
                os.environ.pop("DOUBAO_API_KEY", None)
            else:
                os.environ["DOUBAO_API_KEY"] = old_key

    def test_wsgi_entrypoint_exposes_same_flask_app(self):
        wsgi = importlib.import_module("wsgi")

        self.assertIs(wsgi.app, self.app_module.app)
        self.assertIn("app", getattr(wsgi, "__all__", []))

        response = wsgi.app.test_client().get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "ok")

    def test_react_shell_is_served_from_available_dist(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers.get("Cache-Control", ""))
        body = response.get_data(as_text=True)
        response.close()
        self.assertIn("<!doctype html>", body.lower())
        self.assertIn("IKUNANCE", body)

        asset_response = self.client.get("/logo-white.svg")
        self.assertEqual(asset_response.status_code, 200)
        self.assertIn("image/svg", asset_response.content_type)
        asset_response.close()

    def test_security_headers_are_present_on_api_responses(self):
        response = self.client.get("/api/health")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "SAMEORIGIN")
        self.assertIn("microphone=()", response.headers["Permissions-Policy"])

        os.environ["IKUNANCE_FORCE_HTTPS"] = "1"
        try:
            https_response = self.client.get("/api/health")
            self.assertIn("max-age=31536000", https_response.headers["Strict-Transport-Security"])
        finally:
            os.environ.pop("IKUNANCE_FORCE_HTTPS", None)

    def test_unknown_api_routes_return_stable_json_errors(self):
        missing = self.client.get("/api/not-a-real-route")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.get_json()["status"], "error")

        wrong_method = self.client.put("/api/auth/login")
        self.assertEqual(wrong_method.status_code, 405)
        self.assertEqual(wrong_method.get_json()["msg"], "method not allowed")

    def test_auth_rate_limit_returns_429_with_retry_after(self):
        self.app_module._rate_limit_buckets.clear()
        os.environ["IKUNANCE_AUTH_RATE_LIMIT"] = "1"
        os.environ["IKUNANCE_AUTH_RATE_WINDOW"] = "60"
        try:
            first = self.client.post(
                "/api/auth/login",
                json={"email": "none@example.com", "password": "password123"},
                environ_base={"REMOTE_ADDR": "203.0.113.10"},
            )
            self.assertEqual(first.status_code, 200)
            second = self.client.post(
                "/api/auth/login",
                json={"email": "none@example.com", "password": "password123"},
                environ_base={"REMOTE_ADDR": "203.0.113.10"},
            )
            self.assertEqual(second.status_code, 429)
            self.assertEqual(second.get_json()["status"], "error")
            self.assertIn("Retry-After", second.headers)
        finally:
            os.environ.pop("IKUNANCE_AUTH_RATE_LIMIT", None)
            os.environ.pop("IKUNANCE_AUTH_RATE_WINDOW", None)
            self.app_module._rate_limit_buckets.clear()

    def test_numeric_env_helpers_fall_back_for_invalid_values(self):
        self.assertEqual(self.app_module._positive_int("bad", 8), 8)
        self.assertEqual(self.app_module._positive_int("-1", 8), 8)
        self.assertEqual(self.app_module._positive_int("3", 8), 3)

        old_limit = os.environ.get("IKUNANCE_AUTH_RATE_LIMIT")
        old_window = os.environ.get("IKUNANCE_AUTH_RATE_WINDOW")
        os.environ["IKUNANCE_AUTH_RATE_LIMIT"] = "bad"
        os.environ["IKUNANCE_AUTH_RATE_WINDOW"] = "also-bad"
        try:
            self.app_module._rate_limit_buckets.clear()
            response = self.client.post(
                "/api/auth/login",
                json={"email": "none@example.com", "password": "password123"},
            )
            self.assertNotEqual(response.status_code, 500)
            self.assertEqual(response.get_json()["status"], "error")
        finally:
            if old_limit is None:
                os.environ.pop("IKUNANCE_AUTH_RATE_LIMIT", None)
            else:
                os.environ["IKUNANCE_AUTH_RATE_LIMIT"] = old_limit
            if old_window is None:
                os.environ.pop("IKUNANCE_AUTH_RATE_WINDOW", None)
            else:
                os.environ["IKUNANCE_AUTH_RATE_WINDOW"] = old_window
            self.app_module._rate_limit_buckets.clear()

    def test_runtime_storage_paths_are_absolute_and_configurable(self):
        self.assertTrue(os.path.isabs(self.app_module.USER_DATA_DIR))
        self.assertTrue(os.path.isabs(self.app_module.UPLOAD_FOLDER))
        self.assertTrue(os.path.isabs(self.app_module.auth_service.ACCOUNTS_FILE))
        self.assertIn("user_data", self.app_module.USER_DATA_DIR)
        self.assertIn("custom_sounds", self.app_module.UPLOAD_FOLDER)

    def test_all_symbols_uses_backend_service_contract(self):
        response = self.client.get("/api/all_symbols")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("BTC/USDT", payload)
        self.assertIn("XRP/USDT", payload)
        self.assertIn("HYPE/USDT", payload)
        self.assertIn("PUMP/USDT", payload)
        self.assertEqual(len(payload), len(set(payload)))

    def test_all_symbols_query_exchange_is_passed_to_service(self):
        seen = []
        original = self.app_module._load_linear_usdt_symbols

        def fake_loader(exchange_id, ud):
            seen.append((exchange_id, ud.get("exchange_id")))
            return ["OKB/USDT"]

        self.app_module._all_symbols_cache.clear()
        self.app_module._load_linear_usdt_symbols = fake_loader
        try:
            response = self.client.get("/api/all_symbols?exchange=okx")
            self.assertEqual(response.status_code, 200)
            self.assertIn("OKB/USDT", response.get_json())
            self.assertEqual(seen, [("okx", "okx")])
        finally:
            self.app_module._load_linear_usdt_symbols = original
            self.app_module._all_symbols_cache.clear()

    def test_search_symbols_finds_core_and_hot_symbols(self):
        core = self.client.get("/api/search_symbols?q=xrp&exchange=binance")
        self.assertEqual(core.status_code, 200)
        self.assertIn("XRP/USDT", core.get_json()["data"])

        hot = self.client.get("/api/search_symbols?q=hyp&exchange=binance")
        self.assertEqual(hot.status_code, 200)
        self.assertIn("HYPE/USDT", hot.get_json()["data"])

        empty = self.client.get("/api/search_symbols?q=&exchange=binance")
        self.assertEqual(empty.status_code, 200)
        self.assertEqual(empty.get_json()["data"], [])

    def test_search_symbols_prioritizes_exact_and_prefix_matches(self):
        response = self.client.get("/api/search_symbols?q=ton&exchange=binance")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()["data"]
        self.assertGreaterEqual(len(payload), 1)
        self.assertEqual(payload[0], "TON/USDT")

    def test_search_symbols_returns_empty_after_live_lookup_misses_unknown_candidate(self):
        response = self.client.get("/api/search_symbols?q=abcxyz&exchange=binance")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["data"], [])

        invalid = self.client.get("/api/search_symbols?q=abc-xyz&exchange=binance")
        self.assertEqual(invalid.status_code, 200)
        self.assertEqual(invalid.get_json()["data"], [])

    def test_search_symbols_supports_verified_binance_stock_symbols(self):
        verified_stock_symbols = ["AAPL/USDT", "NVDA/USDT", "QQQ/USDT"]
        stock_patch = mock.patch.object(
            self.app_module,
            "_binance_stock_search_symbols",
            return_value=verified_stock_symbols,
        )
        stock_patch.start()
        self.addCleanup(stock_patch.stop)

        response = self.client.get("/api/search_symbols?q=nvda&exchange=binance_stock")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["data"][0], "NVDA/USDT")

        combined = self.client.get("/api/search_symbols?q=nvda&exchange=binance")
        self.assertEqual(combined.status_code, 200)
        combined_payload = combined.get_json()
        self.assertIn("NVDA/USDT", combined_payload["data"])
        stock_record = next(item for item in combined_payload["records"] if item["displayName"] == "NVDA/USDT")
        self.assertEqual(stock_record["exchangeId"], "binance_stock")
        self.assertEqual(stock_record["marketType"], "stock")

        all_symbols = self.client.get("/api/all_symbols?exchange=binance_stock")
        self.assertEqual(all_symbols.status_code, 200)
        payload = all_symbols.get_json()
        self.assertIn("AAPL/USDT", payload)
        self.assertIn("QQQ/USDT", payload)

    def test_scan_response_has_frontend_required_fields(self):
        response = self.client.get(
            "/api/scan?timeframe=15m&trigger=close&exchange=binance&symbols=BTC/USDT,ETH/USDT"
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("data", payload)
        self.assertIn("errors", payload)
        self.assertGreaterEqual(len(payload["data"]), 1)
        required = {"symbol", "price", "trend", "signal", "detail", "action", "candle_time"}
        self.assertTrue(required.issubset(payload["data"][0].keys()))

    def test_scan_accepts_legacy_wl_query_param(self):
        response = self.client.get("/api/scan?timeframe=15m&trigger=close&exchange=binance&wl=BTCUSDT,ETHUSDT")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([item["symbol"] for item in payload["data"]], ["BTC/USDT", "ETH/USDT"])
        self.assertTrue(all(item["detail"] for item in payload["data"]))

    def test_scan_empty_list_returns_stable_shape(self):
        response = self.client.get("/api/scan?timeframe=15m&trigger=close&exchange=binance&symbols=")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["data"], [])
        self.assertEqual(payload["alerts"], [])
        self.assertEqual(payload["errors"], [])

    def test_scan_rejects_invalid_params_before_work(self):
        bad_timeframe = self.client.get("/api/scan?timeframe=2h&trigger=close&symbols=BTC/USDT")
        self.assertEqual(bad_timeframe.status_code, 400)
        self.assertEqual(bad_timeframe.get_json()["msg"], "invalid timeframe")

        bad_trigger = self.client.get("/api/scan?timeframe=15m&trigger=bad&symbols=BTC/USDT")
        self.assertEqual(bad_trigger.status_code, 400)
        self.assertEqual(bad_trigger.get_json()["msg"], "invalid trigger")

        symbols = ",".join(f"SYM{i}/USDT" for i in range(21))
        too_many = self.client.get(f"/api/scan?timeframe=15m&trigger=close&symbols={symbols}")
        self.assertEqual(too_many.status_code, 400)
        self.assertEqual(too_many.get_json()["msg"], "too many symbols")

    def test_watchlist_accepts_exchange_id_object_contract(self):
        token = self._register_token()
        response = self.client.post(
            "/api/add_symbol",
            json={"symbol": "eth", "exchangeId": "okx", "timeframe": "15m"},
            headers={"X-Token": token},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        self.assertIn({"symbol": "ETH/USDT", "exchange": "okx"}, payload["watchlist"])

        duplicate = self.client.post(
            "/api/add_symbol",
            json={"symbol": "ETH/USDT:USDT", "exchange": "okx", "timeframe": "15m"},
            headers={"X-Token": token},
        )
        self.assertEqual(duplicate.status_code, 200)
        duplicate_payload = duplicate.get_json()
        matches = [item for item in duplicate_payload["watchlist"] if item == {"symbol": "ETH/USDT", "exchange": "okx"}]
        self.assertEqual(len(matches), 1)

    def test_watchlist_limit_allows_twenty_symbols(self):
        token = self._register_token()
        symbols = [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
            "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "TON/USDT",
            "DOT/USDT", "LTC/USDT", "BCH/USDT", "TRX/USDT", "NEAR/USDT",
            "APT/USDT", "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT",
        ]
        for symbol in symbols:
            response = self.client.post(
                "/api/add_symbol",
                json={"symbol": symbol, "exchange": "binance", "timeframe": "15m"},
                headers={"X-Token": token},
            )
            self.assertEqual(response.status_code, 200, response.get_data(as_text=True))

        too_many = self.client.post(
            "/api/add_symbol",
            json={"symbol": "TIA/USDT", "exchange": "binance", "timeframe": "15m"},
            headers={"X-Token": token},
        )
        self.assertEqual(too_many.status_code, 400)

    def test_scan_default_watchlist_uses_twenty_symbols(self):
        token = self._register_token()
        email = self.app_module.auth_service.load_accounts()["sessions"][token]
        symbols = [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
            "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "TON/USDT",
            "DOT/USDT", "LTC/USDT", "BCH/USDT", "TRX/USDT", "NEAR/USDT",
            "APT/USDT", "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT",
        ]
        self.app_module.save_user_config(email, {
            "watchlist": [{"symbol": symbol, "exchange": "binance"} for symbol in symbols],
            "alert_settings": {},
        })

        seen = []

        def fake_scan(symbol, timeframe, trigger_mode, ud):
            seen.append(symbol)
            return None, None

        with mock.patch.object(self.app_module, "_scan_one_symbol", side_effect=fake_scan):
            response = self.client.get("/api/scan?timeframe=15m&trigger=close", headers={"X-Token": token})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(seen), 20)

    def test_watchlist_rejects_empty_symbol_with_400(self):
        response = self.client.post("/api/add_symbol", json={"symbol": ""})
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")

    def test_auth_validation_login_and_logout_contract(self):
        invalid = self.client.post("/api/auth/register", json={"email": "bad", "password": "short"})
        self.assertEqual(invalid.status_code, 200)
        self.assertEqual(invalid.get_json()["status"], "error")

        old_force_https = os.environ.get("IKUNANCE_FORCE_HTTPS")
        os.environ["IKUNANCE_FORCE_HTTPS"] = "1"
        email = f"auth-{uuid.uuid4()}@example.com"
        registered_response = self.client.post(
            "/api/auth/register",
            json={"email": email, "password": "password123", "nickname": "authuser"},
        )
        registered = registered_response.get_json()
        cookie = registered_response.headers.get("Set-Cookie", "")
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Lax", cookie)
        self.assertIn("Secure", cookie)
        if old_force_https is None:
            os.environ.pop("IKUNANCE_FORCE_HTTPS", None)
        else:
            os.environ["IKUNANCE_FORCE_HTTPS"] = old_force_https
        self.assertEqual(registered["status"], "success")
        self.assertGreater(len(registered["token"]), 30)
        accounts = self.app_module.auth_service.load_accounts()
        stored_hash = accounts["users"][email]["password"]
        self.assertTrue(stored_hash.startswith("pbkdf2_sha256$"))
        self.assertNotEqual(stored_hash, hashlib.sha256("password123".encode()).hexdigest())

        checked = self.client.post("/api/auth/check", json={"token": registered["token"]}).get_json()
        self.assertEqual(checked["status"], "ok")
        self.assertEqual(checked["user"]["email"], email)

        logout = self.client.post("/api/auth/logout", json={"token": registered["token"]}).get_json()
        self.assertEqual(logout["status"], "success")
        checked_after_logout = self.client.post("/api/auth/check", json={"token": registered["token"]}).get_json()
        self.assertEqual(checked_after_logout["status"], "error")

    def test_builtin_admin_login_and_check_return_admin_role(self):
        self.app_module._rate_limit_buckets.clear()
        response = self.client.post(
            "/api/auth/login",
            json={"email": "111", "password": "123123123"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["user"]["email"], "111")
        self.assertEqual(payload["user"]["role"], "admin")

        checked = self.client.post("/api/auth/check", json={"token": payload["token"]}).get_json()
        self.assertEqual(checked["status"], "ok")
        self.assertEqual(checked["user"]["email"], "111")
        self.assertEqual(checked["user"]["role"], "admin")

        admin_hash = self.app_module.auth_service.load_accounts()["users"]["111"]["password"]
        self.assertTrue(admin_hash.startswith("pbkdf2_sha256$"))
        self.assertNotEqual(admin_hash, "123123123")

    def test_authenticated_settings_accept_authorization_header_and_logout_clears_cookie(self):
        token = self._register_token()
        saved = self.client.post(
            "/api/save_settings",
            json={"exchangeId": "okx", "watchlist": [{"symbol": "XRP/USDT", "exchange": "okx"}]},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.get_json()["status"], "success")

        settings = self.client.get("/api/get_settings", headers={"Authorization": f"Bearer {token}"}).get_json()
        self.assertEqual(settings["exchangeId"], "okx")
        self.assertEqual(settings["watchlist"], [{"symbol": "XRP/USDT", "exchange": "okx"}])

        logout = self.client.post("/api/auth/logout", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(logout.get_json()["status"], "success")
        self.assertIn("ikun_token=;", logout.headers.get("Set-Cookie", ""))

        checked = self.client.post("/api/auth/check", headers={"Authorization": f"Bearer {token}"}).get_json()
        self.assertEqual(checked["status"], "error")

    def test_legacy_sha256_password_is_upgraded_on_login(self):
        email = f"legacy-{uuid.uuid4()}@example.com"
        legacy_hash = hashlib.sha256("password123".encode()).hexdigest()
        with open(self.app_module.auth_service.ACCOUNTS_FILE, "w", encoding="utf-8") as handle:
            json.dump({
                "users": {
                    email: {
                        "email": email,
                        "password": legacy_hash,
                        "nickname": "legacy",
                    }
                },
                "sessions": {},
            }, handle)

        login = self.client.post(
            "/api/auth/login",
            json={"email": email, "password": "password123"},
        ).get_json()
        self.assertEqual(login["status"], "success")
        upgraded = self.app_module.auth_service.load_accounts()["users"][email]["password"]
        self.assertTrue(upgraded.startswith("pbkdf2_sha256$"))
        self.assertNotEqual(upgraded, legacy_hash)

    def test_change_password_uses_pbkdf2_hash(self):
        token = self._register_token()
        changed = self.client.post(
            "/api/auth/change_password",
            json={
                "token": token,
                "oldPassword": "password123",
                "newPassword": "newpassword123",
            },
        ).get_json()
        self.assertEqual(changed["status"], "success")
        email = self.client.post("/api/auth/check", json={"token": token}).get_json()["user"]["email"]
        stored_hash = self.app_module.auth_service.load_accounts()["users"][email]["password"]
        self.assertTrue(stored_hash.startswith("pbkdf2_sha256$"))
        self.assertNotEqual(stored_hash, hashlib.sha256("newpassword123".encode()).hexdigest())

    def test_post_routes_handle_empty_or_non_json_body_without_500(self):
        for path in ["/api/auth/google", "/api/auth/logout", "/api/delete_sound", "/api/community/posts"]:
            response = self.client.post(path, data="", content_type="text/plain")
            self.assertNotEqual(response.status_code, 500, path)
            self.assertIsNotNone(response.get_json(), path)

    def test_sound_upload_sanitizes_filename_and_delete_rejects_path_traversal(self):
        response = self.client.post(
            "/api/upload_sound",
            data={
                "name": "../../Unsafe Sound",
                "file": (io.BytesIO(b"fake mp3 bytes"), "alert.mp3"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        saved = payload["sounds"][0]["file"]
        self.assertEqual(saved, "Unsafe_Sound.mp3")
        self.assertTrue((Path(self.app_module.UPLOAD_FOLDER) / saved).exists())

        traversal = self.client.post("/api/delete_sound", json={"file": "../Unsafe_Sound.mp3"})
        self.assertEqual(traversal.status_code, 200)
        self.assertTrue((Path(self.app_module.UPLOAD_FOLDER) / saved).exists())

        deleted = self.client.post("/api/delete_sound", json={"file": saved})
        self.assertEqual(deleted.status_code, 200)
        self.assertFalse((Path(self.app_module.UPLOAD_FOLDER) / saved).exists())

    def test_sound_upload_rejects_non_mp3_extension(self):
        response = self.client.post(
            "/api/upload_sound",
            data={
                "name": "bad",
                "file": (io.BytesIO(b"not mp3"), "bad.wav"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "error")

    def test_sound_upload_too_large_returns_json_413(self):
        old_limit = self.app_module.app.config["MAX_CONTENT_LENGTH"]
        self.app_module.app.config["MAX_CONTENT_LENGTH"] = 128
        try:
            response = self.client.post(
                "/api/upload_sound",
                data={
                    "name": "large",
                    "file": (io.BytesIO(b"x" * 1024), "large.mp3"),
                },
                content_type="multipart/form-data",
            )
        finally:
            self.app_module.app.config["MAX_CONTENT_LENGTH"] = old_limit
        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.get_json()["status"], "error")

    def test_ws_subscription_normalizes_and_deduplicates(self):
        self.client.post("/api/ws/subscribe", json={"exchange": "BINANCE", "symbol": "btc/usdt", "timeframe": "15m"})
        self.client.post("/api/ws/subscribe", json={"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "15m"})
        payload = self.client.get("/api/ws/subscriptions").get_json()
        self.assertEqual(payload["subscriptions"].count({"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "15m"}), 1)

    def test_ws_published_signal_is_normalized_for_stream_consumers(self):
        from services import ws_engine

        ws_engine.stop_engine()
        listener = ws_engine.register_sse_listener()
        try:
            ws_engine.publish_signal({
                "exchange": "BINANCE",
                "symbol": "eth/usdt",
                "timeframe": "1H",
                "action": "LONG",
            })
            payload = listener.get(timeout=1)
            self.assertEqual(payload["exchange"], "binance")
            self.assertEqual(payload["symbol"], "ETH/USDT")
            self.assertEqual(payload["timeframe"], "1h")
            self.assertEqual(payload["type"], "signal")
            self.assertTrue(payload["id"])
        finally:
            ws_engine.unregister_sse_listener(listener)

    def test_trigger_alert_publishes_to_stream_consumers(self):
        from services import ws_engine
        from services.signal_engine import SignalResult, trigger_alert

        ws_engine.stop_engine()
        listener = ws_engine.register_sse_listener()
        candle_time = (time.time() - 60) * 1000
        try:
            trigger_alert(
                SignalResult(
                    symbol="BTC/USDT",
                    price=100.0,
                    trend="BULL",
                    signal="test signal",
                    detail="test detail",
                    action="LONG",
                    candle_time=candle_time,
                ),
                {"exchange_id": "binance"},
                "15m",
            )
            payload = listener.get(timeout=1)
            self.assertEqual(payload["exchange"], "binance")
            self.assertEqual(payload["symbol"], "BTC/USDT")
            self.assertEqual(payload["timeframe"], "15m")
            self.assertEqual(payload["action"], "LONG")
        finally:
            ws_engine.unregister_sse_listener(listener)

    def test_ws_email_callback_treats_missing_watchlist_mode_as_favorites(self):
        uid = self._register_email()
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "timeframe": "15m",
            "alert_settings": {"email": True},
        })
        signal = {
            "symbol": "ETH/USDT",
            "exchange": "binance",
            "timeframe": "15m",
            "action": "LONG",
            "type": "test",
            "detail": "detail",
            "price": 100,
            "candle_time": time.time() * 1000,
        }
        with mock.patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             mock.patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            self.app_module._on_ws_signal(signal)
            time.sleep(0.2)
        send_email.assert_not_called()

    def test_ws_email_callback_sends_for_matching_watchlist_symbol(self):
        uid = self._register_email()
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "timeframe": "15m",
            "alert_settings": {"email": True},
        })
        signal = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timeframe": "15m",
            "action": "LONG",
            "type": "test",
            "detail": "detail",
            "trend": "BULL",
            "price": 100,
            "candle_time": time.time() * 1000,
        }
        with mock.patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             mock.patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            self.app_module._on_ws_signal(signal)
            time.sleep(0.2)
        send_email.assert_called()
        subject, content, *_ = send_email.call_args.args
        self.assertIn("📈 上涨 LONG", subject)
        self.assertIn("📈 上涨 LONG", content)

    def test_ws_email_callback_ignores_manual_scan_origin(self):
        uid = self._register_email()
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "timeframe": "15m",
            "alert_settings": {"email": True},
        })
        signal = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timeframe": "15m",
            "action": "LONG",
            "type": "test",
            "detail": "detail",
            "trend": "BULL",
            "price": 100,
            "candle_time": time.time() * 1000,
            "origin": "manual_scan",
        }
        with mock.patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             mock.patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            self.app_module._on_ws_signal(signal)
            time.sleep(0.2)
        send_email.assert_not_called()

    def test_ws_email_callback_defaults_null_exchange_and_timeframe(self):
        uid = self._register_email()
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "exchange_id": None,
            "timeframe": None,
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "alert_settings": {"email": True},
        })
        signal = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timeframe": "15m",
            "action": "SHORT",
            "type": "test",
            "detail": "detail",
            "trend": "BEAR",
            "price": 100,
            "candle_time": time.time() * 1000,
        }
        with mock.patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             mock.patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            self.app_module._on_ws_signal(signal)
            time.sleep(0.2)
        send_email.assert_called()
        subject, content, *_ = send_email.call_args.args
        self.assertIn("📉 下跌 SHORT", subject)
        self.assertIn("Trend:      BEAR", content)
        self.assertIn("Timeframe:  15m", content)

    def test_ws_email_callback_ignores_anonymous_user_configs(self):
        uid = f"anon_{uuid.uuid4()}"
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "timeframe": "15m",
            "alert_settings": {"email": True},
        })
        signal = {
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "timeframe": "15m",
            "action": "LONG",
            "type": "test",
            "detail": "detail",
            "price": 100,
            "candle_time": time.time() * 1000,
        }
        with mock.patch.dict(os.environ, {"IKUNANCE_DISABLE_OUTBOUND": "0"}), \
             mock.patch("services.notification_service.send_email_sync", return_value=(True, "sent")) as send_email:
            self.app_module._on_ws_signal(signal)
            time.sleep(0.2)
        send_email.assert_not_called()

    def test_push_monitor_targets_include_email_enabled_watchlist(self):
        uid = self._register_email()
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "exchange_id": None,
            "timeframe": None,
            "watchlist": ["BTC/USDT", "ETH/USDT"],
            "alert_settings": {"email": True},
        })
        targets = self.app_module._iter_push_monitor_targets()
        keys = {(item["exchange"], item["symbol"], item["timeframe"]) for item in targets}
        self.assertIn(("binance", "BTC/USDT", "15m"), keys)
        self.assertIn(("binance", "ETH/USDT", "15m"), keys)
        btc_target = next(item for item in targets if item["symbol"] == "BTC/USDT")
        self.assertEqual(btc_target["scan_ud"]["_notification_origin"], "push_monitor")
        self.assertEqual(btc_target["scan_ud"]["_dedupe_scope"], "push:binance:BTC/USDT:15m")

    def test_push_monitor_primes_binance_kline_streams_for_targets(self):
        targets = [
            {"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "15m"},
            {"exchange": "binance_stock", "symbol": "AAPL/USDT", "timeframe": "15m"},
            {"exchange": "binance", "symbol": "ETH/USDT", "timeframe": "15m"},
        ]
        with mock.patch.object(self.app_module, "_ensure_binance_kline_streams") as ensure_streams:
            self.app_module._ensure_push_monitor_kline_streams(targets)

        ensure_streams.assert_called_once()
        self.assertEqual(
            set(ensure_streams.call_args.args[0]),
            {("BTC/USDT", "15m"), ("ETH/USDT", "15m")},
        )

    def test_push_monitor_targets_ignore_anonymous_user_configs(self):
        uid = f"anon_{uuid.uuid4()}"
        self.app_module.save_user_config(uid, {
            "email": "sender@example.com",
            "email_pass": "secret",
            "exchange_id": "binance",
            "timeframe": "15m",
            "watchlist": ["BTC/USDT"],
            "alert_settings": {"email": True},
        })
        targets = self.app_module._iter_push_monitor_targets()
        self.assertEqual(targets, [])

    def test_push_monitor_uses_fifteen_minute_scan_window(self):
        old_window = os.environ.get("IKUNANCE_PUSH_MONITOR_WINDOW")
        os.environ["IKUNANCE_PUSH_MONITOR_WINDOW"] = "60"
        try:
            self.assertEqual(self.app_module._push_monitor_cadence_seconds(), 900)
            self.assertEqual(self.app_module._push_monitor_interval_seconds(), 900)
            self.assertEqual(self.app_module._push_monitor_window_seconds(), 60)

            active = self.app_module._push_monitor_window_state(900 + 30)
            self.assertTrue(active["active"])
            self.assertEqual(active["start_ts"], 900)
            self.assertEqual(active["end_ts"], 960)
            self.assertEqual(active["next_start_ts"], 1800)

            inactive = self.app_module._push_monitor_window_state(960)
            self.assertFalse(inactive["active"])
            self.assertEqual(inactive["next_start_ts"], 1800)
            self.assertEqual(inactive["sleep_seconds"], 840)
        finally:
            if old_window is None:
                os.environ.pop("IKUNANCE_PUSH_MONITOR_WINDOW", None)
            else:
                os.environ["IKUNANCE_PUSH_MONITOR_WINDOW"] = old_window

    def test_scan_one_symbol_passes_market_source_to_alert(self):
        class FakeResult:
            symbol = "BTC/USDT"
            price = 100.0
            trend = "BULL"
            signal = "trend continuation"
            detail = "histogram expanding"
            action = "LONG"
            candle_time = time.time() * 1000

        ohlcv = [
            [1_700_000_000_000, 99.0, 101.0, 98.0, 100.0, 1000, "binance_ws_live"],
            [1_700_000_900_000, 100.0, 102.0, 99.0, 101.0, 1000, "binance_ws_live"],
        ]
        with mock.patch.object(self.app_module, "_fetch_ohlcv_cached", return_value=ohlcv) as fetch_ohlcv, \
             mock.patch.object(self.app_module, "_analyze_symbol", return_value=FakeResult()) as analyze_symbol, \
             mock.patch.object(self.app_module, "_trigger_alert", return_value={"symbol": "BTC/USDT"}) as trigger_alert:
            result, signal, error = self.app_module._scan_one_symbol(
                "BTC/USDT",
                "15m",
                "close",
                {"exchange_id": "binance"},
            )

        self.assertIsNone(error)
        self.assertEqual(result.symbol, "BTC/USDT")
        self.assertEqual(signal, {"symbol": "BTC/USDT"})
        self.assertEqual(fetch_ohlcv.call_args.kwargs["limit"], 80)
        self.assertEqual(analyze_symbol.call_args.args[2], "close")
        alert_ud = trigger_alert.call_args.args[1]
        self.assertEqual(alert_ud["_market_source"], "binance_ws_live")
        self.assertEqual(alert_ud["_strategy_source"], "MACD histogram three-bar capture")

    def test_scan_response_includes_triggered_alerts_for_frontend_push(self):
        class FakeResult:
            symbol = "BTC/USDT"
            price = 100.0
            trend = "BULL"
            signal = "test signal"
            detail = "test detail"
            action = "LONG"
            candle_time = time.time() * 1000

        fake_alert = {
            "symbol": "BTC/USDT",
            "signal": "test signal",
            "detail": "test detail",
            "action": "LONG",
            "price": 100.0,
            "timeframe": "15m",
        }
        with mock.patch.object(self.app_module, "_scan_one_symbol", return_value=(FakeResult(), fake_alert)), \
             mock.patch.object(self.app_module, "send_all_notifications", return_value={"status": "success"}):
            response = self.client.get("/api/scan?timeframe=15m&trigger=close&exchange=binance&symbols=BTC/USDT")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["alerts"], [fake_alert])
        self.assertEqual(payload["data"][0]["symbol"], "BTC/USDT")

    def test_web_signals_tolerates_invalid_since(self):
        response = self.client.get("/api/web_signals?since=not-a-number")
        self.assertEqual(response.status_code, 200)
        self.assertIn("signals", response.get_json())

    def test_web_signals_are_filtered_by_user_watchlist(self):
        from services import signal_engine

        token = self._register_token()
        email = self.app_module.auth_service.load_accounts()["sessions"][token]
        self.app_module.save_user_config(email, {
            "exchange_id": "binance",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "watchlist_mode": "favorites",
            "alert_settings": {},
        })
        old_signals = list(signal_engine._web_signals)
        old_dedup = set(signal_engine._web_dedup)
        signal_engine._web_signals[:] = [
            {"symbol": "BTC/USDT", "exchange": "binance", "action": "LONG", "_ts": time.time()},
            {"symbol": "ETH/USDT", "exchange": "binance", "action": "LONG", "_ts": time.time()},
        ]
        signal_engine._web_dedup.clear()
        try:
            response = self.client.get("/api/web_signals", headers={"X-Token": token})
            self.assertEqual(response.status_code, 200)
            symbols = [item["symbol"] for item in response.get_json()["signals"]]
            self.assertEqual(symbols, ["BTC/USDT"])
        finally:
            signal_engine._web_signals[:] = old_signals
            signal_engine._web_dedup.clear()
            signal_engine._web_dedup.update(old_dedup)

    def test_mobile_recent_signals_are_filtered_and_formatted(self):
        from services import signal_engine

        token = self._register_token()
        email = self.app_module.auth_service.load_accounts()["sessions"][token]
        self.app_module.save_user_config(email, {
            "exchange_id": "binance",
            "watchlist": [{"symbol": "BTC/USDT", "exchange": "binance"}],
            "watchlist_mode": "favorites",
            "timeframe": "15m",
            "alert_settings": {},
        })
        old_history_file = signal_engine._SIGNALS_FILE
        old_history_dedup = signal_engine._history_dedup
        tmp = Path(tempfile.mkdtemp()) / "history.json"
        signal_engine._SIGNALS_FILE = str(tmp)
        signal_engine._history_dedup = None
        now_text = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            signal_engine.append_signal_history({
                "symbol": "BTC/USDT", "exchange": "binance", "action": "LONG",
                "type": "趋势演进", "detail": "绿柱首次增强", "price": 1,
                "timeframe": "15m", "candle_time": 1_700_000_000_000, "time": now_text,
            })
            signal_engine.append_signal_history({
                "symbol": "ETH/USDT", "exchange": "binance", "action": "SHORT",
                "type": "趋势演进", "detail": "红柱首次增强", "price": 1,
                "timeframe": "15m", "candle_time": 1_700_000_900_000, "time": now_text,
            })
            response = self.client.get("/api/mobile/recent-signals", headers={"X-Token": token})
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual([item["symbol"] for item in payload["signals"]], ["BTC/USDT"])
            self.assertIn("title", payload["signals"][0])
            self.assertIn("body", payload["signals"][0])
            self.assertIn("I-KUNANCE realtime signal notification", payload["signals"][0]["body"])
        finally:
            signal_engine._SIGNALS_FILE = old_history_file
            signal_engine._history_dedup = old_history_dedup

    def test_market_movers_returns_frontend_success_contract(self):
        tickers = {
            "AAA/USDT": {"percentage": 20, "last": 1},
            "BBB/USDT": {"percentage": 15, "last": 1},
            "CCC/USDT": {"percentage": 10, "last": 1},
            "DDD/USDT": {"percentage": 8, "last": 1},
            "EEE/USDT": {"percentage": 5, "last": 1},
            "FFF/USDT": {"percentage": 1, "last": 1},
            "VVV/USDT": {"percentage": -30, "last": 1},
            "WWW/USDT": {"percentage": -25, "last": 1},
            "XXX/USDT": {"percentage": -12, "last": 1},
            "YYY/USDT": {"percentage": -8, "last": 1},
            "ZZZ/USDT": {"percentage": -3, "last": 1},
            "SMALL/USDT": {"percentage": -1, "last": 1},
        }
        with mock.patch.object(self.app_module, "_fetch_tickers_safe", return_value=tickers):
            response = self.client.get("/api/market_movers")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        self.assertIsInstance(payload["data"], list)
        self.assertEqual(len(payload["data"]), 10)
        self.assertEqual([item["symbol"] for item in payload["data"][:5]], ["AAA/USDT", "BBB/USDT", "CCC/USDT", "DDD/USDT", "EEE/USDT"])
        self.assertEqual([item["symbol"] for item in payload["data"][5:]], ["VVV/USDT", "WWW/USDT", "XXX/USDT", "YYY/USDT", "ZZZ/USDT"])
        first = payload["data"][0]
        self.assertTrue({"symbol", "price", "percentage", "change", "exchange"}.issubset(first.keys()))

    def test_misc_api_routes_smoke_without_live_dependencies(self):
        token = self._register_token()
        self.client.post(
            "/api/save_settings",
            json={"exchangeId": "okx"},
            headers={"X-Token": token},
        )

        requests = [
            ("GET", "/api/list_sounds", None),
            ("GET", "/api/signal_history", None),
            ("GET", "/api/market_movers", None),
            ("GET", "/api/scan_io", None),
            ("POST", "/api/test_push", {"channel": "email"}),
            ("POST", "/api/ai/chat", {"messages": [], "stream": False}),
            ("GET", "/api/community/news", None),
        ]
        for method, path, body in requests:
            headers = {"X-Token": token} if path in {"/api/scan_io", "/api/ai/chat"} else {}
            if method == "GET":
                response = self.client.get(path, headers=headers)
            else:
                response = self.client.post(path, json=body, headers=headers)
            self._assert_json_not_500(response, path)

        ai_payload = self.client.post(
            "/api/ai/chat",
            json={"messages": [], "stream": False},
            headers={"X-Token": token},
        ).get_json()
        self.assertEqual(ai_payload["status"], "error")
        self.assertIn("key", ai_payload["msg"].lower())

    def test_scan_io_returns_stable_shape_when_exchange_has_no_symbols(self):
        class EmptyExchange:
            symbols = []
            markets = {}

            def load_markets(self):
                return None

            def fetch_tickers(self, symbols):
                return {}

        with mock.patch.object(self.app_module, "get_exchange", return_value=EmptyExchange()):
            response = self.client.get("/api/scan_io")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["data"], [])
        self.assertIn("errors", payload)

    def test_community_post_rejects_non_string_content_without_500(self):
        response = self.client.post(
            "/api/community/posts",
            json={"content": {"bad": "shape"}, "tag": ["not", "string"]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("content", payload["msg"])

    def test_ai_chat_non_stream_upstream_failures_return_stable_json(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def read(self):
                return b'{"choices":[]}'

        old_key = os.environ.get("DOUBAO_API_KEY")
        os.environ["DOUBAO_API_KEY"] = "test-key"
        try:
            with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("network down")):
                unavailable = self.client.post(
                    "/api/ai/chat",
                    json={"messages": [{"role": "user", "content": "hi"}], "stream": False},
                )
            self.assertEqual(unavailable.status_code, 502)
            self.assertEqual(unavailable.get_json()["status"], "error")
            self.assertIn("upstream", unavailable.get_json()["msg"])

            with mock.patch("urllib.request.urlopen", return_value=FakeResponse()):
                malformed = self.client.post(
                    "/api/ai/chat",
                    json={"messages": [{"role": "user", "content": "hi"}], "stream": False},
                )
            self.assertEqual(malformed.status_code, 502)
            self.assertEqual(malformed.get_json()["msg"], "invalid Doubao response")
        finally:
            if old_key is None:
                os.environ.pop("DOUBAO_API_KEY", None)
            else:
                os.environ["DOUBAO_API_KEY"] = old_key

    def test_community_post_delete_requires_owner(self):
        token = self._register_token()
        created = self.client.post(
            "/api/community/posts",
            json={"content": "hello market", "tag": "test"},
            headers={"X-Token": token},
        ).get_json()
        self.assertEqual(created["status"], "success")
        post_id = created["post"]["id"]

        denied = self.client.delete(f"/api/community/posts/{post_id}").get_json()
        self.assertEqual(denied["status"], "error")

        deleted = self.client.delete(f"/api/community/posts/{post_id}", headers={"X-Token": token}).get_json()
        self.assertEqual(deleted["status"], "success")

    def test_community_pagination_and_anonymous_identity_are_stable(self):
        listed = self.client.get("/api/community/posts?page=bad&page_size=999")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.get_json()["status"], "success")

        created = self.client.post("/api/community/posts", json={"content": "anon post", "tag": "x"})
        self.assertEqual(created.status_code, 200)
        self.assertIn("ikun_sid=", created.headers.get("Set-Cookie", ""))
        post_id = created.get_json()["post"]["id"]

        liked = self.client.post(f"/api/community/posts/{post_id}/like")
        self.assertEqual(liked.get_json()["post"]["likes"], 1)
        unliked = self.client.post(f"/api/community/posts/{post_id}/like")
        self.assertEqual(unliked.get_json()["post"]["likes"], 0)

    def test_indicator_library_validates_limits_and_missing_delete(self):
        token = self._register_token()
        too_long = self.client.post(
            "/api/indicators/save",
            json={"name": "x" * 81, "code": "//@version=5"},
            headers={"X-Token": token},
        )
        self.assertEqual(too_long.status_code, 400)
        self.assertEqual(too_long.get_json()["status"], "error")

        saved = self.client.post(
            "/api/indicators/save",
            json={"name": "MACD helper", "code": "//@version=5\nindicator('x')"},
            headers={"X-Token": token},
        )
        self.assertEqual(saved.status_code, 200)
        payload = saved.get_json()
        self.assertEqual(payload["status"], "success")
        indicator_id = payload["indicators"][0]["id"]

        missing = self.client.delete("/api/indicators/not-there", headers={"X-Token": token})
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.get_json()["status"], "error")

        deleted = self.client.delete(f"/api/indicators/{indicator_id}", headers={"X-Token": token})
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(deleted.get_json()["indicators"], [])

    def test_save_settings_normalizes_watchlist_payload(self):
        token = self._register_token()
        response = self.client.post(
            "/api/save_settings",
            json={
                "exchangeId": "bybit",
                "watchlist": [
                    "btc",
                    {"symbol": "SOL/USDT:USDT", "exchangeId": "okx"},
                    {"symbol": "SOL/USDT", "exchange": "okx"},
                ],
            },
            headers={"X-Token": token},
        )
        self.assertEqual(response.status_code, 200)

        settings = self.client.get("/api/get_settings", headers={"X-Token": token}).get_json()
        self.assertEqual(settings["exchangeId"], "bybit")
        self.assertEqual(settings["watchlist"], [
            {"symbol": "BTC/USDT", "exchange": "bybit"},
            {"symbol": "SOL/USDT", "exchange": "okx"},
        ])

    def test_save_settings_sanitizes_scalar_fields_without_breaking_contract(self):
        token = self._register_token()
        response = self.client.post(
            "/api/save_settings",
            json={
                "apiKey": {"bad": "shape"},
                "secretKey": ["bad"],
                "emailPass": 123,
                "doubaoApiKey": "  doubao-secret  ",
                "webhookUrl": " https://example.invalid/hook ",
                "timeframe": "2h",
                "triggerMode": "bad",
                "watchlistMode": {"bad": "shape"},
                "alertSettings": ["bad"],
            },
            headers={"X-Token": token},
        )
        self.assertEqual(response.status_code, 200)

        settings = self.client.get("/api/get_settings", headers={"X-Token": token}).get_json()
        self.assertEqual(settings["apiKey"], "")
        self.assertEqual(settings["secretKey"], "")
        self.assertEqual(settings["emailPass"], "")
        self.assertEqual(settings["doubaoApiKey"], "doubao-secret")
        self.assertEqual(settings["webhookUrl"], "https://example.invalid/hook")
        self.assertEqual(settings["timeframe"], "15m")
        self.assertEqual(settings["triggerMode"], "close")
        self.assertEqual(settings["watchlistMode"], "")


if __name__ == "__main__":
    unittest.main()
