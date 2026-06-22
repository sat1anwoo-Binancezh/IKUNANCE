import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_ROOT = PROJECT_ROOT / "frontend"


class FrontendSourceTests(unittest.TestCase):
    def test_frontend_build_contract_files_exist(self):
        required = [
            "package.json",
            "vite.config.js",
            "index.html",
            "src/main.jsx",
            "src/App.jsx",
            "src/GlobalNav.jsx",
            "src/styles/app.css",
            "src/styles/global-nav.css",
            "src/hooks/useAudio.js",
            "src/hooks/useAlerts.js",
            "src/hooks/useSignalStream.js",
            "src/components/AppSidebar.jsx",
            "src/components/AlertsTab.jsx",
            "src/components/SettingsModal.jsx",
            "src/components/SignalAlert.jsx",
            "src/components/WatchlistTab.jsx",
        ]
        for rel in required:
            self.assertTrue((FRONTEND_ROOT / rel).exists(), rel)

    def test_package_json_has_build_script(self):
        package = json.loads((FRONTEND_ROOT / "package.json").read_text(encoding="utf-8"))
        self.assertEqual(package["scripts"]["build"], "vite build")

    def test_watchlist_limit_and_signal_storage_are_scoped(self):
        watchlist_source = (FRONTEND_ROOT / "src/hooks/useWatchlist.js").read_text(encoding="utf-8")
        app_source = (FRONTEND_ROOT / "src/App.jsx").read_text(encoding="utf-8")
        nav_source = (FRONTEND_ROOT / "src/GlobalNav.jsx").read_text(encoding="utf-8")
        nav_css = (FRONTEND_ROOT / "src/styles/global-nav.css").read_text(encoding="utf-8")
        app_css = (FRONTEND_ROOT / "src/styles/app.css").read_text(encoding="utf-8")
        alerts_source = (FRONTEND_ROOT / "src/hooks/useAlerts.js").read_text(encoding="utf-8")
        stream_source = (FRONTEND_ROOT / "src/hooks/useSignalStream.js").read_text(encoding="utf-8")
        alerts_tab_source = (FRONTEND_ROOT / "src/components/AlertsTab.jsx").read_text(encoding="utf-8")

        self.assertIn("const WATCHLIST_LIMIT = 20", watchlist_source)
        self.assertIn("const [marketMovers, setMarketMovers] = useState([])", watchlist_source)
        self.assertIn("const [symbolsVerified, setSymbolsVerified] = useState(false)", watchlist_source)
        self.assertIn("未通过官方标的校验", watchlist_source)
        self.assertNotIn("自选上限10", watchlist_source)
        self.assertIn("const storageScope = effectiveUser?.email || 'anon'", app_source)
        self.assertIn("mergeWatchlistItems(existing, serverList)", app_source)
        self.assertNotIn("authToken || localStorage.getItem('ikun_token') || effectiveUser?.email", app_source)
        self.assertIn("res.status === 'ok'", app_source)
        self.assertIn("if (Array.isArray(d.watchlist))", app_source)
        self.assertIn("syncServerWatchlistToLocal(d.watchlist", app_source)
        self.assertIn("DEFAULT_ALERT_SETTINGS", app_source)
        self.assertIn("ikun_alert_settings_", app_source)
        self.assertIn("writeAlertSettingsStorage(storageScope, settings)", app_source)
        self.assertIn("normalizeAlertSettings(d.alertSettings)", app_source)
        self.assertIn("ALERT_LOG_KEY}_", alerts_source)
        self.assertIn("params.set('token', token)", stream_source)
        self.assertIn("gn-nav", nav_source)
        self.assertIn("gn-logo", nav_source)
        self.assertIn("THEMES", nav_source)
        self.assertIn(".gn-nav", nav_css)
        self.assertIn(".gn-btn--premium", nav_css)
        self.assertIn(":root{--bg-color:#030812", app_css)
        self.assertIn("<MarketPage", app_source)
        self.assertIn("effectivePage === 'market'", app_source)
        self.assertNotIn("requestedPage === 'market' ? 'monitor'", app_source)
        self.assertIn("const navActivePage = effectivePage === 'monitor' && activeTab === 'alerts' ? 'signals' : effectivePage", app_source)
        self.assertIn('activePage={navActivePage}', app_source)
        self.assertIn('https://followin.io/', app_source)
        self.assertNotIn('title="Binance Square"', app_source)
        self.assertIn("formatCopySymbol", alerts_tab_source)
        self.assertIn("copyText(formatCopySymbol(item.symbol))", alerts_tab_source)
        self.assertNotIn("hasDropdown", nav_source)
        self.assertIn("structured=1", watchlist_source)
        self.assertIn("res?.records", watchlist_source)
        self.assertIn("binance_stock", watchlist_source)
        index_source = (FRONTEND_ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn("__ikunBootGuardInstalled", index_source)
        self.assertIn("/assets/ikun-runtime-fixes.js", index_source)
        self.assertTrue((FRONTEND_ROOT / "public/assets/ikun-runtime-fixes.js").exists())

    def test_frontend_build_succeeds_when_dependencies_are_installed(self):
        if not (FRONTEND_ROOT / "node_modules").exists():
            self.skipTest("frontend node_modules is not installed")
        npm = shutil.which("npm") or shutil.which("npm.cmd")
        if not npm:
            self.skipTest("npm is not available")
        with tempfile.TemporaryDirectory(prefix=".vite-test-dist-", dir=FRONTEND_ROOT) as temp_dist:
            completed = subprocess.run(
                [npm, "run", "build", "--", "--outDir", temp_dist, "--emptyOutDir"],
                cwd=FRONTEND_ROOT,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=120,
                check=False,
            )
        self.assertEqual(completed.returncode, 0, completed.stdout)


if __name__ == "__main__":
    unittest.main()
