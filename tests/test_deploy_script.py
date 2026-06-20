import importlib.util
import io
import os
import re
import subprocess
import sys
import unittest
import urllib.error
from unittest import mock
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = PROJECT_ROOT / "deploy_script.py"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"


def env_names_from_example():
    names = set()
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Z][A-Z0-9_]+)=", line.strip())
        if match:
            names.add(match.group(1))
    return names


def load_deploy_module():
    spec = importlib.util.spec_from_file_location("ikunance_deploy_script", DEPLOY_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DeployScriptTest(unittest.TestCase):
    def test_script_uses_environment_variables_and_current_workspace_paths(self):
        source = DEPLOY_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("IKUNANCE_DEPLOY_HOST", source)
        self.assertIn("IKUNANCE_DEPLOY_USER", source)
        self.assertIn("IKUNANCE_DEPLOY_PASSWORD", source)
        self.assertIn("IKUNANCE_DEPLOY_HEALTH_URL", source)
        self.assertIn("IKUNANCE_DEPLOY_HEALTH_TIMEOUT", source)
        self.assertIn("IKUNANCE_DEPLOY_PORT", source)
        self.assertIn("IKUNANCE_DEPLOY_WORKERS", source)
        self.assertIn("IKUNANCE_DEPLOY_THREADS", source)
        self.assertIn("IKUNANCE_DEPLOY_DATA_DIR", source)
        self.assertIn("IKUNANCE_APP_VERSION", source)
        self.assertIn("IKUNANCE_FRONTEND_DIST=", source)
        self.assertIn("REQUIRED_DEPLOY_ENV", source)
        self.assertIn("OPTIONAL_DEPLOY_ENV_DEFAULTS", source)
        self.assertIn("RUNTIME_ENV_EXPORTS", source)
        self.assertIn("PROJECT_ROOT", source)
        self.assertIn("python3 -m venv .venv", source)
        self.assertIn(".venv/bin/python -m pip install -r requirements.txt", source)
        self.assertIn("mkdir -p", source)
        self.assertIn("cp -n user_data/*.json", source)
        self.assertIn("signal_history.json", source)
        self.assertIn("alerted_signals.json", source)
        self.assertIn("IKUNANCE_DATA_DIR=", source)
        self.assertIn("logs/run.log", source)
        self.assertIn("tail -n 80", source)
        self.assertIn(".venv/bin/python -m gunicorn", source)
        self.assertIn("-k gthread", source)
        self.assertIn("--threads", source)
        self.assertIn("wsgi:app", source)
        self.assertIn("wait_for_health", source)
        self.assertNotIn("IKUNANCE', local_rel", source)
        self.assertNotIn("ssh.connect(host, username=user, password='", source)

    def test_env_example_documents_deploy_and_runtime_env_contract(self):
        source = DEPLOY_SCRIPT.read_text(encoding="utf-8")
        documented = env_names_from_example()
        required_names = set(re.findall(r'"(IKUNANCE_[A-Z0-9_]+)"', source))
        required_names.update({
            "IKUNANCE_DEPLOY_DATA_DIR",
            "IKUNANCE_DEPLOY_HEALTH_URL",
            "IKUNANCE_DEPLOY_HEALTH_TIMEOUT",
        })
        missing = sorted(name for name in required_names if name not in documented)
        self.assertEqual(missing, [])

    def test_dry_run_does_not_connect_and_lists_current_workspace_files(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used-in-dry-run",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DRY RUN: deployment plan", result.stdout)
        self.assertIn(str(PROJECT_ROOT / "backend" / "app.py"), result.stdout)
        self.assertIn(str(PROJECT_ROOT / "backend" / "wsgi.py"), result.stdout)
        self.assertIn(str(PROJECT_ROOT / "backend" / "app" / "services" / "storage_service.py"), result.stdout)
        self.assertIn(str(PROJECT_ROOT / "dist" / "index.html"), result.stdout)
        self.assertIn(str(PROJECT_ROOT / "dist" / "logo-white.svg"), result.stdout)

    def test_dry_run_includes_every_backend_service_module(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used-in-dry-run",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

        services_dir = PROJECT_ROOT / "backend" / "app" / "services"
        for service_file in services_dir.glob("*.py"):
            self.assertIn(str(service_file), result.stdout)

        self.assertIn(str(PROJECT_ROOT / "backend" / "requirements.txt"), result.stdout)

    def test_invalid_deploy_port_fails_before_any_connection(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used",
            "IKUNANCE_DEPLOY_PORT": "not-a-port",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("IKUNANCE_DEPLOY_PORT must be a positive integer", result.stderr)

    def test_relative_remote_paths_fail_before_any_connection(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used",
            "IKUNANCE_DEPLOY_REMOTE": "relative/path",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("IKUNANCE_DEPLOY_REMOTE must be an absolute remote path", result.stderr)

        env["IKUNANCE_DEPLOY_REMOTE"] = "/www/wwwroot/ikunance"
        env["IKUNANCE_DEPLOY_DATA_DIR"] = "../data"
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("IKUNANCE_DEPLOY_DATA_DIR must be an absolute remote path", result.stderr)

    def test_multi_worker_deploy_is_blocked_for_in_memory_stream_engine(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used",
            "IKUNANCE_DEPLOY_WORKERS": "2",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("IKUNANCE_DEPLOY_WORKERS must be 1", result.stderr)

    def test_single_thread_deploy_is_blocked_for_sse_streams(self):
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used",
            "IKUNANCE_DEPLOY_THREADS": "1",
            "IKUNANCE_DEPLOY_DRY_RUN": "1",
        })
        result = subprocess.run(
            [sys.executable, str(DEPLOY_SCRIPT)],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("IKUNANCE_DEPLOY_THREADS must be at least 2", result.stderr)

    def test_health_wait_reports_http_status_and_body_snippet(self):
        deploy_script = load_deploy_module()
        error = urllib.error.HTTPError(
            "http://example.invalid/api/health",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b"backend boot failed"),
        )
        with mock.patch.object(deploy_script.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503: backend boot failed"):
                deploy_script.wait_for_health("http://example.invalid/api/health", timeout_seconds=1)

    def test_deploy_uses_configured_health_timeout(self):
        deploy_script = load_deploy_module()
        env = os.environ.copy()
        env.update({
            "IKUNANCE_DEPLOY_HOST": "example.invalid",
            "IKUNANCE_DEPLOY_USER": "deploy",
            "IKUNANCE_DEPLOY_PASSWORD": "not-used",
            "IKUNANCE_DEPLOY_HEALTH_TIMEOUT": "37",
        })
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(deploy_script, "build_manifest", return_value=[]), \
                 mock.patch.object(deploy_script, "run_remote_command", return_value=""), \
                 mock.patch.object(deploy_script, "wait_for_health", return_value={"status": "ok", "version": "test", "uptime_seconds": 1}) as wait_mock, \
                 mock.patch.dict(sys.modules, {"paramiko": mock.Mock()}):
                deploy_script.deploy()
        self.assertEqual(wait_mock.call_args.kwargs["timeout_seconds"], 37)


if __name__ == "__main__":
    unittest.main()
