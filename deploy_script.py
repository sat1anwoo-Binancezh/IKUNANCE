import os
import posixpath
import shlex
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_ROOT = PROJECT_ROOT / "backend"
DIST_ROOT = PROJECT_ROOT / "dist"
OPTIONAL_BACKEND_DATA_FILES = [
    BACKEND_ROOT / "data" / "binance_futures_symbols_snapshot.json",
]

REQUIRED_DEPLOY_ENV = [
    "IKUNANCE_DEPLOY_HOST",
    "IKUNANCE_DEPLOY_USER",
    "IKUNANCE_DEPLOY_PASSWORD",
]

OPTIONAL_DEPLOY_ENV_DEFAULTS = {
    "IKUNANCE_DEPLOY_REMOTE": "/www/wwwroot/ikunance",
    "IKUNANCE_DEPLOY_SSH_PORT": "22",
    "IKUNANCE_DEPLOY_PORT": "5000",
    "IKUNANCE_DEPLOY_WORKERS": "1",
    "IKUNANCE_DEPLOY_THREADS": "6",
    "IKUNANCE_DEPLOY_DRY_RUN": "0",
    "IKUNANCE_DEPLOY_HEALTH_TIMEOUT": "20",
    "IKUNANCE_DEPLOY_LOG_RETENTION_DAYS": "7",
}

RUNTIME_ENV_EXPORTS = [
    "IKUNANCE_PORT",
    "IKUNANCE_DATA_DIR",
    "IKUNANCE_FRONTEND_DIST",
    "IKUNANCE_APP_VERSION",
    "IKUNANCE_LIVE_MARKET",
    "IKUNANCE_BINANCE_PROVIDER",
    "IKUNANCE_SYMBOL_SYNC_NETWORK",
    "IKUNANCE_ALLOW_BINANCE_SPOT_KLINE_FALLBACK",
    "IKUNANCE_MARKET_PROXY",
]


def require_env(name, default=None):
    value = os.environ.get(name, default)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(value).strip()


def require_positive_int_env(name, default):
    value = require_env(name, default)
    if not value.isdigit() or int(value) <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def require_single_worker_env(name, default):
    value = require_positive_int_env(name, default)
    if value != "1":
        raise RuntimeError(f"{name} must be 1 until the in-memory stream engine is externalized")
    return value


def require_thread_count_env(name, default):
    value = require_positive_int_env(name, default)
    if int(value) < 2:
        raise RuntimeError(f"{name} must be at least 2 so SSE streams do not block API scans")
    if int(value) > 8:
        raise RuntimeError(f"{name} must be 8 or lower on the 2GB deployment profile")
    return value


def require_absolute_remote_path(name, value):
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must be an absolute remote path")
    if any(part in {"..", ""} for part in value.split("/")[1:]):
        raise RuntimeError(f"{name} must not contain empty or parent path segments")
    return value.rstrip("/")


def require_file(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing local file: {path}")
    return path


def iter_backend_files():
    candidates = sorted(BACKEND_ROOT.glob("*.py"))
    candidates.append(BACKEND_ROOT / "requirements.txt")
    service_root = BACKEND_ROOT / "app" / "services"
    candidates.extend(sorted(service_root.glob("*.py")))

    for path in candidates:
        require_file(path)
        yield path, path.relative_to(PROJECT_ROOT).as_posix()


def build_manifest(base_remote):
    manifest = []
    for local_path, remote_rel in iter_backend_files():
        manifest.append((local_path, f"{base_remote}/{remote_rel}"))

    for local_path in OPTIONAL_BACKEND_DATA_FILES:
        if local_path.exists():
            remote_rel = local_path.relative_to(BACKEND_ROOT).as_posix()
            manifest.append((local_path, f"{base_remote}/backend/{remote_rel}"))

    dist_local = require_file(DIST_ROOT)
    require_file(dist_local / "index.html")
    for path in sorted(dist_local.rglob("*")):
        if path.is_file():
            remote_rel = path.relative_to(dist_local).as_posix()
            manifest.append((require_file(path), f"{base_remote}/frontend/dist/{remote_rel}"))
    return manifest


def ensure_remote_dir(sftp, remote_dir):
    parts = [part for part in remote_dir.replace("\\", "/").split("/") if part]
    current = "/" if remote_dir.startswith("/") else ""
    for part in parts:
        current = f"{current.rstrip('/')}/{part}" if current else part
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)
        except IOError:
            try:
                sftp.mkdir(current)
            except IOError:
                sftp.stat(current)


def wait_for_health(url, timeout_seconds=20):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                body = response.read().decode("utf-8", errors="replace")
                if 200 <= response.status < 300:
                    payload = json.loads(body)
                    if payload.get("status") == "ok":
                        return payload
                    last_error = f"unexpected health payload: {payload}"
                else:
                    last_error = f"HTTP {response.status}: {body[:300]}"
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = f"HTTP {exc.code}: {body[:300]}"
        except json.JSONDecodeError as exc:
            last_error = f"invalid health JSON: {exc}"
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"Health check failed for {url}: {last_error}")


def run_remote_command(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    if exit_code != 0:
        error = stderr.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Remote command failed ({exit_code}): {command}\n{error}")
    return stdout.read().decode("utf-8", errors="replace")


def deploy():
    host = require_env("IKUNANCE_DEPLOY_HOST")
    user = require_env("IKUNANCE_DEPLOY_USER")
    pw = require_env("IKUNANCE_DEPLOY_PASSWORD")
    ssh_port = int(require_positive_int_env("IKUNANCE_DEPLOY_SSH_PORT", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_SSH_PORT"]))
    base_remote = require_absolute_remote_path(
        "IKUNANCE_DEPLOY_REMOTE",
        require_env("IKUNANCE_DEPLOY_REMOTE", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_REMOTE"]).rstrip("/"),
    )
    app_port = require_positive_int_env("IKUNANCE_DEPLOY_PORT", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_PORT"])
    app_workers = require_single_worker_env("IKUNANCE_DEPLOY_WORKERS", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_WORKERS"])
    app_threads = require_thread_count_env("IKUNANCE_DEPLOY_THREADS", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_THREADS"])
    health_timeout = int(require_positive_int_env("IKUNANCE_DEPLOY_HEALTH_TIMEOUT", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_HEALTH_TIMEOUT"]))
    log_retention_days = int(require_positive_int_env("IKUNANCE_DEPLOY_LOG_RETENTION_DAYS", OPTIONAL_DEPLOY_ENV_DEFAULTS["IKUNANCE_DEPLOY_LOG_RETENTION_DAYS"]))
    data_dir = require_absolute_remote_path(
        "IKUNANCE_DEPLOY_DATA_DIR",
        os.environ.get("IKUNANCE_DEPLOY_DATA_DIR", f"{base_remote}/backend/data").strip(),
    )
    app_version = os.environ.get("IKUNANCE_APP_VERSION", str(int(time.time()))).strip()
    dry_run = os.environ.get("IKUNANCE_DEPLOY_DRY_RUN", "0").strip() == "1"
    health_url = os.environ.get("IKUNANCE_DEPLOY_HEALTH_URL", "").strip()
    market_proxy = (
        os.environ.get("IKUNANCE_MARKET_PROXY")
        or os.environ.get("IKUNANCE_PROXY")
        or ""
    ).strip()
    if not health_url:
        health_url = f"http://{host}:{app_port}/api/health"

    local_manifest = build_manifest(base_remote)

    if dry_run:
        print("DRY RUN: deployment plan")
        for local_path, remote_path in local_manifest:
            print(f"{local_path} -> {remote_path}")
        return

    import paramiko

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        print(f"Connecting to {host}:{ssh_port}...")
        ssh.connect(host, port=ssh_port, username=user, password=pw)
        quoted_base = shlex.quote(base_remote)
        run_remote_command(ssh, f"mkdir -p {quoted_base}")
        quoted_lock = shlex.quote(f"{base_remote}/.deploy.lock")
        run_remote_command(
            ssh,
            (
                f'if [ -d {quoted_lock} ] && find {quoted_lock} -maxdepth 0 -mmin +30 | grep -q .; then rm -rf {quoted_lock}; fi; '
                f'if ! mkdir {quoted_lock}; then echo "another deployment is already running"; exit 2; fi'
            ),
        )

        sftp = ssh.open_sftp()

        for local_path, remote_path in local_manifest:
            ensure_remote_dir(sftp, posixpath.dirname(remote_path))
            print(f"Uploading {local_path} to {remote_path}...")
            sftp.put(str(local_path), remote_path)

        sftp.close()

        quoted_data = shlex.quote(data_dir)

        print("Preparing Python virtualenv...")
        run_remote_command(
            ssh,
            (
                'command -v python3 >/dev/null || '
                '(apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3); '
                'python3 -m venv --help >/dev/null 2>&1 || '
                '(apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3-pip)'
            ),
        )

        print("Installing backend dependencies...")
        run_remote_command(
            ssh,
            (
                f'cd {quoted_base}/backend && '
                f'python3 -m venv .venv && '
                f'.venv/bin/python -m pip install --upgrade pip setuptools wheel && '
                f'.venv/bin/python -m pip install -r requirements.txt'
            ),
        )
        run_remote_command(ssh, f'mkdir -p {quoted_data}/user_data {quoted_data}/custom_sounds {quoted_base}/backend/logs')
        run_remote_command(
            ssh,
            (
                f'cd {quoted_base}/backend && '
                f'if [ -d user_data ]; then cp -n user_data/*.json {quoted_data}/user_data/ 2>/dev/null || true; fi && '
                f'if [ -f signal_history.json ] && [ ! -f {quoted_data}/signal_history.json ]; then cp signal_history.json {quoted_data}/signal_history.json; fi && '
                f'if [ -f alerted_signals.json ] && [ ! -f {quoted_data}/alerted_signals.json ]; then cp alerted_signals.json {quoted_data}/alerted_signals.json; fi'
            ),
        )
        run_remote_command(
            ssh,
            (
                f'cd {quoted_base}/backend && mkdir -p logs && '
                f'ts=$(date +%Y%m%d-%H%M%S) && '
                f'for f in run.log access.log error.log; do '
                f'if [ -s "logs/$f" ]; then mv "logs/$f" "logs/${{f%.log}}.$ts.log"; fi; '
                f'done && '
                f'find logs -type f -name "*.log" -mtime +{log_retention_days} -delete'
            ),
        )
        run_remote_command(
            ssh,
            (
                f'find {quoted_base}/frontend -maxdepth 1 -type d -name "dist.backup-*" -mtime +{log_retention_days} -exec rm -rf {{}} + 2>/dev/null || true; '
                f'find {quoted_base}/frontend/dist -maxdepth 1 -type f -name "index.html.backup-*" -mtime +{log_retention_days} -delete 2>/dev/null || true'
            ),
        )

        print("Restarting backend...")
        run_remote_command(
            ssh,
            (
                f'cd {quoted_base}/backend && '
                f'if [ -f logs/gunicorn.pid ]; then '
                f'oldpid=$(cat logs/gunicorn.pid 2>/dev/null || true); '
                f'if [ -n "$oldpid" ]; then kill "$oldpid" 2>/dev/null || true; sleep 2; kill -9 "$oldpid" 2>/dev/null || true; fi; '
                f'rm -f logs/gunicorn.pid; '
                f'fi'
            ),
        )
        run_remote_command(ssh, "pkill -f '[g]unicorn.*wsgi:app' || true")
        run_remote_command(ssh, f'fuser -k {app_port}/tcp || true')
        proxy_env = f'IKUNANCE_MARKET_PROXY={shlex.quote(market_proxy)} ' if market_proxy else ''
        start_command = (
            f'cd {quoted_base}/backend && '
            f'IKUNANCE_PORT={app_port} '
            f'IKUNANCE_DATA_DIR={shlex.quote(data_dir)} '
            f'IKUNANCE_FRONTEND_DIST={quoted_base}/frontend/dist '
            f'IKUNANCE_APP_VERSION={shlex.quote(app_version)} '
            f'IKUNANCE_LIVE_MARKET=1 '
            f'IKUNANCE_BINANCE_PROVIDER=official '
            f'IKUNANCE_SYMBOL_SYNC_NETWORK=1 '
            f'IKUNANCE_ALLOW_BINANCE_SPOT_KLINE_FALLBACK=1 '
            f'{proxy_env}'
            f'IKUNANCE_SINGLE_INSTANCE_LOCK=1 '
            f'IKUNANCE_ACCESS_LOG=0 '
            f'IKUNANCE_SCAN_WORKERS=4 '
            f'IKUNANCE_SCAN_CONCURRENCY=1 '
            f'IKUNANCE_SCAN_RATE_LIMIT=12 '
            f'IKUNANCE_SCAN_RATE_WINDOW=60 '
            f'IKUNANCE_LIVE_FETCH_MIN_INTERVAL=0.35 '
            f'IKUNANCE_LIVE_FETCH_BACKOFF_BASE=5 '
            f'IKUNANCE_LIVE_FETCH_BACKOFF_MAX=60 '
            f'IKUNANCE_PUSH_MONITOR_INTERVAL=900 '
            f'IKUNANCE_PUSH_MONITOR_SYMBOL_PAUSE=0.5 '
            f'nohup .venv/bin/python -m gunicorn -w {app_workers} -k gthread --threads {app_threads} -b 0.0.0.0:{app_port} '
            f'--pid logs/gunicorn.pid --access-logfile logs/access.log --error-logfile logs/error.log --capture-output '
            f'wsgi:app > logs/run.log 2>&1 &'
        )
        run_remote_command(ssh, start_command)

        print(f"Checking health: {health_url}")
        try:
            payload = wait_for_health(health_url, timeout_seconds=health_timeout)
        except Exception:
            log_tail = run_remote_command(ssh, f'tail -n 80 {quoted_base}/backend/logs/run.log || true')
            print("Remote run.log tail:")
            print(log_tail)
            raise
        try:
            run_remote_command(ssh, f'rmdir {quoted_lock} || true')
        except Exception as cleanup_error:
            print(f"Warning: deployment lock cleanup skipped: {cleanup_error}")
        ssh.close()
        print(f"Deployment complete! version={payload.get('version')} uptime={payload.get('uptime_seconds')}s")
    except Exception as e:
        print(f"Deployment failed: {e}")
        try:
            if "quoted_lock" in locals():
                run_remote_command(ssh, f'rmdir {quoted_lock} || true')
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass
        raise

if __name__ == "__main__":
    deploy()
