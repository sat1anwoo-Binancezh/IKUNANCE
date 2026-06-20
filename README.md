# IKUN111 / IKUNANCE

This is the deployable IKUNANCE workspace. The backend, frontend build output,
tests, deployment script, and project rules are all rooted in this directory.

## Layout

- `backend/`: Flask backend service.
- `backend/wsgi.py`: production WSGI entrypoint for `gunicorn wsgi:app`.
- `backend/requirements.txt`: Python packages required on the server.
- `dist/`: production frontend build copied by the deployment script.
- `frontend/`: frontend source and build configuration.
- `scripts/verify_project.py`: deployment preflight verifier.
- `tests/`: regression tests for backend, deployment, frontend source, and preflight.
- `SOUL.md`: project collaboration rules.

## Local Verification

Run the full backend and deployment regression suite:

```bash
python -m unittest discover -s tests -v
```

Run the deployment preflight:

```bash
python scripts/verify_project.py
```

The preflight checks required files, production frontend assets, temporary
artifacts, backend requirements, Python syntax, secret patterns, deployment
dry-run output, Python tests, and the frontend build.

## Local Backend Smoke

```bash
cd backend
python app.py
```

Production entrypoint smoke:

```bash
cd backend
python -m gunicorn -w 1 -b 0.0.0.0:5000 wsgi:app
```

Then open:

```text
http://127.0.0.1:5000/api/health
```

## Deployment

Copy `.env.example` into your private environment and fill real values there.
Do not commit real server passwords, API keys, email authorization codes, or
tokens.

Important deployment variables:

- `IKUNANCE_DEPLOY_HOST`
- `IKUNANCE_DEPLOY_USER`
- `IKUNANCE_DEPLOY_PASSWORD`
- `IKUNANCE_DEPLOY_SSH_PORT`
- `IKUNANCE_DEPLOY_REMOTE`
- `IKUNANCE_DEPLOY_PORT`
- `IKUNANCE_DEPLOY_WORKERS`
- `IKUNANCE_DEPLOY_THREADS`
- `IKUNANCE_DEPLOY_LOG_RETENTION_DAYS`
- `IKUNANCE_DEPLOY_DATA_DIR`
- `IKUNANCE_DEPLOY_HEALTH_URL`
- `IKUNANCE_DEPLOY_HEALTH_TIMEOUT`
- `IKUNANCE_DEPLOY_DRY_RUN`
- `IKUNANCE_MARKET_PROXY` if the server's direct outbound route is blocked by Binance.

The realtime stream engine is still in-process, so keep
`IKUNANCE_DEPLOY_WORKERS=1`. The server should still use threaded workers;
on the 2GB deployment profile keep `IKUNANCE_DEPLOY_THREADS` between `2` and
`8`; the default is `6`. Keep scan concurrency at `1` and scan workers at `4`
unless the server is upgraded.

Check the upload plan first:

```bash
set IKUNANCE_DEPLOY_DRY_RUN=1
python deploy_script.py
```

After the plan is correct, set `IKUNANCE_DEPLOY_DRY_RUN=0` and deploy. The
script uploads backend files and `dist/`, creates `backend/.venv`, installs
backend requirements there, starts `.venv/bin/python -m gunicorn -k gthread --threads ${IKUNANCE_DEPLOY_THREADS} wsgi:app`,
starts with bounded scan/push defaults for a 2GB server, checks `/api/health`, and prints the remote
`logs/run.log` tail if health fails.

## Runtime Notes

- Runtime data lives under `IKUNANCE_DATA_DIR`.
- Do not overwrite the data directory with code artifacts.
- `IKUNANCE_DEPLOY_REMOTE` and `IKUNANCE_DEPLOY_DATA_DIR` must be absolute
  remote paths.
- `IKUNANCE_DISABLE_OUTBOUND=1` is useful for local and dry-run verification.
- `IKUNANCE_BINANCE_PROVIDER=official` makes Binance live market data use the
  repo-native official Futures REST/WebSocket provider instead of ccxt.
- `IKUNANCE_SYMBOL_SYNC_NETWORK=1` refreshes the Futures symbol snapshot from
  official `fapi.binance.com/fapi/v1/exchangeInfo` when live market mode is on.
- `IKUNANCE_ALLOW_BINANCE_SPOT_KLINE_FALLBACK=1` allows the temporary spot
  `data-api.binance.vision` bootstrap fallback when Futures REST is blocked;
  those rows are marked as non-strict Futures data.
- `IKUNANCE_MARKET_PROXY` accepts `http://`, `https://`, or `socks5://` proxy URLs for Binance market data. If Binance returns HTTP 451 from the server IP, configure a compliant Binance-reachable route or change the server/IP; the production backend no longer fabricates synthetic prices when live market data is enabled.
- `IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK=1` is only for local testing. Keep it `0` in production.
- `IKUNANCE_FORCE_HTTPS=1` enables HSTS and secure cookies in production.
- `IKUNANCE_SINGLE_INSTANCE_LOCK=1` prevents duplicate backend instances from sharing the same data directory.
- `IKUNANCE_SCAN_WORKERS=4`, `IKUNANCE_SCAN_CONCURRENCY=1`, and `IKUNANCE_SCAN_RATE_LIMIT=12` are the conservative defaults for a 2GB server.
- `IKUNANCE_PUSH_MONITOR_INTERVAL=900` keeps scans aligned to 15-minute Kline boundaries; `IKUNANCE_PUSH_MONITOR_SYMBOL_PAUSE=0.5` keeps background email scans from hammering the market API.
- `DOUBAO_API_KEY` may be set globally or stored through the app settings.
