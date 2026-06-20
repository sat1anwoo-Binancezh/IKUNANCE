# IKUNANCE Deployment Checklist

Use this checklist before uploading the project to the server.

## 1. Local Verification

- Run backend and deployment tests:

```bash
python -m unittest discover -s tests -v
```

- Run the full preflight:

```bash
python scripts/verify_project.py
```

- Confirm both commands pass before deploying.

## 2. Environment

- Copy `.env.example` into your private runtime environment.
- Fill real values for:
  - `IKUNANCE_DEPLOY_HOST`
  - `IKUNANCE_DEPLOY_USER`
  - `IKUNANCE_DEPLOY_PASSWORD`
  - `IKUNANCE_DEPLOY_SSH_PORT`
  - `IKUNANCE_DEPLOY_REMOTE`
  - `IKUNANCE_DEPLOY_DATA_DIR`
- Keep `IKUNANCE_DEPLOY_WORKERS=1`.
- Keep `IKUNANCE_DEPLOY_THREADS=6` for the 2GB deployment profile. Valid range is `2` to `8`.
- Keep `IKUNANCE_SCAN_WORKERS=4`, `IKUNANCE_SCAN_CONCURRENCY=1`, and `IKUNANCE_PUSH_MONITOR_INTERVAL=900` unless the scan schedule is deliberately redesigned.
- Keep `IKUNANCE_BINANCE_PROVIDER=official` so live Binance market data uses the repo-native official Futures REST/WebSocket provider instead of ccxt.
- Keep `IKUNANCE_SYMBOL_SYNC_NETWORK=1` in production so the symbol search snapshot can refresh from official Futures `exchangeInfo`.
- Keep `IKUNANCE_DEPLOY_REMOTE` and `IKUNANCE_DEPLOY_DATA_DIR` as absolute remote paths.
- Set `IKUNANCE_MARKET_PROXY` only when the server's direct route to Binance is blocked. Use a private, stable, Binance-reachable route; never use a random public proxy.
- Keep `IKUNANCE_ALLOW_SYNTHETIC_MARKET_FALLBACK=0` in production so Binance failures surface as errors instead of fake prices.
- Keep secrets out of git, screenshots, logs, and shared chats.

## 3. Dry Run

```bash
set IKUNANCE_DEPLOY_DRY_RUN=1
python deploy_script.py
```

Check that the upload plan includes:

- `backend/app.py`
- `backend/wsgi.py`
- `backend/requirements.txt`
- `backend/app/services/*.py`
- `dist/index.html`
- `dist/assets/*`

## 4. Deploy

```bash
set IKUNANCE_DEPLOY_DRY_RUN=0
python deploy_script.py
```

The script will:

- upload backend files and `dist/`
- install `backend/requirements.txt`
- create `backend/.venv`, install `requirements.txt` inside it, and start `.venv/bin/python -m gunicorn -w 1 -k gthread --threads ${IKUNANCE_DEPLOY_THREADS} -b 0.0.0.0:${IKUNANCE_DEPLOY_PORT} wsgi:app`
- enforce the single-instance lock, bounded scan concurrency, push monitor spacing, and log retention defaults
- check `/api/health`
- print `backend/logs/run.log` tail if health fails

## 5. Post-Deploy Smoke

Open these URLs after deployment:

- `/api/health`
- `/`
- `/api/search_symbols?q=btc&exchange=binance`
- `/api/market_movers`

Expected health payload:

- `status` is `ok`
- `frontendDist.exists` is `true`
- `diagnostics.frontendReady` is `true`
- `diagnostics.dataWritable` is `true`
- `marketProxyConfigured` matches whether `IKUNANCE_MARKET_PROXY` is set

## 6. Rollback

If health fails:

- read the printed `logs/run.log` tail
- restore the previous server directory or previous uploaded files
- keep `IKUNANCE_DATA_DIR` unchanged
- rerun `python deploy_script.py` only after fixing the root cause

Never delete the production data directory during rollback.
