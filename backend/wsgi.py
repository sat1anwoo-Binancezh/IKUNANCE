import sys
from importlib import util
from pathlib import Path


def _load_flask_app():
    existing = sys.modules.get("app")
    if existing is not None and hasattr(existing, "app"):
        return existing.app

    app_path = Path(__file__).resolve().with_name("app.py")
    spec = util.spec_from_file_location("ikunance_backend_app", app_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load Flask app from {app_path}")
    module = util.module_from_spec(spec)
    sys.modules.setdefault("ikunance_backend_app", module)
    spec.loader.exec_module(module)
    return module.app


app = _load_flask_app()


__all__ = ["app"]
