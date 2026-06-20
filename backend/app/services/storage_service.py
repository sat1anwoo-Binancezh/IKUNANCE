import json
import os
import uuid
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = BACKEND_ROOT / "data"


def get_data_dir() -> Path:
    configured = os.environ.get("IKUNANCE_DATA_DIR", "").strip()
    data_dir = Path(configured) if configured else DEFAULT_DATA_DIR
    return data_dir.expanduser().resolve()


def get_data_path(*parts: str) -> str:
    return str(get_data_dir().joinpath(*parts))


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: str, data, *, ensure_ascii: bool = False, indent=None) -> None:
    target = Path(path)
    ensure_parent_dir(str(target))
    temp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=ensure_ascii, indent=indent)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def ensure_storage_dirs() -> None:
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "user_data").mkdir(parents=True, exist_ok=True)
    (data_dir / "custom_sounds").mkdir(parents=True, exist_ok=True)
