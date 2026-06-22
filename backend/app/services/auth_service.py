import hashlib
import hmac
import json
import os
import re
import secrets
import uuid
from typing import Dict, Tuple

from .storage_service import atomic_write_json, get_data_path


ACCOUNTS_FILE = get_data_path("accounts.json")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PBKDF2_ITERATIONS = 260000
DEFAULT_ADMIN_ID = "111"
DEFAULT_ADMIN_PASSWORD = "123123123"


def _empty_accounts() -> Dict:
    return {"users": {}, "sessions": {}}


def _admin_id() -> str:
    return (os.environ.get("IKUNANCE_ADMIN_ID") or DEFAULT_ADMIN_ID).strip() or DEFAULT_ADMIN_ID


def _admin_password() -> str:
    return os.environ.get("IKUNANCE_ADMIN_PASSWORD") or DEFAULT_ADMIN_PASSWORD


def _user_role(user: Dict) -> str:
    return "admin" if user.get("role") == "admin" else "user"


def _user_payload(account_id: str, user: Dict) -> Dict:
    return {
        "email": user.get("email") or account_id,
        "nickname": user.get("nickname") or account_id,
        "role": _user_role(user),
    }


def _normalize_accounts(data) -> Dict:
    if not isinstance(data, dict):
        return _empty_accounts()
    data.setdefault("users", {})
    data.setdefault("sessions", {})
    if not isinstance(data["users"], dict):
        data["users"] = {}
    if not isinstance(data["sessions"], dict):
        data["sessions"] = {}
    return data


def _ensure_builtin_admin(accounts: Dict) -> bool:
    admin_id = _admin_id()
    users = accounts.setdefault("users", {})
    user = users.get(admin_id)
    changed = False
    if not isinstance(user, dict):
        user = {}
        users[admin_id] = user
        changed = True

    for key, value in {
        "email": admin_id,
        "nickname": "admin",
        "role": "admin",
        "builtin": True,
    }.items():
        if user.get(key) != value:
            user[key] = value
            changed = True
    if not user.get("created_at"):
        user["created_at"] = int(__import__("time").time())
        changed = True
    if not _verify_password(_admin_password(), user.get("password", "")) or _needs_password_upgrade(user.get("password", "")):
        user["password"] = _hash_password(_admin_password())
        changed = True
    return changed


def load_accounts() -> Dict:
    changed = False
    if not os.path.exists(ACCOUNTS_FILE):
        data = _empty_accounts()
        changed = True
    else:
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            data = _empty_accounts()
            changed = True
    data = _normalize_accounts(data)
    changed = _ensure_builtin_admin(data) or changed
    if changed:
        save_accounts(data)
    return data


def save_accounts(accounts: Dict) -> None:
    atomic_write_json(ACCOUNTS_FILE, accounts, ensure_ascii=False, indent=2)


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        (password or "").encode(),
        salt.encode(),
        PBKDF2_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${digest}"


def _legacy_hash_password(password: str) -> str:
    return hashlib.sha256((password or "").encode()).hexdigest()


def _verify_password(password: str, stored_hash: str) -> bool:
    stored_hash = stored_hash or ""
    if stored_hash.startswith("pbkdf2_sha256$"):
        try:
            _, iterations, salt, digest = stored_hash.split("$", 3)
            candidate = hashlib.pbkdf2_hmac(
                "sha256",
                (password or "").encode(),
                salt.encode(),
                int(iterations),
            ).hex()
            return hmac.compare_digest(candidate, digest)
        except (TypeError, ValueError):
            return False
    return hmac.compare_digest(stored_hash, _legacy_hash_password(password))


def _needs_password_upgrade(stored_hash: str) -> bool:
    return not (stored_hash or "").startswith("pbkdf2_sha256$")


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def register(email: str, password: str, nickname: str = "") -> Tuple[bool, Dict]:
    email = (email or "").strip().lower()
    if not EMAIL_RE.match(email):
        return False, {"status": "error", "msg": "请输入有效邮箱"}
    if not password or len(password) < 8:
        return False, {"status": "error", "msg": "密码至少 8 位"}
    accounts = load_accounts()
    if email in accounts["users"]:
        return False, {"status": "error", "msg": "账号已存在"}
    accounts["users"][email] = {
        "email": email,
        "password": _hash_password(password),
        "nickname": nickname or email.split("@")[0],
        "role": "user",
        "created_at": int(__import__("time").time()),
    }
    token = _new_token()
    accounts["sessions"][token] = email
    save_accounts(accounts)
    return True, {"status": "success", "token": token, **_user_payload(email, accounts["users"][email])}


def login(email: str, password: str) -> Tuple[bool, Dict]:
    email = (email or "").strip().lower()
    accounts = load_accounts()
    user = accounts["users"].get(email)
    if not user or not _verify_password(password, user.get("password", "")):
        return False, {"status": "error", "msg": "邮箱或密码错误"}
    if _needs_password_upgrade(user.get("password", "")):
        accounts["users"][email]["password"] = _hash_password(password)
    token = _new_token()
    accounts["sessions"][token] = email
    save_accounts(accounts)
    return True, {"status": "success", "token": token, **_user_payload(email, user)}


def google_login(email: str, name: str = "", gid: str = "", picture: str = "") -> Tuple[bool, Dict]:
    email = (email or "").strip().lower()
    if not EMAIL_RE.match(email):
        return False, {"status": "error", "msg": "请输入有效邮箱"}
    accounts = load_accounts()
    accounts["users"].setdefault(email, {
        "email": email,
        "password": "",
        "nickname": name or email.split("@")[0],
        "google_id": gid,
        "picture": picture,
        "role": "user",
    })
    accounts["users"][email]["role"] = _user_role(accounts["users"][email])
    token = _new_token()
    accounts["sessions"][token] = email
    save_accounts(accounts)
    return True, {"status": "success", "token": token, **_user_payload(email, accounts["users"][email])}


def check_session(token: str) -> Tuple[bool, Dict]:
    accounts = load_accounts()
    email = accounts.get("sessions", {}).get(token or "")
    if not email:
        return False, {"status": "error", "msg": "未登录"}
    user = accounts.get("users", {}).get(email, {})
    return True, {"status": "success", **_user_payload(email, user)}


def logout(token: str) -> None:
    accounts = load_accounts()
    accounts.get("sessions", {}).pop(token or "", None)
    save_accounts(accounts)


def change_password(email: str, old_password: str, new_password: str) -> Tuple[bool, Dict]:
    accounts = load_accounts()
    user = accounts.get("users", {}).get(email)
    if not user:
        return False, {"status": "error", "msg": "账户不存在"}
    if not _verify_password(old_password, user.get("password", "")):
        return False, {"status": "error", "msg": "当前密码错误"}
    accounts["users"][email]["password"] = _hash_password(new_password)
    save_accounts(accounts)
    return True, {"status": "success", "msg": "密码已更新"}
