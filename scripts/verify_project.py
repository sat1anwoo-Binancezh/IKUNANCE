import argparse
import py_compile
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_ROOT = PROJECT_ROOT / "frontend"

REQUIRED_FILES = [
    "backend/app.py",
    "backend/wsgi.py",
    "backend/requirements.txt",
    "backend/app/services/auth_service.py",
    "backend/app/services/exchange_service.py",
    "backend/app/services/storage_service.py",
    "deploy_script.py",
    "DEPLOYMENT_CHECKLIST.md",
    "README.md",
    "dist/index.html",
    "dist/logo-white.svg",
    "frontend/package.json",
    "frontend/vite.config.js",
    "frontend/src/App.jsx",
    "frontend/src/main.jsx",
    "tests/test_backend_core.py",
    "tests/test_deploy_script.py",
    "tests/test_docs.py",
    "tests/test_frontend_source.py",
]

SECRET_PATTERNS = [
    re.compile(r"ssh\.connect\([^)]*password\s*=\s*['\"][^'\"]+['\"]", re.I),
    re.compile(r"\broot\b[^#\n]{0,80}\b(password|passwd|pwd)\b\s*[:=]\s*['\"][^'\"]+['\"]", re.I),
    re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b[^#\n]{0,80}\broot\b", re.I),
]

SCAN_EXTENSIONS = {".py", ".js", ".jsx", ".json", ".md", ".txt", ".example", ".env"}
SKIP_PARTS = {"node_modules", "__pycache__", ".git", "frontend\\dist", "backend\\data"}
FORBIDDEN_ROOT_FILES = {
    "app_decompiled.py",
    "app_uncompyle.py",
}

REQUIRED_BACKEND_REQUIREMENTS = {
    "flask",
    "flask-cors",
    "gunicorn",
    "numpy",
    "requests",
    "paramiko",
    "ccxt",
}


def rel(path):
    return path.relative_to(PROJECT_ROOT).as_posix()


def run(command, cwd=PROJECT_ROOT, env=None):
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=180,
    )
    if completed.returncode != 0:
        joined = " ".join(str(part) for part in command)
        raise RuntimeError(f"Command failed: {joined}\n{completed.stdout}")
    return completed.stdout


def check_required_files():
    missing = [item for item in REQUIRED_FILES if not (PROJECT_ROOT / item).exists()]
    if missing:
        raise RuntimeError("Missing required files: " + ", ".join(missing))


def check_dist_manifest():
    assets = list((PROJECT_ROOT / "dist" / "assets").glob("*"))
    js_assets = [path for path in assets if path.suffix == ".js"]
    css_assets = [path for path in assets if path.suffix == ".css"]
    if not js_assets or not css_assets:
        raise RuntimeError("dist/assets must contain at least one JS file and one CSS file")


def should_scan(path):
    text = str(path.relative_to(PROJECT_ROOT)).replace("/", "\\")
    if any(part in text.split("\\") for part in SKIP_PARTS):
        return False
    if "frontend\\package-lock.json" in text:
        return False
    return path.suffix in SCAN_EXTENSIONS or path.name == ".env.example"


def check_secret_patterns():
    findings = []
    for path in PROJECT_ROOT.rglob("*"):
        if not path.is_file() or not should_scan(path):
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for pattern in SECRET_PATTERNS:
            if pattern.search(content):
                findings.append(rel(path))
                break
    if findings:
        raise RuntimeError("Potential hardcoded deployment credentials: " + ", ".join(sorted(set(findings))))


def check_no_temp_artifacts():
    leftovers = [name for name in FORBIDDEN_ROOT_FILES if (PROJECT_ROOT / name).exists()]
    if leftovers:
        raise RuntimeError("Temporary recovery artifacts must not be deployed: " + ", ".join(sorted(leftovers)))


def check_backend_requirements():
    requirements = PROJECT_ROOT / "backend" / "requirements.txt"
    packages = set()
    for line in requirements.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        name = re.split(r"[<>=!~\[]", item, 1)[0].strip().lower()
        packages.add(name)
    missing = sorted(REQUIRED_BACKEND_REQUIREMENTS - packages)
    if missing:
        raise RuntimeError("backend/requirements.txt missing production packages: " + ", ".join(missing))


def check_python_compile():
    errors = []
    for path in sorted((PROJECT_ROOT / "backend").rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"{rel(path)}: {exc.msg}")
    if errors:
        raise RuntimeError("Python source compile failed:\n" + "\n".join(errors))


def check_deploy_dry_run():
    env = os.environ.copy()
    env.update({
        "IKUNANCE_DEPLOY_HOST": "example.invalid",
        "IKUNANCE_DEPLOY_USER": "deploy",
        "IKUNANCE_DEPLOY_PASSWORD": "not-used-in-dry-run",
        "IKUNANCE_DEPLOY_DRY_RUN": "1",
    })
    output = run([sys.executable, "deploy_script.py"], env=env)
    required = [
        "/backend/app.py",
        "/backend/wsgi.py",
        "/frontend/dist/index.html",
        "/frontend/dist/logo-white.svg",
    ]
    missing = [item for item in required if item not in output]
    if missing:
        raise RuntimeError("Dry-run manifest missing: " + ", ".join(missing))


def check_python_tests():
    run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"])


def check_frontend_build():
    npm = shutil.which("npm") or shutil.which("npm.cmd")
    if not npm:
        raise RuntimeError("npm is not available")
    if not (FRONTEND_ROOT / "node_modules").exists():
        raise RuntimeError("frontend/node_modules is missing; run npm install in frontend/")
    run([npm, "run", "build"], cwd=FRONTEND_ROOT)


def main():
    parser = argparse.ArgumentParser(description="IKUNANCE deployment preflight verifier")
    parser.add_argument("--skip-tests", action="store_true", help="Skip Python unittest suite")
    parser.add_argument("--skip-frontend-build", action="store_true", help="Skip npm run build")
    args = parser.parse_args()

    checks = [
        ("required files", check_required_files),
        ("dist manifest", check_dist_manifest),
        ("temp artifacts", check_no_temp_artifacts),
        ("backend requirements", check_backend_requirements),
        ("python compile", check_python_compile),
        ("secret scan", check_secret_patterns),
        ("deploy dry-run", check_deploy_dry_run),
    ]
    if not args.skip_tests:
        checks.append(("python tests", check_python_tests))
    if not args.skip_frontend_build:
        checks.append(("frontend build", check_frontend_build))

    for name, check in checks:
        check()
        print(f"PASS {name}")
    print("IKUNANCE preflight passed")


if __name__ == "__main__":
    main()
