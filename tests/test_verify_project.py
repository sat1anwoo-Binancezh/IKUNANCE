import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VERIFY_SCRIPT = PROJECT_ROOT / "scripts" / "verify_project.py"


class VerifyProjectTest(unittest.TestCase):
    def test_preflight_static_and_dry_run_checks_pass(self):
        result = subprocess.run(
            [
                sys.executable,
                str(VERIFY_SCRIPT),
                "--skip-tests",
                "--skip-frontend-build",
            ],
            cwd=PROJECT_ROOT,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("PASS required files", result.stdout)
        self.assertIn("PASS dist manifest", result.stdout)
        self.assertIn("PASS temp artifacts", result.stdout)
        self.assertIn("PASS backend requirements", result.stdout)
        self.assertIn("PASS python compile", result.stdout)
        self.assertIn("PASS secret scan", result.stdout)
        self.assertIn("PASS deploy dry-run", result.stdout)

    def test_preflight_source_tracks_expected_risks(self):
        source = VERIFY_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("SECRET_PATTERNS", source)
        self.assertIn("FORBIDDEN_ROOT_FILES", source)
        self.assertIn("REQUIRED_BACKEND_REQUIREMENTS", source)
        self.assertIn("py_compile.compile", source)
        self.assertIn("IKUNANCE_DEPLOY_DRY_RUN", source)
        self.assertIn("frontend build", source)


if __name__ == "__main__":
    unittest.main()
