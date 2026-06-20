import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DocsTest(unittest.TestCase):
    def test_readme_keeps_deploy_commands_and_health_contract(self):
        readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("python scripts/verify_project.py", readme)
        self.assertIn("python deploy_script.py", readme)
        self.assertIn("gunicorn -w 1", readme)
        self.assertIn("/api/health", readme)
        self.assertIn("IKUNANCE_DEPLOY_HEALTH_TIMEOUT", readme)
        self.assertIn("IKUNANCE_DEPLOY_WORKERS=1", readme)
        self.assertIn("absolute", readme.lower())

    def test_deployment_checklist_keeps_release_steps(self):
        checklist = (PROJECT_ROOT / "DEPLOYMENT_CHECKLIST.md").read_text(encoding="utf-8")
        self.assertIn("python -m unittest discover -s tests -v", checklist)
        self.assertIn("python scripts/verify_project.py", checklist)
        self.assertIn("IKUNANCE_DEPLOY_DRY_RUN=1", checklist)
        self.assertIn("python deploy_script.py", checklist)
        self.assertIn("gunicorn -w 1", checklist)
        self.assertIn("/api/health", checklist)
        self.assertIn("Never delete the production data directory", checklist)


if __name__ == "__main__":
    unittest.main()
