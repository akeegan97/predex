from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from predex.env import find_repo_dotenv, load_repo_dotenv


class DotenvTests(unittest.TestCase):
    def test_load_repo_dotenv_loads_values_without_overwriting_existing_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv_path = root / ".env"
            dotenv_path.write_text(
                "KALSHI_KEY_ID=test-key\n"
                "KALSHI_PRIVATE_KEY_PEM='line1\\nline2'\n",
                encoding="utf-8",
            )

            original_cwd = Path.cwd()
            original_key = os.environ.get("KALSHI_KEY_ID")
            original_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
            os.environ["KALSHI_KEY_ID"] = "already-set"
            os.environ.pop("KALSHI_PRIVATE_KEY_PEM", None)
            try:
                os.chdir(root)
                self.assertEqual(find_repo_dotenv(), dotenv_path)
                loaded = load_repo_dotenv()
                self.assertEqual(loaded, dotenv_path)
                self.assertEqual(os.environ["KALSHI_KEY_ID"], "already-set")
                self.assertEqual(os.environ["KALSHI_PRIVATE_KEY_PEM"], "line1\nline2")
            finally:
                os.chdir(original_cwd)
                if original_key is None:
                    os.environ.pop("KALSHI_KEY_ID", None)
                else:
                    os.environ["KALSHI_KEY_ID"] = original_key
                if original_pem is None:
                    os.environ.pop("KALSHI_PRIVATE_KEY_PEM", None)
                else:
                    os.environ["KALSHI_PRIVATE_KEY_PEM"] = original_pem
