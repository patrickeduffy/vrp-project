from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from vrp.reference_history.sources import normalize_sofr_frame

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "load_reference_history_test_module",
    ROOT / "scripts/load_reference_history.py",
)
loader_cli = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(loader_cli)


class ReferenceHistoryCliTests(unittest.TestCase):
    def test_validate_only_reads_runtime_contract_without_writing_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            (project / "config").mkdir()
            (project / "data").mkdir()
            (project / "data/sofr.csv").write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n2018-04-04,1.74\n",
                encoding="utf-8",
            )
            runtime = {
                "canonical": {
                    "sofr_cache": "data/sofr.csv",
                    "spy_eod": "data/not-read.parquet",
                    "wilder_rsi": "data/not-read-rsi.parquet",
                    "rv21d": "data/not-read-rv.parquet",
                }
            }
            (project / "config/runtime.json").write_text(
                json.dumps(runtime), encoding="utf-8"
            )
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--runtime-config",
                    "config/runtime.json",
                    "--validate-only",
                ]
            )
            partial = normalize_sofr_frame(
                pd.DataFrame(
                    {
                        "observation_date": ["2018-04-03", "2018-04-04"],
                        "SOFR": ["1.83", "1.74"],
                    }
                ),
                enforce_production_coverage=False,
            )
            with patch.object(loader_cli, "_stable_validate", return_value=partial):
                result = loader_cli.run(args)
            self.assertEqual(result[0]["dataset_key"], "FRED_SOFR")
            self.assertIsNone(result[0]["definition_content_sha256"])
            self.assertEqual(result[0]["row_count"], 2)
            self.assertFalse((project / "data/reference_history").exists())

    def test_generation_cannot_be_negative(self):
        args = loader_cli.parse_args(["sofr", "--generation", "-1", "--validate-only"])
        with self.assertRaisesRegex(ValueError, "non-negative"):
            loader_cli.run(args)


if __name__ == "__main__":
    unittest.main()
