from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vrp.reference_history.artifacts import ContentAddressedArtifactStore, prepare_history
from vrp.reference_history.sources import normalize_sofr_csv


class ReferenceHistoryArtifactTests(unittest.TestCase):
    def test_inputs_are_frozen_before_normalization_and_reverified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sofr.csv"
            source.write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n2018-04-04,1.74\n",
                encoding="utf-8",
            )
            store = ContentAddressedArtifactStore(root / "artifacts")
            frozen = store.freeze_inputs({"sofr": source})
            source.write_text(
                "observation_date,SOFR\n2018-04-03,9.99\n",
                encoding="utf-8",
            )
            history = normalize_sofr_csv(
                frozen.inputs["sofr"].path,
                enforce_production_coverage=False,
            )
            prepared = prepare_history(history, frozen_inputs=frozen, store=store)
            store.verify_prepared(prepared)
            self.assertEqual(str(history.rows[0].rate_percent), "1.830000000000")
            self.assertNotEqual(source.read_bytes(), frozen.inputs["sofr"].path.read_bytes())

            frozen.inputs["sofr"].path.write_text("tampered", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "digest|size"):
                store.verify_prepared(prepared)

    def test_unsafe_logical_names_cannot_escape_store(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sofr.csv"
            source.write_text("observation_date,SOFR\n2018-04-03,1.83\n", encoding="utf-8")
            store = ContentAddressedArtifactStore(root / "artifacts")
            with self.assertRaises(ValueError):
                store.freeze_inputs({"../outside": source})
            with self.assertRaises(ValueError):
                store.freeze_inputs({"normalized_history": source})


if __name__ == "__main__":
    unittest.main()
