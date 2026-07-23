from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration.eod_lock import (  # noqa: E402
    EOD_WRITER_LOCK_PATH_ENV,
    EOD_WRITER_LOCK_TOKEN_ENV,
    EodWriterAlreadyRunningError,
    EodWriterDelegationError,
    delegated_eod_writer_environment,
    eod_writer_lock_path,
    eod_writer_execution_lock,
    exclusive_eod_canonical_writer_lock,
    exclusive_eod_writer_lock,
)


class EodWriterLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temporary.name).resolve()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_second_writer_is_rejected_and_lock_is_released_on_exit(self):
        with exclusive_eod_writer_lock(self.project_root):
            with self.assertRaises(EodWriterAlreadyRunningError):
                with exclusive_eod_writer_lock(self.project_root):
                    self.fail("a second writer acquired the project lock")

        with exclusive_eod_writer_lock(self.project_root) as lease:
            self.assertTrue(lease.path.is_file())

    def test_canonical_writer_lock_survives_independently_of_operation_gate(self):
        with exclusive_eod_canonical_writer_lock(self.project_root):
            with self.assertRaises(EodWriterAlreadyRunningError):
                with exclusive_eod_canonical_writer_lock(self.project_root):
                    self.fail("a second canonical writer acquired the lock")

        with exclusive_eod_canonical_writer_lock(self.project_root) as lock_path:
            self.assertTrue(lock_path.is_file())

    def test_delegated_child_proves_parent_ownership(self):
        with exclusive_eod_writer_lock(self.project_root) as parent:
            delegated = delegated_eod_writer_environment(parent)
            with eod_writer_execution_lock(
                self.project_root,
                environment=delegated,
            ) as child:
                self.assertEqual(child, parent)

    def test_invalid_or_unheld_delegation_is_rejected(self):
        with exclusive_eod_writer_lock(self.project_root) as parent:
            delegated = delegated_eod_writer_environment(parent)
            delegated[EOD_WRITER_LOCK_TOKEN_ENV] = "0" * 64
            with self.assertRaises(EodWriterDelegationError):
                with eod_writer_execution_lock(
                    self.project_root,
                    environment=delegated,
                ):
                    self.fail("invalid delegation was accepted")

        stale = delegated_eod_writer_environment(parent)
        with self.assertRaises(EodWriterDelegationError):
            with eod_writer_execution_lock(
                self.project_root,
                environment=stale,
            ):
                self.fail("an unheld delegation was accepted")

    def test_delegation_proof_is_not_inherited_by_pipeline_children(self):
        with exclusive_eod_writer_lock(self.project_root) as parent:
            delegated = delegated_eod_writer_environment(parent)
            with patch.dict(os.environ, delegated, clear=False):
                with eod_writer_execution_lock(self.project_root):
                    self.assertNotIn(EOD_WRITER_LOCK_PATH_ENV, os.environ)
                    self.assertNotIn(EOD_WRITER_LOCK_TOKEN_ENV, os.environ)
                self.assertEqual(
                    os.environ[EOD_WRITER_LOCK_PATH_ENV],
                    delegated[EOD_WRITER_LOCK_PATH_ENV],
                )
                self.assertEqual(
                    os.environ[EOD_WRITER_LOCK_TOKEN_ENV],
                    delegated[EOD_WRITER_LOCK_TOKEN_ENV],
                )

    def test_partial_delegation_fails_closed(self):
        with self.assertRaises(EodWriterDelegationError):
            with eod_writer_execution_lock(
                self.project_root,
                environment={EOD_WRITER_LOCK_PATH_ENV: "C:/incomplete"},
            ):
                self.fail("partial delegation was accepted")

    def test_symlinked_lock_file_is_rejected_without_touching_target(self):
        lock_path = eod_writer_lock_path(self.project_root)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        target = self.project_root / "outside-lock-target.txt"
        target.write_text("preserve", encoding="utf-8")
        try:
            lock_path.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"file symlinks are unavailable: {exc}")

        with self.assertRaises(OSError):
            with exclusive_eod_writer_lock(self.project_root):
                self.fail("a symlinked lock file was accepted")
        self.assertEqual(target.read_text(encoding="utf-8"), "preserve")


if __name__ == "__main__":
    unittest.main(verbosity=2)
