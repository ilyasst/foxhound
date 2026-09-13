from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from foxhound.scheduler_cutover import (
    SchedulerCutoverError,
    prepare_cutover,
    verify_cutover,
)


RETAINED_PREFIX = (
    b"# synthetic scheduler\r\n"
    b"# python -m gw.task_close --apply is a historical note\r\n"
    b"0 * * * * run source-example --mode safe\r\n"
    b"23 * * * * python -m gw.task_registry --persona demo --apply\r\n"
)
REMOVED = (
    b"26 * * * * python -m gw.task_close --persona demo --apply\r\n",
    b"30 4 * * 0 gw tasks archive-stale --apply --persona demo\r\n",
    b"0 * * * * gw scheduled-run --job task-inventory --command synthetic\r\n",
    b"10 * * * * gw scheduled-run --job task-workflow --command synthetic\r\n",
    b"*/5 * * * * gw scheduled-run --job task-workflow-agent --command synthetic\r\n",
    b"0 9 * * 1 gw scheduled-run --job task-weekly-review --command synthetic\r\n",
)
RETAINED_SUFFIX = (
    b"5 * * * * run claims-rebind --mode safe\n"
    b"# final comment without newline"
)


class SchedulerCutoverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.snapshot = self.root / "snapshot"
        self.candidate = self.root / "candidate"
        self.rollback = self.root / "rollback"
        self.payload = RETAINED_PREFIX + b"".join(REMOVED) + RETAINED_SUFFIX
        self._write_snapshot(self.payload)

    def _write_snapshot(self, payload: bytes) -> None:
        self.snapshot.write_bytes(payload)
        self.snapshot.chmod(0o600)

    def test_prepares_exact_candidate_and_rollback(self) -> None:
        report = prepare_cutover(
            self.snapshot,
            candidate_path=self.candidate,
            rollback_path=self.rollback,
        )
        expected = RETAINED_PREFIX + RETAINED_SUFFIX
        self.assertEqual(self.candidate.read_bytes(), expected)
        self.assertEqual(self.rollback.read_bytes(), self.payload)
        self.assertEqual(report.removed_jobs, 6)
        self.assertEqual(report.retained_creation_registry, 1)
        self.assertEqual(report.candidate_bytes, len(expected))
        self.assertEqual(report.retained_bytes, len(expected))
        self.assertEqual(report.candidate_sha256, report.retained_sha256)
        self.assertEqual(report.source_sha256, report.rollback_sha256)
        self.assertEqual(self.candidate.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.rollback.stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            verify_cutover(
                self.snapshot,
                candidate_path=self.candidate,
                rollback_path=self.rollback,
            ),
            report,
        )

    def test_missing_duplicate_and_ambiguous_jobs_fail_closed(self) -> None:
        cases = (
            self.payload.replace(REMOVED[0], b""),
            self.payload + b"\n" + REMOVED[0],
            self.payload.replace(
                REMOVED[0],
                b"* * * * * python -m gw.task_close --apply "
                b"--job task-inventory synthetic\r\n",
            ),
            self.payload.replace(
                b"python -m gw.task_registry --persona demo --apply", b"true"
            ),
            self.payload + b"\n" + (
                b"24 * * * * python -m gw.task_registry --persona demo --apply\n"
            ),
        )
        for payload in cases:
            with self.subTest(payload_size=len(payload)):
                self.candidate.unlink(missing_ok=True)
                self.rollback.unlink(missing_ok=True)
                self._write_snapshot(payload)
                with self.assertRaises(SchedulerCutoverError):
                    prepare_cutover(
                        self.snapshot,
                        candidate_path=self.candidate,
                        rollback_path=self.rollback,
                    )
                self.assertFalse(self.candidate.exists())
                self.assertFalse(self.rollback.exists())

    def test_unsafe_and_non_regular_inputs_are_refused(self) -> None:
        self.snapshot.chmod(0o644)
        with self.assertRaises(SchedulerCutoverError):
            prepare_cutover(
                self.snapshot,
                candidate_path=self.candidate,
                rollback_path=self.rollback,
            )
        self.snapshot.unlink()
        self.snapshot.symlink_to(self.root / "missing")
        with self.assertRaises(SchedulerCutoverError):
            prepare_cutover(
                self.snapshot,
                candidate_path=self.candidate,
                rollback_path=self.rollback,
            )
        self.assertFalse(self.candidate.exists())
        self.assertFalse(self.rollback.exists())

    def test_existing_or_changed_artifacts_are_refused(self) -> None:
        self.candidate.write_text("do not overwrite", encoding="utf-8")
        self.candidate.chmod(0o600)
        with self.assertRaises(SchedulerCutoverError):
            prepare_cutover(
                self.snapshot,
                candidate_path=self.candidate,
                rollback_path=self.rollback,
            )
        self.assertEqual(
            self.candidate.read_text(encoding="utf-8"), "do not overwrite"
        )
        self.candidate.unlink()
        prepare_cutover(
            self.snapshot,
            candidate_path=self.candidate,
            rollback_path=self.rollback,
        )
        self.candidate.write_bytes(self.candidate.read_bytes() + b"\n")
        self.candidate.chmod(0o600)
        with self.assertRaises(SchedulerCutoverError):
            verify_cutover(
                self.snapshot,
                candidate_path=self.candidate,
                rollback_path=self.rollback,
            )

    def test_cli_output_is_content_free(self) -> None:
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(
            Path(__file__).resolve().parents[1] / "src"
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "foxhound.scheduler_cutover",
                "prepare",
                "--snapshot",
                str(self.snapshot),
                "--candidate",
                str(self.candidate),
                "--rollback",
                str(self.rollback),
            ],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(completed.stdout)
        self.assertEqual(report["removed_jobs"], 6)
        combined = completed.stdout + completed.stderr
        self.assertNotIn(str(self.root), combined)
        self.assertNotIn("source-example", combined)
        self.assertNotIn("task_registry", combined)


if __name__ == "__main__":
    unittest.main()
