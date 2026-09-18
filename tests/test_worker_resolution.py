#!/usr/bin/env python3
"""Synthetic tests for locating the task worker that belongs to a release."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from foxhound.execution_worker import (
    RUN_STATE_SCHEMA_VERSION,
    _report,
)
from foxhound.worker_resolution import (
    REPORT_SCHEMA,
    REPORT_SCHEMA_VERSION,
    WorkerMismatch,
    is_worker_command,
    resolve_worker_command,
    verify_worker,
    worker_report,
)


def _completed(stdout: str, returncode: int = 0):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr="")


def _answering(document, returncode: int = 0):
    """A stand-in for subprocess.run that answers with ``document``."""
    def run(argv, **_kwargs):
        run.argv = argv
        payload = document if isinstance(document, str) else json.dumps(document)
        return _completed(payload, returncode)
    run.argv = None
    return run


class WorkerCommandShapeTests(unittest.TestCase):
    def test_a_bare_console_script_name_is_accepted(self):
        self.assertTrue(is_worker_command("foxhound-task-worker"))

    def test_an_absolute_path_is_accepted(self):
        self.assertTrue(is_worker_command("/srv/example/venv/bin/worker"))

    def test_values_that_could_not_be_argv0_safely_are_refused(self):
        for value in (
            "",
            "worker with spaces",
            "/srv/example/bin/worker with spaces",
            "worker;rm",
            "/srv/example/$(id)/worker",
            "../worker",
            "./worker",
            "relative/worker",
            "-worker",
            None,
            123,
            b"foxhound-task-worker",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_worker_command(value))


class ResolveWorkerCommandTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.bin = Path(self._temporary.name) / "bin"
        self.bin.mkdir()
        self.interpreter = self.bin / "python3"
        self.interpreter.write_text("", encoding="utf-8")

    def _install_worker(self, name: str = "foxhound-task-worker") -> Path:
        worker = self.bin / name
        worker.write_text("", encoding="utf-8")
        worker.chmod(0o700)
        return worker

    def test_a_bare_name_resolves_beside_the_running_interpreter(self):
        worker = self._install_worker()
        self.assertEqual(
            resolve_worker_command(
                "foxhound-task-worker", interpreter=self.interpreter
            ),
            str(worker),
        )

    def test_an_absolute_command_is_left_alone(self):
        # The deployment has already said which worker it means; a layout we
        # did not anticipate keeps its one escape hatch.
        self._install_worker()
        explicit = "/srv/example/other/bin/foxhound-task-worker"
        self.assertEqual(
            resolve_worker_command(explicit, interpreter=self.interpreter),
            explicit,
        )

    def test_a_missing_neighbour_falls_back_to_the_bare_name(self):
        self.assertEqual(
            resolve_worker_command(
                "foxhound-task-worker", interpreter=self.interpreter
            ),
            "foxhound-task-worker",
        )

    def test_a_neighbour_that_is_not_executable_falls_back(self):
        worker = self.bin / "foxhound-task-worker"
        worker.write_text("", encoding="utf-8")
        worker.chmod(0o600)
        self.assertEqual(
            resolve_worker_command(
                "foxhound-task-worker", interpreter=self.interpreter
            ),
            "foxhound-task-worker",
        )

    def test_a_directory_named_like_the_worker_falls_back(self):
        (self.bin / "foxhound-task-worker").mkdir()
        self.assertEqual(
            resolve_worker_command(
                "foxhound-task-worker", interpreter=self.interpreter
            ),
            "foxhound-task-worker",
        )

    def test_a_resolved_path_that_could_not_be_argv0_safely_falls_back(self):
        # A directory with a space in it must not become part of a prompt.
        awkward = Path(self._temporary.name) / "bin dir"
        awkward.mkdir()
        worker = awkward / "foxhound-task-worker"
        worker.write_text("", encoding="utf-8")
        worker.chmod(0o700)
        self.assertEqual(
            resolve_worker_command(
                "foxhound-task-worker", interpreter=awkward / "python3"
            ),
            "foxhound-task-worker",
        )

    def test_an_invalid_command_is_refused(self):
        with self.assertRaises(ValueError):
            resolve_worker_command("worker with spaces")


class WorkerReportTests(unittest.TestCase):
    def _document(self, **overrides):
        document = {
            "schema": REPORT_SCHEMA,
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_state_schema_version": RUN_STATE_SCHEMA_VERSION,
            "revision": "synthetic-revision",
        }
        document.update(overrides)
        return document

    def test_a_well_formed_report_is_returned(self):
        run = _answering(self._document())
        document = worker_report("foxhound-task-worker", run=run)
        self.assertEqual(document["run_state_schema_version"],
                         RUN_STATE_SCHEMA_VERSION)
        self.assertEqual(run.argv, ["foxhound-task-worker", "report"])

    def test_a_worker_that_cannot_be_run_is_a_mismatch(self):
        def run(argv, **_kwargs):
            raise OSError("synthetic missing worker")
        with self.assertRaises(WorkerMismatch):
            worker_report("foxhound-task-worker", run=run)

    def test_a_worker_that_times_out_is_a_mismatch(self):
        def run(argv, **_kwargs):
            raise subprocess.TimeoutExpired(argv, 1)
        with self.assertRaises(WorkerMismatch):
            worker_report("foxhound-task-worker", run=run)

    def test_a_failing_worker_is_a_mismatch(self):
        # An older worker predates the subcommand and exits non-zero.
        with self.assertRaises(WorkerMismatch):
            worker_report(
                "foxhound-task-worker", run=_answering("", returncode=2)
            )

    def test_unparseable_output_is_a_mismatch(self):
        with self.assertRaises(WorkerMismatch):
            worker_report(
                "foxhound-task-worker", run=_answering("not json at all")
            )

    def test_a_foreign_or_malformed_document_is_a_mismatch(self):
        for overrides in (
            {"schema": "something.else"},
            {"schema_version": REPORT_SCHEMA_VERSION + 1},
            {"run_state_schema_version": "5"},
            {"run_state_schema_version": True},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(WorkerMismatch):
                    worker_report(
                        "foxhound-task-worker",
                        run=_answering(self._document(**overrides)),
                    )

    def test_a_bare_list_is_a_mismatch(self):
        with self.assertRaises(WorkerMismatch):
            worker_report("foxhound-task-worker", run=_answering([1, 2, 3]))


class VerifyWorkerTests(unittest.TestCase):
    def _document(self, **overrides):
        document = {
            "schema": REPORT_SCHEMA,
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_state_schema_version": RUN_STATE_SCHEMA_VERSION,
            "revision": "synthetic-revision",
        }
        document.update(overrides)
        return document

    def test_a_matching_worker_is_accepted(self):
        document = verify_worker(
            "foxhound-task-worker",
            run_state_schema_version=RUN_STATE_SCHEMA_VERSION,
            run=_answering(self._document()),
        )
        self.assertEqual(document["revision"], "synthetic-revision")

    def test_a_worker_on_an_older_run_state_schema_is_refused(self):
        # This is the outage: the runner writes the new version and the worker
        # refuses a schema it does not know, once per claimed run.
        with self.assertRaises(WorkerMismatch):
            verify_worker(
                "foxhound-task-worker",
                run_state_schema_version=RUN_STATE_SCHEMA_VERSION,
                run=_answering(
                    self._document(
                        run_state_schema_version=RUN_STATE_SCHEMA_VERSION - 1
                    )
                ),
            )

    def test_a_different_revision_on_the_same_schema_is_not_refused(self):
        # A worker built from the same contract but installed another way is
        # not a fault; refusing it would make development checkouts unrunnable.
        document = verify_worker(
            "foxhound-task-worker",
            run_state_schema_version=RUN_STATE_SCHEMA_VERSION,
            run=_answering(self._document(revision="synthetic-other")),
        )
        self.assertEqual(document["revision"], "synthetic-other")


class ReportContractTests(unittest.TestCase):
    """The two halves of the handshake must not drift apart."""

    def test_the_worker_report_satisfies_the_runner_check(self):
        document = verify_worker(
            "foxhound-task-worker",
            run_state_schema_version=RUN_STATE_SCHEMA_VERSION,
            run=_answering(_report()),
        )
        self.assertEqual(document["schema"], REPORT_SCHEMA)
        self.assertEqual(document["schema_version"], REPORT_SCHEMA_VERSION)

    def test_the_report_is_json_serialisable(self):
        # It is printed, so a value that cannot be serialised is a crash in
        # the one code path that exists to diagnose crashes.
        json.dumps(_report(), sort_keys=True)


if __name__ == "__main__":
    unittest.main()
