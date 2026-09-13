#!/usr/bin/env python3
"""Synthetic tests for strict Foxhound agent profiles."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.agent_profiles import (
    MAX_MANIFEST_BYTES,
    AgentProfileError,
    AgentProfileRegistry,
    WORKER_COMMAND_TOKEN,
    general_profile,
    load_registry,
    main,
    parse_profile,
)


EXPECTED_GENERAL_PROMPT_SHA256 = (
    "9db37521f682499a999039fe3580c9c004c0b16378ed57758a8eaa6dd4adb9bb"
)


def profile_document(profile_id: str = "specialist") -> dict[str, object]:
    return {
        "schema": "foxhound.agent-profile",
        "schema_version": 1,
        "profile_id": profile_id,
        "display_name": "Synthetic Specialist",
        "runtime": "hermes",
        "prompt_template": (
            f"First use {WORKER_COMMAND_TOKEN} context. Synthetic guidance."
        ),
        "toolsets": ["terminal", "file"],
        "max_turns": 50,
        "timeout_seconds": 1_800,
        "claim_lease_seconds": 2_700,
        "heartbeat_seconds": 60,
        "kill_grace_seconds": 30,
        "allowed_phases": ["plan", "execute"],
    }


class AgentProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)

    def _directory(self, name: str = "profiles") -> Path:
        directory = self.root / name
        directory.mkdir(mode=0o700)
        return directory

    def _write(
        self,
        directory: Path,
        document: object,
        name: str = "specialist.json",
    ) -> Path:
        path = directory / name
        path.write_text(json.dumps(document), encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_general_profile_is_exact_runner_compatibility_profile(self):
        profile = general_profile()
        prompt = profile.render_prompt("foxhound-task-worker")

        self.assertEqual(profile.profile_id, "general")
        self.assertEqual(profile.runtime, "hermes")
        self.assertEqual(profile.max_turns, 12)
        self.assertEqual(profile.timeout_seconds, 240)
        self.assertEqual(
            hashlib.sha256(prompt.encode()).hexdigest(),
            EXPECTED_GENERAL_PROMPT_SHA256,
        )
        self.assertNotIn(WORKER_COMMAND_TOKEN, prompt)
        self.assertIn("synthetic-worker context", profile.render_prompt(
            "synthetic-worker"
        ))
        with self.assertRaises(AgentProfileError):
            profile.render_prompt("worker; command")

    def test_public_coder_example_is_synthetic_and_structurally_valid(self):
        path = (
            Path(__file__).parents[1]
            / "examples"
            / "agent-profiles"
            / "example-coder.json"
        )
        profile = parse_profile(json.loads(path.read_text(encoding="utf-8")))

        self.assertEqual(
            (profile.profile_id, profile.display_name, profile.runtime),
            ("example-coder", "Example Coder", "hermes"),
        )
        self.assertEqual(
            (
                profile.toolsets,
                profile.max_turns,
                profile.timeout_seconds,
                profile.claim_lease_seconds,
                profile.allowed_phases,
            ),
            (
                ("terminal", "file", "web", "vision"),
                50,
                1_800,
                2_700,
                ("plan", "execute", "external_action"),
            ),
        )
        prompt = profile.render_prompt("synthetic-worker")
        self.assertIn("Synthetic example only", prompt)
        self.assertIn("synthetic-worker context", prompt)
        self.assertIn("synthetic-worker draft --outcome OUTCOME", prompt)
        self.assertIn("synthetic-worker record RESULT_FILE", prompt)
        self.assertNotIn(WORKER_COMMAND_TOKEN, prompt)

    def test_revision_is_stable_complete_and_prompt_is_not_public(self):
        first = parse_profile(profile_document())
        second = parse_profile(profile_document())
        changed_document = profile_document()
        changed_document["max_turns"] = 51
        changed = parse_profile(changed_document)

        self.assertEqual(first.revision, second.revision)
        self.assertNotEqual(first.revision, changed.revision)
        self.assertEqual(len(first.revision), 64)
        rendered = json.dumps(first.public_summary(include_policy=True))
        self.assertNotIn("prompt", rendered)
        self.assertNotIn("Synthetic guidance", rendered)

    def test_registry_never_falls_back_for_unknown_or_changed_revision(self):
        profile = parse_profile(profile_document())
        registry = AgentProfileRegistry((general_profile(), profile))

        self.assertEqual(registry.resolve(profile.profile_id, profile.revision), profile)
        for profile_id, revision in (
            ("missing", profile.revision),
            (profile.profile_id, "0" * 64),
            (profile.profile_id, None),
        ):
            with self.subTest(profile_id=profile_id, revision=revision):
                with self.assertRaises(AgentProfileError):
                    registry.resolve(profile_id, revision)
        with self.assertRaises(AgentProfileError):
            AgentProfileRegistry((profile, profile))

    def test_manifest_shape_refuses_missing_unknown_and_command_fields(self):
        base = profile_document()
        variants = []
        missing = dict(base)
        missing.pop("profile_id")
        variants.append(missing)
        for forbidden in ("argv", "command", "environment", "secret", "model"):
            document = dict(base)
            document[forbidden] = "synthetic-value"
            variants.append(document)
        document = dict(base)
        document["schema_version"] = 2
        variants.append(document)
        document = dict(base)
        document["schema_version"] = True
        variants.append(document)

        for document in variants:
            with self.subTest(keys=sorted(document)):
                with self.assertRaises(AgentProfileError):
                    parse_profile(document)
        for document in (None, [], "profile"):
            with self.subTest(document=document):
                with self.assertRaises(AgentProfileError):
                    parse_profile(document)

    def test_identity_runtime_prompt_tools_and_phases_are_bounded(self):
        base = profile_document()
        variants: list[tuple[str, object]] = [
            ("profile_id", "Uppercase"),
            ("profile_id", "a" * 33),
            ("display_name", " padded "),
            ("display_name", "line\nbreak"),
            ("display_name", "a" * 65),
            ("runtime", "shell"),
            ("prompt_template", "missing worker token"),
            ("prompt_template", f"{WORKER_COMMAND_TOKEN}\0bad"),
            ("toolsets", []),
            ("toolsets", ["terminal", "terminal"]),
            ("toolsets", ["unrestricted"]),
            ("toolsets", "terminal"),
            ("allowed_phases", []),
            ("allowed_phases", ["execute", "execute"]),
            ("allowed_phases", ["admin"]),
            ("allowed_phases", "execute"),
        ]
        for field, value in variants:
            document = dict(base)
            document[field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(AgentProfileError):
                    parse_profile(document)

    def test_numeric_limits_and_timing_relationships_fail_closed(self):
        base = profile_document()
        variants = (
            ("max_turns", 0),
            ("max_turns", 201),
            ("max_turns", True),
            ("timeout_seconds", 29),
            ("timeout_seconds", 3_301),
            ("claim_lease_seconds", 299),
            ("claim_lease_seconds", 3_601),
            ("heartbeat_seconds", 4),
            ("heartbeat_seconds", 601),
            ("kill_grace_seconds", 0),
            ("kill_grace_seconds", 121),
        )
        for field, value in variants:
            document = dict(base)
            document[field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(AgentProfileError):
                    parse_profile(document)

        for changes in (
            {"timeout_seconds": 2_670},
            {"heartbeat_seconds": 601},
            {"claim_lease_seconds": 300, "heartbeat_seconds": 100},
            {
                "claim_lease_seconds": 300,
                "timeout_seconds": 30,
                "heartbeat_seconds": 280,
                "kill_grace_seconds": 20,
            },
        ):
            document = dict(base)
            document.update(changes)
            with self.subTest(changes=changes):
                with self.assertRaises(AgentProfileError):
                    parse_profile(document)

    def test_direct_construction_cannot_bypass_structured_collections(self):
        profile = general_profile()
        for changes in (
            {"toolsets": ["terminal"]},
            {"allowed_phases": ["plan"]},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(AgentProfileError):
                    replace(profile, **changes)

    def test_private_registry_loads_owner_only_manifests(self):
        directory = self._directory()
        self._write(directory, profile_document())

        registry = load_registry(directory)

        self.assertEqual(
            [profile.profile_id for profile in registry.list()],
            ["general", "specialist"],
        )

    def test_private_directory_refuses_relative_permissive_symlink_and_git(self):
        directory = self._directory()
        with self.assertRaises(AgentProfileError):
            load_registry(Path("relative"))

        directory.chmod(0o750)
        with self.assertRaises(AgentProfileError):
            load_registry(directory)
        directory.chmod(0o700)

        alias = self.root / "alias"
        alias.symlink_to(directory, target_is_directory=True)
        with self.assertRaises(AgentProfileError):
            load_registry(alias)

        checkout = self._directory("checkout")
        (checkout / ".git").mkdir(mode=0o700)
        (checkout / ".git" / "HEAD").write_text(
            "ref: refs/heads/main\n", encoding="utf-8"
        )
        contained = checkout / "profiles"
        contained.mkdir(mode=0o700)
        with self.assertRaises(AgentProfileError):
            load_registry(contained)

        directory_info = directory.lstat()
        foreign_directory = mock.Mock(
            st_mode=directory_info.st_mode,
            st_uid=directory_info.st_uid + 1,
        )
        with mock.patch.object(Path, "lstat", return_value=foreign_directory):
            with self.assertRaises(AgentProfileError):
                load_registry(directory)

    def test_private_entries_refuse_unsafe_shapes(self):
        cases = ("mode", "symlink", "filename", "extension", "directory")
        for case in cases:
            with self.subTest(case=case):
                directory = self._directory(f"profiles-{case}")
                if case == "mode":
                    self._write(directory, profile_document()).chmod(0o640)
                elif case == "symlink":
                    target = self._write(
                        self.root, profile_document(), "outside.json"
                    )
                    (directory / "specialist.json").symlink_to(target)
                elif case == "filename":
                    self._write(directory, profile_document(), "different.json")
                elif case == "extension":
                    self._write(directory, profile_document(), "specialist.txt")
                else:
                    (directory / "nested.json").mkdir(mode=0o700)
                with self.assertRaises(AgentProfileError):
                    load_registry(directory)

        directory = self._directory("profiles-owner")
        path = self._write(directory, profile_document())
        file_info = path.stat()
        foreign_file = mock.Mock(
            st_mode=file_info.st_mode,
            st_uid=file_info.st_uid + 1,
            st_size=file_info.st_size,
        )
        with mock.patch(
            "foxhound.agent_profiles.os.fstat", return_value=foreign_file
        ):
            with self.assertRaises(AgentProfileError):
                load_registry(directory)

    def test_private_json_refuses_duplicates_invalid_encoding_and_excess(self):
        directory = self._directory("profiles-duplicate")
        path = directory / "specialist.json"
        path.write_text('{"schema":1,"schema":2}', encoding="utf-8")
        path.chmod(0o600)
        with self.assertRaises(AgentProfileError):
            load_registry(directory)

        directory = self._directory("profiles-encoding")
        path = directory / "specialist.json"
        path.write_bytes(b"\xff")
        path.chmod(0o600)
        with self.assertRaises(AgentProfileError):
            load_registry(directory)

        directory = self._directory("profiles-size")
        path = directory / "specialist.json"
        path.write_bytes(b" " * (MAX_MANIFEST_BYTES + 1))
        path.chmod(0o600)
        with self.assertRaises(AgentProfileError):
            load_registry(directory)

        directory = self._directory("profiles-count")
        for index in range(33):
            profile_id = f"agent-{index}"
            self._write(
                directory,
                profile_document(profile_id),
                f"{profile_id}.json",
            )
        with self.assertRaises(AgentProfileError):
            load_registry(directory)

    def test_cli_output_is_content_free_for_success_and_failure(self):
        directory = self._directory()
        private_value = "synthetic-private-prompt-value"
        document = profile_document()
        document["prompt_template"] = (
            f"{WORKER_COMMAND_TOKEN} context. {private_value}"
        )
        self._write(directory, document)

        for arguments in (
            ["--directory", str(directory), "list"],
            ["--directory", str(directory), "show", "specialist"],
            ["--directory", str(directory), "validate"],
        ):
            output = StringIO()
            errors = StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                code = main(arguments)
            rendered = output.getvalue() + errors.getvalue()
            self.assertEqual(code, 0)
            self.assertNotIn(private_value, rendered)
            self.assertNotIn(str(directory), rendered)

        self._write(directory, {"private": private_value})
        output = StringIO()
        errors = StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["--directory", str(directory), "validate"])
        rendered = output.getvalue() + errors.getvalue()
        self.assertEqual(code, os.EX_CONFIG)
        self.assertNotIn(private_value, rendered)
        self.assertNotIn(str(directory), rendered)


if __name__ == "__main__":
    unittest.main()
