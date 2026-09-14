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
    "5f3f7913642d395afe9ab3f6b02dd2e7ba98bdf522170735d2dd28c09a432aa0"
)
EXPECTED_GENERAL_REVISION = (
    "54977e3c1d8c1d4ec0e4b0fea2d3ee3740db5af2a41cc99ae348e9fed64e1bec"
)
IMMEDIATE_PREVIOUS_GENERAL_REVISION = (
    "a1ad27c9d410bd9dd67129ee0165d77a81fa2e785643379635ed93f4ebc0a59b"
)
DATE_SEMANTICS_GENERAL_REVISION = (
    "9b80c488634e614127dd1da15facbfbf78fe3f38ac000c0f2dde30c394aeb23a"
)
EARLIER_GENERAL_REVISION = (
    "6f999d7bbb0210f186d83bff149fb5828f1d177026b6cbe76455352d71b08b74"
)
PREVIOUS_GENERAL_REVISION = (
    "1143d16a81afd8ad52240ca92c6a66ac8d9e95d822b807bba53bdb8385629523"
)
SUPERSEDED_GENERAL_REVISION = (
    "5d841390306c6e53c452c00d6dab624378c58cd2f36b3228f66929fc9061a6b9"
)
LEGACY_GENERAL_REVISION = (
    "f0171b0e9e09e547d9b344223d31b6de1bc0e6d13cb5b8c891fda9d0a7b0db94"
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

    def _write_store(
        self,
        name: str,
        variants: dict[str, list[dict[str, object]]],
        *,
        state: str = "active",
    ) -> Path:
        directory = self._directory(name)
        revisions = directory / "revisions"
        revisions.mkdir(mode=0o700)
        profiles: dict[str, object] = {}
        for profile_id, documents in variants.items():
            folder = revisions / profile_id
            folder.mkdir(mode=0o700)
            history = []
            for document in documents:
                profile = parse_profile(document)
                self._write(
                    folder, profile.document(), f"{profile.revision}.json"
                )
                history.append(profile.revision)
            profiles[profile_id] = {
                "state": state,
                "revision": history[-1],
                "history": history,
            }
        self._write_catalog(directory, profiles)
        return directory

    def _write_catalog(self, directory: Path, profiles: object) -> None:
        self._write(
            directory,
            {
                "schema": "foxhound.agent-profile-catalog",
                "schema_version": 1,
                "profiles": profiles,
            },
            "catalog.json",
        )

    def test_general_profile_is_exact_runner_compatibility_profile(self):
        profile = general_profile()
        prompt = profile.render_prompt("foxhound-task-worker")

        self.assertEqual(profile.profile_id, "general")
        self.assertEqual(profile.runtime, "hermes")
        self.assertEqual(profile.revision, EXPECTED_GENERAL_REVISION)
        self.assertEqual(profile.max_turns, 50)
        self.assertEqual(profile.timeout_seconds, 1_800)
        self.assertEqual(profile.claim_lease_seconds, 2_700)
        self.assertEqual(profile.heartbeat_seconds, 60)
        self.assertEqual(profile.kill_grace_seconds, 30)
        self.assertEqual(
            hashlib.sha256(prompt.encode()).hexdigest(),
            EXPECTED_GENERAL_PROMPT_SHA256,
        )
        self.assertNotIn(WORKER_COMMAND_TOKEN, prompt)
        self.assertIn("synthetic-worker context", profile.render_prompt(
            "synthetic-worker"
        ))
        for required in (
            "runtime.today",
            "runtime.next_week",
            "do not calculate or substitute another range",
            "verify every weekday/date pair against the worker-provided values",
            "one bounded pass",
            "Do not narrate intended work instead of doing it",
            "search before concluding that evidence is missing",
            "draft --outcome OUTCOME",
            "Do not hand-author or experimentally probe the envelope schema",
            "draft and record that partial result",
            "release` only when no truthful reviewable artifact can be produced",
            "only the exact reviewed action",
        ):
            with self.subTest(required=required):
                self.assertIn(required, prompt)
        self.assertNotIn("If useful work cannot be completed", prompt)
        with self.assertRaises(AgentProfileError):
            profile.render_prompt("worker; command")

    def test_superseded_general_revisions_are_resolution_only(self):
        registry = load_registry()
        current = general_profile()

        self.assertEqual(registry.list(), (current,))
        self.assertEqual(registry.get("general"), current)
        self.assertEqual(
            registry.resolve_current("general", current.revision), current
        )
        legacy = registry.resolve("general", LEGACY_GENERAL_REVISION)
        superseded = registry.resolve("general", SUPERSEDED_GENERAL_REVISION)
        previous = registry.resolve("general", PREVIOUS_GENERAL_REVISION)
        immediate_previous = registry.resolve(
            "general", IMMEDIATE_PREVIOUS_GENERAL_REVISION
        )
        date_semantics = registry.resolve(
            "general", DATE_SEMANTICS_GENERAL_REVISION
        )
        earlier = registry.resolve("general", EARLIER_GENERAL_REVISION)

        for retained in (
            legacy, superseded, previous, earlier, date_semantics,
            immediate_previous,
        ):
            with self.subTest(revision=retained.revision):
                with self.assertRaises(AgentProfileError):
                    registry.resolve_current("general", retained.revision)
                self.assertEqual(retained.toolsets, current.toolsets)
                # A retained revision keeps the prompt it was published with.
                # Rebuilding it from the current text would change its digest
                # and strand the workflows pinned to it.
                self.assertNotEqual(
                    retained.prompt_template, current.prompt_template
                )
        self.assertEqual(
            legacy.prompt_template, superseded.prompt_template
        )
        self.assertNotEqual(previous.prompt_template, legacy.prompt_template)
        self.assertNotEqual(
            immediate_previous.prompt_template, previous.prompt_template
        )
        self.assertEqual(
            (
                legacy.max_turns,
                legacy.timeout_seconds,
                legacy.claim_lease_seconds,
                legacy.heartbeat_seconds,
                legacy.kill_grace_seconds,
            ),
            (12, 240, 900, 60, 10),
        )
        self.assertEqual(
            (superseded.max_turns, superseded.timeout_seconds),
            (50, 1_800),
        )
        self.assertEqual(legacy.allowed_phases, current.allowed_phases)

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

    def test_registry_refuses_ambiguous_historical_revisions(self):
        current = general_profile()
        historical = replace(current, max_turns=49)
        withdrawn = parse_profile(profile_document("withdrawn"))
        registry = AgentProfileRegistry(
            (current,), historical_profiles=(historical, withdrawn)
        )
        self.assertEqual(
            registry.resolve(historical.profile_id, historical.revision),
            historical,
        )

        self.assertEqual(
            registry.resolve("withdrawn", withdrawn.revision), withdrawn
        )
        self.assertEqual(registry.list(), (current,))
        self.assertIsNone(registry.get("withdrawn"))
        with self.assertRaises(AgentProfileError):
            registry.resolve_current("withdrawn", withdrawn.revision)

        for invalid in (
            (current,),
            (historical, historical),
        ):
            with self.subTest(revisions=len(invalid)):
                with self.assertRaises(AgentProfileError):
                    AgentProfileRegistry(
                        (current,), historical_profiles=invalid
                    )

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

    def test_versioned_store_lists_active_and_resolves_its_history(self):
        historical = profile_document()
        historical["max_turns"] = 40
        current = profile_document()
        directory = self._write_store(
            "store", {"specialist": [historical, current]}
        )
        previous = parse_profile(historical).revision
        offered = parse_profile(current).revision

        registry = load_registry(directory)

        self.assertEqual(
            [profile.profile_id for profile in registry.list()],
            ["general", "specialist"],
        )
        self.assertEqual(registry.get("specialist").revision, offered)
        self.assertEqual(
            registry.resolve("specialist", previous).max_turns, 40
        )
        with self.assertRaises(AgentProfileError):
            registry.resolve_current("specialist", previous)

        catalog = json.loads(
            (directory / "catalog.json").read_text(encoding="utf-8")
        )
        catalog["profiles"]["specialist"]["state"] = "disabled"
        self._write_catalog(directory, catalog["profiles"])
        registry = load_registry(directory)

        self.assertEqual(
            [profile.profile_id for profile in registry.list()], ["general"]
        )
        self.assertIsNone(registry.get("specialist"))
        for revision in (previous, offered):
            self.assertEqual(
                registry.resolve("specialist", revision).revision, revision
            )

    def test_versioned_store_refuses_unsafe_catalogs_and_revisions(self):
        def catalog_entry(directory: Path) -> dict[str, object]:
            document = json.loads(
                (directory / "catalog.json").read_text(encoding="utf-8")
            )
            return document["profiles"]

        def rewrite(directory: Path, profiles: object) -> None:
            self._write_catalog(directory, profiles)

        def misplace(directory: Path) -> None:
            """File one profile's revision under another profile's ID."""
            entry = catalog_entry(directory)["specialist"]
            folder = directory / "revisions" / "other-agent"
            folder.mkdir(mode=0o700)
            self._write(
                folder, profile_document(), f"{entry['revision']}.json"
            )
            rewrite(directory, {"other-agent": entry})

        cases: dict[str, object] = {
            "schema": lambda directory: self._write(
                directory,
                {"schema": "other", "schema_version": 1, "profiles": {}},
                "catalog.json",
            ),
            "unknown-field": lambda directory: rewrite(
                directory,
                {
                    "specialist": {
                        **catalog_entry(directory)["specialist"],
                        "prompt": "synthetic",
                    }
                },
            ),
            "reserved-id": lambda directory: rewrite(
                directory, {"general": catalog_entry(directory)["specialist"]}
            ),
            "revision-not-current": lambda directory: rewrite(
                directory,
                {
                    "specialist": {
                        **catalog_entry(directory)["specialist"],
                        "revision": "0" * 64,
                    }
                },
            ),
            "duplicate-history": lambda directory: rewrite(
                directory,
                {
                    "specialist": {
                        **catalog_entry(directory)["specialist"],
                        "history": [
                            catalog_entry(directory)["specialist"]["revision"]
                        ] * 2,
                    }
                },
            ),
            "unknown-state": lambda directory: rewrite(
                directory,
                {
                    "specialist": {
                        **catalog_entry(directory)["specialist"],
                        "state": "retired",
                    }
                },
            ),
            "missing-revision": lambda directory: (
                directory / "revisions" / "specialist"
                / f"{catalog_entry(directory)['specialist']['revision']}.json"
            ).unlink(),
            "digest-mismatch": lambda directory: self._write(
                directory / "revisions" / "specialist",
                {**profile_document(), "max_turns": 40},
                f"{catalog_entry(directory)['specialist']['revision']}.json",
            ),
            "misplaced-profile": lambda directory: misplace(directory),
            "catalog-mode": lambda directory: (
                directory / "catalog.json"
            ).chmod(0o644),
            "revisions-mode": lambda directory: (
                directory / "revisions"
            ).chmod(0o750),
            "revision-mode": lambda directory: (
                directory / "revisions" / "specialist"
                / f"{catalog_entry(directory)['specialist']['revision']}.json"
            ).chmod(0o604),
        }
        for name, mutate in cases.items():
            with self.subTest(case=name):
                directory = self._write_store(
                    f"store-{name}", {"specialist": [profile_document()]}
                )
                mutate(directory)
                with self.assertRaises(AgentProfileError):
                    load_registry(directory)

        directory = self._write_store(
            "store-linked", {"specialist": [profile_document()]}
        )
        target = directory / "revisions" / "specialist"
        linked = directory / "revisions" / "linked"
        linked.symlink_to(target, target_is_directory=True)
        entry = json.loads(
            (directory / "catalog.json").read_text(encoding="utf-8")
        )["profiles"]["specialist"]
        self._write_catalog(directory, {"linked": entry})
        with self.assertRaises(AgentProfileError):
            load_registry(directory)

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
