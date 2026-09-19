#!/usr/bin/env python3
"""Synthetic tests for the versioned private agent-profile store."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import stat
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.agent_profiles import (
    CATALOG_NAME,
    REVISIONS_DIRECTORY,
    WORKER_COMMAND_TOKEN,
    AgentProfileError,
    load_registry,
)
from foxhound.profile_store import (
    DRAFT_SCHEMA,
    DRAFT_SCHEMA_VERSION,
    DRAFTS_DIRECTORY,
    MAX_FRAGMENT_BYTES,
    OVERLAYS_DIRECTORY,
    POLICY_NAME,
    SHARED_DIRECTORY,
    ProfileStoreError,
    compose,
    delete,
    diagnose,
    initialize,
    install,
    list_profiles,
    load_drafts,
    main,
    mirror,
    migrate,
    publish,
    set_state,
    validate,
)


SHARED_TEXT = f"# Shared\nCall `{WORKER_COMMAND_TOKEN} context` first.\n"
ROLE_TEXT = "# Role\nSurvey the Example Org repositories.\n"
OVERLAY_TEXT = "# Overlay\nProject Alpha only.\n"


def policy_document(
    profile_id: str = "example-scout", **changes: object
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema": DRAFT_SCHEMA,
        "schema_version": DRAFT_SCHEMA_VERSION,
        "profile_id": profile_id,
        "shared": ["hermes.md"],
        "role": "role.md",
        "overlays": [],
        "display_name": "Example Scout",
        "runtime": "hermes",
        "toolsets": ["terminal", "file"],
        "max_turns": 50,
        "timeout_seconds": 1_800,
        "claim_lease_seconds": 2_700,
        "heartbeat_seconds": 60,
        "kill_grace_seconds": 30,
        "allowed_phases": ["plan", "execute"],
    }
    document.update(changes)
    return document


def flat_manifest(profile_id: str = "example-scout") -> dict[str, object]:
    return {
        "schema": "foxhound.agent-profile",
        "schema_version": 1,
        "profile_id": profile_id,
        "display_name": "Example Scout",
        "runtime": "hermes",
        "prompt_template": (
            f"# Role\nCall `{WORKER_COMMAND_TOKEN} context` first."
        ),
        "toolsets": ["terminal", "file"],
        "max_turns": 50,
        "timeout_seconds": 1_800,
        "claim_lease_seconds": 2_700,
        "heartbeat_seconds": 60,
        "kill_grace_seconds": 30,
        "allowed_phases": ["plan", "execute"],
    }


class ProfileStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.root.chmod(0o700)
        self.source = self.root / "source"
        self.target = self.root / "installed"
        initialize(self.source)
        self.write_shared("hermes.md", SHARED_TEXT)
        self.write_overlay("example-project.md", OVERLAY_TEXT)
        self.write_draft()

    def write_shared(self, name: str, text: str) -> Path:
        path = self.source / SHARED_DIRECTORY / name
        path.write_text(text, encoding="utf-8")
        return path

    def write_overlay(self, name: str, text: str) -> Path:
        path = self.source / OVERLAYS_DIRECTORY / name
        path.write_text(text, encoding="utf-8")
        return path

    def write_draft(
        self,
        profile_id: str = "example-scout",
        role: str = ROLE_TEXT,
        **changes: object,
    ) -> Path:
        directory = self.source / DRAFTS_DIRECTORY / profile_id
        directory.mkdir(mode=0o700, exist_ok=True)
        (directory / "role.md").write_text(role, encoding="utf-8")
        (directory / POLICY_NAME).write_text(
            json.dumps(policy_document(profile_id, **changes)),
            encoding="utf-8",
        )
        return directory

    def catalog(self) -> dict[str, object]:
        return json.loads(
            (self.source / CATALOG_NAME).read_text(encoding="utf-8")
        )

    def revision_path(self, profile_id: str, revision: str) -> Path:
        return (
            self.source / REVISIONS_DIRECTORY / profile_id / f"{revision}.json"
        )

    def published_revision(self, profile_id: str = "example-scout") -> str:
        entry = self.catalog()["profiles"][profile_id]
        return str(entry["revision"])

    def test_publication_compiles_every_component_into_one_revision(self):
        self.write_draft(overlays=["example-project.md"])

        report = publish(self.source, ["example-scout"])
        revision = report["published"][0]["revision"]
        registry = load_registry(install_into(self, self.target))
        profile = registry.get("example-scout")

        self.assertEqual(profile.revision, revision)
        self.assertEqual(
            profile.prompt_template,
            "\n\n".join(
                (SHARED_TEXT.strip(), ROLE_TEXT.strip(), OVERLAY_TEXT.strip())
            ),
        )
        self.assertEqual(publish(self.source, ["example-scout"])["published"], [])

        digests = {revision}
        for change in ("shared", "role", "overlay", "policy"):
            with self.subTest(change=change):
                if change == "shared":
                    self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")
                elif change == "role":
                    self.write_draft(
                        role=ROLE_TEXT + "Revised.\n",
                        overlays=["example-project.md"],
                    )
                elif change == "overlay":
                    self.write_overlay(
                        "example-project.md", OVERLAY_TEXT + "Revised.\n"
                    )
                else:
                    self.write_draft(
                        role=ROLE_TEXT + "Revised.\n",
                        overlays=["example-project.md"],
                        max_turns=40,
                    )
                published = publish(self.source, ["example-scout"])["published"]
                self.assertEqual(len(published), 1)
                self.assertNotIn(published[0]["revision"], digests)
                digests.add(published[0]["revision"])

    def test_shared_change_republishes_every_active_profile(self):
        self.write_draft("example-clerk", display_name="Example Clerk")
        publish(self.source, ["example-scout", "example-clerk"])
        first = {
            profile_id: self.published_revision(profile_id)
            for profile_id in ("example-scout", "example-clerk")
        }
        set_state(self.source, "example-clerk", "disabled")

        self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")
        report = publish(self.source, all_active=True)

        self.assertEqual(
            [entry["profile_id"] for entry in report["published"]],
            ["example-scout"],
        )
        self.assertNotEqual(
            self.published_revision("example-scout"), first["example-scout"]
        )
        self.assertEqual(
            self.published_revision("example-clerk"), first["example-clerk"]
        )
        for profile_id, revision in first.items():
            self.assertTrue(self.revision_path(profile_id, revision).is_file())

    def test_mirror_copies_changed_shared_fragments_before_validation(self):
        publish(self.source, ["example-scout"])
        replica = self.root / "replica"
        initialize(replica)
        mirror(self.source, replica)

        revised = SHARED_TEXT + "Revised shared guidance.\n"
        self.write_shared("hermes.md", revised)
        publish(self.source, ["example-scout"])

        report = mirror(self.source, replica)

        self.assertEqual(report["pending"], [])
        self.assertEqual(validate(replica)["pending"], [])
        self.assertEqual(
            compose_prompt(self.source), compose_prompt(replica)
        )

    def test_mirror_refuses_an_unpublished_source_before_writing(self):
        publish(self.source, ["example-scout"])
        replica = self.root / "replica"
        initialize(replica)
        mirror(self.source, replica)
        mirrored = (replica / SHARED_DIRECTORY / "hermes.md").read_text(
            encoding="utf-8"
        )
        self.write_shared("hermes.md", SHARED_TEXT + "Unpublished edit.\n")

        with self.assertRaises(ProfileStoreError):
            mirror(self.source, replica)

        self.assertEqual(
            (replica / SHARED_DIRECTORY / "hermes.md").read_text(
                encoding="utf-8"
            ),
            mirrored,
        )
        self.assertEqual(validate(replica)["pending"], [])

    def test_mirror_refuses_a_genuinely_divergent_history(self):
        publish(self.source, ["example-scout"])
        replica = self.root / "replica"
        initialize(replica)
        mirror(self.source, replica)
        (replica / SHARED_DIRECTORY / "hermes.md").write_text(
            SHARED_TEXT + "Replica-only guidance.\n", encoding="utf-8"
        )
        publish(replica, ["example-scout"])
        self.write_shared("hermes.md", SHARED_TEXT + "Source-only guidance.\n")
        publish(self.source, ["example-scout"])

        with self.assertRaises(ProfileStoreError):
            mirror(self.source, replica)

    def test_publication_preserves_the_preceding_revision_exactly(self):
        publish(self.source, ["example-scout"])
        first = self.published_revision()
        first_bytes = self.revision_path("example-scout", first).read_bytes()

        self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")
        publish(self.source, ["example-scout"])
        second = self.published_revision()

        self.assertNotEqual(first, second)
        self.assertEqual(
            self.revision_path("example-scout", first).read_bytes(), first_bytes
        )
        self.assertEqual(
            self.catalog()["profiles"]["example-scout"]["history"],
            [first, second],
        )
        registry = load_registry(install_into(self, self.target))
        self.assertEqual(registry.get("example-scout").revision, second)
        self.assertEqual(
            registry.resolve("example-scout", first).revision, first
        )
        with self.assertRaises(AgentProfileError):
            registry.resolve_current("example-scout", first)

    def test_republishing_an_earlier_revision_offers_it_again(self):
        publish(self.source, ["example-scout"])
        first = self.published_revision()
        first_bytes = self.revision_path("example-scout", first).read_bytes()
        self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")
        publish(self.source, ["example-scout"])
        second = self.published_revision()

        self.write_shared("hermes.md", SHARED_TEXT)
        report = publish(self.source, ["example-scout"])

        self.assertEqual(report["published"][0]["revision"], first)
        self.assertEqual(self.published_revision(), first)
        self.assertEqual(
            self.catalog()["profiles"]["example-scout"]["history"],
            [second, first],
        )
        self.assertEqual(
            self.revision_path("example-scout", first).read_bytes(), first_bytes
        )

    def test_catalog_only_advances_after_the_revision_is_durable(self):
        publish(self.source, ["example-scout"])
        first = self.published_revision()
        self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")

        with mock.patch(
            "foxhound.profile_store._write_catalog",
            side_effect=ProfileStoreError("agent profile store is unavailable"),
        ):
            with self.assertRaises(ProfileStoreError):
                publish(self.source, ["example-scout"])

        self.assertEqual(self.published_revision(), first)
        self.assertEqual(validate(self.source)["unpublished_files"], 1)
        registry = load_registry(install_into(self, self.target))
        self.assertEqual(registry.get("example-scout").revision, first)

    def test_disabled_profile_is_unselectable_but_still_resolves(self):
        publish(self.source, ["example-scout"])
        revision = self.published_revision()
        installed = install_into(self, self.target)

        self.assertEqual(
            set_state(self.source, "example-scout", "disabled")["changed"], True
        )
        install(self.source, self.target)
        registry = load_registry(installed)

        self.assertEqual(
            [profile.profile_id for profile in registry.list()], ["general"]
        )
        self.assertIsNone(registry.get("example-scout"))
        self.assertEqual(
            registry.resolve("example-scout", revision).revision, revision
        )
        with self.assertRaises(AgentProfileError):
            registry.resolve_current("example-scout", revision)

        self.assertEqual(
            set_state(self.source, "example-scout", "active")["changed"], True
        )
        install(self.source, self.target)
        self.assertEqual(
            load_registry(installed).get("example-scout").revision, revision
        )

    def test_installation_is_owner_only_idempotent_and_conflict_safe(self):
        publish(self.source, ["example-scout"])
        revision = self.published_revision()

        first = install(self.source, self.target)
        second = install(self.source, self.target)

        self.assertEqual(first["revisions_copied"], 1)
        self.assertEqual(second["revisions_copied"], 0)
        installed_revision = (
            self.target / REVISIONS_DIRECTORY / "example-scout"
            / f"{revision}.json"
        )
        for path, mode in (
            (self.target, 0o700),
            (self.target / REVISIONS_DIRECTORY, 0o700),
            (self.target / CATALOG_NAME, 0o600),
            (installed_revision, 0o600),
        ):
            with self.subTest(mode=mode):
                self.assertEqual(stat.S_IMODE(path.lstat().st_mode), mode)

        document = json.loads(installed_revision.read_text(encoding="utf-8"))
        document["max_turns"] = 12
        installed_revision.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(ProfileStoreError):
            install(self.source, self.target)
        with self.assertRaises(AgentProfileError):
            load_registry(self.target)

    def test_unreferenced_revision_files_are_reported_not_loaded(self):
        publish(self.source, ["example-scout"])
        installed = install_into(self, self.target)
        stray = (
            installed / REVISIONS_DIRECTORY / "example-scout"
            / f"{'a' * 64}.json"
        )
        stray.write_text("{}", encoding="utf-8")
        stray.chmod(0o600)

        registry = load_registry(installed)

        self.assertEqual(registry.get("example-scout").revision,
                         self.published_revision())
        self.assertEqual(
            diagnose(self.source, self.target)["target"]["unreferenced_files"], 1
        )

    def test_deletion_requires_proof_that_no_stored_work_uses_the_profile(self):
        publish(self.source, ["example-scout"])
        revision = self.published_revision()
        database = self.root / "evidence.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute(
            "CREATE TABLE task_execution_workflows (task_id INTEGER, "
            "agent_profile_id TEXT, agent_profile_revision TEXT)"
        )
        connection.execute(
            "INSERT INTO task_execution_workflows VALUES (1, ?, ?)",
            ("example-scout", revision),
        )
        connection.commit()
        connection.close()

        with self.assertRaises(ProfileStoreError):
            delete(self.source, "example-scout", [database])
        set_state(self.source, "example-scout", "disabled")
        with self.assertRaises(ProfileStoreError):
            delete(self.source, "example-scout", [])
        with self.assertRaises(ProfileStoreError):
            delete(self.source, "example-scout", [database])
        with self.assertRaises(ProfileStoreError):
            delete(self.source, "example-scout", [self.root / "absent.sqlite3"])

        connection = sqlite3.connect(database)
        connection.execute("DELETE FROM task_execution_workflows")
        connection.commit()
        connection.close()
        report = delete(self.source, "example-scout", [database])

        self.assertEqual(report["revisions_removed"], 1)
        self.assertTrue(report["draft_retained"])
        self.assertEqual(self.catalog()["profiles"], {})
        self.assertFalse(self.revision_path("example-scout", revision).exists())
        self.assertTrue(
            (self.source / DRAFTS_DIRECTORY / "example-scout" / POLICY_NAME)
            .is_file()
        )

    def test_flat_migration_preserves_every_pinned_revision(self):
        flat = self.root / "flat"
        flat.mkdir(mode=0o700)
        path = flat / "example-clerk.json"
        path.write_text(
            json.dumps(flat_manifest("example-clerk")), encoding="utf-8"
        )
        path.chmod(0o600)
        pinned = load_registry(flat).get("example-clerk").revision

        preview = migrate(flat, self.source, dry_run=True)
        self.assertFalse(preview["applied"])
        self.assertEqual(self.catalog()["profiles"], {})

        report = migrate(flat, self.source)
        installed = install_into(self, self.target)

        self.assertTrue(report["applied"])
        self.assertTrue(report["source_retained"])
        self.assertTrue(path.is_file())
        self.assertEqual(report["migrated"][0]["revision"], pinned)
        self.assertEqual(
            load_registry(installed).resolve_current("example-clerk", pinned)
            .revision,
            pinned,
        )
        self.assertNotIn("example-clerk", validate(self.source)["pending"])
        with self.assertRaises(ProfileStoreError):
            migrate(flat, self.source)

        drafted = self.root / "flat-drafted"
        drafted.mkdir(mode=0o700)
        path = drafted / "example-scout.json"
        path.write_text(json.dumps(flat_manifest()), encoding="utf-8")
        path.chmod(0o600)
        with self.assertRaises(ProfileStoreError):
            migrate(drafted, self.source)

    def test_migration_refuses_a_prompt_it_cannot_reproduce(self):
        flat = self.root / "flat-spaced"
        flat.mkdir(mode=0o700)
        document = flat_manifest("example-clerk")
        document["prompt_template"] = f"\n{document['prompt_template']}\n"
        path = flat / "example-clerk.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        path.chmod(0o600)

        with self.assertRaises(ProfileStoreError):
            migrate(flat, self.source)
        self.assertEqual(self.catalog()["profiles"], {})

    def test_migration_writes_nothing_when_one_manifest_is_refused(self):
        flat = self.root / "flat-mixed"
        flat.mkdir(mode=0o700)
        for profile_id, prompt in (
            ("example-clerk", None),
            ("example-courier", "  indented and unreproducible  "),
        ):
            document = flat_manifest(profile_id)
            if prompt is not None:
                document["prompt_template"] = (
                    f"{prompt}{WORKER_COMMAND_TOKEN} context"
                )
            path = flat / f"{profile_id}.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            path.chmod(0o600)

        with self.assertRaises(ProfileStoreError):
            migrate(flat, self.source)

        self.assertEqual(self.catalog()["profiles"], {})
        self.assertFalse(
            (self.source / DRAFTS_DIRECTORY / "example-clerk").exists()
        )
        self.assertEqual(validate(self.source)["unpublished_files"], 0)

    def test_validation_reports_drafts_that_are_not_published(self):
        publish(self.source, ["example-scout"])
        self.write_draft("example-clerk", display_name="Example Clerk")
        self.write_shared("hermes.md", SHARED_TEXT + "Revised.\n")

        report = validate(self.source)

        self.assertEqual(report["profiles"], 1)
        self.assertEqual(report["drafts"], 2)
        self.assertEqual(report["pending"], ["example-clerk", "example-scout"])
        self.assertEqual(
            report["pending_inputs"]["example-scout"]["shared"], ["hermes.md"]
        )
        self.assertEqual(report["missing_drafts"], [])
        self.assertEqual(
            list_profiles(self.source)["profiles"][0]["profile_id"],
            "example-scout",
        )

    def test_diagnostics_count_permissive_entries_without_naming_them(self):
        publish(self.source, ["example-scout"])
        install_into(self, self.target)
        (self.source / CATALOG_NAME).chmod(0o644)

        report = diagnose(self.source, self.target)

        self.assertEqual(report["source"]["permissive_entries"], 1)
        self.assertEqual(report["target"]["permissive_entries"], 0)
        self.assertTrue(report["target"]["current"])

        (self.target / CATALOG_NAME).chmod(0o644)
        self.target.chmod(0o755)
        report = diagnose(self.source, self.target)

        self.assertEqual(report["target"]["permissive_entries"], 2)
        with self.assertRaises(AgentProfileError):
            load_registry(self.target)
        self.assertNotIn(
            str(self.source), json.dumps(report, sort_keys=True)
        )

    def test_draft_shapes_fail_closed(self):
        variants: list[tuple[str, dict[str, object]]] = [
            ("unknown-field", {"model": "synthetic"}),
            ("reserved-id", {"profile_id": "general"}),
            ("traversal", {"role": "../role.md"}),
            ("absolute", {"role": "/role.md"}),
            ("extension", {"role": "role.txt"}),
            ("duplicate-fragment", {"shared": ["hermes.md", "hermes.md"]}),
            ("unknown-toolset", {"toolsets": ["terminal", "network"]}),
            ("schema-version", {"schema_version": 2}),
            ("unsafe-timing", {"claim_lease_seconds": 300}),
            (
                "fragment-count",
                {"shared": [f"part-{index}.md" for index in range(9)]},
            ),
        ]
        for name, changes in variants:
            with self.subTest(case=name):
                self.write_draft(**changes)
                with self.assertRaises(AgentProfileError):
                    publish(self.source, ["example-scout"])
        self.write_draft()

        missing = policy_document()
        missing.pop("role")
        directory = self.source / DRAFTS_DIRECTORY / "example-scout"
        (directory / POLICY_NAME).write_text(
            json.dumps(missing), encoding="utf-8"
        )
        with self.assertRaises(AgentProfileError):
            publish(self.source, ["example-scout"])

    def test_fragment_content_and_links_fail_closed(self):
        outside = self.root / "outside.md"
        outside.write_text(ROLE_TEXT, encoding="utf-8")
        cases = {
            "symlink": None,
            "carriage-return": ROLE_TEXT.replace("\n", "\r\n"),
            "null": "# Role\x00\n",
            "oversize": "a" * (MAX_FRAGMENT_BYTES + 1),
            "empty-prompt": "",
        }
        for name, text in cases.items():
            with self.subTest(case=name):
                path = (
                    self.source / DRAFTS_DIRECTORY / "example-scout"
                    / "role.md"
                )
                path.unlink()
                if text is None:
                    path.symlink_to(outside)
                else:
                    path.write_text(text, encoding="utf-8")
                with self.assertRaises(AgentProfileError):
                    publish(self.source, ["example-scout"])
                path.unlink()
                path.write_text(ROLE_TEXT, encoding="utf-8")

    def test_publication_selection_is_explicit(self):
        for selected in ([], ["absent-profile"]):
            with self.subTest(selected=selected):
                with self.assertRaises(ProfileStoreError):
                    publish(self.source, selected)
        with self.assertRaises(ProfileStoreError):
            publish(self.source, ["example-scout"], all_active=True)
        with self.assertRaises(ProfileStoreError):
            publish(self.source, all_active=True)
        with self.assertRaises(ProfileStoreError):
            set_state(self.source, "example-scout", "disabled")

    def test_command_output_is_content_free_for_success_and_failure(self):
        self.write_draft(role=f"{ROLE_TEXT}synthetic-private-prompt-value\n")
        publish(self.source, ["example-scout"])
        commands = (
            ["--source", str(self.source), "validate"],
            ["--source", str(self.source), "list"],
            ["--source", str(self.source), "publish", "--all-active"],
            ["--source", str(self.source), "install", "--target",
             str(self.target)],
            ["--source", str(self.source), "doctor", "--target",
             str(self.target)],
            ["--source", str(self.source), "disable", "--profile",
             "example-scout"],
            ["--source", str(self.source), "enable", "--profile",
             "example-scout"],
        )
        for arguments in commands:
            with self.subTest(command=arguments[2]):
                rendered = self.capture(arguments, expected=0)
                self.assertNotIn("synthetic-private-prompt-value", rendered)
                self.assertNotIn(str(self.source), rendered)
                self.assertNotIn("Example Scout", rendered)

        self.write_draft(role="synthetic-private-prompt-value\r\n")
        rendered = self.capture(
            ["--source", str(self.source), "publish", "--all-active"],
            expected=os.EX_CONFIG,
        )
        self.assertNotIn("synthetic-private-prompt-value", rendered)
        self.assertNotIn(str(self.source), rendered)
        self.assertIn("prompt fragment is invalid", rendered)

        for target, error in (
            ("foxhound.profile_store._load_catalog", OSError("private path")),
            ("foxhound.profile_store._store_root", OSError("private path")),
        ):
            with self.subTest(target=target):
                with mock.patch(target, side_effect=error):
                    rendered = self.capture(
                        ["--source", str(self.source), "list"],
                        expected=os.EX_CONFIG,
                    )
                self.assertNotIn("private path", rendered)
                self.assertNotIn(str(self.source), rendered)

    def test_public_example_store_publishes_a_valid_profile(self):
        example = Path(__file__).resolve().parent.parent / "examples"
        source = self.root / "example-store"
        shutil.copytree(example / "agent-profile-store", source)
        initialize(source)

        report = publish(source, ["example-scout"])
        installed = self.root / "example-installed"
        install(source, installed)
        profile = load_registry(installed).get("example-scout")

        self.assertEqual(
            profile.revision, report["published"][0]["revision"]
        )
        self.assertEqual(profile.display_name, "Example Scout")
        self.assertEqual(profile.allowed_phases, ("plan",))
        self.assertIn("Example Scout", profile.prompt_template)
        self.assertIn("Project Alpha", profile.prompt_template)
        self.assertEqual(validate(source)["pending"], [])

    def capture(self, arguments: list[str], *, expected: int) -> str:
        output = StringIO()
        errors = StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(arguments)
        self.assertEqual(code, expected)
        return output.getvalue() + errors.getvalue()


def install_into(test: ProfileStoreTests, target: Path) -> Path:
    install(test.source, target)
    return target


def compose_prompt(source: Path) -> str:
    draft = load_drafts(source)["example-scout"]
    return compose(source, draft).prompt_template


if __name__ == "__main__":
    unittest.main()
