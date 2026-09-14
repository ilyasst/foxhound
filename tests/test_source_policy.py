from __future__ import annotations

import unittest

from foxhound.source_policy import (
    SOURCE_POLICIES,
    SourcePolicy,
    planning_grants,
    provenance_roles_for,
    source_kinds_accepting,
)
from foxhound.task_execution import WorkflowStatus, _initial_status


class SourcePolicyTests(unittest.TestCase):
    def test_teams_is_explicit_at_each_candidate_gate(self):
        policy = SOURCE_POLICIES["teams"]

        self.assertTrue(policy.accepts_candidates)
        self.assertTrue(policy.accepts_shadow_observations)
        self.assertTrue(policy.accepts_native_intake)

    def test_a_machine_that_declares_nothing_asks_about_everything(self):
        """Planning authority is no longer decided in this registry. It is
        a judgement about one machine and its operator, so a machine that
        has declared nothing must ask about every task rather than inherit
        a decision made elsewhere.
        """
        nothing = planning_grants(None)
        self.assertEqual(nothing, frozenset())
        for kind in SOURCE_POLICIES:
            with self.subTest(kind=kind):
                self.assertEqual(
                    _initial_status(kind, nothing),
                    WorkflowStatus.AWAITING_START,
                )

    def test_a_machine_grants_exactly_what_it_names(self):
        granted = planning_grants(["issue"])
        self.assertEqual(
            _initial_status("issue", granted), WorkflowStatus.QUEUED)
        for kind in SOURCE_POLICIES:
            if kind == "issue":
                continue
            with self.subTest(kind=kind):
                self.assertEqual(
                    _initial_status(kind, granted),
                    WorkflowStatus.AWAITING_START,
                )

    def test_a_misspelled_grant_is_refused_not_ignored(self):
        # Silently dropping an unrecognised kind would narrow authority
        # without telling anyone; silently accepting it would widen it.
        for bad in (["isue"], ["issue", "nonsense"], "issue", 7):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    planning_grants(bad)

    def test_only_an_addressable_source_is_offered_as_a_link(self):
        """A card states where work came from whichever source it is. Only
        a source whose origin names something openable is offered as a
        link, because a link that does not resolve is worse than a plain
        name the reader can search for.
        """
        # An explicit list on purpose: this is an authority registry, and a
        # new kind should not join it without someone changing this line.
        self.assertEqual(
            source_kinds_accepting("addressable_origin"),
            {"issue", "review_request", "mention"},
        )
        # Forge-shaped origins carry host/owner/name and a number. The rest
        # name something opaque, and are stated rather than linked.
        for kind in ("meeting", "email", "teams", "legacy", "calendar",
                     "alert"):
            with self.subTest(kind=kind):
                self.assertFalse(
                    SOURCE_POLICIES[kind].addressable_origin)

    def test_a_new_source_is_not_addressable_by_default(self):
        # Adding a source is an authority decision. Presentation defaults
        # to the cautious answer so a new kind cannot inherit a link shape
        # that does not fit it simply by being added.
        policy = SourcePolicy(True, True, True, False)
        self.assertFalse(policy.addressable_origin)

    def test_every_declared_source_answers_every_capability(self):
        # The registry is the one place these are decided; a kind missing
        # from it is a kind whose authority nobody reviewed.
        for kind, policy in SOURCE_POLICIES.items():
            with self.subTest(kind=kind):
                for capability in (
                    "accepts_candidates",
                    "accepts_shadow_observations",
                    "accepts_native_intake",
                    "addressable_origin",
                ):
                    self.assertIsInstance(
                        getattr(policy, capability), bool)
                self.assertIsInstance(policy.provenance_roles, frozenset)
                self.assertEqual(
                    provenance_roles_for(kind), policy.provenance_roles
                )
                self.assertTrue(policy.provenance_roles)

    def test_a_declared_kind_grants_no_authority_on_its_own(self):
        """A kind is declared before anything produces it, so the authority
        question is answered while it is still cheap. Declaring one grants
        nothing anywhere: authority is a machine's own decision, and a
        machine that has not made it is asked about every kind.
        """
        nothing = planning_grants(None)
        for kind in SOURCE_POLICIES:
            with self.subTest(kind=kind):
                self.assertEqual(
                    _initial_status(kind, nothing),
                    WorkflowStatus.AWAITING_START,
                )

    def test_unknown_capability_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "unknown source-policy"):
            source_kinds_accepting("unknown")


if __name__ == "__main__":
    unittest.main()
