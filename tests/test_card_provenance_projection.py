"""The bounds on projected provenance, where the bounds are written."""

from __future__ import annotations

import unittest

from foxhound.card_provenance import (
    MAX_CARD_EXTRACT_CHARS,
    MAX_PROJECTED_SOURCE_NAME,
    MAX_PROJECTED_SOURCES,
    CardSourceEvidence,
    origin_kind_subquery,
    origin_payload_subquery,
    provenance_document,
)


def source(role: str = "message", name: str = "synthetic", extract: str = "x"):
    return CardSourceEvidence(name=name, role=role, extract=extract)


class ProvenanceProjectionTests(unittest.TestCase):
    def test_nothing_recorded_is_absent_rather_than_an_empty_object(self) -> None:
        """A reader is told "no origin recorded" by the section not rendering.

        An object with an empty kind and no sources reads as an origin that
        exists and is blank, which is a different claim.
        """
        self.assertIsNone(provenance_document("", ()))
        self.assertIsNone(provenance_document(None, ()))

    def test_a_kind_with_no_evidence_still_says_where_it_came_from(self) -> None:
        document = provenance_document("email", ())
        self.assertEqual(document, {"kind": "email", "sources": []})

    def test_each_field_is_bounded_where_it_is_serialized(self) -> None:
        long = source(
            role="r" * (MAX_PROJECTED_SOURCE_NAME + 50),
            name="n" * (MAX_PROJECTED_SOURCE_NAME + 50),
            extract="e" * (MAX_CARD_EXTRACT_CHARS + 500),
        )
        document = provenance_document("k" * 500, [long])
        self.assertEqual(len(document["kind"]), MAX_PROJECTED_SOURCE_NAME)
        shown = document["sources"][0]
        self.assertEqual(len(shown["role"]), MAX_PROJECTED_SOURCE_NAME)
        self.assertEqual(len(shown["name"]), MAX_PROJECTED_SOURCE_NAME)
        self.assertEqual(len(shown["extract"]), MAX_CARD_EXTRACT_CHARS)

    def test_the_number_of_sources_is_bounded(self) -> None:
        many = [source(role=f"role-{index}") for index in range(40)]
        document = provenance_document("meeting", many)
        self.assertEqual(len(document["sources"]), MAX_PROJECTED_SOURCES)
        # The bound keeps the first sources, so the order a producer recorded
        # is the order a reader sees.
        self.assertEqual(document["sources"][0]["role"], "role-0")

    def test_a_multi_line_extract_survives(self) -> None:
        """The property the regular expressions it replaces did not have."""
        document = provenance_document(
            "meeting", [source(extract="first line\nsecond line")]
        )
        self.assertEqual(
            document["sources"][0]["extract"], "first line\nsecond line"
        )

    def test_one_definition_of_each_subquery_formatted_per_caller(self) -> None:
        """Two copies of one fragment is a copy that stops agreeing."""
        for build in (origin_kind_subquery, origin_payload_subquery):
            card, workflow = build("c"), build("w")
            self.assertIn("b.task_id=c.task_id", card)
            self.assertIn("b.task_id=w.task_id", workflow)
            self.assertEqual(
                card.replace("c.task_id", "TASK"),
                workflow.replace("w.task_id", "TASK"),
            )


if __name__ == "__main__":
    unittest.main()
