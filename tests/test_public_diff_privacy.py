"""The public-diff guard catches obvious leaks and permits safe examples.

Ported from the sibling `gw` repository's test of the same guard, and
rewritten as a `unittest.TestCase` (gw's version is a bare function meant to
be run as a standalone script) so it is actually collected by this
repository's test runner: `python3 -m unittest discover` only finds
`TestCase` subclasses, not bare `test_*` functions, and a test that is
silently never collected is worse than no test.

The unsafe fixtures below are built by string concatenation on purpose
(matching the sibling's approach) so that nothing that looks like a real
identifier is ever written as a literal in this file.
"""

import unittest

from tools.check_public_diff import findings


class PublicDiffGuardTests(unittest.TestCase):
    def test_safe_examples_pass(self):
        safe = (
            "+contact: developer@example.com\n"
            "+host: host-a\n"
            "+address: 192.0.2.10\n"
            "+path: /srv/example/data\n"
            "+docs: https://docs.github.com/example\n"
            "+schema: https://json-schema.org/draft/2020-12/schema\n"
        )
        self.assertEqual(findings(safe), [])

    def test_each_mechanical_class_is_caught(self):
        unsafe = "\n".join((
            "+contact: " + "operator" + "@" + "internal.test",
            "+path: /" + "home" + "/operator/private",
            "+address: " + "10" + ".20.30.40",
            "+command: " + "ssh" + " production-node",
            "+source: https://" + "private.example.invalid" + "/records",
        ))
        labels = "\n".join(findings(unsafe))
        self.assertIn("email address", labels)
        self.assertIn("home path", labels)
        self.assertIn("IPv4 address", labels)
        self.assertIn("remote host identifier", labels)
        self.assertIn("URL host", labels)

    def test_configured_private_terms_are_caught(self):
        configured = findings(
            "+owner: Example Private Person\n+school: private-campus.example\n",
            private_terms=("Example Private Person", "private-campus.example"),
        )
        self.assertEqual(len(configured), 2)
        self.assertTrue(
            all("configured private term" in label for label in configured)
        )

    def test_unchanged_context_lines_are_ignored(self):
        diff = "\n".join((
            " context: " + "operator" + "@" + "internal.test",
            "-removed: " + "10" + ".20.30.40",
        ))
        self.assertEqual(findings(diff), [])


if __name__ == "__main__":
    unittest.main()
