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
            "+contact: " + "operator" + "@" + "company" + ".com",
            "+path: /" + "home" + "/operator/private",
            "+address: " + "8.8.8" + ".8",
            "+ $ " + "ssh" + " production-node",
            "+source: https://" + "private.company" + ".com/records",
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

    def test_acceptance_criteria_for_reserved_domains_and_networks(self):
        safe_diff = "\n".join((
            "+contact: alice@example.invalid",
            "+docs: https://private.test/docs",
            "+bind: 0.0.0.0",
            "+private1: 10.1.2.3",
            "+private2: 172.16.0.1",
            "+private3: 192.168.1.100",
            # The remote command regex was tightened to require start of line or prompt,
            # so prose like this is no longer flagged.
            "+prose: Over a non-interactive ssh this reports to the server",
        ))
        self.assertEqual(findings(safe_diff), [])

    def test_command_matching_still_works(self):
        unsafe_diff = "\n".join((
            "+ssh root@production.test",
            "+ $ scp file root@198.51.100.1",
            "+  rsync -avz local/ root@remote-node",
        ))
        labels = "\n".join(findings(unsafe_diff))
        self.assertIn("remote host identifier", labels)
        self.assertEqual(labels.count("remote host identifier"), 3)


if __name__ == "__main__":
    unittest.main()
