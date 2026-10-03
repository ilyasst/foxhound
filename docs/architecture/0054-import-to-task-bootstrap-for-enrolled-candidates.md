# ADR 0054: Import-to-task bootstrap for enrolled candidates

## Status

Accepted.

## Context

A knowledge producer can enrol forge repositories so their open issues are
published as task candidates in the producer's offline candidate-feed outbox.
The ``foxhound-shadow-cycle`` command imports those candidates into the Foxhound
inbox alongside meeting and email candidates already in the same feed. Issue
candidates use the ``issue`` source kind, which is accepted by the inbox,
the task ledger, and the execution workflow.

Enrolling a repository does not schedule the shadow cycle. Without a scheduled
invocation, candidates sit in the producer outbox and nobody imports them.
[Issue #189](https://github.com/ilyasst/foxhound/issues/189) recorded this gap.

Importing a candidate and creating a task from it are two separate decisions:
import is a one-way, append-only storage operation that requires no external
connection; task creation is an explicit activation decision that may consult
producer state. Maintaining that boundary means enrolled-repository candidates
follow the same import-then-bootstrap path as every other source kind.

## Decision

The ``foxhound-shadow-cycle`` command is the mechanism that imports enrolled-
repository candidates. It is scheduled by the host — not by the enrolment
command — alongside its normal cadence for meeting and email candidates. The
same stream ID carries all source kinds, and the import does not distinguish
between them.

After import, the task bootstrap converts inbox candidates into active tasks.
Two bootstrap paths exist:

1. **Shadow bootstrap** (``foxhound-task-bootstrap``): the legacy path that
   consults producer task state via the GW client. It is the default path for
   meeting, email, and teams candidates, and it is also the path for issue
   candidates from enrolled repositories. It reconciles producer task decisions,
   applies speaker merges, and creates tasks only for candidates the producer
   has not explicitly declined.

2. **Native intake** (``foxhound-native-intake``): the final path that fixes
   the feed cursor at activation time and accepts every later candidate by
   stable identity without consulting producer state. It is a one-way
   activation that permanently disables the legacy bootstrap for that producer.
   Enrolled-repository candidates may also follow this path once native intake
   is activated for the stream.

Both paths are explicit operator decisions. Enrolling a repository produces
candidates, but does not activate either bootstrap. The gap between import and
task creation is deliberate: it lets the operator judge what work to activate
before spending agent time on it.

The shadow cycle distinguishes three outbox states on its own:

- **Empty** (no new pages since the last cursor): a successful run with
  ``unchanged`` disposition, exit code 0, and content-free JSON output.
- **Absent** (outbox directory not yet created): distinct error on stderr,
  exit 1. The producer has not written any pages yet, which is normal
  immediately after enrolment before the first producer cycle runs.
- **Unreadable** (permission or I/O error): distinct error on stderr, exit 1.

These are not failures of the enrolled-repository feature — they are normal
operational states of an offline feed. A deployment monitor should treat them
as expected rather than as alerts.

## Consequences

- Enrolled-repository issue candidates are imported by the same shadow cycle
  that imports meeting and email candidates. There is no separate import path
  for enrolled repositories.

- Enrolling a repository requires the operator to also schedule the shadow
  cycle if it is not already scheduled. The enrolment command does not
  configure scheduling, because scheduling is a host-level deployment concern,
  not a producer concern.

- The ``issue`` source kind is accepted by all three source-policy gates
  (candidates, shadow observations, native intake) and by the execution
  workflow. It does not require special treatment at import or bootstrap.

- The ``issue`` source kind is addressable: cards can render a link to the
  forge issue. This is true for enrolled-repository issues just as it is for
  any other issue candidate.

- Granting ``plan_without_asking`` for the ``issue`` kind in the machine's
  source policy allows enrolled-repository tasks to skip the reader start
  gate and proceed directly to planning. This is the expected configuration
  for a machine that enrolls repositories with the intent of working their
  issues.

## Failure and rollback

The shadow cycle is idempotent and overlap-safe via ``fcntl.flock`` on a lock
file in the database parent directory. A failed cycle between candidate and
observation imports leaves no false receipt; the next cycle replays the
committed candidate prefix and resumes. See
[ADR 0008](0008-shadow-import-cycle.md).

If the producer outbox is absent, the cycle exits with a distinct message
rather than creating the database or touching the inbox. This is normal when
a repository is enrolled but the producer has not yet run.
