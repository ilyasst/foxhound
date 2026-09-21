# ADR 0053: Run summaries retired

## Status

Accepted. Supersedes the run-summary decision recorded in the Consequences of
[ADR 0045](0045-automation-grants-in-run-state.md).

## Context

ADR 0045 granted a phase the authority to advance without a reader gate, and
observed that such a run was otherwise entirely silent. It concluded that a
reader told nothing cannot tell an automated phase that advanced from one that
never ran, and left a bounded, controlless run summary card behind each
granted advance.

Everything that ADR promised about the card held. It carried no controls, it
settled on delivery, it was bounded by its own capacity band, and at most one
was active per task. The premise underneath it did not.

A grant is only ever applied to `awaiting_plan` and `awaiting_external`.
`completed`, `declined` and `ineligible` are excluded by `_granted_advance`
and always raise a card. So a granted workflow was never silent: it was going
to report itself at the end, on a card carrying the work, the deliverables and
the controls to act on them. What the summary added was an interim notice that
a phase the reader had already delegated had in fact been delegated.

Measured on one deployment the day the fleet was fully granted: 94 summaries
against 68 cards that wanted an answer. The surface a reader reads became
majority notification, and the argument that a summary "cannot displace a card
that wants an answer" is true only of capacity. It is not true of attention,
which is the scarce thing the ceilings exist to protect.

An informational card is also the wrong shape for the one thing a reader does
want at that moment. [Issue #618](https://github.com/ilyasst/foxhound/issues/618)
recorded it: a reader who disagrees with an auto-advanced conclusion has
nothing to press. The answer to that is not a control on a notification —
it is that the phase which carries the consequence still gates, and that is
where a reader can still say no.

## Decision

Nothing writes a run summary. The insert that followed a granted advance is
gone; the grant, the phase advance and the `phase_granted` event are unchanged.

The `summary_only` column stays. Dropping it would rewrite a table the card
service reads on every claim, to erase a distinction the surviving rows still
need: they carry `kind='result_review'`, and one that lost the marker would
read as a decision card nobody ever answered.

Schema 55 settles the rows already in the table — `pending`, `delivering` and
`delivered` summaries become `cancelled`, with the consumer and claim fields
cleared — and drops the partial index that bounded the summary queue. It does
not retract what was already delivered. Those messages are read history; a
migration that deleted eighty of a reader's messages would be a larger and
less reversible act than the one being asked for.

Delivered summaries are not merely tidied away. A summary settled on delivery
and stayed `delivered` for good, so a capacity band that counted one would
lose that slot permanently; the console's band is two wide, and two leftover
rows would stop every card reaching it. Every read of the card table therefore
continues to exclude `summary_only=1`, in the claim path, in both stats
routes, in each capacity band and in delivery health.

The claim payload no longer carries `informational`. The gateway already
treats that field as optional in both directions, so no ordering against that
repository is required — see [ADR 0043](0043-pinned-release-checkout.md).

## Consequences

A granted advance is now invisible until the work it leads to is reviewed.
That is the intended trade: the reader sees where a task ended up rather than
each phase it passed through, and the terminal card is the one that can be
acted on.

A reader who wants to see delegated phases as they happen has the ledger:
`phase_granted` is still recorded against every one, with its version, phase
and timestamp. Nothing about what happened stopped being observable; only the
unprompted message stopped being sent.

A deployment that wants gates back turns off the grant that skipped them. That
is the honest control, and the only one that changes what a reader is asked.

## Failure and rollback

A release is promoted while runs are in flight, so a build that still emits
summaries can insert one moments before this build starts reading the table.
Such a row is inert rather than harmful: every read excludes it, so no reader
is offered it and no band or count includes it. It also stays `pending`
indefinitely, because the sweep that retires stale cards reads the same
excluded population. That is accepted — an invisible row is cheaper than a
sweep kept alive permanently for a window measured in seconds.

Reverting to a release that predates this one resumes writing summaries, and
the rows this migration cancelled stay cancelled. Nothing needs to be undone
for that to be correct: a cancelled summary reports a run that finished long
ago, and the reader has already seen it or already stopped caring.
