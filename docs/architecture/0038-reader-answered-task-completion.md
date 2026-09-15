# ADR 0038: A task is closed by a reader answering, never by a detector

Status: proposed.

## Context

Nothing infers that a task is finished any more. The staged scheduler cutover
(ADR 0021) removed the legacy lifecycle closure job, and Foxhound never
replaced it: a task moves only when a reader taps a card, and that card asks
"Task done?" while showing nothing at all about why it is asking.

The engine that was removed did two things at once, and only one of them was
wrong. Measured over a legacy corpus of roughly 1,300 tasks, the silent
high-confidence auto-close fired 21 times, while the path that asked the reader
first accounted for 524 closures. Of the questions the reader answered, about a
third came back "still open".

Both halves of that measurement matter. An inference wrong a third of the time
cannot be allowed to close a task by itself. An inference right two thirds of
the time, over 524 closures, is not something to throw away either — it is most
of how the backlog ever shrank.

There is a second, sharper lesson in how the old engine failed. Reopening a
task cleared the fields recording why it had been closed, and the candidate
query then re-closed it from the same evidence on the next pass. A reader's
"no" was not durable, so the system argued with them.

## Decision

The judgement is restored and the automation is not.

**One: a detection is a question, not a transition.** A detector records
evidence that a task looks finished. Recording it changes no task status. The
closure, when it comes, goes through the ordinary ledger transition and the
ordinary outcome export — the same path a reader takes when they close a task
unprompted. There is no confidence value that shortens this path, because every
value ends at the same question.

**Two: no new card kind.** The question is carried by the task's own review
card. A task with a card already waiting has the question attached to that
card; only a task with no card at all gets one made for it. A reader meets one
card per task, as before, and that card now knows why it is asking.

**Three: the card has to justify itself.** Source, date, the verbatim
quotation, and the stated reason that evidence closes *this* task rather than a
similar one — plus the task's own justifying extract (ADR 0032) where it has
one, so the reader can see both halves of the match. Two controls, Mark as done
and Reopen, and no third: Snooze and Drop answer "is this still live?", and
this card asks "is this finished?".

**Four: identity is the evidence, not the judgement about it.** A record is
keyed by the source and the folded quotation, excluding the reason and the
detector. Two detectors quoting the same sentence are one question. One
detector re-running over an unchanged source is one question.

**Five: the refusal is the durable part.** Answers are recorded against the
evidence, append-only, and a settled record keeps occupying its key. A refused
suggestion therefore cannot be raised again at all — not merely cannot be
raised soon. This is the direct fix for the old engine's argument with the
reader, and it is a consequence of the previous decision rather than a separate
mechanism.

**Six: quality is measurable before it is annoying.** Accept and reject totals
per detector are recoverable as aggregates, with no task, source or quotation
attached. A detector whose rejections dwarf its acceptances is visible as a
ratio rather than as a reputation.

## Consequences

A done-check bypasses the weekly review rhythm. That rhythm exists so an
untouched task is not asked about repeatedly; evidence that the task is
finished is exactly the event it should not delay, and a reader who has just
been told why we think the work is done is not being asked the same question
twice.

A card already claimed, delivered or snoozed is left alone, and its task's
question waits for the next pass. Binding to a card in flight would change
nothing the reader has been shown while marking the question as asked.

A cancelled card releases its question rather than taking it down. Bound to a
card nobody will ever see, it could never be asked again, because the identity
key refuses a second record for the same evidence.

One task carries at most a bounded number of unanswered questions, and one is
offered per scheduling pass. A detector cannot turn a single task into a queue
of cards; past the bound it is refused, which is visible, rather than backing
up, which is not.

Dropping a task settles its question as superseded — neither an acceptance nor
a refusal. The reader discarded the task and never reached the question, and
scoring the detector on that would measure the backlog rather than the matcher.

## Not decided here

What produces detections. This ADR fixes what happens to one once it exists;
finding them means reading newly arrived sources and judging, against an open
task, whether a passage closes it. That judgement needs a facility Foxhound
does not have, and needs it for more than this: deciding whether two tasks are
the same ask is the same kind of question. Until then, the recording interface
is the boundary, and it is deliberately detector-agnostic.
