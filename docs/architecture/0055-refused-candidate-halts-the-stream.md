# ADR 0055: A refused candidate halts the stream, and says why

Status: accepted.

## Context

A candidate-feed page is applied atomically: the first refused candidate rolls
the page back and nothing is applied. Because pages are applied in cursor
order, the stream then stops at that page and every later page waits behind it.

In production this turned a one-line producer defect into a multi-hour outage.
A producer bug (ilyasst/gw#1196) exported revised candidates at lifecycle
generation 1, the inbox refused them as generation conflicts, and the cursor
sat at one page while the producer went on writing pages that could never be
imported. Two consumer-side properties made that far worse than the fault
warranted, and both are decided here.

The refusal itself was correct. A new revision arriving at a generation the
inbox already holds is exactly the signal that a producer has stopped
advancing its counter, and the inbox has no other way to notice.

## Decision

### 1. The generation check stays strict

A candidate arriving at the schema version already stored, with fields added
but the generation unmoved, is **not** excused as a cumulative contract
upgrade. `_is_cumulative_contract_upgrade` continues to require
`incoming_version > current_version`.

Advancing the generation is the producer's responsibility. This refusal is the
only signal the inbox has that a producer has stopped doing so; excusing it
would have swallowed ilyasst/gw#1196 rather than surfacing it, and a future
recurrence would pass silently instead of stopping.

A consequence is that the upgrade path is unreachable for whichever cumulative
version is currently the newest — which is the version every candidate
converges on. That is accepted, not overlooked. It is pinned by
`test_upgrade_path_is_reachable_for_every_non_newest_version`, which fails when
a newer cumulative version is added, so that whoever adds one re-reads this
decision and confirms the additive `shared_shape` normalization covers the
fields the new version introduces.

### 2. A refusal still halts the stream, and is diagnosable

The page stays atomic and the stream still stops. A refused candidate is **not**
skipped, and is **not** applied partially.

The alternative — recording the refusal and applying the rest of the page —
lets the inbox's state diverge from the producer's ledger, with nothing
responsible for reconciling it later. A loud stop is preferable to silent
divergence in a boundary whose purpose is that the two sides agree.

What was unacceptable was not the stop but that it was undiagnosable. The
per-candidate `ImportRefusal` was computed and then discarded at four
successive layers, so the CLI reported only `candidate feed import failed`, and
recovering the actual reason meant calling `_apply_candidate` by hand in a REPL
against a copy of the database. The reason and the offending `candidate_id` now
travel to the CLI. Both stay content-free: the reason is a fixed vocabulary and
the identifier is an opaque digest.

## Quarantine procedure

Because a refusal halts the stream, an operator needs a way to return the
outbox to a consistent state when a page can never be imported — for example a
page whose candidates carry a generation that the inbox already holds, which no
later export can repair.

Work on copies first. Foxhound refuses a world-readable parent directory, so
the copy needs mode `0700`.

1. **Read the reason.** Run the import and note the reported refusal and
   `candidate_id`:

   ```
   foxhound-candidate-feed-import \
     --outbox <outbox> --database <database> --stream-id <stream>
   ```

   It names the refusal and the record, for example
   `generation_conflict (candidate tc_...)`.

2. **Confirm it is unrepairable.** A page that merely arrived early is fixed by
   correcting the producer and re-exporting. Quarantine only a page whose
   contents can never be accepted as written.

3. **Stop the producer.** Stop the timer that runs the export, so the outbox
   does not grow while it is being repaired.

4. **Move the unimportable pages out of the outbox**, keeping them — they are
   the record of what happened:

   ```
   mv <outbox>/page-<from>-<to>.json <quarantine>/
   ```

   Move a contiguous suffix, so the outbox ends exactly at the inbox's cursor.
   The import validates that page history is contiguous and that filenames
   match their cursor range; a gap in the middle is refused.

5. **Verify consistency before resuming.** Re-run the import. It must report
   `disposition: unchanged` with every remaining page replayed, and the cursor
   unmoved.

6. **Fix the producer, then restart the timer.** Restarting before the producer
   is fixed re-exports the same records and stops the stream again.

Quarantined pages are permanent. They carry the generation they were written
with, and re-adding them re-creates the refusal.

## Consequences

- A producer that stops advancing generations is still detected, at the cost of
  stopping intake until it is fixed.
- The cost of that stop is now minutes of reading a message rather than hours
  of tracing internals.
- Adding a cumulative schema version requires revisiting this ADR; a test
  enforces that.
- Quarantine remains a manual, deliberate operator action. It is not automatic,
  because discarding a record the producer still believes it delivered should
  not happen without someone deciding to.
