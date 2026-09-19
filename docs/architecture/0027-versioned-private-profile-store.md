# ADR 0027: Versioned private agent-profile store

## Status

Accepted.

## Context

[ADR 0026](0026-shared-private-agent-profiles.md) placed each private agent in
one flat JSON manifest. That layout has three problems once an agent is
actually tuned.

Replacing a manifest destroys the preceding policy. A workflow that is durably
pinned to the exact revision it started with then fails closed, even though the
only intended change was for future work.

A prompt is long prose, and prose inside a JSON string is unpleasant to edit
and impossible to review as a diff. Several agents also share the same
operating instructions, which a per-profile manifest can only duplicate.

Finally, the flat layout offers no safe way to stop offering an agent. Deleting
its manifest also deletes the history that started work may still need, and
there is no way to prove that nothing needs it.

## Decision

The private store becomes a versioned store with an editable source and a
compiled, immutable result.

An operator edits Markdown fragments and one policy document per profile:
shared instructions used by every agent, role instructions for one agent, and
an optional overlay that narrows an agent to one project or instance.

Publication compiles the shared fragments, the role instructions, the selected
overlays, and the policy fields into exactly one effective manifest. Its
SHA-256 digest over that complete document is the profile revision, so every
behavior-affecting component is covered: changing any fragment, overlay, or
policy field produces a different revision. Because shared instructions are
part of every compiled profile, editing them requires republishing each active
profile.

A catalog names, for each profile, the single revision offered for new
selection, its state, and its ordered revision history. Publication writes the
new revision file first and only then advances the catalog atomically. An
existing revision file is never rewritten; publishing identical content is a
no-op. Disabling a profile removes it from the catalog's active set without
touching its revisions.

The runtime loader reads the catalog, then reads exactly the revision files the
catalog names. Only an active profile's current revision is listed, selectable,
and returned by ordinary lookup. Every other named revision, including every
revision of a disabled profile, resolves exactly for a workflow already pinned
to it. A revision file the catalog does not name is never read; the management
commands report it instead of failing an unrelated run.

Ownership, regular-file, filename, digest, duplicate, conflict, and symlink
checks fail closed. The installed directory the services load must be
owner-only: `0700` directories and `0600` files. The editable source may be
distributed by an approved private synchronization system that does not
preserve permissions, so its mode is diagnosed and reported rather than
enforced; `install` is what produces the owner-only copy the services read.

Permanent deletion is refused unless the profile is already disabled and stored
work proves the profile unused. The proof is the execution databases
themselves: deletion requires at least one database and refuses if any table
carrying an agent profile column still references the profile.

The flat layout migrates explicitly. Migration reads a flat directory, writes
the prompt as a role fragment and the remaining fields as a policy document,
and refuses unless recompiling reproduces the original digest exactly, so
existing pins keep resolving. The flat directory is only read, so reverting
means pointing the services back at it.

Management commands cover initialization, validation, publication, listing,
disabling, enabling, installation, permission diagnostics, deletion, and
migration. Their output carries identifiers, states, revisions, and counts
only: never prompt text, fragment content, or store paths.

An editable store can mirror another editable store only as a fast-forward:
the destination may not name a revision absent from the source. Before the
catalog advances, the mirror copies every revision and each draft input it
references, including shared and overlay fragments. It then validates the
destination. This matters because a revision is compiled from those inputs;
copying catalog history alone could make byte-identical drafts render a
different prompt on the receiving host. Validation reports the shared, role,
and overlay inputs responsible for any remaining unpublished draft. A source
whose own drafts are unpublished is refused before anything is written, so a
store that cannot reproduce itself never half-replaces another one's drafts.

The repository holds the format, the commands, and their tests, plus a visibly
fictional example source store in `examples/agent-profile-store/`. Real
fragments, policies, catalogs, profile identities, and deployment paths stay in
the approved private store.

## Consequences

- A profile can be retuned without stranding work already pinned to its
  previous policy.
- Prompts are reviewable Markdown, and shared instructions exist once.
- Withdrawing an agent from selection is separate from destroying its history,
  and destruction requires evidence.
- Editing shared instructions is a fleet-wide change: every active profile must
  be republished and reinstalled, and each gets a new revision.
- A deployment now has two steps, publish and install, and must reinstall
  before a new revision can be selected.
- A fast-forward mirror reproduces both the immutable revisions and their
  editable inputs; a genuinely divergent history remains an operator
  reconciliation, never an automatic overwrite.

## Failure and rollback

If publication fails part way, the catalog still names the previous revision
and the unreferenced new file is reported by validation; republish or remove
it. If an installed directory is unavailable or inconsistent, the services stop
offering its profiles and refuse to claim workflows bound to them; they never
fall back to another profile. To roll back a revision, disable the profile or
republish the previous fragments, which restores that earlier revision as the
offered one. Do not edit an installed revision file by hand: its digest no
longer matches and the whole store fails closed.
