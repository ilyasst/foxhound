# ADR 0048: Choose the inference backend per runner, not per machine

## Status

Accepted.

## Decision

An execution runner may declare the inference backend its agents run on, as
`agent_model` and `agent_provider` in that runner's deployment configuration.
The runner passes them to the agent command as arguments, ahead of the
subcommand, for the processes it starts and nothing else.

Both unset is the default and means the agent runtime's configured backend.
Then no argument is added, and nothing here reads or writes that runtime's
configuration — a deployment that declares no selection renders exactly the
command it rendered before these fields existed.

The alternative was to point the agent runtime itself at the wanted backend.
That is rejected. The runtime's configuration is shared by every use of it on
the machine, including interactive sessions and unrelated automation, so it
answers a much larger question than the one being asked. Making one runner slot
use a different model should not change what a person gets when they open that
runtime themselves. A deployment-level selection also survives the thing a
global default cannot: two runners on one machine, deliberately on different
backends.

Three rules keep the selection from failing late:

- A provider without a model is refused when the configuration loads. Carrying
  whatever model the runtime happens to hold across to a different provider is
  a mismatch, and one discovered by the agent runtime costs a claimed workflow
  to learn.
- Each value must be a single argument: no whitespace, no embedded NUL, no
  leading dash. The alternative — folding flags into the free-form
  `agent_command` string, which the runner splits — expresses the same thing
  with none of the validation and no way to see it in the field it belongs to.
- A corrective resume uses the same selection as the turn it is finishing. That
  turn's session is being resumed to close it out; finishing it on another
  backend would hand the accumulated context to a different model for the one
  call that decides what gets recorded.

## Consequences

Moving a slot between backends is one configuration edit and a restart of that
slot, reversible the same way, and visible to anyone reading the deployment
document. A mixed deployment is expressible: slots that share a machine need
not share a model.

The selection is deployment state, so it is not recorded in the run state a
worker reads, and a run does not carry the name of the model that produced it.
Runs from differently configured slots are told apart by their runner slot.

Nothing here validates that the named model or provider exists; that belongs to
the agent runtime, which reports it. A name that the runtime cannot resolve
fails the run rather than the configuration.
