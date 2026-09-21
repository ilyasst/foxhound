#!/bin/sh
set -e

HOOK_PATH=$(git rev-parse --git-path hooks/reference-transaction)
mkdir -p "$(dirname "$HOOK_PATH")"

if [ -f "$HOOK_PATH" ] && grep -q "Agent Guard" "$HOOK_PATH"; then
    echo "Agent Guard is already installed."
    exit 0
fi

GUARD_BLOCK='
# Agent Guard
# Protects against unattended agent commits, branch switches, and resets.
if [ -n "$HERMES_CRON_SESSION" ] || [ -n "$HERMES_SESSION_SOURCE" ] || [ -n "$FOXHOUND_WORKFLOW_STATE" ]; then
    if [ "$1" = "prepared" ]; then
        REJECTION_MSG="[Agent Guard] Ref updates in this shared checkout are blocked for unattended agents.\nUse the sanctioned '\''act worktree'\'' alternative instead."
        printf "%b\n" "$REJECTION_MSG" >&2
        if [ -n "$TERMINAL_CWD" ]; then
            printf "%b\n" "$REJECTION_MSG" >> "$TERMINAL_CWD/agent-guard-rejection.log"
        fi
        exit 1
    fi
fi'

if [ -f "$HOOK_PATH" ]; then
    first_line=$(head -n 1 "$HOOK_PATH")
    rest=$(tail -n +2 "$HOOK_PATH")
    printf "%s\n%s\n%s\n" "$first_line" "$GUARD_BLOCK" "$rest" > "$HOOK_PATH"
else
    printf "#!/bin/sh\n%s\n" "$GUARD_BLOCK" > "$HOOK_PATH"
fi

chmod +x "$HOOK_PATH"
echo "Agent Guard installed."
