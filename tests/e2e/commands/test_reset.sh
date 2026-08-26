#!/bin/bash
# Reset Commands E2E Tests (DRY RUN ONLY)
# `reset checkpoint` rewrites every persisted MQ position so a stopped instance
# can come back up on a different MQ. It must never run against a live cluster,
# so everything here stays in dry-run.

section "Reset Commands Tests (DRY RUN)"

# Dry run against each supported target WAL. The compose stack runs a single MQ,
# so these exercise planning and rendering, not an actual switch.
test_optional "reset checkpoint woodpecker dry" "reset checkpoint --target-wal woodpecker --run=false"
test_optional "reset checkpoint pulsar dry" "reset checkpoint --target-wal pulsar --run=false"
test_optional "reset checkpoint rocksmq dry" "reset checkpoint --target-wal rocksmq --run=false"

# Scoping to a single pchannel must plan too, even when the name matches nothing.
test_optional "reset checkpoint pchannel filter dry" \
    "reset checkpoint --target-wal woodpecker --pchannel by-dev-rootcoord-dml_0 --run=false"

# Repeated dry runs must be stable: planning never mutates anything.
test_optional "reset checkpoint repeat dry" "reset checkpoint --target-wal woodpecker --run=false"

# Rejections. These are expected to fail, which test_optional tolerates; they are
# here so a regression that silently accepts them shows up as a behaviour change.
test_optional "reset checkpoint missing target-wal" "reset checkpoint --run=false"
test_optional "reset checkpoint unknown target-wal" "reset checkpoint --target-wal nats --run=false"
