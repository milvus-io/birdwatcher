#!/bin/bash
# Remove Commands E2E Tests (DRY RUN ONLY)
# Tests remove commands with --run=false to prevent actual deletions

section "Remove Commands Tests (DRY RUN)"

# All remove commands use --run=false for safety
# These commands should parse and analyze without making changes

test_optional "remove segment dry" "remove segment --run=false"
test_optional "remove binlog dry" "remove binlog --run=false"
test_optional "remove channel dry" "remove channel --run=false"
test_optional "remove session dry" "remove session --run=false"
test_optional "remove bulkinsert dry" "remove bulkinsert --run=false"
test_optional "remove compaction-task dry" "remove compaction-task --run=false"
test_optional "remove etcd-config dry" "remove etcd-config --run=false"
test_optional "remove index dry" "remove index --run=false"
test_optional "remove segment-orphan dry" "remove segment-orphan --run=false"
test_optional "remove stats-task dry" "remove stats-task --run=false"

# Collection cleanup commands
test_optional "remove collection-clean dry" "remove collection-clean --run=false"
test_optional "remove collection-dropping dry" "remove collection-dropping --run=false"
test_optional "remove dirty-importing-segment dry" "remove dirty-importing-segment --run=false"
