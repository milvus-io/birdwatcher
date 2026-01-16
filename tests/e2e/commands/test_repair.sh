#!/bin/bash
# Repair Commands E2E Tests (DRY RUN ONLY)
# Tests repair commands with --run=false to prevent actual modifications

section "Repair Commands Tests (DRY RUN)"

# All repair commands use --run=false for safety
# These commands should parse and analyze without making changes

test_optional "repair segment dry" "repair segment --run=false"
test_optional "repair checkpoint dry" "repair checkpoint --run=false"
test_optional "repair channel dry" "repair channel --run=false"
test_optional "repair channel-watched dry" "repair channel-watched --run=false"
test_optional "repair segment-empty dry" "repair segment-empty --run=false"
test_optional "repair segment-partition-drop dry" "repair segment-partition-drop --run=false"
test_optional "repair collection-info dry" "repair collection-info --run=false"
test_optional "repair collection-legacy dry" "repair collection-legacy --run=false"
test_optional "repair mixed-binlogs dry" "repair mixed-binlogs --run=false"
test_optional "repair manual-compaction dry" "repair manual-compaction --run=false"
test_optional "repair index-metric dry" "repair index-metric --run=false"
test_optional "repair add-index-param dry" "repair add-index-param --run=false"

# WAL related repairs
test_optional "repair wal-broadcast-task dry" "repair wal-broadcast-task --run=false"
test_optional "repair wal-recovery-storage dry" "repair wal-recovery-storage --run=false"
