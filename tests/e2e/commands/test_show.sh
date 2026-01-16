#!/bin/bash
# Show Commands E2E Tests
# Tests all read-only show commands with JSON output

section "Show Commands Tests (JSON Output)"

# Database commands
test_json_command "show database" "show database --format json"

# Collection commands
test_json_command "show collections" "show collections --format json"

# Session commands
test_json_command "show session" "show session --format json"

# Segment commands
test_json_command "show segment" "show segment --format json"

# Partition commands
# Note: show partition requires collection ID, partition-loaded works without it
test_json_command "show partition-loaded" "show partition-loaded --format json"

# Index commands
test_json_command "show index" "show index --format json"
test_json_command "show segment-index" "show segment-index --format json"

# Checkpoint commands
# Note: checkpoint requires --collection flag, use test_optional for now
test_optional "show checkpoint json" "show checkpoint --format json"

# Channel commands
test_json_command "show channel-watched" "show channel-watched --format json"

# Collection loaded
test_json_command "show collection-loaded" "show collection-loaded --format json"

# Replica
test_json_command "show replica" "show replica --format json"

# Alias
test_json_command "show alias" "show alias --format json"

# Resource group
test_json_command "show resource-group" "show resource-group --format json"

# Compaction
# Note: use --ignoreDone=false to include completed compaction tasks
test_json_command "show compactions" "show compactions --ignoreDone=false --format json"

# User
test_json_command "show user" "show user --format json"

# Stats task
test_json_command "show stats-task" "show stats-task --format json"

# Bulk insert jobs
test_json_command "show bulkinsert" "show bulkinsert --format json"

# Commands that may not support JSON yet or require special setup
test_optional "show config-etcd" "show config-etcd --format json"
test_optional "show collection-history" "show collection-history --format json"
test_optional "show replicate" "show replicate --format json"
test_optional "show wal-broadcast" "show wal-broadcast --format json"
test_optional "show wal-distribution" "show wal-distribution --format json"
test_optional "show json-stats" "show json-stats --format json"
test_optional "show loaded-json-stats" "show loaded-json-stats --format json"
test_optional "show etcd-kv-tree" "show etcd-kv-tree --format json"
test_optional "show segment-loaded" "show segment-loaded --format json"
test_optional "show configurations" "show configurations --format json"
test_optional "show current-version" "show current-version --format json"
