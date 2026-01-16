#!/bin/bash
# Show Commands E2E Tests
# Tests all read-only show commands

section "Show Commands Tests"

# Database commands
test_command "show database" "show database" 0 "Database\|database"
test_command "show database by name" "show database --name default" 0

# Collection commands
test_command "show collections" "show collections" 0
test_command "show collection by name" "show collections --name test_collection" 0 "test_collection"
test_optional "show collection by id" "show collections --id 0"

# Session commands
test_command "show session" "show session" 0
test_json_command "show session json" "show session --format json"

# Segment commands
test_command "show segment" "show segment" 0
test_optional "show segment by collection" "show segment --collection 0"
test_optional "show segment format line" "show segment --format line"

# Partition commands
test_command "show partition" "show partition" 0
test_optional "show partition-loaded" "show partition-loaded"

# Index commands
test_command "show index" "show index" 0
test_optional "show segment-index" "show segment-index"

# Checkpoint commands
test_optional "show checkpoint" "show checkpoint --collection 0"

# Channel commands
test_command "show channel-watched" "show channel-watched" 0
test_json_command "show channel-watched json" "show channel-watched --format json"

# Collection loaded
test_optional "show collection-loaded" "show collection-loaded"

# Replica
test_optional "show replica" "show replica"

# Alias
test_command "show alias" "show alias" 0

# Resource group
test_optional "show resource-group" "show resource-group"

# Compaction
test_optional "show compaction" "show compaction"

# User
test_command "show user" "show user" 0
test_json_command "show user json" "show user --format json"

# Config
test_optional "show config-etcd" "show config-etcd"

# Stats task
test_optional "show stats-task" "show stats-task"

# Collection history
test_optional "show collection-history" "show collection-history"

# Bulk insert jobs
test_optional "show bulkinsert" "show bulkinsert"

# Replicate (CDC related)
test_optional "show replicate" "show replicate"

# WAL related commands (may not be available in all versions)
test_optional "show wal-broadcast" "show wal-broadcast"
test_optional "show wal-distribution" "show wal-distribution"

# JSON stats (requires MinIO access)
test_optional "show json-stats" "show json-stats"
test_optional "show loaded-json-stats" "show loaded-json-stats"

# etcd kv tree
test_optional "show etcd-kv-tree" "show etcd-kv-tree"
