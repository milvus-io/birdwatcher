#!/bin/bash
# Show Commands E2E Tests
# Tests all read-only show commands

section "Show Commands Tests"

# Database commands
test_command "show database" "show database" 0 "Database|database"
test_command "show database by name (default)" "show database --name default" 0
test_optional "show database by name (test_db)" "show database --name test_db"
test_json_command "show database json" "show database --format json"

# Collection commands
test_command "show collections" "show collections" 0
test_command "show collection by name" "show collections --name test_collection" 0 "test_collection"
test_optional "show collection (simple_collection)" "show collections --name simple_collection"
test_optional "show collection (binary_collection)" "show collections --name binary_collection"
test_json_command "show collections json" "show collections --format json"

# Session commands
test_command "show session" "show session" 0
test_json_command "show session json" "show session --format json"

# Segment commands - with actual data from test_collection
test_command "show segment" "show segment" 0
test_optional "show segment by collection" "show segment --collection test_collection"
test_optional "show segment format line" "show segment --format line"
test_json_command "show segment json" "show segment --format json"

# Partition commands
test_command "show partition" "show partition" 0
test_optional "show partition by collection" "show partition --collection test_collection"
test_optional "show partition-loaded" "show partition-loaded"
# Note: show partition requires --collection flag for JSON output
test_optional "show partition json" "show partition --collection test_collection --format json"

# Index commands
test_command "show index" "show index" 0
test_optional "show segment-index" "show segment-index"
test_json_command "show index json" "show index --format json"

# Checkpoint commands
test_optional "show checkpoint" "show checkpoint"
test_optional "show checkpoint by collection" "show checkpoint --collection test_collection"
# Note: show checkpoint requires --collection flag for JSON output
test_optional "show checkpoint json" "show checkpoint --collection test_collection --format json"

# Channel commands
test_command "show channel-watched" "show channel-watched" 0
# Note: channel-watched may have empty data in test environment
test_optional "show channel-watched json" "show channel-watched --format json"

# Collection loaded - test_collection should be loaded
test_optional "show collection-loaded" "show collection-loaded"
test_json_command "show collection-loaded json" "show collection-loaded --format json"

# Replica - should have data since test_collection is loaded
test_optional "show replica" "show replica"
test_json_command "show replica json" "show replica --format json"

# Alias - test_alias and test_alias_2 were created
test_command "show alias" "show alias" 0
test_json_command "show alias json" "show alias --format json"

# Resource group
test_optional "show resource-group" "show resource-group"

# Compaction - compaction was triggered in test data setup
# Note: command is "show compactions" (plural)
test_optional "show compactions" "show compactions"
# Note: use --ignoreDone=false to include completed compaction tasks
test_json_command "show compactions json" "show compactions --ignoreDone=false --format json"

# User - root user and test_user should exist
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

# Additional show commands for comprehensive coverage
test_optional "show segment-loaded" "show segment-loaded"
test_optional "show configurations" "show configurations"
test_optional "show current-version" "show current-version"
