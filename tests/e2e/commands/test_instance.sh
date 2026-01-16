#!/bin/bash
# Instance Commands E2E Tests
# Tests instance-level read-only commands

section "Instance Commands Tests"

# Balance explain (read-only analysis)
test_optional "balance-explain" "balance-explain"

# Show log level (read-only)
test_optional "show-log-level" "show-log-level"

# Health check
test_optional "healthzcheck" "healthzcheck"

# List healthz check endpoints
test_optional "list-healthzcheck" "list-healthzcheck"

# List metrics port
test_optional "list-metricsport" "list-metricsport"

# Get configuration (read-only)
test_optional "getconfiguration" "getconfiguration"

# Get distribution (read-only)
test_optional "getdistribution" "getdistribution"

# Probe (read-only analysis)
test_optional "probe" "probe"

# Fetch metrics (read-only)
test_optional "fetch-metrics" "fetch-metrics"

# List events
test_optional "list-events" "listenevents"

# Jemalloc stats (may not be available)
test_optional "jemallocstats" "jemallocstats"

# Check partition key
test_optional "check-partitionkey" "check-partitionkey"
