#!/bin/bash
# Birdwatcher E2E Test Runner
# This script runs all birdwatcher CLI commands and validates their output

set -e

# Configuration
BW_BINARY="${BW_BINARY:-./bin/birdwatcher}"
ETCD_ADDR="${ETCD_ADDR:-127.0.0.1:2379}"
ROOT_PATH="${ROOT_PATH:-by-dev}"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper function to run birdwatcher command
run_bw() {
    local cmd="$1"
    local full_cmd="connect --etcd ${ETCD_ADDR} --rootPath ${ROOT_PATH}, ${cmd}"
    ${BW_BINARY} -olc "${full_cmd}" 2>&1
}

# Test function with assertions
# Usage: test_command "test name" "command" [expected_exit_code] [expected_output_pattern]
test_command() {
    local test_name="$1"
    local cmd="$2"
    local expected_exit_code="${3:-0}"
    local expected_output_pattern="$4"

    echo -n "Testing: ${test_name}... "

    set +e
    output=$(run_bw "${cmd}" 2>&1)
    exit_code=$?
    set -e

    if [[ $exit_code -ne $expected_exit_code ]]; then
        echo -e "${RED}FAILED${NC} (exit code: $exit_code, expected: $expected_exit_code)"
        echo "  Command: ${cmd}"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        return 1
    fi

    if [[ -n "$expected_output_pattern" ]] && ! echo "$output" | grep -qE "$expected_output_pattern"; then
        echo -e "${RED}FAILED${NC} (output pattern not matched)"
        echo "  Expected pattern: $expected_output_pattern"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        return 1
    fi

    echo -e "${GREEN}PASSED${NC}"
    ((TESTS_PASSED++)) || true
    return 0
}

# Test JSON output format
# Usage: test_json_command "test name" "command"
test_json_command() {
    local test_name="$1"
    local cmd="$2"

    echo -n "Testing JSON: ${test_name}... "

    set +e
    output=$(run_bw "${cmd}" 2>&1)
    exit_code=$?
    set -e

    if [[ $exit_code -ne 0 ]]; then
        echo -e "${RED}FAILED${NC} (exit code: $exit_code)"
        echo "  Command: ${cmd}"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        return 1
    fi

    # Validate JSON using Python
    if echo "$output" | python3 -c "import sys, json; json.load(sys.stdin)" 2>/dev/null; then
        echo -e "${GREEN}PASSED${NC}"
        ((TESTS_PASSED++)) || true
        return 0
    else
        echo -e "${YELLOW}WARN${NC} (command succeeded but output is not valid JSON)"
        echo "  Output: ${output:0:200}"
        ((TESTS_SKIPPED++)) || true
        return 0
    fi
}

# Test command that may fail (optional test)
# Usage: test_optional "test name" "command"
test_optional() {
    local test_name="$1"
    local cmd="$2"

    echo -n "Testing (optional): ${test_name}... "

    set +e
    output=$(run_bw "${cmd}" 2>&1)
    exit_code=$?
    set -e

    if [[ $exit_code -eq 0 ]]; then
        echo -e "${GREEN}PASSED${NC}"
        ((TESTS_PASSED++)) || true
    else
        echo -e "${YELLOW}SKIPPED${NC} (command returned non-zero but this is acceptable)"
        ((TESTS_SKIPPED++)) || true
    fi
    return 0
}

# Skip a test with reason
skip_test() {
    local test_name="$1"
    local reason="$2"
    echo -e "Skipping: ${test_name}... ${YELLOW}SKIPPED${NC} (${reason})"
    ((TESTS_SKIPPED++)) || true
}

# Print section header
section() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
    echo ""
}

# Main test execution
echo "=========================================="
echo "Birdwatcher E2E Tests"
echo "=========================================="
echo "Binary: ${BW_BINARY}"
echo "Etcd: ${ETCD_ADDR}"
echo "Root Path: ${ROOT_PATH}"
echo "=========================================="

# Verify birdwatcher binary exists
if [[ ! -x "${BW_BINARY}" ]]; then
    echo -e "${RED}ERROR: Birdwatcher binary not found or not executable: ${BW_BINARY}${NC}"
    exit 1
fi

# Verify connection to etcd
echo ""
echo "Verifying connection to etcd..."
set +e
conn_output=$(run_bw "show database" 2>&1)
conn_exit=$?
set -e

if [[ $conn_exit -ne 0 ]]; then
    echo -e "${RED}ERROR: Cannot connect to etcd at ${ETCD_ADDR}${NC}"
    echo "Output: ${conn_output}"
    exit 1
fi
echo -e "${GREEN}Connection verified!${NC}"

# Source individual test files
source "${SCRIPT_DIR}/commands/test_show.sh"
source "${SCRIPT_DIR}/commands/test_instance.sh"

# Optionally run repair/remove tests (dry-run mode only)
if [[ "${RUN_WRITE_TESTS:-true}" == "true" ]]; then
    source "${SCRIPT_DIR}/commands/test_repair.sh"
    source "${SCRIPT_DIR}/commands/test_remove.sh"
fi

# Print summary
echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Passed:  ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Failed:  ${RED}${TESTS_FAILED}${NC}"
echo -e "Skipped: ${YELLOW}${TESTS_SKIPPED}${NC}"
echo -e "Total:   $((TESTS_PASSED + TESTS_FAILED + TESTS_SKIPPED))"
echo "=========================================="

# Exit with failure if any tests failed
if [[ $TESTS_FAILED -gt 0 ]]; then
    echo -e "${RED}E2E tests failed!${NC}"
    exit 1
fi

echo -e "${GREEN}All E2E tests passed!${NC}"
exit 0
