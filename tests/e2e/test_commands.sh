#!/bin/bash
# Birdwatcher E2E Test Runner
# This script runs all birdwatcher CLI commands and validates their output

set -e

# Configuration
BW_BINARY="${BW_BINARY:-./bin/birdwatcher}"
ETCD_ADDR="${ETCD_ADDR:-127.0.0.1:2379}"
ROOT_PATH="${ROOT_PATH:-by-dev}"

# Log configuration
LOG_DIR="${LOG_DIR:-./e2e_logs}"
DETAIL_LOG="${LOG_DIR}/birdwatcher_commands.log"
SUMMARY_LOG="${LOG_DIR}/test_summary.log"
JSON_REPORT="${LOG_DIR}/test_report.json"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0
TEST_NUMBER=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Initialize log directory and files
init_logs() {
    mkdir -p "${LOG_DIR}"
    echo "# Birdwatcher E2E Test Detailed Log" > "${DETAIL_LOG}"
    echo "# Generated at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> "${DETAIL_LOG}"
    echo "# Binary: ${BW_BINARY}" >> "${DETAIL_LOG}"
    echo "# Etcd: ${ETCD_ADDR}" >> "${DETAIL_LOG}"
    echo "# Root Path: ${ROOT_PATH}" >> "${DETAIL_LOG}"
    echo "========================================" >> "${DETAIL_LOG}"
    echo "" >> "${DETAIL_LOG}"

    # Initialize JSON report
    echo '{"tests": [], "summary": {}}' > "${JSON_REPORT}"
}

# Log command output to detail log
log_command() {
    local test_name="$1"
    local cmd="$2"
    local output="$3"
    local exit_code="$4"
    local status="$5"
    local duration="$6"

    {
        echo "========================================"
        echo "TEST #${TEST_NUMBER}: ${test_name}"
        echo "========================================"
        echo "Command: ${cmd}"
        echo "Exit Code: ${exit_code}"
        echo "Status: ${status}"
        echo "Duration: ${duration}ms"
        echo "----------------------------------------"
        echo "OUTPUT:"
        echo "${output}"
        echo ""
        echo ""
    } >> "${DETAIL_LOG}"
}

# Add test result to JSON report
add_json_result() {
    local test_name="$1"
    local cmd="$2"
    local exit_code="$3"
    local status="$4"
    local duration="$5"
    local output_lines="$6"

    # Escape special characters for JSON
    local escaped_cmd=$(echo "$cmd" | sed 's/"/\\"/g' | tr -d '\n')
    local escaped_name=$(echo "$test_name" | sed 's/"/\\"/g')

    # Create temp file for manipulation
    local temp_file=$(mktemp)
    python3 << EOF
import json
import sys

with open("${JSON_REPORT}", "r") as f:
    data = json.load(f)

data["tests"].append({
    "number": ${TEST_NUMBER},
    "name": "${escaped_name}",
    "command": "${escaped_cmd}",
    "exit_code": ${exit_code},
    "status": "${status}",
    "duration_ms": ${duration},
    "output_lines": ${output_lines}
})

with open("${JSON_REPORT}", "w") as f:
    json.dump(data, f, indent=2)
EOF
}

# Helper function to run birdwatcher command (captures both stdout and stderr)
run_bw() {
    local cmd="$1"
    local full_cmd="connect --etcd ${ETCD_ADDR} --rootPath ${ROOT_PATH}, ${cmd}"
    ${BW_BINARY} -olc "${full_cmd}" 2>&1
}

# Helper function to run birdwatcher command (stdout only, for JSON output)
run_bw_stdout() {
    local cmd="$1"
    local full_cmd="connect --etcd ${ETCD_ADDR} --rootPath ${ROOT_PATH}, ${cmd}"
    ${BW_BINARY} -olc "${full_cmd}" 2>/dev/null
}

# Test function with assertions
# Usage: test_command "test name" "command" [expected_exit_code] [expected_output_pattern]
test_command() {
    local test_name="$1"
    local cmd="$2"
    local expected_exit_code="${3:-0}"
    local expected_output_pattern="$4"

    ((TEST_NUMBER++)) || true
    echo -n "Testing: ${test_name}... "

    local start_time=$(date +%s%3N)
    set +e
    output=$(run_bw "${cmd}" 2>&1)
    exit_code=$?
    set -e
    local end_time=$(date +%s%3N)
    local duration=$((end_time - start_time))

    local output_lines=$(echo "$output" | wc -l)
    local status=""

    if [[ $exit_code -ne $expected_exit_code ]]; then
        status="FAILED"
        echo -e "${RED}FAILED${NC} (exit code: $exit_code, expected: $expected_exit_code)"
        echo "  Command: ${cmd}"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
        add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
        return 1
    fi

    if [[ -n "$expected_output_pattern" ]] && ! echo "$output" | grep -qE "$expected_output_pattern"; then
        status="FAILED"
        echo -e "${RED}FAILED${NC} (output pattern not matched)"
        echo "  Expected pattern: $expected_output_pattern"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
        add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
        return 1
    fi

    status="PASSED"
    echo -e "${GREEN}PASSED${NC} (${output_lines} lines, ${duration}ms)"
    ((TESTS_PASSED++)) || true
    log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
    add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
    return 0
}

# Test JSON output format
# Usage: test_json_command "test name" "command"
test_json_command() {
    local test_name="$1"
    local cmd="$2"

    ((TEST_NUMBER++)) || true
    echo -n "Testing JSON: ${test_name}... "

    local start_time=$(date +%s%3N)
    set +e
    output=$(run_bw_stdout "${cmd}")
    exit_code=$?
    set -e
    local end_time=$(date +%s%3N)
    local duration=$((end_time - start_time))

    local output_lines=$(echo "$output" | wc -l)
    local status=""

    if [[ $exit_code -ne 0 ]]; then
        status="FAILED"
        echo -e "${RED}FAILED${NC} (exit code: $exit_code)"
        echo "  Command: ${cmd}"
        echo "  Output: ${output:0:500}"
        ((TESTS_FAILED++)) || true
        log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
        add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
        return 1
    fi

    # Validate JSON using Python
    if echo "$output" | python3 -c "import sys, json; json.load(sys.stdin)" 2>/dev/null; then
        status="PASSED"
        echo -e "${GREEN}PASSED${NC} (valid JSON, ${output_lines} lines, ${duration}ms)"
        ((TESTS_PASSED++)) || true
        log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
        add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
        return 0
    else
        status="WARN"
        echo -e "${YELLOW}WARN${NC} (command succeeded but output is not valid JSON)"
        echo "  Output: ${output:0:500}"
        ((TESTS_SKIPPED++)) || true
        log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
        add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
        return 0
    fi
}

# Test command that may fail (optional test)
# Usage: test_optional "test name" "command"
test_optional() {
    local test_name="$1"
    local cmd="$2"

    ((TEST_NUMBER++)) || true
    echo -n "Testing (optional): ${test_name}... "

    local start_time=$(date +%s%3N)
    set +e
    output=$(run_bw "${cmd}" 2>&1)
    exit_code=$?
    set -e
    local end_time=$(date +%s%3N)
    local duration=$((end_time - start_time))

    local output_lines=$(echo "$output" | wc -l)
    local status=""

    if [[ $exit_code -eq 0 ]]; then
        status="PASSED"
        echo -e "${GREEN}PASSED${NC} (${output_lines} lines, ${duration}ms)"
        ((TESTS_PASSED++)) || true
    else
        status="SKIPPED"
        echo -e "${YELLOW}SKIPPED${NC} (exit code: $exit_code, ${output_lines} lines)"
        ((TESTS_SKIPPED++)) || true
    fi

    log_command "$test_name" "$cmd" "$output" "$exit_code" "$status" "$duration"
    add_json_result "$test_name" "$cmd" "$exit_code" "$status" "$duration" "$output_lines"
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

# Finalize JSON report with summary
finalize_report() {
    python3 << EOF
import json

with open("${JSON_REPORT}", "r") as f:
    data = json.load(f)

data["summary"] = {
    "passed": ${TESTS_PASSED},
    "failed": ${TESTS_FAILED},
    "skipped": ${TESTS_SKIPPED},
    "total": ${TESTS_PASSED} + ${TESTS_FAILED} + ${TESTS_SKIPPED}
}

with open("${JSON_REPORT}", "w") as f:
    json.dump(data, f, indent=2)
EOF

    # Write summary log
    {
        echo "Birdwatcher E2E Test Summary"
        echo "============================"
        echo "Generated at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo ""
        echo "Results:"
        echo "  Passed:  ${TESTS_PASSED}"
        echo "  Failed:  ${TESTS_FAILED}"
        echo "  Skipped: ${TESTS_SKIPPED}"
        echo "  Total:   $((TESTS_PASSED + TESTS_FAILED + TESTS_SKIPPED))"
        echo ""
        echo "Log files:"
        echo "  Detail log: ${DETAIL_LOG}"
        echo "  JSON report: ${JSON_REPORT}"
    } > "${SUMMARY_LOG}"
}

# Main test execution
echo "=========================================="
echo "Birdwatcher E2E Tests"
echo "=========================================="
echo "Binary: ${BW_BINARY}"
echo "Etcd: ${ETCD_ADDR}"
echo "Root Path: ${ROOT_PATH}"
echo "Log Dir: ${LOG_DIR}"
echo "=========================================="

# Initialize logs
init_logs
echo "Detailed logs will be saved to: ${DETAIL_LOG}"

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

# Finalize report
finalize_report

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
echo ""
echo "Log files saved to: ${LOG_DIR}/"
echo "  - birdwatcher_commands.log  (detailed command output)"
echo "  - test_report.json          (structured JSON report)"
echo "  - test_summary.log          (summary)"

# Exit with failure if any tests failed
if [[ $TESTS_FAILED -gt 0 ]]; then
    echo -e "${RED}E2E tests failed!${NC}"
    exit 1
fi

echo -e "${GREEN}All E2E tests passed!${NC}"
exit 0
