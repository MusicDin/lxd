#!/bin/bash
# parsers/system-tests.sh
# Parses LXD system-tests job logs to extract:
# - Failed test/command
# - Error message
# - Backend and test group
# - Confidence level
# Output: Populates $REPRO_TMP/parse.result

set -e

echo "=== Parsing system-tests logs ===" >&2

meta_file="$REPRO_TMP/job.meta"
log_file="$REPRO_TMP/job.log"
result_file="$REPRO_TMP/parse.result"

if [[ ! -f "$log_file" ]]; then
  echo "ERROR: Log file not found at $log_file" >&2
  exit 1
fi

if [[ ! -f "$meta_file" ]]; then
  echo "ERROR: Metadata file not found at $meta_file" >&2
  exit 1
fi

# Source metadata
set +e
source "$meta_file"
set -e

# Initialize result variables
failed_command=""
error_message=""
backend=""
test_group=""
confidence="Low"
failed_step=""

echo "Analyzing logs for job: $job_name" >&2

# Extract matrix context if available (format: "cluster, btrfs" or similar)
if [[ -n "$matrix_context" ]]; then
  # Try to parse as "group, backend"
  if [[ "$matrix_context" =~ ([^,]*),([^,]*) ]]; then
    test_group="${BASH_REMATCH[1]// /}"
    backend="${BASH_REMATCH[2]// /}"
  fi
fi

echo "Initial context - test_group: $test_group, backend: $backend" >&2

# Look for sub_test boundaries and failed assertions/panics
# Pattern: "sub_test" followed by description, then error
declare -a sub_tests
declare -a errors

# Find all sub_test sections
while IFS= read -r line; do
  if [[ "$line" =~ sub_test\ \"([^\"]*)\" ]]; then
    sub_tests+=("${BASH_REMATCH[1]}")
  fi
done < "$log_file"

echo "Found ${#sub_tests[@]} sub_tests" >&2

# Find panic/segfault (highest priority)
if panic_line=$(grep -n 'panic:' "$log_file" | tail -1); then
  error_message=$(echo "$panic_line" | cut -d: -f2- | head -c 200)
  confidence="High"
  failed_step="panic"
  echo "Found panic: $error_message" >&2
fi

# Find assertion failures if no panic
if [[ -z "$failed_step" ]] && assertion_line=$(grep -n 'AssertionError\|assert\|FAIL:' "$log_file" | tail -1); then
  error_message=$(echo "$assertion_line" | cut -d: -f2- | head -c 200)
  confidence="Medium"
  failed_step="assertion"
  echo "Found assertion failure: $error_message" >&2
fi

# Look for lxc/lxd command failures
if [[ -z "$failed_step" ]]; then
  # Find lines with lxc or lxd commands that failed (preceded by + )
  cmd_pattern='^[[:space:]]*\+[[:space:]]*(lxc|lxd|test/main.sh).*'
  
  # Get last invoked command before error
  last_cmd=$(grep -E "$cmd_pattern" "$log_file" | tail -1 | sed 's/^[[:space:]]*+[[:space:]]*//' || echo "")
  
  if [[ -n "$last_cmd" ]]; then
    failed_command="$last_cmd"
    confidence="Medium"
    failed_step="command"
    echo "Found failed command: $failed_command" >&2
  fi
fi

# Look for timeout
if [[ -z "$failed_step" ]] && grep -qi 'timeout\|timed out' "$log_file"; then
  error_message="Test timed out"
  confidence="Low"
  failed_step="timeout"
  echo "Found timeout" >&2
fi

# Extract backend from logs if not already found
if [[ -z "$backend" ]]; then
  if backend_line=$(grep -i 'LXD_BACKEND=' "$log_file" | head -1); then
    backend=$(echo "$backend_line" | sed -E 's/.*LXD_BACKEND=([^ :]*).*/\1/')
  fi
fi

# Extract test group from logs if not already found
if [[ -z "$test_group" ]]; then
  if group_line=$(grep -i 'group[=:]' "$log_file" | head -1); then
    test_group=$(echo "$group_line" | sed -E 's/.*group[=:]([^ :,]*).*/\1/')
  fi
fi

# Default backend to dir if still not found (common default)
[[ -z "$backend" ]] && backend="dir"

echo "Parsed results: backend=$backend, group=$test_group, confidence=$confidence" >&2

# Write result file
{
  echo "failed_command=\"$failed_command\""
  echo "error_message=\"$error_message\""
  echo "backend=\"$backend\""
  echo "test_group=\"$test_group\""
  echo "confidence=\"$confidence\""
  echo "failed_step=\"$failed_step\""
  echo "parse_status=success"
} > "$result_file"

echo "Parse result saved to $result_file" >&2
cat "$result_file" >&2
