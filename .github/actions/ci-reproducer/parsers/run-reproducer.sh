#!/bin/bash
# parsers/run-reproducer.sh
# Executes the minimal reproducer command with safety constraints.
# Output: Populates $REPRO_TMP/reproducer.result

set -e

echo "=== Running reproducer ===" >&2

result_file="$REPRO_TMP/parse.result"
repro_result="$REPRO_TMP/reproducer.result"

if [[ ! -f "$result_file" ]]; then
  echo "ERROR: Parse result not found" >&2
  {
    echo "reproducer_status=error"
    echo "reproducer_exit_code=1"
    echo "reproducer_stdout='Parse result not found'"
    echo "reproducer_stderr=''"
    echo "reproducer_command=''"
  } > "$repro_result"
  exit 1
fi

# Source parsed results
set +e
source "$result_file"
set -e

# Check if validation passed
if [[ "$validation_safe" != "true" ]]; then
  echo "Validation failed. Skipping reproducer execution." >&2
  echo "Reason: $validation_reason" >&2
  {
    echo "reproducer_status=skipped"
    echo "reproducer_exit_code=0"
    echo "reproducer_stdout='Reproducer skipped due to validation failure'"
    echo "reproducer_stderr='$validation_reason'"
    echo "reproducer_command=''"
  } > "$repro_result"
  exit 0
fi

# Derive the minimal reproducer command
# For system-tests, the basic structure is: test/main.sh GROUP BACKEND
reproducer_cmd=""

if [[ -n "$failed_command" ]]; then
  # If we have the exact failed command, try to run it directly
  reproducer_cmd="$failed_command"
else
  # Fall back to constructing test/main.sh command
  if [[ -n "$test_group" ]] && [[ -n "$backend" ]]; then
    reproducer_cmd="test/main.sh system-tests:$test_group $backend"
  elif [[ -n "$test_group" ]]; then
    reproducer_cmd="test/main.sh system-tests:$test_group"
  else
    echo "WARNING: Could not derive reproducer command. Insufficient context." >&2
    {
      echo "reproducer_status=uncertain"
      echo "reproducer_exit_code=1"
      echo "reproducer_stdout='Could not derive reproducer command'"
      echo "reproducer_stderr='Missing test group and/or backend'"
      echo "reproducer_command=''"
    } > "$repro_result"
    exit 0
  fi
fi

echo "Reproducer command: $reproducer_cmd" >&2

# Validate command syntax
if ! [[ "$reproducer_cmd" =~ ^(test/main.sh|lxc|lxd) ]]; then
  echo "ERROR: Invalid reproducer command (suspicious syntax)" >&2
  {
    echo "reproducer_status=error"
    echo "reproducer_exit_code=1"
    echo "reproducer_stdout='Invalid reproducer command syntax'"
    echo "reproducer_stderr='Command does not match expected patterns'"
    echo "reproducer_command=''"
  } > "$repro_result"
  exit 1
fi

# Run with timeout and capture output
timeout_duration="${TIMEOUT_SECONDS:-900}"
echo "Running with timeout=$timeout_duration seconds" >&2

repro_stdout="$REPRO_TMP/reproducer.stdout"
repro_stderr="$REPRO_TMP/reproducer.stderr"

# Run reproducer with timeout
reproducer_exit_code=0
if timeout "$timeout_duration" bash -c "$reproducer_cmd" > "$repro_stdout" 2> "$repro_stderr"; then
  echo "Reproducer completed successfully (exit code 0)" >&2
  reproducer_status="success"
else
  reproducer_exit_code=$?
  echo "Reproducer failed with exit code: $reproducer_exit_code" >&2
  if [[ $reproducer_exit_code -eq 124 ]]; then
    echo "Note: Exit code 124 indicates timeout" >&2
    reproducer_status="timeout"
  else
    reproducer_status="failed"
  fi
fi

# Capture output (limit to last N lines for readability)
stdout_content=$(tail -20 "$repro_stdout" 2>/dev/null || echo "")
stderr_content=$(tail -20 "$repro_stderr" 2>/dev/null || echo "")

# Escape newlines for shell variable
stdout_content="${stdout_content//$'\n'/\\n}"
stderr_content="${stderr_content//$'\n'/\\n}"

echo "Reproducer output (last 20 lines of stdout):" >&2
echo "$stdout_content" >&2

# Save result
{
  echo "reproducer_status=\"$reproducer_status\""
  echo "reproducer_exit_code=$reproducer_exit_code"
  echo "reproducer_command=\"$reproducer_cmd\""
  echo "reproducer_stdout=\"$stdout_content\""
  echo "reproducer_stderr=\"$stderr_content\""
} > "$repro_result"

echo "Reproducer result saved to $repro_result" >&2
cat "$repro_result" >&2
