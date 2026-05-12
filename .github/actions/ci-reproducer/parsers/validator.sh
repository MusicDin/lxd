#!/bin/bash
# parsers/validator.sh
# Validates that it's safe to run the reproducer by checking:
# - No disk space issues
# - No OOM conditions
# - No kernel panics
# - No environmental mismatches
# Output: Sets validation_safe, validation_reason

set -e

echo "=== Validating reproducer safety ===" >&2

log_file="$REPRO_TMP/job.log"
result_file="$REPRO_TMP/parse.result"

if [[ ! -f "$log_file" ]]; then
  echo "ERROR: Log file not found" >&2
  exit 1
fi

validation_safe="true"
validation_reason=""

# Check for disk space issues
if grep -qi 'no space left\|disk full\|enospc' "$log_file"; then
  validation_safe="false"
  validation_reason="Disk space issue detected in logs"
  echo "WARNING: $validation_reason" >&2
fi

# Check for OOM (out of memory)
if grep -qi 'out of memory\|oom killer\|killed.*due to memory' "$log_file"; then
  validation_safe="false"
  validation_reason="OOM (Out of Memory) detected in logs"
  echo "WARNING: $validation_reason" >&2
fi

# Check for kernel panics
if grep -qi 'kernel panic\|kernel dump\|general protection fault' "$log_file"; then
  validation_safe="false"
  validation_reason="Kernel panic detected in logs"
  echo "WARNING: $validation_reason" >&2
fi

# Check for dmesg errors that suggest hardware/environment issues
if grep -qi 'segfault at.*ip.*sp\|bad page state\|buffer I/O error' "$log_file"; then
  # These might be hardware/driver issues
  validation_safe="false"
  validation_reason="Potential hardware/driver issue detected in logs"
  echo "WARNING: $validation_reason" >&2
fi

# Check for network connectivity issues
if grep -qi 'network unreachable\|connection refused\|connection reset\|no route to host' "$log_file"; then
  validation_safe="false"
  validation_reason="Network connectivity issue detected"
  echo "WARNING: $validation_reason" >&2
fi

# Check for flaky test indicators
# If the test name appears with "flaky" or similar, confidence should be low
if grep -qi 'flaky\|retry\|intermittent' "$log_file"; then
  echo "NOTE: Test may be flaky. Setting lower confidence." >&2
fi

# Check free disk space on current system (if available)
if command -v df &> /dev/null; then
  free_percent=$(df / | awk 'NR==2 {print int(100-$5)}')
  if (( free_percent < 10 )); then
    validation_safe="false"
    validation_reason="Low disk space available ($free_percent% free)"
    echo "WARNING: $validation_reason" >&2
  fi
fi

echo "Validation result: safe=$validation_safe, reason='$validation_reason'" >&2

# Save validation result
if [[ -f "$result_file" ]]; then
  {
    cat "$result_file"
    echo "validation_safe=\"$validation_safe\""
    echo "validation_reason=\"$validation_reason\""
  } > "$result_file.tmp"
  mv "$result_file.tmp" "$result_file"
else
  {
    echo "validation_safe=\"$validation_safe\""
    echo "validation_reason=\"$validation_reason\""
  } > "$result_file"
fi

echo "Validation saved to $result_file" >&2
cat "$result_file" >&2
