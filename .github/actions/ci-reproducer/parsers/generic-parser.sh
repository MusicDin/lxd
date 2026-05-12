#!/bin/bash
# generic-parser.sh
# Generic parser for GitHub Actions failure logs.
# Output: Populates $REPRO_TMP/parse.result

set -e

echo "=== Parsing logs (generic) ===" >&2

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

set +e
source "$meta_file"
set -e

failed_job="${job_name:-unknown}"
failed_step_from_meta="${failed_step:-}"
failed_step="${failed_step_from_meta}"
failed_command=""
error_message=""
likely_cause=""
confidence="Low"

# Primary: identify the failing command from the GitHub Actions ##[group]Run pattern.
# After timestamp stripping, lines look like: "##[group]Run exit 1" or "##[group]Run make build".
failed_command=$(grep -E '^##\[group\]Run ' "$log_file" | tail -1 | sed -E 's/^##\[group\]Run //' || true)

# Secondary fallback: shell trace lines (only present when set -x is active).
if [[ -z "$failed_command" ]]; then
  failed_command=$(grep -E '^\+\s+.+|^\$\s+.+' "$log_file" | tail -1 | sed -E 's/^\+\s+|^\$\s+//' || true)
fi

# Tertiary fallback: well-known tooling invocations anywhere in the log.
if [[ -z "$failed_command" ]]; then
  failed_command=$(grep -E '(^|\s)(make|go test|go build|go vet|golangci-lint|pytest|npm test|cargo test)' "$log_file" | tail -1 || true)
fi

# Primary error signal: GitHub Actions ##[error] annotation (already stripped of timestamp).
error_message=$(grep -E '^##\[error\]' "$log_file" | tail -1 | sed -E 's/^##\[error\]//' || true)

# Fallback error signal: known error keywords.
if [[ -z "$error_message" ]]; then
  error_message=$(grep -Ei 'panic:|fatal:|error:|assert|failed|timed out|exit code [0-9]+|No such file|permission denied|undefined reference|cannot find|segmentation fault' "$log_file" | tail -1 || true)
fi

# If step not provided by API metadata, infer from ##[group]Run in logs.
if [[ -z "$failed_step" ]]; then
  failed_step=$(grep -E '^##\[group\]Run ' "$log_file" | tail -1 | sed -E 's/^##\[group\]Run //' || true)
fi

# Determine likely cause using observed evidence.
err_lc=$(echo "$error_message" | tr '[:upper:]' '[:lower:]')
if [[ "$err_lc" == *"timed out"* ]]; then
  likely_cause="Timeout in failing step (possible deadlock, long-running test, or slow environment)"
elif [[ "$err_lc" == *"no such file"* ]] || [[ "$err_lc" == *"cannot find"* ]]; then
  likely_cause="Missing file/path or incorrect working directory"
elif [[ "$err_lc" == *"permission denied"* ]]; then
  likely_cause="Permission issue when executing command or accessing files"
elif [[ "$err_lc" == *"undefined reference"* ]] || [[ "$err_lc" == *"undefined:"* ]]; then
  likely_cause="Build symbol/API mismatch or missing dependency"
elif [[ "$err_lc" == *"panic:"* ]] || [[ "$err_lc" == *"segmentation fault"* ]]; then
  likely_cause="Runtime crash in code under test"
elif [[ -n "$error_message" ]]; then
  likely_cause="Command or test failure in CI step"
else
  likely_cause="Insufficient evidence in logs to determine root cause"
fi

# Confidence from evidence completeness.
score=0
[[ -n "$failed_job" && "$failed_job" != "unknown" ]] && score=$((score + 1))
[[ -n "$failed_step" ]] && score=$((score + 1))
[[ -n "$failed_command" ]] && score=$((score + 1))
[[ -n "$error_message" ]] && score=$((score + 1))

if (( score >= 4 )); then
  confidence="High"
elif (( score >= 2 )); then
  confidence="Medium"
else
  confidence="Low"
fi

{
  echo "failed_job=\"$failed_job\""
  echo "failed_step=\"$failed_step\""
  echo "failed_command=\"$failed_command\""
  echo "error_message=\"$error_message\""
  echo "likely_cause=\"$likely_cause\""
  echo "confidence=\"$confidence\""
  echo "parse_status=\"success\""
} > "$result_file"

echo "Parse result saved to $result_file" >&2
cat "$result_file" >&2
