#!/bin/bash
# log-fetcher.sh
# Fetches failed job logs using GitHub CLI and extracts metadata.
# Output: Populates $REPRO_TMP/job.log, $REPRO_TMP/job.meta

set -e

echo "=== Fetching job logs ===" >&2

if [[ -z "$RUN_ID" ]]; then
  echo "ERROR: RUN_ID not set" >&2
  exit 1
fi

if [[ -z "$GITHUB_TOKEN" ]]; then
  echo "ERROR: GITHUB_TOKEN not set" >&2
  exit 1
fi

# Determine failed job name from workflow run
# If FAILED_JOB_NAME not provided, detect from run
FAILED_JOB="${FAILED_JOB_NAME:-}"

if [[ -z "$FAILED_JOB" ]]; then
  # Try to find the most recent failed job in this run
  # Query the API to get job information
  echo "Detecting failed job from run $RUN_ID..." >&2
  
  jobs_json=$(gh api repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs --paginate 2>/dev/null || echo "{}")
  
  # Find jobs with conclusion=failure, exclude this step (ci-reproducer itself)
  failed_jobs=$(echo "$jobs_json" | jq -r '.jobs[]? | select(.conclusion=="failure" and .name != "ci-reproducer") | .name' 2>/dev/null || echo "")
  
  if [[ -z "$failed_jobs" ]]; then
    echo "WARNING: Could not detect failed job from API. Attempting direct log fetch..." >&2
    FAILED_JOB="system-tests"
  else
    # Use the first failed job
    FAILED_JOB=$(echo "$failed_jobs" | head -1)
  fi
fi

echo "Target failed job: $FAILED_JOB" >&2

# Fetch full logs for the run and filter for job
# Note: gh run view doesn't directly support job filtering, so we get full logs and parse
log_file="$REPRO_TMP/job.log"
meta_file="$REPRO_TMP/job.meta"

echo "Fetching logs via gh run view..." >&2
if ! gh run view "$RUN_ID" --log > "$log_file" 2>&1; then
  echo "ERROR: Failed to fetch logs from run $RUN_ID" >&2
  cat "$log_file" >&2
  exit 1
fi

# Extract metadata from log headers
# Look for job name, matrix context (if any), and error patterns
echo "Extracting metadata..." >&2

# Get matrix context from job name in logs
# Pattern: "Job: system-tests (cluster, btrfs)" or similar
matrix_context=$(grep -oP 'Job: [^(]*\(\K[^)]*' "$log_file" | head -1 || echo "")
job_name=$(grep -oP 'Job: \K[^(]*' "$log_file" | head -1 || echo "$FAILED_JOB")

{
  echo "job_name=$job_name"
  echo "matrix_context=$matrix_context"
  echo "run_id=$RUN_ID"
  echo "run_attempt=$RUN_ATTEMPT"
  echo "actor=$ACTOR"
  echo "log_file=$log_file"
} > "$meta_file"

# Check if log is non-empty
if [[ ! -s "$log_file" ]]; then
  echo "ERROR: Fetched log is empty" >&2
  exit 1
fi

log_lines=$(wc -l < "$log_file")
echo "Fetched $log_lines lines of logs for job: $job_name" >&2

# Quick validation: Look for error patterns
if ! grep -qi 'panic\|error\|failed\|timeout' "$log_file"; then
  echo "WARNING: No obvious error patterns found in logs. May be flaky or skipped." >&2
fi

echo "Log fetch complete. Metadata:" >&2
cat "$meta_file" >&2
