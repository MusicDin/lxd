#!/bin/bash
# log-fetcher.sh
# Fetches failed job logs using GitHub CLI and extracts metadata.
# Output: Populates $REPRO_TMP/job.log, $REPRO_TMP/job.meta, $REPRO_TMP/jobs.json

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

mkdir -p "$REPRO_TMP"

log_file="$REPRO_TMP/job.log"
meta_file="$REPRO_TMP/job.meta"
jobs_file="$REPRO_TMP/jobs.json"

echo "Detecting failed job from run $RUN_ID..." >&2

if ! gh api "repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs" --paginate > "$jobs_file" 2>/dev/null; then
  echo "ERROR: Failed retrieving jobs list for run $RUN_ID" >&2
  exit 1
fi

failed_job_name="${FAILED_JOB_NAME:-}"
failed_job_id=""
failed_step_name=""

if [[ -n "$failed_job_name" ]]; then
  failed_job_id=$(jq -r --arg name "$failed_job_name" '.jobs[] | select(.name==$name and .conclusion=="failure") | .id' "$jobs_file" | head -1)
fi

if [[ -z "$failed_job_id" ]]; then
  failed_job_id=$(jq -r '.jobs[] | select(.conclusion=="failure" and .name!="CI Reproducer") | .id' "$jobs_file" | head -1)
fi

if [[ -z "$failed_job_id" ]]; then
  echo "ERROR: No failed job found in run $RUN_ID" >&2
  exit 1
fi

failed_job_name=$(jq -r --argjson id "$failed_job_id" '.jobs[] | select(.id==$id) | .name' "$jobs_file" | head -1)
failed_step_name=$(jq -r --argjson id "$failed_job_id" '.jobs[] | select(.id==$id) | (.steps[]? | select(.conclusion=="failure") | .name)' "$jobs_file" | head -1)

echo "Target failed job: $failed_job_name (id: $failed_job_id)" >&2

echo "Fetching logs via GitHub API for failed job..." >&2
if ! gh api "repos/$GITHUB_REPOSITORY/actions/jobs/$failed_job_id/logs" > "$log_file" 2>/dev/null || [[ ! -s "$log_file" ]]; then
  echo "WARNING: Job-scoped log fetch failed, falling back to full run logs" >&2
  zip_file="$REPRO_TMP/run_logs.zip"
  if ! gh api "repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/logs" > "$zip_file" 2>/dev/null; then
    echo "ERROR: Failed to fetch logs from run $RUN_ID" >&2
    exit 1
  fi
  if ! unzip -p "$zip_file" > "$log_file" 2>/dev/null || [[ ! -s "$log_file" ]]; then
    echo "ERROR: Failed to extract run logs for run $RUN_ID" >&2
    exit 1
  fi
fi

# Extract metadata from log headers
# Look for job name, matrix context (if any), and step context
echo "Extracting metadata..." >&2

# Get matrix context from job name in logs
# Pattern: "Job: system-tests (cluster, btrfs)" or similar
matrix_context=$(grep -oP 'Job: [^(]*\(\K[^)]*' "$log_file" | head -1 || echo "")
job_name=$(grep -oP 'Job: \K[^(]*' "$log_file" | head -1 || echo "$failed_job_name")

{
  echo "job_name=$job_name"
  echo "job_id=$failed_job_id"
  echo "failed_step=$failed_step_name"
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
if ! grep -qi 'panic\|error\|failed\|timeout\|fatal\|exit code' "$log_file"; then
  echo "WARNING: No obvious error patterns found in logs. May be flaky or skipped." >&2
fi

echo "Log fetch complete. Metadata:" >&2
cat "$meta_file" >&2
