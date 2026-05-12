#!/bin/bash
# report.sh
# Generates the final Markdown report matching the specified format.
# Output: Writes to $GITHUB_STEP_SUMMARY and sets output variables

set -e

echo "=== Generating report ===" >&2

meta_file="$REPRO_TMP/job.meta"
result_file="$REPRO_TMP/parse.result"
repro_result="$REPRO_TMP/reproducer.result"

# Source all result files
set +e
[[ -f "$meta_file" ]] && source "$meta_file"
[[ -f "$result_file" ]] && source "$result_file"
[[ -f "$repro_result" ]] && source "$repro_result"
set -e

# Build report
report=""

# === CI Reproducer Report Header ===
report+="# CI Reproducer Report"$'\n\n'

# === Summary Section ===
report+="## Summary"$'\n\n'

if [[ -n "$job_name" ]]; then
  report+="- **Failed job:** \`$job_name\`"$'\n'
fi

if [[ -n "$failed_step" ]]; then
  report+="- **Failed step:** \`$failed_step\`"$'\n'
fi

if [[ -n "$reproducer_command" ]]; then
  report+="- **Failed command:** \`${reproducer_command:0:100}\`"$'\n'
fi

if [[ -n "$error_message" ]]; then
  # Truncate long error messages
  truncated_error="${error_message:0:150}"
  [[ ${#error_message} -gt 150 ]] && truncated_error+="..."
  report+="- **Key error:** \`${truncated_error}\`"$'\n'
fi

if [[ -n "$failed_command" ]] || [[ -n "$error_message" ]]; then
  # Infer likely cause
  likely_cause=""
  if [[ "$failed_step" == "panic" ]]; then
    likely_cause="Code panic (segmentation fault or runtime error in LXD daemon)"
  elif [[ "$failed_step" == "assertion" ]]; then
    likely_cause="Test assertion failure (logic error in LXD or test)"
  elif [[ "$failed_step" == "timeout" ]]; then
    likely_cause="Test timeout (excessive duration, deadlock, or resource contention)"
  elif [[ "$validation_safe" != "true" ]]; then
    likely_cause="Environment issue: $validation_reason"
  elif grep -q 'flaky' "$REPRO_TMP/job.log" 2>/dev/null; then
    likely_cause="Likely flaky/intermittent test (timing or race condition)"
  else
    likely_cause="Test failure (see error message for details)"
  fi
  report+="- **Likely cause:** $likely_cause"$'\n'
fi

report+="- **Confidence:** $confidence"$'\n\n'

# === Reproducer Section ===
report+="## Reproducer"$'\n\n'
report+="Provide the smallest command a developer can run from the repository root:"$'\n\n'

if [[ -n "$reproducer_command" ]]; then
  report+="\`\`\`bash"$'\n'
  report+="# from repository root"$'\n'
  report+="$reproducer_command"$'\n'
  report+="\`\`\`"$'\n\n'
else
  report+="Unable to derive minimal reproducer command from available logs."$'\n\n'
fi

# Reproduction status
report+="Reproduction: "
if [[ "$reproducer_status" == "success" ]]; then
  report+="**not confirmed** (command ran but did not reproduce failure)"$'\n\n'
elif [[ "$reproducer_status" == "failed" ]]; then
  report+="**confirmed** (command reproduced failure with exit code $reproducer_exit_code)"$'\n\n'
elif [[ "$reproducer_status" == "timeout" ]]; then
  report+="**not confirmed** (reproducer timed out after $TIMEOUT_SECONDS seconds)"$'\n\n'
elif [[ "$reproducer_status" == "skipped" ]]; then
  report+="**not attempted** ($validation_reason)"$'\n\n'
else
  report+="**not attempted** (unable to derive command)"$'\n\n'
fi

# === Potential Fix Section ===
report+="## Potential Fix"$'\n\n'

if [[ "$confidence" == "High" ]] && [[ "$failed_step" == "panic" ]]; then
  report+="Panic detected in logs. Recommended action:"$'\n'
  report+="- Check stack trace in logs for pointer dereference or nil access"$'\n'
  report+="- Verify boundary conditions in code near the panic"$'\n'
  report+="- Consider adding defensive checks or error handling"$'\n\n'
elif [[ "$confidence" == "Medium" ]] && [[ "$failed_step" == "assertion" ]]; then
  report+="Assertion failure detected. Recommended action:"$'\n'
  report+="- Review the assertion condition and expected vs actual values"$'\n'
  report+="- Check for race conditions or ordering issues"$'\n'
  report+="- Verify state transitions are correct"$'\n\n'
elif [[ "$confidence" == "Low" ]] && [[ "$reproducer_status" == "timeout" ]]; then
  report+="Test timed out. This could indicate:"$'\n'
  report+="- Deadlock or resource contention"$'\n'
  report+="- Slow test environment"$'\n'
  report+="- Flaky/intermittent behavior"$'\n\n'
  report+="Recommend: Run locally to reproduce, increase timeout, or refactor test."$'\n\n'
else
  report+="No clear fix identified from logs. Reason: Insufficient error context."$'\n\n'
fi

# === Validation Section ===
report+="## Validation"$'\n\n'
report+="Commands actually executed:"$'\n\n'

if [[ "$reproducer_status" == "skipped" ]]; then
  report+="- Reproducer not executed (validation failure)"$'\n\n'
elif [[ "$reproducer_status" == "uncertain" ]]; then
  report+="- Unable to derive reproducer command"$'\n\n'
else
  report+="- \`${reproducer_command:0:100}\` → $reproducer_status"$'\n\n'
  
  # Show output snippet if available
  if [[ -n "$reproducer_stdout" ]] && [[ "$reproducer_stdout" != "\\n" ]]; then
    report+="**Last output:**"$'\n'
    report+="\`\`\`"$'\n'
    report+="${reproducer_stdout:0:500}"$'\n'
    report+="\`\`\`"$'\n\n'
  fi
fi

# === Notes Section ===
report+="## Notes"$'\n\n'

report+="- **Run context:** Run ID \`$RUN_ID\`, Attempt \`$RUN_ATTEMPT\`, Actor \`$ACTOR\`"$'\n'

if [[ "$confidence" == "Low" ]]; then
  report+="- **Confidence is Low:** The reproducer is uncertain. Test may be flaky or environment-dependent."$'\n'
fi

if [[ -n "$validation_reason" ]]; then
  report+="- **Validation note:** $validation_reason"$'\n'
fi

if [[ "$reproducer_exit_code" -eq 0 ]] && [[ "$reproducer_status" != "skipped" ]]; then
  report+="- **No failure reproduced:** The minimal command succeeded, but CI failed. This suggests:"$'\n'
  report+="  - Test is flaky or timing-dependent"$'\n'
  report+="  - Environment differs between CI and local setup"$'\n'
  report+="  - Reproduction command is incomplete"$'\n'
fi

report+="- **How to use:** Copy the reproducer command and run from repository root. Adjust as needed for local environment."$'\n\n'

# Write to $GITHUB_STEP_SUMMARY
if [[ -z "$GITHUB_STEP_SUMMARY" ]]; then
  echo "WARNING: GITHUB_STEP_SUMMARY not set. Output to stdout instead." >&2
  echo "$report"
else
  echo "$report" >> "$GITHUB_STEP_SUMMARY"
  echo "Report written to $GITHUB_STEP_SUMMARY" >&2
fi

# Set GitHub Actions outputs
if [[ -n "$GITHUB_OUTPUT" ]]; then
  {
    echo "report<<EOF"
    echo "$report"
    echo "EOF"
    echo "confidence=$confidence"
    echo "reproducer-command=$reproducer_command"
  } >> "$GITHUB_OUTPUT"
  echo "Outputs set in $GITHUB_OUTPUT" >&2
fi

echo "Report generation complete" >&2
