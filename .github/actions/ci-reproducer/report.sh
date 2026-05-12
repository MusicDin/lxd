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

if [[ -n "$failed_job" ]]; then
  report+="- **Failed job:** \`$failed_job\`"$'\n'
elif [[ -n "$job_name" ]]; then
  report+="- **Failed job:** \`$job_name\`"$'\n'
fi

if [[ -n "$failed_step" ]]; then
  report+="- **Failed step:** \`$failed_step\`"$'\n'
fi

if [[ -n "$failed_command" ]]; then
  report+="- **Failed command:** \`${failed_command:0:160}\`"$'\n'
fi

if [[ -n "$error_message" ]]; then
  # Truncate long error messages
  truncated_error="${error_message:0:150}"
  [[ ${#error_message} -gt 150 ]] && truncated_error+="..."
  report+="- **Key error:** \`${truncated_error}\`"$'\n'
fi

if [[ -n "$likely_cause" ]]; then
  report+="- **Likely cause:** $likely_cause"$'\n'
elif [[ -n "$error_message" ]]; then
  report+="- **Likely cause:** Failure inferred from log evidence"$'\n'
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
elif [[ "$reproducer_status" == "not-run" ]]; then
  report+="**not attempted** (derived command was not safe to execute in CI reproducer)"$'\n\n'
else
  report+="**not attempted** (unable to derive command)"$'\n\n'
fi

# === Potential Fix Section ===
report+="## Potential Fix"$'\n\n'

if [[ -n "$failed_command" ]]; then
  report+="Start by re-running the failing command locally and compare output:"$'\n'
  report+="- \`$failed_command\`"$'\n'
fi

if [[ -n "$error_message" ]]; then
  report+="Focus fix area from error signal:"$'\n'
  report+="- \`${error_message:0:180}\`"$'\n'
fi

if [[ "$reproducer_status" == "skipped" ]] || [[ "$reproducer_status" == "uncertain" ]] || [[ "$reproducer_status" == "not-run" ]] || [[ -z "$failed_command" ]]; then
  report+="If command evidence is incomplete, enable shell tracing (for example \`set -x\`) in the failing step to improve next run analysis."$'\n\n'
else
  report+="Validate the suspected fix by re-running the same command in CI and locally."$'\n\n'
fi

# === Validation Section ===
report+="## Validation"$'\n\n'
report+="Commands actually executed:"$'\n\n'

if [[ "$reproducer_status" == "skipped" ]]; then
  report+="- Reproducer not executed (validation failure)"$'\n\n'
elif [[ "$reproducer_status" == "uncertain" ]] || [[ "$reproducer_status" == "not-run" ]]; then
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
