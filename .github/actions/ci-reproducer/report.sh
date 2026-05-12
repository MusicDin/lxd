#!/bin/bash
# report.sh
# Builds the CI failure report deterministically from parsed facts.
# Calls GitHub Models ONLY for three reasoning fields: likely_cause, confidence,
# and potential_fix. Everything else is filled by the shell directly.
# Edit prompts/analyze.md to change AI reasoning instructions.
# Model is selected via REPRO_MODEL env var (action input, default gpt-4o-mini).

set -e

echo "=== Generating report ===" >&2

meta_file="$REPRO_TMP/job.meta"
result_file="$REPRO_TMP/parse.result"
repro_result="$REPRO_TMP/reproducer.result"
context_file="$REPRO_TMP/code.context"
prompt_file="$(dirname "$0")/prompts/analyze.md"
model="${REPRO_MODEL:-gpt-4o-mini}"

# Source all parsed result files.
set +e
[[ -f "$meta_file" ]]    && source "$meta_file"
[[ -f "$result_file" ]]  && source "$result_file"
[[ -f "$repro_result" ]] && source "$repro_result"
set -e

# ── Ask AI only for the three reasoning fields ───────────────────────────────
# The shell knows every concrete value already.
# AI provides: likely_cause, confidence, potential_fix — nothing else.
ai_likely_cause=""
ai_confidence=""
ai_potential_fix=""

if [[ -f "$prompt_file" ]] && [[ -n "$GITHUB_TOKEN" ]]; then
  echo "Requesting AI reasoning from GitHub Models (model: $model)..." >&2

  system_prompt=$(cat "$prompt_file")

  user_msg_file=$(mktemp)
  {
    echo "## Failure facts"
    echo "- Job: ${failed_job:-unknown}"
    echo "- Step: ${failed_step:-unknown}"
    echo "- Command: ${failed_command:-unknown}"
    echo "- Error: ${error_message:-none}"
    echo "- Run ID: ${RUN_ID:-unknown}"
    echo "- Run attempt: ${RUN_ATTEMPT:-1}"
    echo "- Actor: ${ACTOR:-unknown}"
    echo "- Reproducer status: ${reproducer_status:-not-run}"
    echo ""
    if [[ -f "$context_file" ]]; then
      echo "## Code evidence"
      cat "$context_file"
    fi
  } > "$user_msg_file"

  ai_payload=$(jq -Rns \
    --arg system "$system_prompt" \
    --arg model  "$model" \
    '{model: $model,
      messages: [{role:"system", content:$system},
                 {role:"user",   content:.}],
      max_tokens: 800}' < "$user_msg_file")
  rm -f "$user_msg_file"

  ai_raw=$(curl -s -f \
    -X POST "https://models.inference.ai.azure.com/chat/completions" \
    -H "Authorization: Bearer $GITHUB_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$ai_payload" 2>/dev/null || true)

  ai_text=$(echo "$ai_raw" | jq -r '.choices[0].message.content // empty' 2>/dev/null || true)

  if [[ -n "$ai_text" ]]; then
    echo "AI reasoning received" >&2
    ai_likely_cause=$(echo "$ai_text" | sed -n '/---BEGIN_LIKELY_CAUSE---/,/---END_LIKELY_CAUSE---/p' \
      | grep -v '^---' | sed '/^$/d')
    ai_confidence=$(echo "$ai_text" | sed -n '/---BEGIN_CONFIDENCE---/,/---END_CONFIDENCE---/p' \
      | grep -v '^---' | sed '/^$/d' | head -1)
    ai_potential_fix=$(echo "$ai_text" | sed -n '/---BEGIN_POTENTIAL_FIX---/,/---END_POTENTIAL_FIX---/p' \
      | grep -v '^---' | sed '/^$/d')
  else
    echo "GitHub Models unavailable; using heuristic fallback for reasoning fields" >&2
  fi
fi

# ── Heuristic fallbacks for reasoning fields ─────────────────────────────────
if [[ -z "$ai_likely_cause" ]]; then
  err_lc=$(echo "${error_message}" | tr '[:upper:]' '[:lower:]')
  if [[ "$err_lc" == *"timed out"* ]]; then
    ai_likely_cause="Timeout in the failing step — possible deadlock or slow environment."
  elif [[ "$err_lc" == *"no such file"* ]] || [[ "$err_lc" == *"cannot find"* ]]; then
    ai_likely_cause="Missing file or incorrect working directory."
  elif [[ "$err_lc" == *"permission denied"* ]]; then
    ai_likely_cause="Permission denied when executing command or accessing files."
  elif [[ "$err_lc" == *"panic:"* ]]; then
    ai_likely_cause="Runtime panic in code under test."
  elif [[ -n "$error_message" ]]; then
    ai_likely_cause="Command or test failure — see error signal below."
  else
    ai_likely_cause="Insufficient evidence to determine root cause without AI reasoning."
  fi
fi

if [[ -z "$ai_confidence" ]]; then
  score=0
  [[ -n "$failed_job"     && "$failed_job"     != "unknown" ]] && score=$((score+1))
  [[ -n "$failed_step"    && "$failed_step"    != "unknown" ]] && score=$((score+1))
  [[ -n "$failed_command" && "$failed_command" != "unknown" ]] && score=$((score+1))
  [[ -n "$error_message"  && "$error_message"  != "none"   ]] && score=$((score+1))
  if   (( score >= 4 )); then ai_confidence="High"
  elif (( score >= 2 )); then ai_confidence="Medium"
  else                        ai_confidence="Low"
  fi
fi

if [[ -z "$ai_potential_fix" ]]; then
  ai_potential_fix="Re-run \`${failed_command}\` locally to reproduce. Check the Code Context section below for referenced source files."
fi

# ── Reproducer status prose ───────────────────────────────────────────────────
case "${reproducer_status:-}" in
  confirmed)   repro_prose="**confirmed** — command reproduced failure with exit code ${reproducer_exit_code}" ;;
  success)     repro_prose="**not confirmed** — command ran but did not reproduce failure" ;;
  timeout)     repro_prose="**not confirmed** — reproducer timed out after ${TIMEOUT_SECONDS}s" ;;
  skipped)     repro_prose="**not attempted** — ${validation_reason}" ;;
  not-run)     repro_prose="**not attempted** — derived command prefix not in allowed execution list" ;;
  *)           repro_prose="**not attempted** — unable to derive command" ;;
esac

# ── Build report deterministically ───────────────────────────────────────────
report="# CI Reproducer Report"$'\n\n'
report+="_Generated by GitHub Models (${model}) · facts populated by CI reproducer_"$'\n\n'

report+="## Summary"$'\n\n'
report+="| Field | Value |"$'\n'
report+="|-------|-------|"$'\n'
report+="| Failed job | \`${failed_job:-unknown}\` |"$'\n'
report+="| Failed step | \`${failed_step:-unknown}\` |"$'\n'
report+="| Failed command | \`${failed_command:-unknown}\` |"$'\n'
report+="| Key error | ${error_message:0:150} |"$'\n'
report+="| Likely cause | ${ai_likely_cause} |"$'\n'
report+="| Confidence | ${ai_confidence} |"$'\n\n'

report+="## Reproducer"$'\n\n'
report+="The smallest command a developer can run from the repository root:"$'\n\n'
report+="\`\`\`bash"$'\n'
report+="# from repository root"$'\n'
report+="${failed_command:-unknown}"$'\n'
report+="\`\`\`"$'\n\n'
report+="Reproduction: ${repro_prose}"$'\n\n'

# Show captured output when the reproducer actually ran.
if [[ "${reproducer_status}" == "confirmed" || "${reproducer_status}" == "success" || "${reproducer_status}" == "failed" ]]; then
  if [[ -f "$REPRO_TMP/reproducer.stdout" ]]; then
    repro_out=$(tail -30 "$REPRO_TMP/reproducer.stdout")
    if [[ -n "$repro_out" ]]; then
      report+="<details><summary>Reproducer output (last 30 lines)</summary>"$'\n\n'
      report+="\`\`\`"$'\n'"$repro_out"$'\n'"\`\`\`"$'\n\n'
      report+="</details>"$'\n\n'
    fi
  fi
  if [[ -f "$REPRO_TMP/reproducer.stderr" ]]; then
    repro_err=$(tail -30 "$REPRO_TMP/reproducer.stderr")
    if [[ -n "$repro_err" ]]; then
      report+="<details><summary>Reproducer stderr (last 30 lines)</summary>"$'\n\n'
      report+="\`\`\`"$'\n'"$repro_err"$'\n'"\`\`\`"$'\n\n'
      report+="</details>"$'\n\n'
    fi
  fi
fi

report+="## Potential Fix"$'\n\n'
report+="${ai_potential_fix}"$'\n\n'

report+="## Notes"$'\n\n'
report+="- Run ID: \`${RUN_ID:-unknown}\`, Attempt \`${RUN_ATTEMPT:-unknown}\`, Actor \`${ACTOR:-unknown}\`"$'\n'
if [[ "${ai_confidence}" == "Low" ]]; then
  report+="- Confidence is Low — test may be flaky or environment-dependent."$'\n'
fi
if [[ -n "${validation_reason}" ]]; then
  report+="- Validation: ${validation_reason}"$'\n'
fi

# ── Append code context as collapsible section ────────────────────────────────
if [[ -f "$context_file" ]]; then
  report+=$'\n\n'"## Code Context"$'\n\n'
  report+="<details><summary>Source files and workflow step referenced in this failure</summary>"$'\n\n'
  report+="$(cat "$context_file")"$'\n\n'
  report+="</details>"
fi

# ── Write output ──────────────────────────────────────────────────────────────
if [[ -z "$GITHUB_STEP_SUMMARY" ]]; then
  echo "WARNING: GITHUB_STEP_SUMMARY not set; printing to stdout" >&2
  echo "$report"
else
  echo "$report" >> "$GITHUB_STEP_SUMMARY"
  echo "Report written to $GITHUB_STEP_SUMMARY" >&2
fi

if [[ -n "$GITHUB_OUTPUT" ]]; then
  {
    echo "report<<EOF"
    echo "$report"
    echo "EOF"
    echo "confidence=${ai_confidence}"
    echo "reproducer-command=${failed_command:-}"
  } >> "$GITHUB_OUTPUT"
  echo "Outputs set in $GITHUB_OUTPUT" >&2
fi

echo "Report generation complete" >&2
