#!/bin/bash
# code-reader.sh
# Reads source files referenced in CI failure logs to provide code context.
# Also reads the workflow step that failed, so AI can see what the step actually does.
# Output: Populates $REPRO_TMP/code.context

set -e

echo "=== Reading code context ===" >&2

result_file="$REPRO_TMP/parse.result"
context_file="$REPRO_TMP/code.context"
log_file="$REPRO_TMP/job.log"
workspace="${GITHUB_WORKSPACE:-.}"

set +e
[[ -f "$result_file" ]] && source "$result_file"
set -e

files_read=0

{
  echo "## CI failure context"
  echo ""
  echo "**Job:** ${failed_job:-unknown}"
  echo "**Step:** ${failed_step:-unknown}"
  echo "**Command:** \`${failed_command:-unknown}\`"
  echo "**Error:** ${error_message:-none}"
  echo ""
} > "$context_file"

# ── 1. Extract file:line references from the log ──────────────────────────────
# Matches patterns like: lxd/foo.go:42, ./shared/bar.go:10:5, test/suites/foo.sh:99
if [[ -f "$log_file" ]]; then
  while IFS= read -r ref; do
    raw_file="${ref%%:*}"
    raw_rest="${ref#*:}"
    linenum="${raw_rest%%:*}"

    # Try paths relative to workspace root
    for candidate in "$workspace/$raw_file" "$raw_file"; do
      if [[ -f "$candidate" ]]; then
        start=$(( linenum - 20 ))
        (( start < 1 )) && start=1
        end=$(( linenum + 20 ))
        {
          echo "## Source: $raw_file (around line $linenum)"
          echo '```'
          awk -v s="$start" -v e="$end" 'NR>=s && NR<=e {printf "%4d | %s\n", NR, $0}' "$candidate"
          echo '```'
          echo ""
        } >> "$context_file"
        files_read=$(( files_read + 1 ))
        break
      fi
    done
  done < <(grep -oE '[a-zA-Z0-9_./-]+\.(go|sh|yaml|yml|py|ts|js):[0-9]+' "$log_file" \
             | sort -u | head -8)
fi

# ── 2. Always include the failing workflow step from the workflow YAML ─────────
# This is critical: for steps like "exit 1", the YAML is the only "source" that
# shows what the step actually does. Without it, Copilot has no code to reason about.
for wf_candidate in \
    "$workspace/.github/workflows/tests.yml" \
    "$workspace/.github/workflows/ci.yml" \
    "$workspace/.github/workflows/build.yml"; do
  [[ -f "$wf_candidate" ]] || continue

  if [[ -n "$failed_step" ]]; then
    step_line=$(grep -n "name: ${failed_step}" "$wf_candidate" 2>/dev/null | head -1 | cut -d: -f1)
    if [[ -n "$step_line" ]]; then
      wf_rel="${wf_candidate#"$workspace/"}"
      start=$(( step_line - 3 ))
      (( start < 1 )) && start=1
      end=$(( step_line + 20 ))
      {
        echo "## Workflow step: \"$failed_step\" ($wf_rel, line $step_line)"
        echo '```yaml'
        awk -v s="$start" -v e="$end" 'NR>=s && NR<=e {printf "%4d | %s\n", NR, $0}' "$wf_candidate"
        echo '```'
        echo ""
      } >> "$context_file"
      files_read=$(( files_read + 1 ))
      break
    fi
  fi
done

# ── 3. Append last 80 lines of the job log as raw evidence ────────────────────
if [[ -f "$log_file" ]]; then
  {
    echo "## Log tail (last 80 lines)"
    echo '```'
    tail -80 "$log_file"
    echo '```'
    echo ""
  } >> "$context_file"
fi

echo "Code context: read $files_read file section(s)" >&2
echo "Context saved to $context_file" >&2
