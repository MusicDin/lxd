# CI Reproducer Action

Automated GitHub Actions composite action for reproducing and diagnosing LXD CI failures.

## Overview

The CI Reproducer analyzes failed CI jobs, derives the minimal reproducer command from logs, executes it safely with resource constraints, and generates a developer-friendly diagnostic report.

## Expected Behavior Contract

This action is expected to satisfy the following requirements:

1. Be reusable and callable from any job or step location in a workflow.
2. Analyze failures from the current workflow run at invocation time.
3. Be generic across job types, not tied to a single suite.
4. Identify failed job, failed step, failed command, key error, and likely cause from evidence.
5. Derive the smallest practical reproducer command from available logs.
6. Run that reproducer only when safe and practical.
7. Report reproduction status truthfully as `confirmed`, `not confirmed`, or `not attempted`.
8. Propose a potential fix based on observed failure context, not hardcoded templates.
9. Write exactly the required Markdown report structure to the job summary.
10. Never claim reproduction or validation succeeded unless a command was actually run.
11. If no failure signal is found, report that explicitly instead of guessing.

Acceptance criteria:

1. No parser logic restricted to a single test suite.
2. No hardcoded logic for synthetic failures.
3. Action can be inserted in any workflow location and still analyze current-run failures.
4. Report fields are backed by real log evidence.
5. Validation section lists only commands actually executed.

**Key Features:**
- Automatic log parsing to identify failed tests and error context
- Minimal reproducer command derivation (no unnecessary setup)
- Safety validation (disk space, OOM, kernel panics, network issues)
- Timeout enforcement and resource limits
- Confidence scoring (Low/Medium/High) for reproducer reliability
- Markdown report output to job summary

## Usage

### In a Workflow

You can insert this action wherever you want.

Recommended pattern: run it in a follow-up job so it can inspect failures across prior jobs in the same run.

```yaml
ci-reproducer:
  if: failure()
  needs: [job-a, job-b]
  runs-on: ubuntu-24.04
  permissions:
    actions: read
    contents: read
  steps:
    - name: Checkout
      uses: actions/checkout@v4
    - name: CI Reproducer
      uses: ./.github/actions/ci-reproducer
      with:
        github-token: ${{ secrets.GITHUB_TOKEN }}
        timeout-seconds: '900'
```

      Use `if: always()` only if you explicitly want diagnostic summaries on successful runs too.

### Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `github-token` | Yes | — | GitHub token for accessing logs via `gh` CLI |
| `timeout-seconds` | No | `900` | Maximum runtime for reproducer (seconds) |
| `failed-job-name` | No | (auto-detect) | Name of failed job; auto-detected if omitted |

### Outputs

| Output | Description |
|--------|-------------|
| `report` | Full Markdown-formatted reproducer report |
| `confidence` | Confidence level: `Low`, `Medium`, or `High` |
| `reproducer-command` | The minimal command to reproduce the failure |

## Report Format

The reproducer generates a Markdown report with the following sections:

### CI Reproducer Report

**Summary** — Key facts about the failure:
- Failed job name
- Failed step
- Failed command (if identifiable)
- Key error message (truncated to 150 chars)
- Likely root cause (inferred)
- Confidence level

**Reproducer** — Minimal command to reproduce:
```bash
# from repository root
<derived command from failing logs>
```
Plus reproduction status: `confirmed`, `not confirmed`, or `not attempted`.

**Potential Fix** — Actionable guidance based on observed failure evidence.

**Validation** — Commands that were actually executed and their status.

**Notes** — Context about environment, flakiness, and how to proceed.

## How It Works

### Step 1: Log Fetching
- Uses `gh api repos/.../actions/jobs/{id}/logs` to download job logs; falls back to the run-level logs archive if the job-scoped fetch is empty
- Extracts job metadata (name, matrix context if applicable)
- Validates log is non-empty and contains error patterns

### Step 2: Generic Log Parsing
- Finds failed jobs/steps and extracts strongest failure signals
- Detects command errors, assertion failures, panics, timeouts, and toolchain errors
- Extracts actionable context for reproducer and fix suggestion
- Assigns confidence based on evidence quality:
  - **High**: clear failing command + clear root error
  - **Medium**: partial command/error context
  - **Low**: ambiguous or incomplete evidence

### Step 3: Validation
Checks logs for environmental issues that would prevent safe reproduction:
- Disk space (< 10% free)
- OOM (out of memory)
- Kernel panic
- Hardware/driver errors
- Network connectivity issues

Skips reproducer execution if validation fails.

### Step 4: Reproducer Execution
- Derives a minimal command from parsed results
- Validates command syntax
- Executes with timeout enforcement (default 15 minutes)
- Captures exit code and output (last 20 lines)

### Step 5: Report Generation
- Formats Markdown report per spec
- Outputs to `$GITHUB_STEP_SUMMARY` (job summary tab)
- Sets GitHub Actions outputs for downstream use

## Interpreting Results

### High Confidence

The reproducer is very likely accurate. Root cause is clear from logs.

**Actions:**
- Run the reproducer command locally to confirm
- Review stack traces or assertion messages for fix
- Check code near the panic/error location

### Medium Confidence

The reproducer is probably accurate, but there may be environmental factors.

**Actions:**
- Run locally with the same command and environment assumptions
- Check for timing-dependent or flaky behavior
- Look for race conditions if applicable

### Low Confidence

The reproducer is uncertain. Common reasons:
- Test timed out (deadlock vs. slow environment)
- Insufficient error context in logs
- Likely flaky/intermittent behavior

**Actions:**
- Run multiple times locally to detect flakiness
- Increase timeout if environment is slow
- Check test code for timing assumptions
- Consider running on faster hardware

## Examples

### Example 1: Build Failure Reproducer

**Report Output:**
```
## Summary
- **Failed job:** `Code`
- **Failed step:** `Build binaries`
- **Failed command:** `make`
- **Key error:** `undefined: SomeSymbol`
- **Likely cause:** Missing import or API mismatch
- **Confidence:** Medium

## Reproducer
```bash
# from repository root
make
```
Reproduction: **confirmed** (command reproduced failure with exit code 2)
```

### Example 2: Timeout Reproducer

**Report Output:**
```
## Summary
- **Failed job:** `Integration`
- **Failed step:** `timeout`
- **Key error:** `Job timed out after 60 minutes`
- **Likely cause:** Long-running test or deadlock
- **Confidence:** Low

## Reproducer
```bash
# from repository root
<derived command unavailable>
```
Reproduction: **not attempted** (insufficient reproducible command evidence)

## Notes
- **Confidence is Low:** The reproducer is uncertain. Test may be flaky or environment-dependent.
```

## Limitations

- **Current maturity**: Confidence quality depends on log quality and command visibility.
- **Log access**: Requires GitHub token with read access to workflow logs (provided by Actions).
- **No retry logic**: Single execution. Flaky tests may not reproduce on first run.
- **Environment-dependent**: Local environment may differ from CI (hardware, network, packages).

## Architecture

```
.github/actions/ci-reproducer/
├── action.yml                 # Composite action definition
├── report.sh                  # Report generation
└── parsers/
    ├── log-fetcher.sh         # Fetch logs via gh CLI
    ├── generic-parser.sh      # Parse failures from any job type
    ├── validator.sh           # Check safety
    └── run-reproducer.sh      # Execute reproducer
```

Each script is independent and can be tested in isolation.

## Development & Testing

### Manual Testing

To test the action locally (requires GitHub CLI and token):

```bash
# Set required environment
export GITHUB_TOKEN="your-token"
export GITHUB_REPOSITORY="canonical/lxd"
export GITHUB_RUN_ID="1234567890"

# Run log fetcher
.github/actions/ci-reproducer/parsers/log-fetcher.sh

# Run parser
.github/actions/ci-reproducer/parsers/generic-parser.sh

# Generate report
.github/actions/ci-reproducer/report.sh
```

### Unit Tests

To add unit tests (Phase 2):

```bash
test/ci-reproducer-tests.sh
```

Tests include:
- Mock log files for various failure scenarios
- Parser accuracy on edge cases
- Report format compliance
- Safety validation logic

## Future Enhancements

- **Phase 2**: Extend to snap-tests and code-tests failures
- **Phase 3**: Add automatic retry logic for flaky test detection
- **Phase 4**: Intelligent escalation (try minimal → full group if needed)
- **Phase 5**: Integration with issue tracking (auto-file bugs for High confidence failures)

## Support

Questions or issues? Open an issue in the LXD repository with the label `ci-reproducer`.
